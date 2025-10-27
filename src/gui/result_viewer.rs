use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

use chrono::{Days, NaiveDate};
use eframe::egui;
use egui_plot::{Corner, Legend, Line, Plot};
use tokio::sync::mpsc;

use crate::{
    CHANNEL_BUFFER_DEFAULT, api, api::BacktestOutputResult, error::VfError,
    utils::datetime::date_to_str,
};

pub struct ResultViewer {
    result_dir: PathBuf,
    vfund_names: Vec<String>,

    load_event_sender: mpsc::Sender<LoadEvent>,
    load_event_receiver: mpsc::Receiver<LoadEvent>,
    results: Vec<(String, BacktestOutputResult, BacktestDailyValues)>,

    plot_start_date: Option<NaiveDate>,
    plot_values_points: HashMap<String, Vec<[f64; 2]>>,

    warning_message: Option<String>,
}

type BacktestDailyValues = Vec<(NaiveDate, f64)>;

enum LoadEvent {
    Finished(Vec<(String, BacktestOutputResult, BacktestDailyValues)>),
    Error(VfError),
}

impl ResultViewer {
    pub fn new(cc: &eframe::CreationContext, result_dir: &Path, vfund_names: &[String]) -> Self {
        cc.egui_ctx.set_visuals(egui::Visuals::dark());

        let (load_event_sender, load_event_receiver) =
            mpsc::channel::<LoadEvent>(CHANNEL_BUFFER_DEFAULT);

        Self {
            result_dir: result_dir.to_path_buf(),
            vfund_names: vfund_names.to_vec(),

            load_event_sender,
            load_event_receiver,
            results: vec![],

            plot_start_date: None,
            plot_values_points: HashMap::new(),

            warning_message: None,
        }
    }

    fn load_results(&mut self) {
        self.warning_message = None;

        self.plot_values_points.clear();

        let result_dir = self.result_dir.clone();
        let vfund_names = self.vfund_names.clone();
        let load_event_sender = self.load_event_sender.clone();

        tokio::spawn(async move {
            match api::load_backtest_results(&result_dir, &vfund_names).await {
                Ok(backtest_results) => {
                    let mut results: Vec<(String, BacktestOutputResult, BacktestDailyValues)> =
                        vec![];

                    for (vfund_name, output_result) in backtest_results {
                        match api::load_backtest_values(&result_dir, &vfund_name).await {
                            Ok(daily_values) => {
                                results.push((vfund_name, output_result, daily_values));
                            }
                            Err(err) => {
                                let _ = load_event_sender.send(LoadEvent::Error(err)).await;
                            }
                        }
                    }

                    let _ = load_event_sender.send(LoadEvent::Finished(results)).await;
                }
                Err(err) => {
                    let _ = load_event_sender.send(LoadEvent::Error(err)).await;
                }
            }
        });
    }

    fn on_load_results(&mut self, event: LoadEvent) {
        match event {
            LoadEvent::Finished(results) => {
                self.plot_start_date = results
                    .iter()
                    .map(|(_, output_result, _)| output_result.options.start_date)
                    .min();

                if let Some(plot_start_date) = self.plot_start_date {
                    for (vfund_name, _, daily_values) in &results {
                        let mut values_points: Vec<[f64; 2]> = vec![];

                        for (date, value) in daily_values {
                            let x = (*date - plot_start_date).num_days() as f64;
                            values_points.push([x, *value]);
                        }

                        self.plot_values_points
                            .insert(vfund_name.to_string(), values_points);
                    }
                }

                self.results = results;
            }
            LoadEvent::Error(err) => self.warning_message = Some(err.to_string()),
        }
    }
}

impl eframe::App for ResultViewer {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        let already_run = ctx.data(|d| {
            d.get_temp::<bool>(egui::Id::new("startup_once"))
                .unwrap_or(false)
        });

        if !already_run {
            self.load_results();

            ctx.data_mut(|d| d.insert_temp(egui::Id::new("startup_once"), true));
        }

        while let Ok(event) = self.load_event_receiver.try_recv() {
            self.on_load_results(event);
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::TopBottomPanel::top("tools_panel")
                .show_separator_line(false)
                .show_inside(ui, |ui| {
                    ui.horizontal_centered(|ui| {
                        if ui.button("↻ Refresh").clicked() {
                            self.load_results();
                        }
                    });
                });

            egui::TopBottomPanel::bottom("status_panel")
                .show_separator_line(false)
                .show_inside(ui, |ui| {
                    ui.horizontal_centered(|ui| {
                        ui.label(
                            egui::RichText::new(format!("🗀 {}", self.result_dir.to_string_lossy()))
                                .color(egui::Color32::DARK_GRAY)
                                .size(12.0),
                        );

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            ui.label(
                                egui::RichText::new(
                                    self.warning_message
                                        .as_ref()
                                        .map(|t| format!("⚠ {t}"))
                                        .unwrap_or_default(),
                                )
                                .color(egui::Color32::DARK_GRAY)
                                .size(12.0),
                            );
                        });
                    });
                });

            egui::CentralPanel::default().show_inside(ui, |ui| {
                Plot::new("plot")
                    .label_formatter(|name, point| {
                        if name.is_empty() {
                            "".to_string()
                        } else {
                            if let Some(plot_start_date) = self.plot_start_date {
                                format!(
                                    "[{}] {} ${:.2}",
                                    date_to_str(&(plot_start_date + Days::new(point.x as u64))),
                                    name,
                                    point.y
                                )
                            } else {
                                "".to_string()
                            }
                        }
                    })
                    .legend(Legend::default().position(Corner::LeftTop))
                    .show(ui, |plot_ui| {
                        for (vfund_name, points) in &self.plot_values_points {
                            let name = if let Some(Some(title)) = self
                                .results
                                .iter()
                                .find(|(n, _, _)| n == vfund_name)
                                .map(|(_, output_result, _)| output_result.title.clone())
                            {
                                &format!("{vfund_name} [{title}]")
                            } else {
                                vfund_name
                            };

                            plot_ui.line(
                                Line::new(name, points.clone())
                                    .width(1.2)
                                    .color(str_to_color(vfund_name)),
                            );
                        }

                        // if !self.plot_order_points.is_empty() {
                        // plot_ui.points(
                        //     Points::new(self.plot_order_points.clone())
                        //         .color(egui::Color32::GOLD)
                        //         .radius(2.8),
                        // );
                        // }
                    });
            });
        });
    }
}

fn str_to_color(s: &str) -> egui::Color32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    let hash = hasher.finish();

    let hue = (hash % 360) as f64;
    let saturation = 0.8;
    let lightness = 1.0;

    let (r, g, b) = hsv::hsv_to_rgb(hue, saturation, lightness);

    egui::Color32::from_rgb(r, g, b)
}
