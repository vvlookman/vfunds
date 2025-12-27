use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

use chrono::{Days, NaiveDate};
use eframe::egui;
use egui_plot::{Corner, Legend, Line, LineStyle, Plot, Points};
use tokio::sync::mpsc;

use crate::{
    CHANNEL_BUFFER_DEFAULT, api, api::BacktestOutputResult, error::VfError, gui::GuiEvent,
    utils::datetime::date_to_str,
};

pub struct ResultViewer {
    gui_event_sender: mpsc::Sender<GuiEvent>,

    result_dir: PathBuf,
    vfund_names: Vec<String>,

    load_event_sender: mpsc::Sender<LoadEvent>,
    load_event_receiver: mpsc::Receiver<LoadEvent>,
    results: Vec<(String, BacktestOutputResult, BacktestDailyValues)>,

    plot_start_date: Option<NaiveDate>,
    plot_end_date: Option<NaiveDate>,
    plot_values_points: HashMap<String, Vec<[f64; 2]>>,
    plot_orders_points: HashMap<String, Vec<[f64; 2]>>,
    plot_cost_line_points: Vec<[f64; 2]>,
    hovered_plot_id: Option<egui::Id>,

    show_orders: bool,
    show_cost_line: bool,
    warning_message: Option<String>,
}

impl ResultViewer {
    pub fn new(
        cc: &eframe::CreationContext,
        gui_event_sender: mpsc::Sender<GuiEvent>,
        result_dir: &Path,
        vfund_names: &[String],
    ) -> Self {
        let mut fonts = egui::FontDefinitions::default();
        {
            let font_name = "Noto Sans Mono";

            fonts.font_data.insert(
                font_name.to_owned(),
                egui::FontData::from_static(include_bytes!(
                    "../../assets/NotoSansMonoCJKsc-Regular.otf"
                ))
                .into(),
            );

            fonts
                .families
                .entry(egui::FontFamily::Proportional)
                .or_default()
                .insert(0, font_name.to_owned());

            fonts
                .families
                .entry(egui::FontFamily::Monospace)
                .or_default()
                .insert(0, font_name.to_owned());
        }

        cc.egui_ctx.set_fonts(fonts);
        cc.egui_ctx.set_visuals(egui::Visuals::dark());

        let (load_event_sender, load_event_receiver) =
            mpsc::channel::<LoadEvent>(CHANNEL_BUFFER_DEFAULT);

        let mut app = Self {
            gui_event_sender,

            result_dir: result_dir.to_path_buf(),
            vfund_names: vfund_names.to_vec(),

            load_event_sender,
            load_event_receiver,
            results: vec![],

            plot_start_date: None,
            plot_end_date: None,
            plot_values_points: HashMap::new(),
            plot_orders_points: HashMap::new(),
            plot_cost_line_points: vec![],
            hovered_plot_id: None,

            show_orders: true,
            show_cost_line: true,
            warning_message: None,
        };

        if let Some(storage) = cc.storage {
            if let Some(show_orders_str) = storage.get_string("show_orders") {
                if let Ok(v) = show_orders_str.parse() {
                    app.show_orders = v;
                }
            }

            if let Some(show_cost_line_str) = storage.get_string("show_cost_line") {
                if let Ok(v) = show_cost_line_str.parse() {
                    app.show_cost_line = v;
                }
            }
        }

        app
    }

    fn load_results(&mut self) {
        self.warning_message = None;

        self.plot_values_points.clear();
        self.plot_orders_points.clear();
        self.plot_cost_line_points.clear();

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
                self.plot_end_date = results
                    .iter()
                    .filter_map(|(_, output_result, _)| output_result.metrics.last_trade_date)
                    .max();

                if let (Some(plot_start_date), Some(plot_end_date)) =
                    (self.plot_start_date, self.plot_end_date)
                {
                    for (vfund_name, output_result, daily_values) in &results {
                        let mut values_points: Vec<[f64; 2]> = vec![];
                        let mut orders_points: Vec<[f64; 2]> = vec![];

                        for (date, value) in daily_values {
                            let x = (*date - plot_start_date).num_days() as f64;
                            let y = *value / output_result.options.init_cash * 100.0;
                            values_points.push([x, y]);

                            if output_result.order_dates.contains(date) {
                                orders_points.push([x, y]);
                            }
                        }

                        self.plot_values_points
                            .insert(vfund_name.to_string(), values_points);
                        self.plot_orders_points
                            .insert(vfund_name.to_string(), orders_points);
                    }

                    self.plot_cost_line_points = vec![
                        [0.0, 100.0],
                        [(plot_end_date - plot_start_date).num_days() as f64, 100.0],
                    ];
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
                        ui.checkbox(&mut self.show_orders, "Show Orders");
                        ui.checkbox(&mut self.show_cost_line, "Show Cost Line");

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.button("↻ Refresh").clicked() {
                                self.load_results();

                                // Notify CLI
                                let gui_event_sender = self.gui_event_sender.clone();
                                tokio::spawn(async move {
                                    let _ = gui_event_sender.send(GuiEvent::Refresh).await;
                                });
                            }
                        });
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
                if let Some(plot_start_date) = self.plot_start_date {
                    let last_trade_date = self
                        .results
                        .iter()
                        .filter_map(|(_, output_result, _)| output_result.metrics.last_trade_date)
                        .max();

                    let plot_response =
                        Plot::new("plot")
                            .label_formatter(|name, point| {
                                if name.is_empty() {
                                    format!("{:.2}%", point.y)
                                } else {
                                    format!(
                                        "[{}] {} {:.2}%",
                                        date_to_str(&(plot_start_date + Days::new(point.x as u64))),
                                        name,
                                        point.y
                                    )
                                }
                            })
                            .allow_scroll(false)
                            .show_grid(false)
                            .x_axis_label(format!(
                                "[{}] ~ [{}]",
                                date_to_str(&plot_start_date),
                                last_trade_date.map_or("-".to_string(), |date| date_to_str(&date))
                            ))
                            .x_axis_formatter(|_, _| "".to_string())
                            .y_axis_formatter(|y, _| format!("{:.0}%", y.value))
                            .legend(Legend::default().position(Corner::LeftTop))
                            .show(ui, |plot_ui| {
                                if self.show_cost_line {
                                    plot_ui.line(
                                        Line::new("", self.plot_cost_line_points.clone())
                                            .width(0.8)
                                            .style(LineStyle::dashed_dense())
                                            .color(egui::Color32::DARK_GRAY),
                                    );
                                }

                                for (vfund_name, points) in &self.plot_values_points {
                                    let name = if let Some(Some(title)) =
                                        self.results.iter().find(|(n, _, _)| n == vfund_name).map(
                                            |(_, output_result, _)| output_result.title.clone(),
                                        ) {
                                        &format!("{vfund_name} [{title}]")
                                    } else {
                                        vfund_name
                                    };

                                    let plot_id = format!("{vfund_name}/line");

                                    let (width, color) =
                                        if self.hovered_plot_id == Some(plot_id.clone().into()) {
                                            (1.2, egui::Color32::GOLD)
                                        } else {
                                            (0.8, str_to_color(vfund_name))
                                        };

                                    plot_ui.line(
                                        Line::new(name, points.clone())
                                            .id(plot_id)
                                            .width(width)
                                            .color(color),
                                    );
                                }

                                if self.show_orders {
                                    for points in self.plot_orders_points.values() {
                                        plot_ui.points(
                                            Points::new("", points.clone())
                                                .radius(2.0)
                                                .color(egui::Color32::GOLD),
                                        );
                                    }
                                }
                            });

                    self.hovered_plot_id = plot_response.hovered_plot_item;
                }
            });
        });
    }

    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        storage.set_string("show_orders", self.show_orders.to_string());
        storage.set_string("show_cost_line", self.show_cost_line.to_string());
        storage.flush();
    }
}

type BacktestDailyValues = Vec<(NaiveDate, f64)>;

enum LoadEvent {
    Finished(Vec<(String, BacktestOutputResult, BacktestDailyValues)>),
    Error(VfError),
}

fn str_to_color(s: &str) -> egui::Color32 {
    // Avoid GOLD color
    const HUE_START: f64 = 70.0;
    const HUE_END: f64 = 380.0;

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    let hash = hasher.finish();

    let hue_raw = (hash % 360) as f64;
    let hue = HUE_START + (HUE_END - HUE_START) * (hue_raw / 360.0);

    let (r, g, b) = hsv::hsv_to_rgb(hue % 360.0, 0.8, 1.0);

    egui::Color32::from_rgb(r, g, b)
}
