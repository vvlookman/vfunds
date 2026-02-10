use std::collections::HashMap;

use chrono::NaiveDate;
use eframe::egui;
use egui_plot::{BoxElem, BoxPlot, BoxSpread, Plot};
use tokio::sync::mpsc;

use crate::{
    CHANNEL_BUFFER_DEFAULT, api, data::series::DailySeries, error::VfError, financial::KlineField,
    ticker::Ticker, utils::datetime::date_to_str,
};

pub struct KlineViewer {
    ticker: Ticker,
    title: String,
    ignore_cache: bool,

    is_loading: bool,
    load_event_sender: mpsc::Sender<LoadEvent>,
    load_event_receiver: mpsc::Receiver<LoadEvent>,

    plot_trade_dates: Vec<NaiveDate>,
    plot_trade_dates_index: HashMap<NaiveDate, usize>,
    plot_y_max: f64,
    plot_y_min: f64,
    plot_boxes: Vec<BoxElem>,

    warning_message: Option<String>,
}

impl KlineViewer {
    pub fn new(
        cc: &eframe::CreationContext,
        ticker: &Ticker,
        title: &str,
        ignore_cache: bool,
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

        Self {
            ticker: ticker.clone(),
            title: title.to_string(),
            ignore_cache,

            is_loading: false,
            load_event_sender,
            load_event_receiver,

            plot_trade_dates: vec![],
            plot_trade_dates_index: HashMap::new(),
            plot_y_max: 0.0,
            plot_y_min: 0.0,
            plot_boxes: vec![],

            warning_message: None,
        }
    }

    fn load_kline(&mut self) {
        self.is_loading = true;

        self.warning_message = None;

        self.plot_y_max = 0.0;
        self.plot_y_min = 0.0;
        self.plot_boxes.clear();

        let ticker = self.ticker.clone();
        let ignore_cache = self.ignore_cache;
        let load_event_sender = self.load_event_sender.clone();

        tokio::spawn(async move {
            match api::load_ticker_kline(&ticker, ignore_cache).await {
                Ok(kline) => {
                    let _ = load_event_sender.send(LoadEvent::Finished(kline)).await;
                }
                Err(err) => {
                    let _ = load_event_sender.send(LoadEvent::Error(err)).await;
                }
            }
        });
    }

    fn on_load_kline(&mut self, event: LoadEvent) {
        match event {
            LoadEvent::Finished(kline) => {
                let mut trade_dates: Vec<NaiveDate> = kline.all_dates();
                trade_dates.sort();

                let mut trade_dates_index: HashMap<NaiveDate, usize> = HashMap::new();
                for (i, date) in trade_dates.iter().enumerate() {
                    trade_dates_index.insert(*date, i);
                }

                if !trade_dates.is_empty() {
                    self.plot_trade_dates = trade_dates;
                    self.plot_trade_dates_index = trade_dates_index;

                    let bull_stroke = egui::Stroke::new(0.8, egui::Color32::RED);
                    let bull_fill = egui::Color32::RED.linear_multiply(0.6);
                    let bear_stroke = egui::Stroke::new(0.8, egui::Color32::GREEN);
                    let bear_fill = egui::Color32::GREEN.linear_multiply(0.6);

                    let mut boxes: Vec<BoxElem> = vec![];

                    for (date, values_map) in kline.all_values::<f64>() {
                        if let Some(date_index) = self.plot_trade_dates_index.get(&date) {
                            let x = *date_index as f64;

                            if let Some(open) = values_map.get(&KlineField::Open.to_string())
                                && let Some(close) = values_map.get(&KlineField::Close.to_string())
                                && let Some(high) = values_map.get(&KlineField::High.to_string())
                                && let Some(low) = values_map.get(&KlineField::Low.to_string())
                            {
                                if *high > self.plot_y_max {
                                    self.plot_y_max = *high;
                                }

                                if *low < self.plot_y_min {
                                    self.plot_y_min = *low;
                                }

                                let center = (open + close) / 2.0;
                                let spread = (open - close).abs() / 2.0;
                                let center_upper = center + spread;
                                let center_lower = center - spread;

                                boxes.push(
                                    BoxElem::new(
                                        x,
                                        BoxSpread::new(
                                            *low,
                                            center_lower,
                                            center,
                                            center_upper,
                                            *high,
                                        ),
                                    )
                                    .box_width(0.8)
                                    .whisker_width(0.5)
                                    .name(format!("[{}]", date_to_str(&date)))
                                    .stroke(if close > open {
                                        bull_stroke
                                    } else {
                                        bear_stroke
                                    })
                                    .fill(if close > open {
                                        bull_fill
                                    } else {
                                        bear_fill
                                    }),
                                );
                            }
                        }
                    }

                    self.plot_boxes = boxes;
                }
            }
            LoadEvent::Error(err) => self.warning_message = Some(err.to_string()),
        }

        self.is_loading = false;
    }
}

impl eframe::App for KlineViewer {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        let already_run = ctx.data(|d| {
            d.get_temp::<bool>(egui::Id::new("startup_once"))
                .unwrap_or(false)
        });

        if !already_run {
            self.load_kline();

            ctx.data_mut(|d| d.insert_temp(egui::Id::new("startup_once"), true));
        }

        while let Ok(event) = self.load_event_receiver.try_recv() {
            self.on_load_kline(event);
        }

        let plot_id = egui::Id::new("$");

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::TopBottomPanel::top("tools_panel")
                .show_separator_line(false)
                .show_inside(ui, |ui| {
                    ui.horizontal_centered(|ui| {
                        ui.label(egui::RichText::new(self.title.clone()));

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            ui.add_enabled_ui(!self.is_loading, |ui| {
                                let text = if self.is_loading {
                                    " Loading... "
                                } else {
                                    " ↻ Refresh "
                                };

                                if ui.button(text).clicked() {
                                    self.load_kline();
                                }
                            });
                        });
                    });
                });

            egui::TopBottomPanel::bottom("status_panel")
                .show_separator_line(false)
                .show_inside(ui, |ui| {
                    ui.horizontal_centered(|ui| {
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
                if let Some(first_trade_date) = self.plot_trade_dates.first()
                    && let Some(last_trade_date) = self.plot_trade_dates.last()
                {
                    let size = ui.available_size();
                    let plot = Plot::new(plot_id)
                        .allow_scroll(false)
                        .data_aspect(
                            0.58 * size.y / size.x * self.plot_trade_dates.len() as f32
                                / (self.plot_y_max - self.plot_y_min) as f32,
                        )
                        .label_formatter(|name, point| {
                            if name.is_empty() {
                                format!("{:.3}", point.y)
                            } else {
                                if let Some(date) = self.plot_trade_dates.get(point.x as usize) {
                                    format!("[{}] {} {:.3}", date_to_str(date), name, point.y)
                                } else {
                                    format!("{} {:.3}", name, point.y)
                                }
                            }
                        })
                        .show_grid(false)
                        .x_axis_label(format!(
                            "[{}] ~ [{}]",
                            date_to_str(first_trade_date),
                            date_to_str(last_trade_date)
                        ))
                        .x_axis_formatter(|_, _| "".to_string())
                        .y_axis_formatter(|y, _| format!("{:.0}", y.value));

                    let formatter = |elem: &BoxElem, _plot: &BoxPlot| {
                        let high = elem.spread.upper_whisker;
                        let low = elem.spread.lower_whisker;

                        let (open, close) = if elem.stroke.color == egui::Color32::RED {
                            (elem.spread.quartile1, elem.spread.quartile3)
                        } else {
                            (elem.spread.quartile3, elem.spread.quartile1)
                        };

                        format!(
                            "{}\nHigh = {:.2}\nOpen = {:.2}\nClose = {:.2}\nLow = {:.2}\n",
                            elem.name, high, open, close, low
                        )
                    };

                    plot.show(ui, |plot_ui| {
                        plot_ui.box_plot(
                            BoxPlot::new(self.ticker.to_string(), self.plot_boxes.clone())
                                .element_formatter(Box::new(formatter)),
                        );
                    });
                }
            });
        });
    }
}

enum LoadEvent {
    Finished(DailySeries),
    Error(VfError),
}
