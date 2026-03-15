//! TUI rendering functions for all three screens (config, progress, done).
//!
//! Uses `ratatui` widgets to draw tabbed configuration forms, real-time stats panels
//! with optional progress bars, scrollable log output, and completion summaries.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Tabs};

use super::app::*;

/// Top-level draw dispatcher -- delegates to the handler for the current screen.
pub fn draw(f: &mut Frame, app: &App) {
    match app.screen {
        Screen::Config => draw_config(f, app),
        Screen::Progress => draw_progress(f, app),
        Screen::Done => draw_done(f, app),
    }
}

fn draw_config(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // tabs
            Constraint::Min(10),   // form
            Constraint::Length(3), // status bar
        ])
        .split(f.area());

    // Operation tabs
    let titles: Vec<Line> = Operation::all()
        .iter()
        .map(|op| Line::from(op.label()))
        .collect();
    let tab_index = match app.operation {
        Operation::Extract => 0,
        Operation::Load => 1,
        Operation::Analytics => 2,
        Operation::MergeCsvs => 3,
    };
    let tabs = Tabs::new(titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Dedalus TUI "),
        )
        .select(tab_index)
        .style(Style::default().fg(Color::White))
        .highlight_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(tabs, chunks[0]);

    // Form fields
    match app.operation {
        Operation::Extract => draw_extract_form(f, app, chunks[1]),
        Operation::Load => draw_load_form(f, app, chunks[1]),
        Operation::Analytics => draw_analytics_form(f, app, chunks[1]),
        Operation::MergeCsvs => draw_merge_form(f, app, chunks[1]),
    }

    // Status bar
    let status_text = if let Some(ref err) = app.error_message {
        Span::styled(format!("  Error: {}", err), Style::default().fg(Color::Red))
    } else {
        Span::styled(
            format!("  Status: {}", app.status_message),
            Style::default().fg(Color::Green),
        )
    };
    let help = Span::styled(
        "  Tab: switch op | Up/Down: fields | Enter: toggle/run | q: quit  ",
        Style::default().fg(Color::DarkGray),
    );
    let status = Paragraph::new(Line::from(vec![status_text, help]))
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(status, chunks[2]);
}

fn field_line<'a>(
    label: &'a str,
    value: &'a str,
    selected: bool,
    is_checkbox: bool,
    checked: bool,
) -> ListItem<'a> {
    let indicator = if selected { ">" } else { " " };
    let content = if is_checkbox {
        let mark = if checked { "x" } else { " " };
        format!("{} {:<16} [{}]", indicator, label, mark)
    } else {
        format!("{} {:<16} [{}]", indicator, label, value)
    };
    let style = if selected {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    ListItem::new(content).style(style)
}

fn draw_extract_form(f: &mut Frame, app: &App, area: Rect) {
    let cfg = &app.extract_config;
    let items: Vec<ListItem> = EXTRACT_FIELDS
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let selected = i == app.field_index;
            match field {
                ExtractField::Input => {
                    field_line("Input file:", &cfg.input, selected, false, false)
                }
                ExtractField::Output => {
                    field_line("Output dir:", &cfg.output, selected, false, false)
                }
                ExtractField::CsvShards => {
                    field_line("CSV shards:", &cfg.csv_shards, selected, false, false)
                }
                ExtractField::BlobShards => {
                    field_line("Blob shards:", &cfg.blob_shards, selected, false, false)
                }
                ExtractField::Limit => {
                    let display = if cfg.limit.is_empty() {
                        "(no limit)"
                    } else {
                        &cfg.limit
                    };
                    field_line("Limit:", display, selected, false, false)
                }
                ExtractField::Checkpoint => {
                    field_line("Checkpoint:", &cfg.checkpoint, selected, false, false)
                }
                ExtractField::DryRun => field_line("Dry run", "", selected, true, cfg.dry_run),
                ExtractField::Resume => field_line("Resume", "", selected, true, cfg.resume),
                ExtractField::NoCache => field_line("No cache", "", selected, true, cfg.no_cache),
                ExtractField::Clean => field_line("Clean", "", selected, true, cfg.clean),
            }
        })
        .collect();
    let title = format!(" Extract Settings ({} fields) ", items.len());
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(list, area);
}

fn draw_load_form(f: &mut Frame, app: &App, area: Rect) {
    let cfg = &app.load_config;
    let items: Vec<ListItem> = LOAD_FIELDS
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let selected = i == app.field_index;
            match field {
                LoadField::Output => field_line("Output dir:", &cfg.output, selected, false, false),
                LoadField::DbPath => field_line("DB path:", &cfg.db_path, selected, false, false),
                LoadField::BatchSize => {
                    field_line("Batch size:", &cfg.batch_size, selected, false, false)
                }
                LoadField::Clean => field_line("Clean", "", selected, true, cfg.clean),
            }
        })
        .collect();
    let title = format!(" Load Settings ({} fields) ", items.len());
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(list, area);
}

fn draw_analytics_form(f: &mut Frame, app: &App, area: Rect) {
    let cfg = &app.analytics_config;
    let items: Vec<ListItem> = ANALYTICS_FIELDS
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let selected = i == app.field_index;
            match field {
                AnalyticsField::Output => {
                    field_line("Output dir:", &cfg.output, selected, false, false)
                }
                AnalyticsField::DbPath => {
                    field_line("DB path:", &cfg.db_path, selected, false, false)
                }
                AnalyticsField::PageRankIterations => field_line(
                    "PR iterations:",
                    &cfg.pagerank_iterations,
                    selected,
                    false,
                    false,
                ),
                AnalyticsField::Damping => {
                    field_line("Damping:", &cfg.damping, selected, false, false)
                }
            }
        })
        .collect();
    let title = format!(" Analytics Settings ({} fields) ", items.len());
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(list, area);
}

fn draw_merge_form(f: &mut Frame, app: &App, area: Rect) {
    let cfg = &app.merge_config;
    let items: Vec<ListItem> = MERGE_FIELDS
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let selected = i == app.field_index;
            match field {
                MergeField::Output => {
                    field_line("Output dir:", &cfg.output, selected, false, false)
                }
            }
        })
        .collect();
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" MergeCsvs Settings "),
    );
    f.render_widget(list, area);
}

fn draw_progress(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // header
            Constraint::Length(14), // stats
            Constraint::Min(6),     // logs
            Constraint::Length(3),  // controls
        ])
        .split(f.area());

    // Header with phase and elapsed
    let elapsed = app
        .start_time
        .map(|t| {
            let secs = t.elapsed().as_secs();
            format!(
                "{:02}:{:02}:{:02}",
                secs / 3600,
                (secs % 3600) / 60,
                secs % 60
            )
        })
        .unwrap_or_else(|| "00:00:00".to_string());

    let articles = app.stats.articles();
    let elapsed_secs = app
        .start_time
        .map(|t| t.elapsed().as_secs_f64())
        .unwrap_or(1.0);
    let throughput = if elapsed_secs > 0.0 {
        articles as f64 / elapsed_secs
    } else {
        0.0
    };

    let header = Paragraph::new(Line::from(vec![
        Span::styled("  Phase: ", Style::default().fg(Color::DarkGray)),
        Span::styled(&app.phase, Style::default().fg(Color::Cyan)),
        Span::raw("       "),
        Span::styled("Elapsed: ", Style::default().fg(Color::DarkGray)),
        Span::styled(elapsed, Style::default().fg(Color::Yellow)),
        Span::raw("       "),
        Span::styled("Throughput: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.0} articles/sec", throughput),
            Style::default().fg(Color::Green),
        ),
    ]))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" {} ", app.operation.label())),
    );
    f.render_widget(header, chunks[0]);

    // Stats table
    draw_stats_panel(f, app, chunks[1]);

    // Log panel
    draw_log_panel(f, app, chunks[2]);

    // Controls
    let controls = Paragraph::new(Line::from(vec![Span::styled(
        "  [c: Cancel]",
        Style::default().fg(Color::Red),
    )]))
    .block(Block::default().borders(Borders::ALL));
    f.render_widget(controls, chunks[3]);
}

fn draw_stats_panel(f: &mut Frame, app: &App, area: Rect) {
    let s = &app.stats;
    let lines = vec![
        Line::from(vec![
            Span::styled("  Articles:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{:<14}", format_num(s.articles())),
                Style::default().fg(Color::White),
            ),
            Span::styled("Edges:     ", Style::default().fg(Color::DarkGray)),
            Span::styled(format_num(s.edges()), Style::default().fg(Color::White)),
        ]),
        Line::from(vec![
            Span::styled("  Blobs:      ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{:<14}", format_num(s.blobs())),
                Style::default().fg(Color::White),
            ),
            Span::styled("Categories:", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_num(s.categories()),
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Cat edges:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{:<14}", format_num(s.category_edges())),
                Style::default().fg(Color::White),
            ),
            Span::styled("See-also:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_num(s.see_also_edges()),
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Infoboxes:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{:<14}", format_num(s.infoboxes())),
                Style::default().fg(Color::White),
            ),
            Span::styled("Images:    ", Style::default().fg(Color::DarkGray)),
            Span::styled(format_num(s.images()), Style::default().fg(Color::White)),
        ]),
        Line::from(vec![
            Span::styled("  Ext links:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{:<14}", format_num(s.external_links())),
                Style::default().fg(Color::White),
            ),
            Span::styled("Invalid:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(format_num(s.invalid()), Style::default().fg(Color::White)),
        ]),
    ];

    // Progress bar if limit is set
    let mut all_lines = lines;
    if !app.extract_config.limit.is_empty()
        && let Ok(limit) = app.extract_config.limit.parse::<u64>()
        && limit > 0
    {
        let pct = (s.articles() as f64 / limit as f64 * 100.0).min(100.0);
        let bar_width = 30usize;
        let filled = (pct / 100.0 * bar_width as f64) as usize;
        let empty = bar_width.saturating_sub(filled);
        all_lines.push(Line::from(""));
        all_lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled("\u{2588}".repeat(filled), Style::default().fg(Color::Cyan)),
            Span::styled(
                "\u{2591}".repeat(empty),
                Style::default().fg(Color::DarkGray),
            ),
            Span::raw(format!("  {:.0}%", pct)),
        ]));
    }

    let stats_widget =
        Paragraph::new(all_lines).block(Block::default().borders(Borders::ALL).title(" Stats "));
    f.render_widget(stats_widget, area);
}

fn draw_log_panel(f: &mut Frame, app: &App, area: Rect) {
    let inner_height = area.height.saturating_sub(2) as usize;
    let log_lines: Vec<String> = if let Ok(logs) = app.logs.lock() {
        let total = logs.len();
        let max_scroll = total.saturating_sub(inner_height);
        let scroll = app.log_scroll.min(max_scroll);
        logs.iter()
            .skip(scroll)
            .take(inner_height)
            .cloned()
            .collect()
    } else {
        vec![]
    };
    let items: Vec<ListItem> = log_lines
        .iter()
        .map(|line| {
            let style = if line.starts_with("[ERROR") {
                Style::default().fg(Color::Red)
            } else if line.starts_with("[WARN ") {
                Style::default().fg(Color::Yellow)
            } else if line.starts_with("[DEBUG") {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default().fg(Color::White)
            };
            ListItem::new(line.as_str()).style(style)
        })
        .collect();
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Log (Up/Down to scroll) "),
    );
    f.render_widget(list, area);
}

fn draw_done(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // title
            Constraint::Min(12),   // summary
            Constraint::Length(3), // controls
        ])
        .split(f.area());

    let has_error = app
        .worker_error
        .lock()
        .map(|e| e.is_some())
        .unwrap_or(false);
    let title_style = if has_error {
        Style::default().fg(Color::Red)
    } else {
        Style::default().fg(Color::Green)
    };
    let title_text = if has_error { " Error " } else { " Done " };
    let title = Paragraph::new(Line::from(Span::styled(
        format!("  {} Complete", app.operation.label()),
        title_style,
    )))
    .block(Block::default().borders(Borders::ALL).title(title_text));
    f.render_widget(title, chunks[0]);

    // Summary
    let mut lines = Vec::new();
    if let Ok(err) = app.worker_error.lock()
        && let Some(ref e) = *err
    {
        lines.push(Line::from(Span::styled(
            format!("  Error: {}", e),
            Style::default().fg(Color::Red),
        )));
        lines.push(Line::from(""));
    }

    if app.operation == Operation::Extract {
        let s = &app.stats;
        lines.extend(vec![
            Line::from(format!("  Indexing time:      {:.2}s", app.indexing_secs)),
            Line::from(format!("  Extraction time:    {:.2}s", app.extraction_secs)),
            Line::from(format!(
                "  Total time:         {:.2}s",
                app.indexing_secs + app.extraction_secs
            )),
            Line::from(""),
            Line::from(format!(
                "  Articles processed: {}",
                format_num(s.articles())
            )),
            Line::from(format!("  Edges extracted:    {}", format_num(s.edges()))),
            Line::from(format!(
                "  See also edges:     {}",
                format_num(s.see_also_edges())
            )),
            Line::from(format!("  Blobs written:      {}", format_num(s.blobs()))),
            Line::from(format!("  Invalid links:      {}", format_num(s.invalid()))),
            Line::from(format!(
                "  Categories found:   {}",
                format_num(s.categories())
            )),
            Line::from(format!(
                "  Category edges:     {}",
                format_num(s.category_edges())
            )),
            Line::from(format!(
                "  Infoboxes found:    {}",
                format_num(s.infoboxes())
            )),
            Line::from(format!("  Images found:       {}", format_num(s.images()))),
            Line::from(format!(
                "  External links:     {}",
                format_num(s.external_links())
            )),
        ]);
    } else {
        lines.push(Line::from(format!("  {}", app.done_message)));
    }

    let summary =
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(" Summary "));
    f.render_widget(summary, chunks[1]);

    let controls = Paragraph::new(Line::from(vec![
        Span::styled(
            "  [r: Return to config]  ",
            Style::default().fg(Color::Cyan),
        ),
        Span::styled("[q: Quit]", Style::default().fg(Color::Red)),
    ]))
    .block(Block::default().borders(Borders::ALL));
    f.render_widget(controls, chunks[2]);
}

fn format_num(n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_num_works() {
        assert_eq!(format_num(0), "0");
        assert_eq!(format_num(123), "123");
        assert_eq!(format_num(1234), "1,234");
        assert_eq!(format_num(1234567), "1,234,567");
    }
}
