//! Interactive terminal UI for configuring and monitoring Dedalus operations.
//!
//! Provides a form-based interface for extract, merge-csvs, analytics, and load
//! operations with real-time progress monitoring, live extraction stats, and log
//! streaming. Built on `ratatui` and `crossterm`.

pub mod app;
pub mod event;
pub mod logging;
pub mod runner;
pub mod ui;

use anyhow::Result;
use crossterm::event::{KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::extract::ExtractionEnd;
use app::{App, Operation, Screen};
use event::{AppEvent, EventHandler};
use logging::TuiLogLayer;

/// Launches the interactive TUI, taking over the terminal until the user quits.
///
/// Sets up a `tracing` subscriber that captures log output into a shared buffer
/// (displayed in the TUI's log panel), initializes the `ratatui` alternate screen,
/// and runs the main event loop.
pub fn run_tui() -> Result<()> {
    // Set up TUI log capture
    let logs: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
    let log_layer = TuiLogLayer::new(Arc::clone(&logs));

    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(log_layer)
        .init();

    // Restore the terminal before any panic message is printed, so a panic
    // (which aborts in release builds) does not leave the shell in raw mode.
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore_terminal();
        previous_hook(info);
    }));

    // Set up terminal
    terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(logs);
    let events = EventHandler::new(Duration::from_millis(250));

    let result = run_app(&mut terminal, &mut app, &events);

    restore_terminal();
    terminal.show_cursor()?;

    result
}

/// Leaves raw mode and the alternate screen. Safe to call more than once.
fn restore_terminal() {
    let _ = terminal::disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen);
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    events: &EventHandler,
) -> Result<()> {
    loop {
        terminal.draw(|f| ui::draw(f, app))?;

        match events.next()? {
            AppEvent::Key(key) => {
                if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
                    return Ok(());
                }

                let quit = match app.screen {
                    Screen::Config => handle_config_key(app, key.code),
                    Screen::Progress => {
                        handle_progress_key(app, key.code);
                        false
                    }
                    Screen::Done => handle_done_key(app, key.code),
                };
                if quit {
                    return Ok(());
                }
            }
            AppEvent::Tick => {
                if app.screen == Screen::Progress {
                    handle_tick(app);
                }
            }
        }
    }
}

/// Handles a key on the config screen. Returns `true` to quit.
///
/// Esc always quits; `q` quits only when the selected field is not a text
/// field, so a path such as `/data/qwiki` can be typed.
fn handle_config_key(app: &mut App, code: KeyCode) -> bool {
    match code {
        KeyCode::Esc => return true,
        KeyCode::Char('q') if app.current_text_field().is_none() => return true,
        KeyCode::Tab => {
            let ops = Operation::all();
            let current = ops.iter().position(|&op| op == app.operation).unwrap_or(0);
            app.operation = ops[(current + 1) % ops.len()];
            app.field_index = 0;
            app.error_message = None;
        }
        KeyCode::BackTab => {
            let ops = Operation::all();
            let current = ops.iter().position(|&op| op == app.operation).unwrap_or(0);
            app.operation = ops[(current + ops.len() - 1) % ops.len()];
            app.field_index = 0;
            app.error_message = None;
        }
        KeyCode::Up => {
            if app.field_index > 0 {
                app.field_index -= 1;
            }
        }
        KeyCode::Down => {
            let max = app.field_count().saturating_sub(1);
            if app.field_index < max {
                app.field_index += 1;
            }
        }
        KeyCode::Enter => {
            if app.current_field_is_checkbox() {
                app.toggle_checkbox();
            } else {
                // Try to run
                match app.validate() {
                    Ok(()) => {
                        app.error_message = None;
                        app.screen = Screen::Progress;
                        runner::start_operation(app);
                    }
                    Err(e) => {
                        app.error_message = Some(e);
                    }
                }
            }
        }
        KeyCode::Char(' ') => {
            if app.current_field_is_checkbox() {
                app.toggle_checkbox();
            } else if let Some(field) = app.current_text_field() {
                field.push(' ');
            }
        }
        KeyCode::Backspace => {
            if let Some(field) = app.current_text_field() {
                field.pop();
            }
        }
        KeyCode::Char(c) => {
            if let Some(field) = app.current_text_field() {
                field.push(c);
            }
        }
        _ => {}
    }
    false
}

fn handle_progress_key(app: &mut App, code: KeyCode) {
    match code {
        // Only extraction can stop part way; the other operations run to the end.
        KeyCode::Char('c') | KeyCode::Esc if app.operation == Operation::Extract => {
            app.cancel.store(true, Ordering::Release);
            app.phase = "Canceling after the current chunk...".to_string();
        }
        KeyCode::Up => {
            let total = app.logs.lock().map(|l| l.len()).unwrap_or(0);
            app.log_offset_from_bottom = (app.log_offset_from_bottom + 1).min(total);
        }
        KeyCode::Down => {
            app.log_offset_from_bottom = app.log_offset_from_bottom.saturating_sub(1);
        }
        KeyCode::End => app.log_offset_from_bottom = 0,
        _ => {}
    }
}

fn handle_done_key(app: &mut App, code: KeyCode) -> bool {
    match code {
        KeyCode::Char('q') | KeyCode::Esc => true,
        KeyCode::Char('r') => {
            app.screen = Screen::Config;
            app.error_message = None;
            app.status_message = "Ready".to_string();
            false
        }
        _ => false,
    }
}

fn handle_tick(app: &mut App) {
    if !app.worker_done.load(Ordering::Acquire) {
        return;
    }
    app.screen = Screen::Done;
    let report = runner::take_extract_report(app);
    let failed = app.worker_error.lock().map(|e| e.is_some()).unwrap_or(true);
    if failed {
        return;
    }
    app.done_message = match (app.operation, report.map(|r| r.end)) {
        (Operation::Extract, Some(ExtractionEnd::Cancelled)) => {
            "Extraction canceled; check Resume to continue from the last checkpoint".to_string()
        }
        (Operation::Extract, _) => "Extraction completed successfully".to_string(),
        (Operation::Analytics, _) => {
            format!(
                "Analytics completed successfully; results in {}",
                crate::analytics::ANALYTICS_FILE
            )
        }
        (Operation::Load, _) => "DuckDB load completed successfully".to_string(),
        (Operation::MergeCsvs, _) => "CSV merge completed successfully".to_string(),
    };
}
