//! Interactive terminal UI for configuring and monitoring Dedalus operations.
//!
//! Provides a form-based interface for extract, import, and merge-csvs operations
//! with real-time progress monitoring, live extraction stats, and log streaming.
//! Built on `ratatui` and `crossterm`.

pub mod app;
pub mod event;
pub mod logging;
pub mod runner;
pub mod ui;

use anyhow::Result;
use crossterm::event::{KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

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

    // Set up terminal
    terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(logs);
    let events = EventHandler::new(Duration::from_millis(250));

    let result = run_app(&mut terminal, &mut app, &events);

    // Restore terminal
    terminal::disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
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

                match app.screen {
                    Screen::Config => handle_config_key(app, key.code),
                    Screen::Progress => handle_progress_key(app, key.code),
                    Screen::Done => {
                        if handle_done_key(app, key.code) {
                            return Ok(());
                        }
                    }
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

fn handle_config_key(app: &mut App, code: KeyCode) {
    match code {
        KeyCode::Char('q') | KeyCode::Esc => {
            // Signal quit by transitioning - handled in run_app
            std::process::exit(0);
        }
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
}

fn handle_progress_key(app: &mut App, code: KeyCode) {
    match code {
        KeyCode::Char('c') | KeyCode::Esc => {
            app.cancel.store(true, Ordering::Release);
            app.phase = "Cancelling...".to_string();
        }
        KeyCode::Up => {
            app.log_scroll = app.log_scroll.saturating_sub(1);
        }
        KeyCode::Down => {
            app.log_scroll = app.log_scroll.saturating_add(1);
        }
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
    if app.worker_done.load(Ordering::Acquire) {
        // Worker finished - compute timings from start_time
        if let Some(start) = app.start_time {
            let total = start.elapsed().as_secs_f64();
            if app.operation == Operation::Extract {
                // Use total time as extraction time (indexing is included)
                app.extraction_secs = total;
            }
        }
        app.screen = Screen::Done;
        if app
            .worker_error
            .lock()
            .map(|e| e.is_none())
            .unwrap_or(false)
        {
            app.done_message = match app.operation {
                Operation::Extract => "Extraction completed successfully".to_string(),
                Operation::Import => "Import completed successfully".to_string(),
                Operation::MergeCsvs => "CSV merge completed successfully".to_string(),
            };
        }
    }

    // Auto-scroll logs to bottom
    if let Ok(logs) = app.logs.lock() {
        let total = logs.len();
        if total > 0 {
            app.log_scroll = total.saturating_sub(1);
        }
    }
}
