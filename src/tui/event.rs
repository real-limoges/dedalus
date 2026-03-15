//! Terminal event handling for the TUI.
//!
//! Spawns a background thread that polls `crossterm` for keyboard events at a
//! configurable tick rate, forwarding them as [`AppEvent`] variants over an `mpsc` channel.

use crossterm::event::{self, Event, KeyEvent};
use std::sync::mpsc;
use std::time::Duration;

/// Events consumed by the TUI main loop.
pub enum AppEvent {
    Key(KeyEvent),
    Tick,
}

/// Polls terminal events on a background thread and sends them over a channel.
pub struct EventHandler {
    rx: mpsc::Receiver<AppEvent>,
    _tx: mpsc::Sender<AppEvent>,
}

impl EventHandler {
    /// Spawns the event-polling thread with the given tick interval.
    pub fn new(tick_rate: Duration) -> Self {
        let (tx, rx) = mpsc::channel();
        let event_tx = tx.clone();
        std::thread::spawn(move || loop {
            if event::poll(tick_rate).unwrap_or(false) {
                if let Ok(Event::Key(key)) = event::read()
                    && event_tx.send(AppEvent::Key(key)).is_err() {
                        break;
                    }
            } else if event_tx.send(AppEvent::Tick).is_err() {
                break;
            }
        });
        Self { rx, _tx: tx }
    }

    /// Blocks until the next event is available.
    pub fn next(&self) -> Result<AppEvent, mpsc::RecvError> {
        self.rx.recv()
    }
}
