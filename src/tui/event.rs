use crossterm::event::{self, Event, KeyEvent};
use std::sync::mpsc;
use std::time::Duration;

pub enum AppEvent {
    Key(KeyEvent),
    Tick,
}

pub struct EventHandler {
    rx: mpsc::Receiver<AppEvent>,
    _tx: mpsc::Sender<AppEvent>,
}

impl EventHandler {
    pub fn new(tick_rate: Duration) -> Self {
        let (tx, rx) = mpsc::channel();
        let event_tx = tx.clone();
        std::thread::spawn(move || loop {
            if event::poll(tick_rate).unwrap_or(false) {
                if let Ok(Event::Key(key)) = event::read() {
                    if event_tx.send(AppEvent::Key(key)).is_err() {
                        break;
                    }
                }
            } else if event_tx.send(AppEvent::Tick).is_err() {
                break;
            }
        });
        Self { rx, _tx: tx }
    }

    pub fn next(&self) -> Result<AppEvent, mpsc::RecvError> {
        self.rx.recv()
    }
}
