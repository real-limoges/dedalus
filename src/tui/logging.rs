//! Tracing layer that captures log events into a shared buffer for TUI display.
//!
//! [`TuiLogLayer`] implements `tracing_subscriber::Layer` and formats each event
//! as `[LEVEL] message field=value ...`, pushing lines into a bounded `VecDeque`
//! that the TUI's log panel reads on each tick.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

const MAX_LOG_LINES: usize = 1000;

/// A `tracing` layer that writes formatted log lines into a shared `VecDeque`.
pub struct TuiLogLayer {
    logs: Arc<Mutex<VecDeque<String>>>,
}

impl TuiLogLayer {
    /// Creates a new layer backed by the given shared log buffer.
    pub fn new(logs: Arc<Mutex<VecDeque<String>>>) -> Self {
        Self { logs }
    }
}

struct MessageVisitor {
    message: String,
    fields: Vec<String>,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        } else {
            self.fields.push(format!("{}={:?}", field.name(), value));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields.push(format!("{}={}", field.name(), value));
        }
    }
}

impl<S: Subscriber> Layer<S> for TuiLogLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let level = *event.metadata().level();
        let level_str = match level {
            Level::ERROR => "ERROR",
            Level::WARN => "WARN ",
            Level::INFO => "INFO ",
            Level::DEBUG => "DEBUG",
            Level::TRACE => "TRACE",
        };

        let mut visitor = MessageVisitor {
            message: String::new(),
            fields: Vec::new(),
        };
        event.record(&mut visitor);

        let line = if visitor.fields.is_empty() {
            format!("[{}] {}", level_str, visitor.message)
        } else {
            format!(
                "[{}] {} {}",
                level_str,
                visitor.message,
                visitor.fields.join(" ")
            )
        };

        if let Ok(mut logs) = self.logs.lock() {
            if logs.len() >= MAX_LOG_LINES {
                logs.pop_front();
            }
            logs.push_back(line);
        }
    }
}
