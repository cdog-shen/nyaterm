use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use nyaterm_agent_client::acp::AcpEvent;

#[derive(Default)]
struct PendingEvents {
    events: VecDeque<AcpEvent>,
    streams: usize,
}

pub(super) struct AcpEventBuffer {
    capacity: usize,
    pending: Mutex<PendingEvents>,
    overflowed: AtomicBool,
}

impl AcpEventBuffer {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            pending: Mutex::new(PendingEvents::default()),
            overflowed: AtomicBool::new(false),
        }
    }

    pub(super) fn push(&self, event: AcpEvent) {
        let stream = match &event.kind {
            AcpEventKind::SessionUpdate { update } => {
                serde_json::to_value(update).ok().is_some_and(|value| {
                    matches!(
                        value
                            .get("sessionUpdate")
                            .and_then(serde_json::Value::as_str),
                        Some("user_message_chunk" | "agent_message_chunk" | "agent_thought_chunk")
                    ) && value
                        .pointer("/content/type")
                        .and_then(serde_json::Value::as_str)
                        == Some("text")
                })
            }
            _ => false,
        };
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if stream && pending.streams >= self.capacity {
            self.overflowed.store(true, Ordering::Release);
        } else {
            pending.streams += usize::from(stream);
            pending.events.push_back(event);
        }
    }

    pub(super) fn drain(&self) -> (Vec<AcpEvent>, bool) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        pending.streams = 0;
        let events = std::mem::take(&mut pending.events);
        let overflowed = self.overflowed.swap(false, Ordering::AcqRel);
        drop(pending);
        (events.into_iter().collect(), overflowed)
    }
}

use crate::features::{NyaTermApp, session::SavedConnectionStartOptions};
use crate::models::session::{AcpSessionConfig, SessionLaunchConfig, SessionRuntimeMetadata};
use futures::StreamExt;
use futures::channel::mpsc::{UnboundedSender, unbounded};
use gpui::{AppContext as _, Context};
use nyaterm_agent_client::acp::{AcpClient, AcpConnectionConfig, AcpEventKind, AcpEventSink};
use nyaterm_core::{AiExecutionProfile, ConnectionType, SavedConnection, uuid};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;

pub(super) enum AcpCommand {
    New,
    List(Option<String>),
    Load(String),
    Prompt(String, String),
    Cancel(String),
    Permission(u64, Option<String>),
    Close,
}

pub(super) struct EventPublisher {
    pub(super) buffer: Arc<AcpEventBuffer>,
    pub(super) notified: AtomicBool,
    pub(super) finished: AtomicBool,
    pub(super) wake: UnboundedSender<()>,
}
impl EventPublisher {
    fn publish(&self, event: AcpEvent) {
        self.buffer.push(event);
        self.notify();
    }
    fn notify(&self) {
        if !self.notified.swap(true, Ordering::AcqRel) {
            let _ = self.wake.unbounded_send(());
        }
    }
    fn finish(&self) {
        self.finished.store(true, Ordering::Release);
        self.notify();
    }
    pub(super) fn drain(&self) -> (Vec<AcpEvent>, bool) {
        let result = self.buffer.drain();
        self.notified.store(false, Ordering::Release);
        if self.buffer.overflowed.load(Ordering::Acquire)
            || !self
                .buffer
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .events
                .is_empty()
        {
            self.notify();
        }
        result
    }
}

pub(super) fn spawn_controller(
    config: AcpConnectionConfig,
    generation: u64,
    publisher: Arc<EventPublisher>,
) -> Result<(mpsc::Sender<AcpCommand>, JoinHandle<()>), String> {
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::Builder::new()
        .name("nyaterm-acp-owner".into())
        .spawn(move || {
            let sink_publisher = publisher.clone();
            let sink = AcpEventSink::new(generation, move |event| sink_publisher.publish(event));
            let handle = match AcpClient::spawn(config, sink) {
                Ok(handle) => handle,
                Err(error) => {
                    publisher.publish(AcpEvent {
                        generation,
                        session_id: None,
                        kind: AcpEventKind::Error {
                            message: error.to_string(),
                        },
                    });
                    publisher.publish(AcpEvent {
                        generation,
                        session_id: None,
                        kind: AcpEventKind::Closed,
                    });
                    publisher.finish();
                    return;
                }
            };
            if let Err(error) = handle.new_session() {
                publisher.publish(AcpEvent {
                    generation,
                    session_id: None,
                    kind: AcpEventKind::Error {
                        message: error.to_string(),
                    },
                });
            }
            while let Ok(command) = rx.recv() {
                let result = match command {
                    AcpCommand::New => handle.new_session(),
                    AcpCommand::List(cursor) => handle.list_sessions(cursor),
                    AcpCommand::Load(id) => handle.load_session(id),
                    AcpCommand::Prompt(id, text) => handle.prompt(id, text),
                    AcpCommand::Cancel(id) => handle.cancel(id),
                    AcpCommand::Permission(id, option) => handle.respond_permission(id, option),
                    AcpCommand::Close => break,
                };
                if let Err(error) = result {
                    publisher.publish(AcpEvent {
                        generation,
                        session_id: None,
                        kind: AcpEventKind::Error {
                            message: error.to_string(),
                        },
                    });
                }
            }
            let _ = handle.close();
            drop(handle);
            publisher.finish();
        })
        .map_err(|error| error.to_string())?;
    Ok((tx, worker))
}

impl NyaTermApp {
    pub(in crate::features) fn start_acp_connection(
        &mut self,
        connection: SavedConnection,
        options: SavedConnectionStartOptions,
        cx: &mut Context<Self>,
    ) {
        let ConnectionType::Acp {
            command,
            working_dir,
        } = connection.config
        else {
            return;
        };
        let id = uuid();
        let generation = 1;
        let config = AcpSessionConfig {
            name: connection.name.clone(),
            connection: AcpConnectionConfig {
                command,
                working_dir: working_dir.into(),
            },
        };
        self.acp.start(id.clone(), generation);
        let input = cx.new(|cx| {
            nyaterm_ui::NyaInputState::new(cx, "")
                .multi_line(Some(4))
                .placeholder(rust_i18n::t!("acpChat.placeholder"))
        });
        self.acp
            .session_mut(&id)
            .expect("ACP session registered")
            .input = Some(input);
        self.register_session_for_start(
            &id,
            SessionRuntimeMetadata {
                confirmed_ssh_runtime_mode: None,
                pending_ssh_startup_command: None,
                ssh_config: None,
                ssh_multiplex_key: None,
                source_connection_id: Some(connection.id.clone()),
                ai_execution_profile: AiExecutionProfile::Disabled,
                launch_config: SessionLaunchConfig::Acp(config.clone()),
                disconnected: false,
            },
            options.tab_placement,
            options.insert_index,
        );
        if let Some(name) = options.custom_name {
            self.session.set_custom_name(id.clone(), name);
        }
        self.session.set_tab_color(&id, options.tab_color);
        self.session.set_tab_locked(&id, options.locked);
        self.shell.show_workspace();
        self.activate_session_id(&id, cx);
        self.persist_connection_used(connection.id, cx);
        let (wake, mut receiver) = unbounded();
        let publisher = Arc::new(EventPublisher {
            buffer: Arc::new(AcpEventBuffer::new(512)),
            notified: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            wake,
        });
        match spawn_controller(config.connection, generation, publisher.clone()) {
            Ok((commands, worker)) => {
                let state = self.acp.session_mut(&id).expect("ACP session registered");
                state.commands = Some(commands);
                state.worker = Some(worker);
            }
            Err(error) => {
                publisher.publish(AcpEvent {
                    generation,
                    session_id: None,
                    kind: AcpEventKind::Error { message: error },
                });
                publisher.publish(AcpEvent {
                    generation,
                    session_id: None,
                    kind: AcpEventKind::Closed,
                });
                publisher.finish();
            }
        }
        cx.spawn(async move |this, cx| {
            while receiver.next().await.is_some() {
                let (events, overflowed) = publisher.drain();
                let closed = events
                    .iter()
                    .any(|event| matches!(event.kind, AcpEventKind::Closed));
                let finished = publisher.finished.load(Ordering::Acquire)
                    && !publisher.buffer.overflowed.load(Ordering::Acquire)
                    && publisher
                        .buffer
                        .pending
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .events
                        .is_empty();
                let result = this.update(cx, |app, cx| {
                    let current = app
                        .acp
                        .session(&id)
                        .is_some_and(|state| state.generation == generation);
                    if !current {
                        return false;
                    }
                    let incomplete_replay = overflowed && app.acp.mark_stream_overflow(&id);
                    for event in events {
                        app.acp.apply(&id, event);
                    }
                    if overflowed
                        && !incomplete_replay
                        && let Some(state) = app.acp.session_mut(&id)
                    {
                        state.mark_live_truncated();
                    }
                    if closed
                        && let Some(state) = app.acp.session_mut(&id)
                        && let Some(commands) = state.commands.take()
                    {
                        let _ = commands.send(AcpCommand::Close);
                    }
                    cx.notify();
                    !finished
                });
                if !matches!(result, Ok(true)) {
                    break;
                }
            }
        })
        .detach();
        cx.notify();
    }

    pub(in crate::features) fn close_acp_session(&mut self, id: &str, cx: &mut Context<Self>) {
        if let Some(worker) = self.acp.remove(id) {
            cx.background_executor()
                .spawn(async move {
                    let _ = worker.join();
                })
                .detach();
        }
    }
}
