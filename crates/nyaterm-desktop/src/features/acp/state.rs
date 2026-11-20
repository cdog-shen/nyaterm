use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;

use nyaterm_agent_client::acp::{AcpEvent, AcpEventKind};
use serde_json::Value;

use super::runtime::AcpCommand;

const MAX_TIMELINE_ENTRIES: usize = 4096;
const MAX_TIMELINE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
pub(in crate::features) struct AcpFeatureState {
    sessions: HashMap<String, AcpSessionState>,
}

struct PendingLoad {
    generation: u64,
    session_id: String,
    timeline: Vec<Value>,
    bytes: usize,
    invalid: bool,
}

pub(in crate::features) struct AcpSessionState {
    pub generation: u64,
    pub active_session_id: Option<String>,
    pub timeline: Vec<Value>,
    timeline_bytes: usize,
    timeline_truncated: bool,
    pub pending_permissions: HashMap<u64, Value>,
    pub session_list: Option<Value>,
    pub history_supported: bool,
    pub history_open: bool,
    pub list_loading: bool,
    pending_list: Option<Option<String>>,
    pending_new: bool,
    next_load_generation: u64,
    pending_load: Option<PendingLoad>,
    pub input: Option<gpui::Entity<nyaterm_ui::NyaInputState>>,
    pub(super) expanded: HashSet<String>,
    pub(super) code_views: HashMap<String, (String, nyaterm_ui::text::NyaTextState)>,
    pub connected: bool,
    pub loading: bool,
    pub closed: bool,
    pub running: bool,
    pub cancelling: bool,
    pub error: Option<String>,
    pub(super) commands: Option<Sender<AcpCommand>>,
    pub(super) worker: Option<JoinHandle<()>>,
}

impl AcpFeatureState {
    pub fn start(&mut self, id: String, generation: u64) {
        if let Some(old) = self.sessions.remove(&id) {
            old.stop();
        }
        self.sessions.insert(
            id,
            AcpSessionState {
                generation,
                active_session_id: None,
                timeline: Vec::new(),
                timeline_bytes: 0,
                timeline_truncated: false,
                pending_permissions: HashMap::new(),
                session_list: None,
                history_supported: false,
                history_open: false,
                list_loading: false,
                pending_list: None,
                pending_new: false,
                next_load_generation: 0,
                pending_load: None,
                input: None,
                expanded: HashSet::new(),
                code_views: HashMap::new(),
                connected: false,
                loading: false,
                closed: false,
                running: false,
                cancelling: false,
                error: None,
                commands: None,
                worker: None,
            },
        );
    }

    pub fn session(&self, id: &str) -> Option<&AcpSessionState> {
        self.sessions.get(id)
    }

    pub(super) fn session_mut(&mut self, id: &str) -> Option<&mut AcpSessionState> {
        self.sessions.get_mut(id)
    }

    pub fn contains(&self, id: &str) -> bool {
        self.sessions.contains_key(id)
    }

    pub(super) fn mark_stream_overflow(&mut self, id: &str) -> bool {
        let Some(session) = self.sessions.get_mut(id) else {
            return false;
        };
        let Some(pending) = session.pending_load.as_mut() else {
            return false;
        };
        pending.invalid = true;
        pending.timeline.clear();
        session.error = Some("ACP history replay was incomplete".into());
        true
    }

    pub fn remove(&mut self, id: &str) -> Option<JoinHandle<()>> {
        self.sessions.remove(id).and_then(AcpSessionState::stop)
    }

    pub fn shutdown(&mut self) -> Vec<JoinHandle<()>> {
        self.sessions
            .drain()
            .filter_map(|(_, session)| session.stop())
            .collect()
    }

    pub fn new_session(&mut self, id: &str) -> Result<(), String> {
        let session = self.sessions.get_mut(id).ok_or("ACP tab not found")?;
        if session.closed
            || session.loading
            || session.running
            || session.list_loading
            || !session.pending_permissions.is_empty()
        {
            return Err("ACP session is busy or closed".into());
        }
        session
            .commands
            .as_ref()
            .ok_or("ACP connection closed")?
            .send(AcpCommand::New)
            .map_err(|_| "ACP connection closed".to_string())?;
        session.pending_new = true;
        session.loading = true;
        session.error = None;
        Ok(())
    }

    pub fn list_sessions(&mut self, id: &str, cursor: Option<String>) -> Result<(), String> {
        let session = self.sessions.get_mut(id).ok_or("ACP tab not found")?;
        if !session.history_supported {
            return Err("ACP history is not supported".into());
        }
        if session.closed || session.loading || session.running || session.list_loading {
            return Err("ACP session is busy or closed".into());
        }
        if let Some(cursor) = &cursor {
            let expected = session
                .session_list
                .as_ref()
                .and_then(|list| list.get("nextCursor"))
                .and_then(Value::as_str);
            if expected != Some(cursor.as_str()) {
                return Err("ACP history cursor is stale".into());
            }
        }
        session
            .commands
            .as_ref()
            .ok_or("ACP connection closed")?
            .send(AcpCommand::List(cursor.clone()))
            .map_err(|_| "ACP connection closed".to_string())?;
        session.pending_list = Some(cursor);
        session.list_loading = true;
        Ok(())
    }

    pub fn load_session(&mut self, id: &str, session_id: String) -> Result<(), String> {
        let session = self.sessions.get_mut(id).ok_or("ACP tab not found")?;
        if !session.history_supported {
            return Err("ACP history is not supported".into());
        }
        if session.closed
            || session.loading
            || session.running
            || session.list_loading
            || !session.pending_permissions.is_empty()
        {
            return Err("ACP session is busy or closed".into());
        }
        session
            .commands
            .as_ref()
            .ok_or("ACP connection closed")?
            .send(AcpCommand::Load(session_id.clone()))
            .map_err(|_| "ACP connection closed".to_string())?;
        session.next_load_generation = session.next_load_generation.wrapping_add(1);
        session.pending_load = Some(PendingLoad {
            generation: session.next_load_generation,
            session_id,
            timeline: Vec::new(),
            bytes: 0,
            invalid: false,
        });
        session.loading = true;
        session.error = None;
        Ok(())
    }

    pub fn prompt(&mut self, id: &str, text: String) -> Result<(), String> {
        let session = self.sessions.get_mut(id).ok_or("ACP tab not found")?;
        if session.running || session.loading || session.closed {
            return Err("ACP session is busy or closed".into());
        }
        let active = session
            .active_session_id
            .clone()
            .ok_or("ACP session not ready")?;
        session
            .commands
            .as_ref()
            .ok_or("ACP connection closed")?
            .send(AcpCommand::Prompt(active, text.clone()))
            .map_err(|_| "ACP connection closed".to_string())?;
        session.push_live(serde_json::json!({
            "sessionUpdate": "user_message_chunk",
            "content": {"type": "text", "text": text}
        }));
        session.running = true;
        session.cancelling = false;
        session.error = session
            .timeline_truncated
            .then(|| "ACP timeline was truncated".into());
        Ok(())
    }

    pub fn cancel(&mut self, id: &str) -> Result<(), String> {
        let session = self.sessions.get_mut(id).ok_or("ACP tab not found")?;
        let active = session
            .active_session_id
            .clone()
            .ok_or("ACP session not ready")?;
        session
            .commands
            .as_ref()
            .ok_or("ACP connection closed")?
            .send(AcpCommand::Cancel(active))
            .map_err(|_| "ACP connection closed".to_string())?;
        session.cancelling = true;
        session.pending_permissions.clear();
        Ok(())
    }

    pub fn respond_permission(
        &mut self,
        id: &str,
        request_id: u64,
        option: Option<String>,
    ) -> Result<(), String> {
        let session = self.sessions.get_mut(id).ok_or("ACP tab not found")?;
        if session.cancelling {
            return Err("ACP session is cancelling".into());
        }
        let request = session
            .pending_permissions
            .get(&request_id)
            .ok_or("unknown ACP permission request")?;
        if let Some(option) = &option
            && !request
                .get("options")
                .and_then(Value::as_array)
                .is_some_and(|options| {
                    options.iter().any(|candidate| {
                        candidate.get("optionId").and_then(Value::as_str) == Some(option)
                    })
                })
        {
            return Err("unknown ACP permission option".into());
        }
        session
            .commands
            .as_ref()
            .ok_or("ACP connection closed")?
            .send(AcpCommand::Permission(request_id, option))
            .map_err(|_| "ACP connection closed".to_string())?;
        session.pending_permissions.remove(&request_id);
        Ok(())
    }

    pub fn apply(&mut self, id: &str, event: AcpEvent) {
        let Some(session) = self.sessions.get_mut(id) else {
            return;
        };
        if session.generation != event.generation || session.closed {
            return;
        }
        let event_id = event.session_id;
        if let (Some(active), Some(event_id)) = (&session.active_session_id, &event_id) {
            let target = session
                .pending_load
                .as_ref()
                .is_some_and(|pending| pending.session_id == *event_id);
            if active != event_id
                && !target
                && !matches!(
                    &event.kind,
                    AcpEventKind::SessionCreated { .. } | AcpEventKind::SessionLoaded { .. }
                )
            {
                return;
            }
        }
        match event.kind {
            AcpEventKind::Connected { capabilities } => {
                session.connected = true;
                session.history_supported =
                    capabilities.load_session && capabilities.session_capabilities.list.is_some();
            }
            AcpEventKind::SessionCreated { .. } => {
                if !session.pending_new && session.active_session_id.is_some() {
                    return;
                }
                session.active_session_id = event_id;
                session.timeline.clear();
                session.timeline_bytes = 0;
                session.timeline_truncated = false;
                session.session_list = None;
                if session.history_open {
                    session.refresh_history();
                }
                session.expanded.clear();
                session.code_views.clear();
                session.pending_permissions.clear();
                session.pending_new = false;
                session.loading = false;
            }
            AcpEventKind::SessionLoaded { .. } => {
                let Some(pending) = session.pending_load.as_ref() else {
                    return;
                };
                if event_id.as_deref() != Some(pending.session_id.as_str())
                    || pending.generation != session.next_load_generation
                {
                    return;
                }
                let pending = session.pending_load.take().expect("pending load");
                if pending.invalid {
                    session.loading = false;
                    session.refresh_history();
                    return;
                }
                session.active_session_id = Some(pending.session_id);
                session.timeline = pending.timeline;
                session.timeline_bytes = pending.bytes;
                session.timeline_truncated = false;
                session.session_list = None;
                session.expanded.clear();
                session.code_views.clear();
                session.pending_permissions.clear();
                session.loading = false;
                session.history_open = false;
            }
            AcpEventKind::SessionList { response } => {
                let Some(cursor) = session.pending_list.take() else {
                    return;
                };
                session.list_loading = false;
                if let Ok(mut page) = serde_json::to_value(response) {
                    if cursor.is_some() {
                        if let (Some(old), Some(new)) = (
                            session
                                .session_list
                                .as_mut()
                                .and_then(|list| list.get_mut("sessions"))
                                .and_then(Value::as_array_mut),
                            page.get_mut("sessions").and_then(Value::as_array_mut),
                        ) {
                            old.append(new);
                            let next = page.get("nextCursor").cloned().unwrap_or(Value::Null);
                            if let Some(list) = session.session_list.as_mut() {
                                list["nextCursor"] = next;
                            }
                        }
                    } else {
                        session.session_list = Some(page);
                    }
                }
            }
            AcpEventKind::SessionUpdate { update } => {
                if let Ok(value) = serde_json::to_value(update) {
                    session.push_update(event_id.as_deref(), value);
                }
            }
            AcpEventKind::UnknownSessionUpdate { kind } => {
                let kind: String = kind
                    .chars()
                    .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '-')
                    .take(80)
                    .collect();
                session.push_update(
                    event_id.as_deref(),
                    serde_json::json!({"sessionUpdate": kind}),
                );
            }
            AcpEventKind::PermissionRequested {
                request_id,
                request,
            } => {
                if !session.cancelling
                    && let Ok(request) = serde_json::to_value(request)
                {
                    session.pending_permissions.insert(request_id, request);
                }
            }
            AcpEventKind::PromptCompleted { .. } => {
                session.running = false;
                session.cancelling = false;
                session.pending_permissions.clear();
            }
            AcpEventKind::ToolCancelled { tool_call_id } => {
                session.push_live(serde_json::json!({
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": tool_call_id,
                    "status": "cancelled"
                }));
            }
            AcpEventKind::Error { message } => {
                let failed_load = session.pending_load.as_ref().is_some_and(|pending| {
                    event_id.is_none() || event_id.as_deref() == Some(pending.session_id.as_str())
                });
                if failed_load {
                    session.pending_load = None;
                    session.loading = false;
                    session.refresh_history();
                } else if session.pending_list.is_some() && event_id.is_none() {
                    session.pending_list = None;
                    session.list_loading = false;
                } else if session.pending_load.is_none() {
                    session.pending_new = false;
                    session.loading = false;
                    session.running = false;
                    session.cancelling = false;
                }
                session.error = Some(message);
            }
            AcpEventKind::Closed => {
                session.connected = false;
                session.loading = false;
                session.closed = true;
                session.running = false;
                session.cancelling = false;
                session.pending_new = false;
                session.pending_load = None;
                session.pending_list = None;
                session.list_loading = false;
                session.pending_permissions.clear();
            }
        }
    }
}

fn push_bounded(timeline: &mut Vec<Value>, bytes: &mut usize, value: Value) -> bool {
    let size = serde_json::to_vec(&value).map_or(usize::MAX, |value| value.len());
    if timeline.len() >= MAX_TIMELINE_ENTRIES || size > MAX_TIMELINE_BYTES.saturating_sub(*bytes) {
        return false;
    }
    *bytes += size;
    timeline.push(value);
    true
}

impl AcpSessionState {
    fn push_update(&mut self, event_id: Option<&str>, value: Value) {
        if let Some(pending) = self
            .pending_load
            .as_mut()
            .filter(|pending| event_id == Some(pending.session_id.as_str()))
        {
            if !pending.invalid && !push_bounded(&mut pending.timeline, &mut pending.bytes, value) {
                pending.invalid = true;
                pending.timeline.clear();
                pending.bytes = 0;
                self.error = Some("ACP history replay was incomplete".into());
            }
        } else if event_id.is_none() || self.active_session_id.as_deref() == event_id {
            self.push_live(value);
        }
    }

    fn push_live(&mut self, value: Value) {
        if !push_bounded(&mut self.timeline, &mut self.timeline_bytes, value) {
            self.mark_live_truncated();
        }
    }

    pub(super) fn mark_live_truncated(&mut self) {
        self.timeline_truncated = true;
        self.error = Some("ACP timeline was truncated".into());
    }

    fn refresh_history(&mut self) {
        if self.history_supported
            && self
                .commands
                .as_ref()
                .is_some_and(|commands| commands.send(AcpCommand::List(None)).is_ok())
        {
            self.pending_list = Some(None);
            self.list_loading = true;
        }
    }

    pub(super) fn stop(mut self) -> Option<JoinHandle<()>> {
        if let Some(commands) = self.commands.take() {
            let _ = commands.send(AcpCommand::Close);
        }
        self.worker.take()
    }
}
