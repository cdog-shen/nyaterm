//! Stable ACP v1 client. Operations enqueue work and publish results through the
//! caller's event sink. Callbacks run on the connection worker and must not block.
//! External calls to `close` and handle drop join that worker; call them off the UI
//! thread. Closing from a callback only schedules shutdown.
mod transport;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use agent_client_protocol::{
    Agent, Client,
    schema::ProtocolVersion,
    v1::schema::{
        CancelNotification, ContentBlock, InitializeRequest, ListSessionsRequest,
        ListSessionsResponse, LoadSessionRequest, LoadSessionResponse, NewSessionRequest,
        NewSessionResponse, PromptRequest, PromptResponse, RequestPermissionOutcome,
        RequestPermissionRequest, RequestPermissionResponse, SelectedPermissionOutcome,
        SessionNotification, SessionUpdate, TextContent, ToolCallStatus,
    },
};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct AcpConnectionConfig {
    pub command: String,
    pub working_dir: PathBuf,
}
impl std::fmt::Debug for AcpConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcpConnectionConfig")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct AcpError(pub String);

impl std::fmt::Display for AcpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for AcpError {}

/// The caller supplies a connection generation and a nonblocking event callback.
#[derive(Clone)]
pub struct AcpEventSink {
    generation: u64,
    callback: Arc<dyn Fn(AcpEvent) + Send + Sync>,
}
impl AcpEventSink {
    pub fn new(generation: u64, callback: impl Fn(AcpEvent) + Send + Sync + 'static) -> Self {
        Self {
            generation,
            callback: Arc::new(callback),
        }
    }
    fn emit(&self, session_id: Option<String>, kind: AcpEventKind) {
        (self.callback)(AcpEvent {
            generation: self.generation,
            session_id,
            kind,
        });
    }
}

#[derive(Clone)]
pub struct AcpEvent {
    pub generation: u64,
    pub session_id: Option<String>,
    pub kind: AcpEventKind,
}
#[derive(Clone)]
pub enum AcpEventKind {
    Connected {
        capabilities: Box<agent_client_protocol::v1::schema::AgentCapabilities>,
    },
    SessionCreated {
        response: NewSessionResponse,
    },
    SessionList {
        response: ListSessionsResponse,
    },
    SessionLoaded {
        response: LoadSessionResponse,
    },
    SessionUpdate {
        update: SessionUpdate,
    },
    UnknownSessionUpdate {
        kind: String,
    },
    PermissionRequested {
        request_id: u64,
        request: RequestPermissionRequest,
    },
    PromptCompleted {
        response: PromptResponse,
    },
    ToolCancelled {
        tool_call_id: String,
    },
    Error {
        message: String,
    },
    Closed,
}
impl std::fmt::Debug for AcpEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcpEvent")
            .field("generation", &self.generation)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for AcpEventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Connected { .. } => "Connected",
            Self::SessionCreated { .. } => "SessionCreated",
            Self::SessionList { .. } => "SessionList",
            Self::SessionLoaded { .. } => "SessionLoaded",
            Self::SessionUpdate { .. } => "SessionUpdate",
            Self::UnknownSessionUpdate { .. } => "UnknownSessionUpdate",
            Self::PermissionRequested { .. } => "PermissionRequested",
            Self::PromptCompleted { .. } => "PromptCompleted",
            Self::ToolCancelled { .. } => "ToolCancelled",
            Self::Error { .. } => "Error",
            Self::Closed => "Closed",
        })
    }
}

enum Command {
    NewSession(u64),
    ListSessions(u64, Option<String>),
    LoadSession(u64, String),
    Prompt(u64, String, String, u64),
    Cancel(String),
    Close,
}

type PermissionResponder = agent_client_protocol::v1::Responder<RequestPermissionResponse>;
#[derive(Default)]
struct PermissionState {
    pending: HashMap<u64, (String, Vec<String>, PermissionResponder)>,
    cancelled: HashSet<String>,
    epochs: HashMap<String, u64>,
}
type PendingPermissions = Arc<Mutex<PermissionState>>;

#[derive(Default)]
struct PendingOperations {
    next_id: AtomicU64,
    operations: Mutex<HashMap<u64, Option<String>>>,
}
impl PendingOperations {
    fn register(&self, session: Option<String>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.operations.lock().unwrap().insert(id, session);
        id
    }
    fn finish(&self, id: u64) -> bool {
        self.operations.lock().unwrap().remove(&id).is_some()
    }
    fn close(&self, sink: &AcpEventSink) {
        let operations = std::mem::take(&mut *self.operations.lock().unwrap());
        for (_, session) in operations {
            sink.emit(
                session,
                AcpEventKind::Error {
                    message: "ACP operation ended when connection closed".into(),
                },
            );
        }
    }
}

fn cancel_permissions(permissions: &PendingPermissions, session: Option<&str>) {
    let responders = {
        let mut state = permissions.lock().unwrap();
        let pending = &mut state.pending;
        let ids: Vec<_> = pending
            .iter()
            .filter(|(_, (s, _, _))| session.is_none_or(|session| session == s))
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter()
            .filter_map(|id| pending.remove(&id))
            .map(|(_, _, responder)| responder)
            .collect::<Vec<_>>()
    };
    for responder in responders {
        let _ = responder.respond(RequestPermissionResponse::new(
            RequestPermissionOutcome::Cancelled,
        ));
    }
}

pub struct AcpClient;
pub struct AcpClientHandle {
    commands: mpsc::UnboundedSender<Command>,
    permissions: PendingPermissions,
    operations: Arc<PendingOperations>,
    stderr_tail: transport::StderrTail,
    worker_id: std::thread::ThreadId,
    worker: Mutex<Option<JoinHandle<()>>>,
}
impl AcpClient {
    pub fn spawn(
        config: AcpConnectionConfig,
        event_sink: AcpEventSink,
    ) -> Result<AcpClientHandle, AcpError> {
        let (child, lines, stderr_tail, transport_error) =
            transport::spawn(&config.command, &config.working_dir, event_sink.clone())
                .map_err(|e| AcpError(e.to_string()))?;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let permissions: PendingPermissions = Arc::new(Mutex::new(PermissionState::default()));
        let worker_permissions = permissions.clone();
        let actor_permissions = permissions.clone();
        let final_permissions = permissions.clone();
        let operations = Arc::new(PendingOperations::default());
        let worker_operations = operations.clone();
        let final_operations = operations.clone();
        let active_tools = Arc::new(Mutex::new(HashMap::<String, HashSet<String>>::new()));
        let notification_tools = active_tools.clone();
        let active_prompts = Arc::new(Mutex::new(HashSet::<String>::new()));
        let worker = std::thread::Builder::new().name("nyaterm-acp".into()).spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread().enable_all().build();
            if let Ok(runtime) = result {
                let sink = event_sink.clone();
                let notification_sink = event_sink.clone();
                let permission_sink = event_sink.clone();
                let next_permission = Arc::new(AtomicU64::new(1));
                let actor_transport_error = transport_error.clone();
                let result = runtime.block_on(Client.v1()
                    .on_receive_notification(async move |notification: SessionNotification, _connection| {
                        let session = notification.session_id.to_string();
                        {
                            let mut tools = notification_tools.lock().unwrap();
                            let session_tools = tools.entry(session.clone()).or_default();
                            match &notification.update {
                                SessionUpdate::ToolCall(call) => {
                                    let id = call.tool_call_id.to_string();
                                    if matches!(call.status, ToolCallStatus::Pending | ToolCallStatus::InProgress) { session_tools.insert(id); } else { session_tools.remove(&id); }
                                }
                                SessionUpdate::ToolCallUpdate(call) => {
                                    if matches!(call.fields.status, Some(ToolCallStatus::Completed | ToolCallStatus::Failed)) { session_tools.remove(&call.tool_call_id.to_string()); }
                                }
                                _ => {}
                            }
                        }
                        notification_sink.emit(Some(session), AcpEventKind::SessionUpdate { update: notification.update });
                        Ok(())
                    }, agent_client_protocol::on_receive_notification!())
                    .on_receive_request(async move |request: RequestPermissionRequest, responder, _connection| {
                        let request_id = next_permission.fetch_add(1, Ordering::Relaxed);
                        let session = request.session_id.to_string();
                        {
                            let mut state = worker_permissions.lock().unwrap();
                            if state.cancelled.contains(&session) {
                                return responder.respond(RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled));
                            }
                            let options = request.options.iter().map(|option| option.option_id.to_string()).collect();
                            state.pending.insert(request_id, (session.clone(), options, responder));
                        }
                        permission_sink.emit(Some(session), AcpEventKind::PermissionRequested { request_id, request });
                        Ok(())
                    }, agent_client_protocol::on_receive_request!())
                    .connect_with(lines, async move |connection: agent_client_protocol::v1::ConnectionTo<Agent>| {
                    let initialize = connection.send_request(InitializeRequest::new(ProtocolVersion::V1)).block_task();
                    tokio::pin!(initialize);
                    let mut queued = VecDeque::new();
                    let init = loop {
                        tokio::select! {
                            result = &mut initialize => break result?,
                            command = rx.recv() => match command {
                                Some(Command::Close) | None => return Ok(()),
                                Some(command) => queued.push_back(command),
                            }
                        }
                    };
                    if init.protocol_version != ProtocolVersion::V1 { return Err(agent_client_protocol::Error::internal_error()); }
                    sink.emit(None, AcpEventKind::Connected { capabilities: Box::new(init.agent_capabilities) });
                    loop {
                        tokio::select! {
                            _ = connection.incoming_closed() => {
                                let message = actor_transport_error.lock().unwrap().clone().unwrap_or_else(|| "ACP connection closed unexpectedly".into());
                                sink.emit(None, AcpEventKind::Error { message });
                                break;
                            },
                            command = async { match queued.pop_front() { Some(command) => Some(command), None => rx.recv().await } } => match command {
                                Some(Command::NewSession(id)) => {
                                    let sink = sink.clone();
                                    let cwd = config.working_dir.clone();
                                    let operations = worker_operations.clone();
                                    connection.prepare_request(NewSessionRequest::new(cwd)).on_receiving_result(async move |result| {
                                        if operations.finish(id) {
                                            match result {
                                                Ok(response) => sink.emit(Some(response.session_id.to_string()), AcpEventKind::SessionCreated { response }),
                                                Err(_) => sink.emit(None, AcpEventKind::Error { message: "ACP session/new failed".into() }),
                                            }
                                        }
                                        Ok(())
                                    })?;
                                }
                                Some(Command::ListSessions(id, cursor)) => {
                                    let sink = sink.clone();
                                    let cwd = config.working_dir.clone();
                                    let operations = worker_operations.clone();
                                    connection.prepare_request(ListSessionsRequest::new().cwd(cwd).cursor(cursor)).on_receiving_result(async move |result| {
                                        if operations.finish(id) {
                                            match result {
                                                Ok(response) => sink.emit(None, AcpEventKind::SessionList { response }),
                                                Err(_) => sink.emit(None, AcpEventKind::Error { message: "ACP session/list failed".into() }),
                                            }
                                        }
                                        Ok(())
                                    })?;
                                }
                                Some(Command::LoadSession(id, session)) => {
                                    let sink = sink.clone();
                                    let cwd = config.working_dir.clone();
                                    let operations = worker_operations.clone();
                                    connection.prepare_request(LoadSessionRequest::new(session.clone(), cwd)).on_receiving_result(async move |result| {
                                        if operations.finish(id) {
                                            match result {
                                                Ok(response) => sink.emit(Some(session), AcpEventKind::SessionLoaded { response }),
                                                Err(_) => sink.emit(Some(session), AcpEventKind::Error { message: "ACP session/load failed".into() }),
                                            }
                                        }
                                        Ok(())
                                    })?;
                                }
                                Some(Command::Prompt(id, session, text, epoch)) => {
                                    if !active_prompts.lock().unwrap().insert(session.clone()) {
                                        worker_operations.finish(id);
                                        sink.emit(Some(session), AcpEventKind::Error { message: "ACP prompt already active for session".into() });
                                        continue;
                                    }
                                    {
                                        let mut state = actor_permissions.lock().unwrap();
                                        if state.epochs.get(&session).copied().unwrap_or(0) == epoch {
                                            state.cancelled.remove(&session);
                                        }
                                    }
                                    let sink = sink.clone();
                                    let operations = worker_operations.clone();
                                    let prompts = active_prompts.clone();
                                    let request = PromptRequest::new(session.clone(), vec![ContentBlock::Text(TextContent::new(text))]);
                                    connection.prepare_request(request).on_receiving_result(async move |result| {
                                        prompts.lock().unwrap().remove(&session);
                                        if operations.finish(id) {
                                            match result {
                                                Ok(response) => sink.emit(Some(session), AcpEventKind::PromptCompleted { response }),
                                                Err(_) => sink.emit(Some(session), AcpEventKind::Error { message: "ACP session/prompt failed".into() }),
                                            }
                                        }
                                        Ok(())
                                    })?;
                                }
                                Some(Command::Cancel(session)) => {
                                    connection.send_notification(CancelNotification::new(session.clone()))?;
                                    if let Some(tools) = active_tools.lock().unwrap().remove(&session) {
                                        for tool_call_id in tools { sink.emit(Some(session.clone()), AcpEventKind::ToolCancelled { tool_call_id }); }
                                    }
                                    cancel_permissions(&actor_permissions, Some(&session));
                                }
                                Some(Command::Close) | None => {
                                    cancel_permissions(&actor_permissions, None);
                                    break;
                                },
                            }
                        }
                    }
                    Ok(())
                }));
                if result.is_err() {
                    let message = transport_error.lock().unwrap().clone().unwrap_or_else(|| "ACP connection failed".into());
                    event_sink.emit(None, AcpEventKind::Error { message });
                }
            } else {
                event_sink.emit(None, AcpEventKind::Error { message: "ACP runtime failed".into() });
            }
            cancel_permissions(&final_permissions, None);
            final_operations.close(&event_sink);
            drop(child);
            event_sink.emit(None, AcpEventKind::Closed);
        }).map_err(|e| AcpError(e.to_string()))?;
        Ok(AcpClientHandle {
            commands: tx,
            permissions,
            operations,
            stderr_tail,
            worker_id: worker.thread().id(),
            worker: Mutex::new(Some(worker)),
        })
    }
}
impl AcpClientHandle {
    /// Raw stderr may contain secrets; callers must not log or forward it as diagnostics.
    pub fn stderr_tail(&self) -> Vec<u8> {
        self.stderr_tail.lock().unwrap().iter().copied().collect()
    }
    pub fn new_session(&self) -> Result<(), AcpError> {
        self.send_work(None, Command::NewSession)
    }
    pub fn list_sessions(&self, cursor: Option<String>) -> Result<(), AcpError> {
        self.send_work(None, |id| Command::ListSessions(id, cursor))
    }
    pub fn load_session(&self, session: String) -> Result<(), AcpError> {
        self.send_work(Some(session.clone()), |id| {
            Command::LoadSession(id, session)
        })
    }
    pub fn prompt(&self, session: String, text: String) -> Result<(), AcpError> {
        let state = self
            .permissions
            .lock()
            .map_err(|_| AcpError("permission lock failed".into()))?;
        let epoch = state.epochs.get(&session).copied().unwrap_or(0);
        self.send_work(Some(session.clone()), |id| {
            Command::Prompt(id, session, text, epoch)
        })
    }
    fn send_work(
        &self,
        session: Option<String>,
        make: impl FnOnce(u64) -> Command,
    ) -> Result<(), AcpError> {
        let id = self.operations.register(session);
        if let Err(error) = self.send(make(id)) {
            self.operations.finish(id);
            return Err(error);
        }
        Ok(())
    }
    pub fn cancel(&self, session: String) -> Result<(), AcpError> {
        let mut state = self
            .permissions
            .lock()
            .map_err(|_| AcpError("permission lock failed".into()))?;
        self.send(Command::Cancel(session.clone()))?;
        state.cancelled.insert(session.clone());
        *state.epochs.entry(session).or_default() += 1;
        Ok(())
    }
    /// Select a supplied option ID, or cancel the request with `None`.
    pub fn respond_permission(&self, id: u64, option: Option<String>) -> Result<(), AcpError> {
        let mut permissions = self
            .permissions
            .lock()
            .map_err(|_| AcpError("permission lock failed".into()))?;
        let (session, options, _) = permissions
            .pending
            .get(&id)
            .ok_or_else(|| AcpError("unknown permission request".into()))?;
        if permissions.cancelled.contains(session) {
            return Err(AcpError("permission request cancelled".into()));
        }
        if let Some(option) = &option
            && !options.contains(option)
        {
            return Err(AcpError("unknown permission option".into()));
        }
        let (_, _, responder) = permissions.pending.remove(&id).unwrap();
        let outcome = match option {
            Some(option) => {
                RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(option))
            }
            None => RequestPermissionOutcome::Cancelled,
        };
        responder
            .respond(RequestPermissionResponse::new(outcome))
            .map_err(|_| AcpError("permission request closed".into()))
    }
    fn send(&self, command: Command) -> Result<(), AcpError> {
        self.commands
            .send(command)
            .map_err(|_| AcpError("ACP connection closed".into()))
    }
    pub fn close(&self) -> Result<(), AcpError> {
        let _ = self.commands.send(Command::Close);
        if self.worker_id == std::thread::current().id() {
            return Ok(());
        }
        let mut worker = self
            .worker
            .lock()
            .map_err(|_| AcpError("ACP worker lock failed".into()))?;
        if let Some(worker) = worker.take() {
            worker
                .join()
                .map_err(|_| AcpError("ACP worker failed".into()))?;
        }
        Ok(())
    }
}
impl Drop for AcpClientHandle {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
