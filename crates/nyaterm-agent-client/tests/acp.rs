use std::sync::mpsc;
use std::time::Duration;

use nyaterm_agent_client::acp::{AcpClient, AcpConnectionConfig, AcpEventKind, AcpEventSink};
use nyaterm_core::test_support::TestTempDir;

fn mock_agent() -> (TestTempDir, std::path::PathBuf) {
    mock_agent_with_script(include_str!("fixtures/acp_agent.py"))
}

fn mock_agent_with_script(source: &str) -> (TestTempDir, std::path::PathBuf) {
    let dir = TestTempDir::new("nyaterm-acp-mock");
    std::fs::create_dir_all(dir.path()).unwrap();
    let script = dir.join("agent.py");
    std::fs::write(&script, source).unwrap();
    #[cfg(windows)]
    let wrapper = dir.join("agent.cmd");
    #[cfg(windows)]
    std::fs::write(
        &wrapper,
        format!("@echo off\r\npython -u \"{}\" %*\r\n", script.display()),
    )
    .unwrap();
    #[cfg(not(windows))]
    let wrapper = {
        use std::os::unix::fs::PermissionsExt;
        let wrapper = dir.join("agent.sh");
        std::fs::write(
            &wrapper,
            format!("#!/bin/sh\nexec python3 -u '{}' \"$@\"\n", script.display()),
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
        wrapper
    };
    (dir, wrapper)
}

#[test]
fn acp_event_kind_is_smaller_than_agent_capabilities() {
    assert!(
        std::mem::size_of::<AcpEventKind>()
            < std::mem::size_of::<agent_client_protocol::v1::schema::AgentCapabilities>()
    );
}

#[test]
fn acp_initializes_with_safe_capabilities_and_creates_a_session() {
    let (dir, command) = mock_agent();
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(42, move |event| {
            tx.send(event).unwrap();
        }),
    )
    .unwrap();
    client.new_session().unwrap();
    loop {
        let event = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("ACP session should complete");
        assert_eq!(event.generation, 42);
        match event.kind {
            AcpEventKind::SessionCreated { response } => {
                assert_eq!(event.session_id.as_deref(), Some("session-1"));
                assert_eq!(response.session_id.to_string(), "session-1");
                break;
            }
            AcpEventKind::Error { message } => panic!("ACP error: {message}"),
            _ => {}
        }
    }
    client.close().unwrap();
}

#[test]
fn acp_loads_replay_lists_pages_and_waits_for_explicit_permission() {
    let (dir, command) = mock_agent();
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(99, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    client.list_sessions(None).unwrap();
    let mut got_first_page = false;
    loop {
        let event = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        if let AcpEventKind::SessionList { response } = event.kind {
            assert_eq!(response.next_cursor.as_deref(), Some("page-2"));
            assert_eq!(response.sessions.len(), 1);
            got_first_page = true;
        }
        if got_first_page {
            break;
        }
    }
    client.list_sessions(Some("page-2".into())).unwrap();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        if let AcpEventKind::SessionList { response } = event.kind {
            assert_eq!(response.next_cursor, None);
            break;
        }
    }
    client.load_session("session-1".into()).unwrap();
    let mut replay = false;
    loop {
        let event = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(event.generation, 99);
        match event.kind {
            AcpEventKind::SessionUpdate { update } => {
                if matches!(
                    update,
                    agent_client_protocol::v1::schema::SessionUpdate::UserMessageChunk(_)
                ) {
                    replay = true;
                }
            }
            AcpEventKind::SessionLoaded { .. } => {
                assert!(replay);
                break;
            }
            AcpEventKind::Error { message } => panic!("{message}"),
            _ => {}
        }
    }
    client.prompt("session-1".into(), "hello".into()).unwrap();
    let mut seen = [false; 5];
    let request_id = loop {
        let event = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(event.generation, 99);
        match event.kind {
            AcpEventKind::SessionUpdate { update } => {
                use agent_client_protocol::v1::schema::SessionUpdate;
                match update {
                    SessionUpdate::AgentMessageChunk(_) => seen[0] = true,
                    SessionUpdate::AgentThoughtChunk(_) => seen[1] = true,
                    SessionUpdate::ToolCall(_) => seen[2] = true,
                    SessionUpdate::ToolCallUpdate(_) => seen[3] = true,
                    SessionUpdate::Plan(_) => seen[4] = true,
                    _ => {}
                }
            }
            AcpEventKind::PermissionRequested {
                request_id,
                request,
            } => {
                assert_eq!(request.options.len(), 2);
                assert_eq!(event.session_id.as_deref(), Some("session-1"));
                break request_id;
            }
            AcpEventKind::Error { message } => panic!("{message}"),
            _ => {}
        }
    };
    assert!(seen.into_iter().all(|seen| seen));
    assert!(
        rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "permission must not be auto-approved"
    );
    client
        .respond_permission(request_id, Some("choice-2".into()))
        .unwrap();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        match event.kind {
            AcpEventKind::PromptCompleted { .. } => break,
            AcpEventKind::Error { message } => panic!("{message}"),
            _ => {}
        }
    }
    client.close().unwrap();
}

#[test]
fn acp_cancel_sends_notification_cancels_permission_and_unfinished_tool() {
    let (dir, command) = mock_agent();
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(3, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    client.prompt("session-1".into(), "cancel".into()).unwrap();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        match event.kind {
            AcpEventKind::PermissionRequested { .. } => break,
            AcpEventKind::Error { message } => panic!("{message}"),
            _ => {}
        }
    }
    client.cancel("session-1".into()).unwrap();
    let mut tool_cancelled = false;
    loop {
        let event = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        match event.kind {
            AcpEventKind::ToolCancelled { tool_call_id } => {
                assert_eq!(tool_call_id, "tool-1");
                tool_cancelled = true;
            }
            AcpEventKind::PromptCompleted { response } => {
                assert_eq!(
                    response.stop_reason,
                    agent_client_protocol::v1::schema::StopReason::Cancelled
                );
                assert!(tool_cancelled);
                break;
            }
            AcpEventKind::Error { message } => panic!("{message}"),
            _ => {}
        }
    }
    client.close().unwrap();
}

#[test]
fn acp_close_interrupts_initialization_that_never_responds() {
    let (dir, command) =
        mock_agent_with_script("import sys, time\nsys.stdin.readline()\ntime.sleep(3)\n");
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, |_| {}),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let worker = std::thread::spawn(move || {
        let _ = tx.send(client.close());
    });
    let result = rx.recv_timeout(Duration::from_secs(1));
    worker.join().unwrap();
    assert!(
        result
            .expect("close should not wait for initialization")
            .is_ok()
    );
}

#[test]
fn acp_invalid_json_fails_connection_without_waiting_for_eof() {
    let (dir, command) = mock_agent_with_script(
        "import sys, time\nsys.stdin.readline()\nprint('not-json', flush=True)\ntime.sleep(3)\n",
    );
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    let event = rx.recv_timeout(Duration::from_secs(1));
    if event.is_err() {
        std::thread::sleep(Duration::from_secs(3));
    }
    client.close().unwrap();
    assert!(matches!(
        event.expect("invalid frame must fail promptly").kind,
        AcpEventKind::Error { .. }
    ));
}

#[test]
fn acp_unexpected_exit_reports_error_and_closed() {
    let script = "import json,sys\nr=json.loads(sys.stdin.readline())\nprint(json.dumps({'jsonrpc':'2.0','id':r['id'],'result':{'protocolVersion':1,'agentCapabilities':{}}}),flush=True)\nsys.exit(23)\n";
    let (dir, command) = mock_agent_with_script(script);
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    let mut error = false;
    loop {
        let event = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("connection should close");
        match event.kind {
            AcpEventKind::Error { .. } => error = true,
            AcpEventKind::Closed => break,
            _ => {}
        }
    }
    client.close().unwrap();
    assert!(error, "unexpected exit must report an error");
}

#[test]
fn acp_close_does_not_block_on_an_agent_that_stops_reading() {
    let script = "import json,sys,time\nr=json.loads(sys.stdin.readline())\nprint(json.dumps({'jsonrpc':'2.0','id':r['id'],'result':{'protocolVersion':1,'agentCapabilities':{}}}),flush=True)\ntime.sleep(3)\n";
    let (dir, command) = mock_agent_with_script(script);
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        if matches!(event.kind, AcpEventKind::Connected { .. }) {
            break;
        }
    }
    client
        .prompt("session-1".into(), "x".repeat(1_000_000))
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = tx.send(client.close());
    });
    let result = rx.recv_timeout(Duration::from_secs(1));
    worker.join().unwrap();
    assert!(
        result
            .expect("close must interrupt a blocked writer")
            .is_ok()
    );
}

#[cfg(windows)]
#[test]
fn acp_close_reaps_cmd_descendant_and_reader_threads() {
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use windows_sys::Win32::Foundation::WAIT_OBJECT_0;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject,
    };

    let (dir, command) = mock_agent();
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        if matches!(event.kind, AcpEventKind::Connected { .. }) {
            break;
        }
    }
    let pid: u32 = std::fs::read_to_string(dir.join("agent.pid"))
        .unwrap()
        .parse()
        .unwrap();
    let raw = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    assert!(!raw.is_null());
    let process = unsafe { OwnedHandle::from_raw_handle(raw) };
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = tx.send(client.close());
    });
    rx.recv_timeout(Duration::from_secs(2))
        .expect("close must join readers")
        .unwrap();
    assert_eq!(
        unsafe { WaitForSingleObject(process.as_raw_handle(), 2_000) },
        WAIT_OBJECT_0
    );
    worker.join().unwrap();
}

#[test]
fn acp_close_completes_pending_prompt_and_permission() {
    let (dir, command) = mock_agent();
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    client.prompt("session-1".into(), "close".into()).unwrap();
    let request_id = loop {
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        if let AcpEventKind::PermissionRequested { request_id, .. } = event.kind {
            break request_id;
        }
    };
    client.close().unwrap();
    assert!(
        client
            .respond_permission(request_id, Some("choice-1".into()))
            .is_err()
    );
    let mut completed = false;
    for event in rx.try_iter() {
        if event.session_id.as_deref() == Some("session-1")
            && matches!(
                event.kind,
                AcpEventKind::PromptCompleted { .. } | AcpEventKind::Error { .. }
            )
        {
            completed = true;
        }
    }
    assert!(
        completed,
        "the pending prompt must be settled before close returns"
    );
}

#[test]
fn acp_executes_command_with_quoted_arguments_without_shell_interpretation() {
    let script = format!(
        "import sys\nassert sys.argv[1:] == ['two words', '']\n{}",
        include_str!("fixtures/acp_agent.py")
    );
    let (dir, command) = mock_agent_with_script(&script);
    let command = format!("\"{}\" \"two words\" \"\"", command.display());
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command,
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        match event.kind {
            AcpEventKind::Connected { .. } => break,
            AcpEventKind::Error { message } => panic!("{message}"),
            _ => {}
        }
    }
    client.close().unwrap();
}

#[test]
fn acp_unsupported_agent_requests_get_method_not_found() {
    let script = r#"
import json,sys

def send(value):
    print(json.dumps(value), flush=True)

def read():
    return json.loads(sys.stdin.readline())

init = read()
send({'jsonrpc':'2.0','id':init['id'],'result':{'protocolVersion':1,'agentCapabilities':{}}})
send({'jsonrpc':'2.0','id':91,'method':'fs/read_text_file','params':{'sessionId':'s','path':'/secret'}})
send({'jsonrpc':'2.0','id':92,'method':'unknown/operation','params':{}})
responses = {}
new = None
while len(responses) < 2 or new is None:
    message = read()
    if message.get('id') in (91,92) and 'error' in message:
        responses[message['id']] = message['error']['code']
    elif message.get('method') == 'session/new':
        new = message['id']
assert responses == {91:-32601, 92:-32601}
send({'jsonrpc':'2.0','id':new,'result':{'sessionId':'ok'}})
for line in sys.stdin:
    pass
"#;
    let (dir, command) = mock_agent_with_script(script);
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    client.new_session().unwrap();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        match event.kind {
            AcpEventKind::SessionCreated { .. } => {
                assert_eq!(event.session_id.as_deref(), Some("ok"));
                break;
            }
            AcpEventKind::Error { message } => panic!("{message}"),
            _ => {}
        }
    }
    client.close().unwrap();
}

#[test]
fn acp_load_completion_is_ordered_between_replay_and_live_updates() {
    let (dir, command) = mock_agent();
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    client.load_session("session-1".into()).unwrap();
    let mut order = Vec::new();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        match event.kind {
            AcpEventKind::SessionUpdate {
                update: agent_client_protocol::v1::schema::SessionUpdate::UserMessageChunk(_),
            } => order.push("replay"),
            AcpEventKind::SessionLoaded { .. } => order.push("loaded"),
            AcpEventKind::SessionUpdate {
                update: agent_client_protocol::v1::schema::SessionUpdate::AgentMessageChunk(_),
            } => {
                order.push("live");
                break;
            }
            AcpEventKind::Error { message } => panic!("{message}"),
            _ => {}
        }
    }
    client.close().unwrap();
    assert_eq!(order, ["replay", "loaded", "live"]);
}

#[test]
fn acp_debug_output_redacts_commands_and_session_contents() {
    let config = AcpConnectionConfig {
        command: "agent --token=private-value".into(),
        working_dir: std::path::PathBuf::from("private-directory"),
    };
    let debug = format!("{config:?}");
    assert!(!debug.contains("private-value"));
    assert!(!debug.contains("private-directory"));

    let (dir, command) = mock_agent();
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    client.prompt("session-1".into(), "hello".into()).unwrap();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        if matches!(
            &event.kind,
            AcpEventKind::SessionUpdate {
                update: agent_client_protocol::v1::schema::SessionUpdate::AgentMessageChunk(_)
            }
        ) {
            assert!(!format!("{event:?}").contains("answer"));
            assert!(!format!("{:?}", event.kind).contains("answer"));
            break;
        }
    }
    client.close().unwrap();
}

#[test]
fn acp_late_permission_after_cancel_is_cancelled_without_asking_user() {
    let script = r#"
import json,sys

def send(value): print(json.dumps(value), flush=True)
def read(): return json.loads(sys.stdin.readline())
r=read()
send({'jsonrpc':'2.0','id':r['id'],'result':{'protocolVersion':1,'agentCapabilities':{}}})
r=read()
prompt_id=r['id']
send({'jsonrpc':'2.0','method':'session/update','params':{'sessionId':'s','update':{'sessionUpdate':'tool_call','toolCallId':'t','title':'run','status':'pending'}}})
r=read()
assert r['method']=='session/cancel'
send({'jsonrpc':'2.0','id':123,'method':'session/request_permission','params':{'sessionId':'s','toolCall':{'toolCallId':'t'},'options':[{'optionId':'allow','name':'Allow','kind':'allow_once'}]}})
r=read()
assert r['id']==123 and r['result']['outcome']=={'outcome':'cancelled'}
send({'jsonrpc':'2.0','id':prompt_id,'result':{'stopReason':'cancelled'}})
for line in sys.stdin: pass
"#;
    let (dir, command) = mock_agent_with_script(script);
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    client.prompt("s".into(), "cancel".into()).unwrap();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        if matches!(
            event.kind,
            AcpEventKind::SessionUpdate {
                update: agent_client_protocol::v1::schema::SessionUpdate::ToolCall(_)
            }
        ) {
            break;
        }
    }
    client.cancel("s".into()).unwrap();
    let mut prompted = false;
    let mut completed = false;
    while let Ok(event) = rx.recv_timeout(Duration::from_secs(2)) {
        match event.kind {
            AcpEventKind::PermissionRequested { .. } => prompted = true,
            AcpEventKind::PromptCompleted { .. } => {
                completed = true;
                break;
            }
            AcpEventKind::Error { .. } => break,
            _ => {}
        }
    }
    client.close().unwrap();
    assert!(!prompted, "cancelled turn must not request new approval");
    assert!(
        completed,
        "agent must receive a cancelled permission response"
    );
}

#[test]
fn acp_event_callback_can_close_connection_without_joining_itself() {
    use nyaterm_agent_client::acp::AcpClientHandle;
    use std::sync::{Arc, Mutex};

    let (dir, command) = mock_agent();
    let slot: Arc<Mutex<Option<Arc<AcpClientHandle>>>> = Arc::new(Mutex::new(None));
    let callback_slot = slot.clone();
    let (tx, rx) = mpsc::channel();
    let client = Arc::new(
        AcpClient::spawn(
            AcpConnectionConfig {
                command: command.to_string_lossy().into_owned(),
                working_dir: dir.path().to_path_buf(),
            },
            AcpEventSink::new(1, move |event| {
                if matches!(event.kind, AcpEventKind::SessionCreated { .. }) {
                    let handle = callback_slot.lock().unwrap().take().unwrap();
                    let _ = tx.send(handle.close());
                }
            }),
        )
        .unwrap(),
    );
    *slot.lock().unwrap() = Some(client.clone());
    client.new_session().unwrap();
    let result = rx.recv_timeout(Duration::from_secs(2));
    assert!(result.expect("callback close must not deadlock").is_ok());
    client.close().unwrap();
}

#[test]
fn acp_rejects_overlapping_prompt_turns_in_the_same_session() {
    let (dir, command) = mock_agent();
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    client.prompt("session-1".into(), "close".into()).unwrap();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        if matches!(event.kind, AcpEventKind::PermissionRequested { .. }) {
            break;
        }
    }
    client.prompt("session-1".into(), "second".into()).unwrap();
    let mut rejected = false;
    while let Ok(event) = rx.recv_timeout(Duration::from_secs(2)) {
        if matches!(event.kind, AcpEventKind::Error { .. })
            && event.session_id.as_deref() == Some("session-1")
        {
            rejected = true;
            break;
        }
    }
    client.close().unwrap();
    assert!(rejected, "only one prompt turn per session may be active");
}

#[test]
fn acp_concurrent_close_waits_for_the_same_worker_cleanup() {
    use std::sync::{Arc, Mutex};

    let (dir, command) = mock_agent();
    let (closed_tx, closed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let client = Arc::new(
        AcpClient::spawn(
            AcpConnectionConfig {
                command: command.to_string_lossy().into_owned(),
                working_dir: dir.path().to_path_buf(),
            },
            AcpEventSink::new(1, move |event| {
                if matches!(event.kind, AcpEventKind::Closed) {
                    let _ = closed_tx.send(());
                    let _ = release_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5));
                }
            }),
        )
        .unwrap(),
    );
    let first_client = client.clone();
    let first = std::thread::spawn(move || first_client.close());
    closed_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("worker should enter close callback");
    let second_client = client.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let second = std::thread::spawn(move || {
        let _ = done_tx.send(second_client.close());
    });
    let premature = done_rx.recv_timeout(Duration::from_millis(200)).is_ok();
    release_tx.send(()).unwrap();
    first.join().unwrap().unwrap();
    second.join().unwrap();
    assert!(
        !premature,
        "every close caller must wait for worker completion"
    );
}

#[test]
fn acp_captures_only_a_bounded_stderr_tail() {
    let script = "import json,sys\nsys.stderr.write('x'*20000+'END')\nsys.stderr.flush()\nr=json.loads(sys.stdin.readline())\nprint(json.dumps({'jsonrpc':'2.0','id':r['id'],'result':{'protocolVersion':1,'agentCapabilities':{}}}),flush=True)\nfor line in sys.stdin: pass\n";
    let (dir, command) = mock_agent_with_script(script);
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        if matches!(event.kind, AcpEventKind::Connected { .. }) {
            break;
        }
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let tail = client.stderr_tail();
        if tail.ends_with(b"END") {
            assert_eq!(tail.len(), 8192);
            assert!(
                tail.iter()
                    .all(|byte| *byte == b'x' || b"END".contains(byte))
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "stderr reader should capture tail"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    client.close().unwrap();
}

#[test]
fn acp_cancel_rejects_permission_response_before_actor_runs() {
    use std::sync::{Arc, Mutex};
    let (dir, command) = mock_agent();
    let (tx, rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            if let AcpEventKind::PermissionRequested { request_id, .. } = event.kind {
                let _ = tx.send(request_id);
                let _ = release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5));
            }
        }),
    )
    .unwrap();
    client.prompt("session-1".into(), "cancel".into()).unwrap();
    let request_id = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    client.cancel("session-1".into()).unwrap();
    let response = client.respond_permission(request_id, Some("choice-1".into()));
    release_tx.send(()).unwrap();
    client.close().unwrap();
    assert!(
        response.is_err(),
        "cancel must immediately prevent approval"
    );
}

#[test]
fn acp_unknown_update_is_reported_and_known_update_still_arrives() {
    let (dir, command) = mock_agent_with_script(include_str!("fixtures/acp_unknown_update.py"));
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    let mut unknown = false;
    let mut known = false;
    while let Ok(event) = rx.recv_timeout(Duration::from_secs(3)) {
        if let AcpEventKind::UnknownSessionUpdate { kind } = &event.kind {
            assert_eq!(kind, "future_feature");
            assert_eq!(event.session_id.as_deref(), Some("future-session"));
            unknown = true;
        }
        if matches!(
            event.kind,
            AcpEventKind::SessionUpdate {
                update: agent_client_protocol::v1::schema::SessionUpdate::AgentMessageChunk(_)
            }
        ) {
            assert_eq!(event.session_id.as_deref(), Some("future-session"));
            known = true;
            break;
        }
    }
    client.close().unwrap();
    assert!(
        unknown,
        "unknown update must be emitted, not silently discarded"
    );
    assert!(known, "stable updates must still reach the SDK");
}

#[test]
fn acp_rejects_oversized_stdout_frame_without_waiting_for_newline() {
    let script = "import sys,time\nsys.stdin.readline()\nsys.stdout.write('x'*(1024*1024+1))\nsys.stdout.flush()\ntime.sleep(5)\n";
    let (dir, command) = mock_agent_with_script(script);
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    let error = rx.recv_timeout(Duration::from_secs(3));
    client.close().unwrap();
    match error.expect("oversized frame must fail promptly").kind {
        AcpEventKind::Error { message } => assert!(message.contains("1 MiB"), "{message}"),
        _ => panic!("expected frame error"),
    }
}

#[test]
fn acp_flooded_stdout_reports_queue_overflow() {
    use std::sync::{Arc, Mutex};
    let script = "import json,sys,time\nr=json.loads(sys.stdin.readline())\nprint(json.dumps({'jsonrpc':'2.0','id':r['id'],'result':{'protocolVersion':1,'agentCapabilities':{}}}),flush=True)\nfor i in range(10000): print(json.dumps({'jsonrpc':'2.0','method':'session/update','params':{'sessionId':'s','update':{'sessionUpdate':'agent_message_chunk','content':{'type':'text','text':'x'}}}}),flush=True)\ntime.sleep(5)\n";
    let (dir, command) = mock_agent_with_script(script);
    let (tx, rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            if matches!(event.kind, AcpEventKind::Connected { .. }) {
                let _ = release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(4));
            } else if let AcpEventKind::Error { message } = event.kind {
                let _ = tx.send(message);
            }
        }),
    )
    .unwrap();
    std::thread::sleep(Duration::from_secs(1));
    let _ = release_tx.send(());
    let error = rx.recv_timeout(Duration::from_secs(3));
    client.close().unwrap();
    assert!(error.expect("flood must report overflow").contains("queue"));
}

#[test]
fn acp_stdin_flood_fails_when_writer_queue_is_full() {
    let script = "import json,sys,time\nr=json.loads(sys.stdin.readline())\nprint(json.dumps({'jsonrpc':'2.0','id':r['id'],'result':{'protocolVersion':1,'agentCapabilities':{}}}),flush=True)\ntime.sleep(10)\n";
    let (dir, command) = mock_agent_with_script(script);
    let (tx, rx) = mpsc::channel();
    let client = AcpClient::spawn(
        AcpConnectionConfig {
            command: command.to_string_lossy().into_owned(),
            working_dir: dir.path().to_path_buf(),
        },
        AcpEventSink::new(1, move |event| {
            let _ = tx.send(event);
        }),
    )
    .unwrap();
    loop {
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        if matches!(event.kind, AcpEventKind::Connected { .. }) {
            break;
        }
    }
    for i in 0..200 {
        if client
            .prompt(format!("session-{i}"), "x".repeat(100_000))
            .is_err()
        {
            break;
        }
    }
    let mut overflow = false;
    while let Ok(event) = rx.recv_timeout(Duration::from_secs(3)) {
        if let AcpEventKind::Error { message } = event.kind
            && message.contains("stdin")
            && message.contains("queue")
        {
            overflow = true;
            break;
        }
    }
    client.close().unwrap();
    assert!(overflow, "blocked stdin must report writer queue overflow");
}
