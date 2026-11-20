use nyaterm_agent_client::acp::{AcpEvent, AcpEventKind};

use super::state::AcpFeatureState;

#[test]
fn acp_stale_events_and_other_tabs_cannot_change_session_state() {
    let mut state = AcpFeatureState::default();
    state.start("first".into(), 1);
    state.start("second".into(), 2);
    state.apply(
        "first",
        AcpEvent {
            generation: 0,
            session_id: None,
            kind: AcpEventKind::Error {
                message: "stale".into(),
            },
        },
    );
    state.apply(
        "second",
        AcpEvent {
            generation: 2,
            session_id: None,
            kind: AcpEventKind::Error {
                message: "second failed".into(),
            },
        },
    );
    assert_eq!(state.session("first").unwrap().error.as_deref(), None);
    assert_eq!(
        state.session("second").unwrap().error.as_deref(),
        Some("second failed")
    );
}

#[test]
fn acp_stream_capacity_never_drops_permission_error_or_closed_events() {
    use super::runtime::AcpEventBuffer;
    let buffer = AcpEventBuffer::new(1);
    buffer.push(AcpEvent {
        generation: 1,
        session_id: Some("session-1".into()),
        kind: AcpEventKind::SessionUpdate {
            update: serde_json::from_value(serde_json::json!({
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "hello"}
            }))
            .unwrap(),
        },
    });
    buffer.push(AcpEvent {
        generation: 1,
        session_id: Some("session-1".into()),
        kind: AcpEventKind::PermissionRequested {
            request_id: 7,
            request: serde_json::from_value(serde_json::json!({
                "sessionId": "session-1",
                "toolCall": {"toolCallId": "tool-1"},
                "options": []
            }))
            .unwrap(),
        },
    });
    buffer.push(AcpEvent {
        generation: 1,
        session_id: None,
        kind: AcpEventKind::Error {
            message: "failure".into(),
        },
    });
    buffer.push(AcpEvent {
        generation: 1,
        session_id: None,
        kind: AcpEventKind::Closed,
    });
    let (events, overflowed) = buffer.drain();
    assert!(!overflowed);
    assert_eq!(events.len(), 4);
    assert!(matches!(events[0].kind, AcpEventKind::SessionUpdate { .. }));
    assert!(matches!(
        events[1].kind,
        AcpEventKind::PermissionRequested { request_id: 7, .. }
    ));
    assert!(matches!(&events[2].kind, AcpEventKind::Error { message } if message == "failure"));
    assert!(matches!(events[3].kind, AcpEventKind::Closed));
}

#[test]
fn acp_full_event_buffer_reports_stream_overflow_without_losing_control_events() {
    use super::runtime::AcpEventBuffer;
    let buffer = AcpEventBuffer::new(1);
    for text in ["first", "second"] {
        buffer.push(AcpEvent {
            generation: 1,
            session_id: Some("session-1".into()),
            kind: AcpEventKind::SessionUpdate {
                update: serde_json::from_value(serde_json::json!({
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": text}
                }))
                .unwrap(),
            },
        });
    }
    buffer.push(AcpEvent {
        generation: 1,
        session_id: None,
        kind: AcpEventKind::Error {
            message: "preserved".into(),
        },
    });
    let (events, overflowed) = buffer.drain();
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0].kind, AcpEventKind::SessionUpdate { .. }));
    assert!(matches!(&events[1].kind, AcpEventKind::Error { message } if message == "preserved"));
    assert!(overflowed);
}

#[test]
fn acp_controller_starts_streams_prompts_and_closes_with_pending_permission() {
    use super::runtime::{AcpCommand, AcpEventBuffer, EventPublisher, spawn_controller};
    use futures::channel::mpsc::unbounded;
    use nyaterm_agent_client::acp::AcpConnectionConfig;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    let dir = tempfile::tempdir().unwrap();
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("nyaterm-agent-client/tests/fixtures/acp_agent.py");
    let (wake, _rx) = unbounded();
    let publisher = Arc::new(EventPublisher {
        buffer: Arc::new(AcpEventBuffer::new(512)),
        notified: AtomicBool::new(false),
        finished: AtomicBool::new(false),
        wake,
    });
    let (commands, worker) = spawn_controller(
        AcpConnectionConfig {
            command: format!(
                "{} -u \"{}\"",
                if cfg!(windows) { "python" } else { "python3" },
                script.display()
            ),
            working_dir: dir.path().to_path_buf(),
        },
        7,
        publisher.clone(),
    )
    .unwrap();
    let mut state = AcpFeatureState::default();
    state.start("tab".into(), 7);
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut sent = false;
    loop {
        assert!(
            Instant::now() < deadline,
            "ACP did not reach permission prompt"
        );
        let (events, overflowed) = publisher.drain();
        assert!(!overflowed);
        for event in events {
            state.apply("tab", event);
        }
        let session = state.session("tab").unwrap();
        if !sent && session.active_session_id.as_deref() == Some("session-1") {
            commands
                .send(AcpCommand::Prompt("session-1".into(), "close".into()))
                .unwrap();
            sent = true;
        }
        if !session.pending_permissions.is_empty() {
            break;
        }
        assert!(session.error.is_none(), "{:?}", session.error);
        std::thread::sleep(Duration::from_millis(10));
    }
    let session = state.session("tab").unwrap();
    assert_eq!(
        super::projection::project_timeline(&session.timeline)
            .iter()
            .filter(|item| matches!(item, super::projection::TimelineItem::Tool { id, .. } if id == "tool-1"))
            .count(),
        1
    );
    commands.send(AcpCommand::Close).unwrap();
    worker.join().unwrap();
    let events = publisher.drain().0;
    assert!(
        events
            .iter()
            .any(|event| matches!(event.kind, AcpEventKind::Closed))
    );
    for event in events {
        state.apply("tab", event);
    }
    assert!(state.session("tab").unwrap().pending_permissions.is_empty());
    assert!(!state.session("tab").unwrap().connected);
}

#[test]
fn acp_missing_executable_reports_startup_failure_and_closed() {
    use super::runtime::{AcpEventBuffer, EventPublisher, spawn_controller};
    use futures::channel::mpsc::unbounded;
    use nyaterm_agent_client::acp::AcpConnectionConfig;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    let dir = tempfile::tempdir().unwrap();
    let (wake, _rx) = unbounded();
    let publisher = Arc::new(EventPublisher {
        buffer: Arc::new(AcpEventBuffer::new(32)),
        notified: AtomicBool::new(false),
        finished: AtomicBool::new(false),
        wake,
    });
    let (_commands, worker) = spawn_controller(
        AcpConnectionConfig {
            command: "nyaterm-missing-acp-executable-34ff7".into(),
            working_dir: dir.path().into(),
        },
        8,
        publisher.clone(),
    )
    .unwrap();
    worker.join().unwrap();
    let (events, overflow) = publisher.drain();
    assert!(!overflow);
    assert!(
        events
            .iter()
            .any(|e| matches!(e.kind, AcpEventKind::Error { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e.kind, AcpEventKind::Closed))
    );
}

#[test]
fn acp_saved_connection_registers_nonterminal_session_and_closes() {
    use crate::features::test_support::app_with_visible_local_session;
    use crate::test_support::{TestAppCleanup, TestConfigDir};
    use gpui::{AppContext as _, TestAppContext};
    use nyaterm_core::{AiExecutionProfile, SavedConnection};
    use nyaterm_transport::SessionKind;
    use std::time::{Duration, Instant};
    let root = TestConfigDir::new("nyaterm-acp-app-lifecycle");
    let working = tempfile::tempdir().unwrap();
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("nyaterm-agent-client/tests/fixtures/acp_agent.py");
    let connection: SavedConnection = serde_json::from_value(serde_json::json!({
        "id":"acp-fixture", "name":"ACP fixture", "type":"acp",
        "command":format!("{} -u \"{}\"", if cfg!(windows) { "python" } else { "python3" }, script.display()),
        "working_dir":working.path().to_string_lossy()
    }))
    .unwrap();
    let mut cx = TestAppContext::single();
    let _cleanup = TestAppCleanup::new(&cx);
    let app = app_with_visible_local_session(&mut cx, root.path(), "other");
    let id = cx.update_entity(&app, |app, cx| {
        assert!(!app.continue_saved_connection_start(
            connection.clone(),
            crate::features::session::SavedConnectionStartOptions {
                suppress_serial_prompt: true,
                ..Default::default()
            },
            cx,
        ));
        assert!(!app.continue_saved_connection_start(
            connection.clone(),
            crate::features::session::SavedConnectionStartOptions {
                mcp_open_operation_id: Some("acp-mcp-test".into()),
                ..Default::default()
            },
            cx,
        ));
        assert_eq!(app.session.active_id(), Some("other"));
        assert!(app.continue_saved_connection_start(connection, Default::default(), cx));
        let id = app.session.active_id().unwrap().to_owned();
        assert_eq!(
            app.session.session_info(&id).unwrap().kind,
            SessionKind::Acp
        );
        assert_eq!(
            app.session.metadata(&id).unwrap().ai_execution_profile,
            AiExecutionProfile::Disabled
        );
        assert!(!app.terminal_sync_session(&id));
        assert!(app.active_terminal_input_blocked());
        app.start_recording_for_session(&id, nyaterm_recording::recording::RecordingMode::Raw, cx);
        assert!(!app.recording.is_recording(&id));
        assert!(app.write_session_input_recorded(&id, b"command\n").is_err());
        assert!(
            app.session
                .can_transfer_sessions(std::slice::from_ref(&id))
                .is_err()
        );
        id
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        cx.run_until_parked();
        if cx.update_entity(&app, |app, _| {
            app.acp.session(&id).unwrap().active_session_id.is_some()
        }) {
            break;
        }
        assert!(Instant::now() < deadline, "ACP session did not start");
        std::thread::sleep(Duration::from_millis(10));
    }
    cx.update_entity(&app, |app, _| {
        app.acp.list_sessions(&id, None).unwrap();
        app.acp.prompt(&id, "hello".into()).unwrap();
    });
    loop {
        cx.run_until_parked();
        let ready = cx.update_entity(&app, |app, _| {
            let state = app.acp.session(&id).unwrap();
            state.session_list.is_some() && !state.pending_permissions.is_empty()
        });
        if ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "ACP prompt did not request permission"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    cx.update_entity(&app, |app, _| {
        let request_id = *app
            .acp
            .session(&id)
            .unwrap()
            .pending_permissions
            .keys()
            .next()
            .unwrap();
        assert!(
            app.acp
                .respond_permission(&id, request_id, Some("invalid".into()))
                .is_err()
        );
        assert!(
            app.acp
                .session(&id)
                .unwrap()
                .pending_permissions
                .contains_key(&request_id)
        );
        app.acp
            .respond_permission(&id, request_id, Some("choice-2".into()))
            .unwrap();
    });
    loop {
        cx.run_until_parked();
        if cx.update_entity(&app, |app, _| !app.acp.session(&id).unwrap().running) {
            break;
        }
        assert!(Instant::now() < deadline, "ACP prompt did not finish");
        std::thread::sleep(Duration::from_millis(10));
    }
    cx.update_entity(&app, |app, _| {
        let timeline = &app.acp.session(&id).unwrap().timeline;
        let kinds: Vec<_> = timeline
            .iter()
            .filter_map(|value| value.get("sessionUpdate").and_then(|value| value.as_str()))
            .collect();
        assert_eq!(
            kinds,
            [
                "user_message_chunk",
                "agent_message_chunk",
                "agent_thought_chunk",
                "tool_call",
                "tool_call_update",
                "plan"
            ]
        );
        assert_eq!(
            timeline[4].get("status").and_then(|value| value.as_str()),
            Some("completed")
        );
        app.acp.load_session(&id, "session-1".into()).unwrap();
    });
    loop {
        cx.run_until_parked();
        if cx.update_entity(&app, |app, _| {
            let session = app.acp.session(&id).unwrap();
            !session.loading
                && session.timeline.iter().any(|item| {
                    item.pointer("/content/text")
                        .and_then(|value| value.as_str())
                        == Some("replay")
                })
        }) {
            break;
        }
        assert!(Instant::now() < deadline, "ACP load did not replay");
        std::thread::sleep(Duration::from_millis(10));
    }
    cx.update_entity(&app, |app, _| {
        app.acp.prompt(&id, "cancel".into()).unwrap();
    });
    loop {
        cx.run_until_parked();
        if cx.update_entity(&app, |app, _| {
            !app.acp.session(&id).unwrap().pending_permissions.is_empty()
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "ACP cancel prompt did not request permission"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    cx.update_entity(&app, |app, _| app.acp.cancel(&id).unwrap());
    loop {
        cx.run_until_parked();
        if cx.update_entity(&app, |app, _| !app.acp.session(&id).unwrap().running) {
            break;
        }
        assert!(Instant::now() < deadline, "ACP cancel did not finish");
        std::thread::sleep(Duration::from_millis(10));
    }
    cx.update_entity(&app, |app, _| {
        assert!(app.acp.session(&id).unwrap().timeline.iter().any(|item| {
            item.get("toolCallId").and_then(|v| v.as_str()) == Some("tool-1")
                && item.get("status").and_then(|v| v.as_str()) == Some("cancelled")
        }));
    });
    cx.update_entity(&app, |app, cx| {
        app.close_session(id.clone(), cx);
        assert!(!app.acp.contains(&id));
        assert!(app.session.session_info(&id).is_none());
    });
}

#[test]
fn acp_all_control_kinds_bypass_stream_capacity_in_order() {
    use super::runtime::AcpEventBuffer;
    let buffer = AcpEventBuffer::new(0);
    let kinds = vec![
        AcpEventKind::Connected {
            capabilities: Box::new(serde_json::from_value(serde_json::json!({})).unwrap()),
        },
        AcpEventKind::SessionCreated {
            response: serde_json::from_value(serde_json::json!({"sessionId":"s"})).unwrap(),
        },
        AcpEventKind::SessionList {
            response: serde_json::from_value(serde_json::json!({"sessions":[]})).unwrap(),
        },
        AcpEventKind::SessionLoaded {
            response: serde_json::from_value(serde_json::json!({})).unwrap(),
        },
        AcpEventKind::PermissionRequested {
            request_id: 3,
            request: serde_json::from_value(
                serde_json::json!({"sessionId":"s", "toolCall":{"toolCallId":"t"}, "options":[]}),
            )
            .unwrap(),
        },
        AcpEventKind::PromptCompleted {
            response: serde_json::from_value(serde_json::json!({"stopReason":"end_turn"})).unwrap(),
        },
        AcpEventKind::ToolCancelled {
            tool_call_id: "t".into(),
        },
        AcpEventKind::Error {
            message: "error".into(),
        },
        AcpEventKind::Closed,
    ];
    for kind in kinds {
        buffer.push(AcpEvent {
            generation: 1,
            session_id: Some("s".into()),
            kind,
        });
    }
    let (events, overflowed) = buffer.drain();
    assert!(!overflowed);
    assert_eq!(events.len(), 9);
    assert!(matches!(events[0].kind, AcpEventKind::Connected { .. }));
    assert!(matches!(
        events[1].kind,
        AcpEventKind::SessionCreated { .. }
    ));
    assert!(matches!(events[2].kind, AcpEventKind::SessionList { .. }));
    assert!(matches!(events[3].kind, AcpEventKind::SessionLoaded { .. }));
    assert!(matches!(
        events[4].kind,
        AcpEventKind::PermissionRequested { request_id: 3, .. }
    ));
    assert!(matches!(
        events[5].kind,
        AcpEventKind::PromptCompleted { .. }
    ));
    assert!(matches!(events[6].kind, AcpEventKind::ToolCancelled { .. }));
    assert!(matches!(events[7].kind, AcpEventKind::Error { .. }));
    assert!(matches!(events[8].kind, AcpEventKind::Closed));
}

#[test]
fn acp_permission_response_is_single_use_after_enqueue() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut state = AcpFeatureState::default();
    state.start("tab".into(), 1);
    let session = state.session_mut("tab").unwrap();
    session.commands = Some(tx);
    session
        .pending_permissions
        .insert(1, serde_json::json!({"options":[{"optionId":"allow"}]}));
    assert!(
        state
            .respond_permission("tab", 1, Some("allow".into()))
            .is_ok()
    );
    assert!(
        !state
            .session("tab")
            .unwrap()
            .pending_permissions
            .contains_key(&1)
    );
    assert!(
        state
            .respond_permission("tab", 1, Some("allow".into()))
            .is_err()
    );
}

#[test]
fn acp_prompt_immediately_adds_user_message() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut state = AcpFeatureState::default();
    state.start("tab".into(), 1);
    let session = state.session_mut("tab").unwrap();
    session.commands = Some(tx);
    session.active_session_id = Some("session".into());
    state.prompt("tab", "hello\nworld".into()).unwrap();
    let timeline = &state.session("tab").unwrap().timeline;
    assert_eq!(timeline.len(), 1);
    assert_eq!(timeline[0]["sessionUpdate"], "user_message_chunk");
    assert_eq!(timeline[0]["content"]["text"], "hello\nworld");
}

#[test]
fn acp_projection_groups_message_ids_and_contiguous_chunks_and_updates_tools() {
    use super::projection::{MessageRole, TimelineItem, project_timeline};
    let raw = vec![
        serde_json::json!({"sessionUpdate":"agent_message_chunk", "messageId":"one", "content":{"type":"text","text":"Hello"}}),
        serde_json::json!({"sessionUpdate":"agent_thought_chunk", "content":{"type":"text","text":"thinking"}}),
        serde_json::json!({"sessionUpdate":"agent_message_chunk", "messageId":"one", "content":{"type":"text","text":" world"}}),
        serde_json::json!({"sessionUpdate":"tool_call", "toolCallId":"tool", "title":"Read file", "status":"in_progress"}),
        serde_json::json!({"sessionUpdate":"tool_call_update", "toolCallId":"tool", "status":"completed"}),
        serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text","text":"A"}}),
        serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text","text":"B"}}),
        serde_json::json!({"sessionUpdate":"unknown_future_event", "secret":"must not render"}),
    ];
    let result = project_timeline(&raw);
    assert!(
        matches!(&result[0], TimelineItem::Message { role: MessageRole::Agent, text, .. } if text == "Hello world")
    );
    assert!(
        matches!(&result[1], TimelineItem::Message { role: MessageRole::Thought, text, .. } if text == "thinking")
    );
    assert!(
        matches!(&result[2], TimelineItem::Tool { id, title, status, .. } if id == "tool" && title == "Read file" && status == "completed")
    );
    assert!(
        matches!(&result[3], TimelineItem::Message { role: MessageRole::Agent, text, .. } if text == "AB")
    );
    assert!(matches!(&result[4], TimelineItem::Unknown { kind } if kind == "unknown_future_event"));
    assert_eq!(result.len(), 5);
}

#[test]
fn acp_cancel_invalidates_permissions_and_waits_for_completion() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut state = AcpFeatureState::default();
    state.start("tab".into(), 1);
    let session = state.session_mut("tab").unwrap();
    session.commands = Some(tx);
    session.active_session_id = Some("session".into());
    session.running = true;
    session
        .pending_permissions
        .insert(4, serde_json::json!({"options":[{"optionId":"allow"}]}));
    state.cancel("tab").unwrap();
    assert!(state.session("tab").unwrap().cancelling);
    assert!(state.session("tab").unwrap().running);
    assert!(
        state
            .respond_permission("tab", 4, Some("allow".into()))
            .is_err()
    );
    state.apply(
        "tab",
        AcpEvent {
            generation: 1,
            session_id: Some("session".into()),
            kind: AcpEventKind::PromptCompleted {
                response: serde_json::from_value(serde_json::json!({"stopReason":"cancelled"}))
                    .unwrap(),
            },
        },
    );
    assert!(!state.session("tab").unwrap().running);
    assert!(!state.session("tab").unwrap().cancelling);
}

#[test]
fn acp_page_renders_cards_and_accepts_composer_keyboard_input() {
    use super::runtime::AcpCommand;
    use crate::features::NyaTermApp;
    use crate::features::test_support::app_with_visible_local_session;
    use crate::test_support::{TestAppCleanup, TestConfigDir};
    use gpui::{
        AppContext as _, Entity, IntoElement, ParentElement as _, Render, Styled as _,
        TestAppContext, div,
    };
    use nyaterm_ui::NyaInputState;

    struct Host(Entity<NyaTermApp>);
    impl Render for Host {
        fn render(
            &mut self,
            _: &mut gpui::Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            div().size_full().child(
                self.0
                    .update(cx, |app, cx| app.acp_view("fixture".into(), cx)),
            )
        }
    }
    let root = TestConfigDir::new("nyaterm-acp-page");
    let mut cx = TestAppContext::single();
    cx.update(nyaterm_ui::init_components);
    let _cleanup = TestAppCleanup::new(&cx);
    let app = app_with_visible_local_session(&mut cx, root.path(), "other");
    let (tx, rx) = std::sync::mpsc::channel();
    cx.update_entity(&app, |app, cx| {
        app.sync_component_theme(cx);
        app.acp.start("fixture".into(), 1);
        let input = cx.new(|cx| NyaInputState::new(cx, "").multi_line(Some(4)));
        let session = app.acp.session_mut("fixture").unwrap();
        session.input = Some(input);
        session.commands = Some(tx);
        session.connected = true;
        session.active_session_id = Some("session".into());
        session.timeline = vec![
            serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text","text":"# Answer\n\n```rust\nlet x = 1;\n```"}}),
            serde_json::json!({"sessionUpdate":"tool_call", "toolCallId":"tool", "title":"Read", "status":"completed", "rawOutput":"output"}),
            serde_json::json!({"sessionUpdate":"plan", "entries":[{"content":"Next", "status":"pending"}]}),
        ];
        session.pending_permissions.insert(5, serde_json::json!({"toolCall":{"title":"Run"}, "options":[{"optionId":"deny", "name":"Deny"}]}));
    });
    let host = app.clone();
    let (_, window) = cx.add_window_view(move |_, _| Host(host));
    window.update(|window, cx| {
        window.activate_window();
        let _ = window.draw(cx);
    });
    window.run_until_parked();
    assert!(window.debug_bounds("acp-page").is_some());
    assert!(window.debug_bounds("acp-message-0").is_some());
    assert!(window.debug_bounds("acp-tool-1").is_some());
    let detail = window.debug_bounds("acp-detail-1-0").unwrap();
    window.simulate_click(detail.center(), gpui::Modifiers::default());
    window.update(|window, cx| {
        window.refresh();
        let _ = window.draw(cx);
    });
    window.run_until_parked();
    assert!(window.debug_bounds("acp-code-1-0").is_some());
    assert!(window.debug_bounds("acp-plan-2").is_some());
    assert!(window.debug_bounds("acp-permission-5").is_some());
    let option = window.debug_bounds("acp-option-5-0").unwrap();
    window.simulate_click(option.center(), gpui::Modifiers::default());
    window.run_until_parked();
    assert!(
        matches!(rx.try_recv().unwrap(), AcpCommand::Permission(5, Some(option)) if option == "deny")
    );
    window.update(|window, cx| {
        app.update(cx, |app, cx| app.focus_acp_composer("fixture", window, cx));
        let _ = window.draw(cx);
    });
    window.run_until_parked();
    window.simulate_input("hello ACP");
    window.simulate_keystrokes("enter");
    window.simulate_input("next line");
    window.simulate_keystrokes("secondary-enter");
    window.run_until_parked();
    assert!(
        matches!(rx.try_recv().unwrap(), AcpCommand::Prompt(session, text) if session == "session" && text == "hello ACP\nnext line")
    );
    window.update(|_, cx| {
        let state = app.read(cx).acp.session("fixture").unwrap();
        assert!(state.running);
        assert!(state.pending_permissions.is_empty());
        assert_eq!(
            state.timeline.last().unwrap()["content"]["text"],
            "hello ACP\nnext line"
        );
    });
    window.update(|window, cx| {
        app.update(cx, |app, cx| {
            let state = app.acp.session_mut("fixture").unwrap();
            for index in 0..24 {
                state.timeline.push(serde_json::json!({"sessionUpdate":"agent_message_chunk", "messageId":format!("long-{index}"), "content":{"type":"text", "text":"long message\n".repeat(12)}}));
            }
            cx.notify();
        });
        window.refresh();
        let _ = window.draw(cx);
    });
    window.run_until_parked();
    let viewport = window.debug_bounds("acp-transcript-viewport").unwrap();
    let before = window.debug_bounds("acp-message-0").unwrap().top();
    window.simulate_event(gpui::ScrollEvent {
        position: viewport.center(),
        delta: gpui::ScrollDelta::Pixels(gpui::point(gpui::px(0.), gpui::px(-450.))),
        modifiers: Default::default(),
        touch_phase: gpui::TouchPhase::Moved,
        source: gpui::ScrollSource::Pointer,
    });
    window.run_until_parked();
    window.update(|window, cx| {
        let _ = window.draw(cx);
    });
    let after = window.debug_bounds("acp-message-0").unwrap().top();
    assert!(after < before, "ACP transcript should scroll");
}

#[test]
fn acp_projection_exposes_only_tool_details_and_safe_unknown_kind() {
    use super::projection::{TimelineItem, project_timeline};
    let result = project_timeline(&[
        serde_json::json!({"sessionUpdate":"tool_call", "toolCallId":"one", "title":"Edit", "rawInput":{"path":"a.rs"}, "rawOutput":"done", "content":[{"type":"diff","path":"a.rs", "oldText":"old", "newText":"new"}], "secretField":"do-not-render"}),
        serde_json::json!({"sessionUpdate":"plan", "entries":[{"content":"Inspect", "status":"completed"}]}),
        serde_json::json!({"sessionUpdate":"future", "secretField":"do-not-render"}),
    ]);
    let TimelineItem::Tool { details, .. } = &result[0] else {
        panic!("tool card missing");
    };
    assert!(
        details
            .iter()
            .any(|(name, value)| name == "rawInput" && value.contains("a.rs"))
    );
    assert!(
        details
            .iter()
            .any(|(name, value)| name == "rawOutput" && value == "done")
    );
    assert!(details.iter().any(|(name, value)| name.starts_with("diff")
        && value.contains("old")
        && value.contains("new")));
    assert!(!format!("{result:?}").contains("do-not-render"));
    assert!(
        matches!(&result[1], TimelineItem::Plan { entries } if entries == &["completed: Inspect"])
    );
    assert!(matches!(&result[2], TimelineItem::Unknown { kind } if kind == "future"));
}

#[test]
fn acp_completed_turn_and_failed_enqueue_do_not_approve_permissions() {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut state = AcpFeatureState::default();
    state.start("tab".into(), 1);
    let session = state.session_mut("tab").unwrap();
    session.commands = Some(tx);
    session.active_session_id = Some("session".into());
    session
        .pending_permissions
        .insert(4, serde_json::json!({"options":[{"optionId":"allow"}]}));
    drop(rx);
    assert!(
        state
            .respond_permission("tab", 4, Some("allow".into()))
            .is_err()
    );
    assert!(
        state
            .session("tab")
            .unwrap()
            .pending_permissions
            .contains_key(&4)
    );
    assert!(state.prompt("tab", "not sent".into()).is_err());
    assert!(state.session("tab").unwrap().timeline.is_empty());
    state.apply(
        "tab",
        AcpEvent {
            generation: 1,
            session_id: Some("session".into()),
            kind: AcpEventKind::PromptCompleted {
                response: serde_json::from_value(serde_json::json!({"stopReason":"end_turn"}))
                    .unwrap(),
            },
        },
    );
    assert!(
        state
            .respond_permission("tab", 4, Some("allow".into()))
            .is_err()
    );
    assert!(state.session("tab").unwrap().pending_permissions.is_empty());
}

#[test]
fn acp_loading_state_clears_when_session_load_completes() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut state = AcpFeatureState::default();
    state.start("tab".into(), 1);
    state.session_mut("tab").unwrap().commands = Some(tx);
    state.apply(
        "tab",
        AcpEvent {
            generation: 1,
            session_id: None,
            kind: AcpEventKind::Connected {
                capabilities: Box::new(
                    serde_json::from_value(
                        serde_json::json!({"loadSession":true, "sessionCapabilities":{"list":{}}}),
                    )
                    .unwrap(),
                ),
            },
        },
    );
    state.load_session("tab", "session".into()).unwrap();
    assert!(state.session("tab").unwrap().loading);
    state.apply(
        "tab",
        AcpEvent {
            generation: 1,
            session_id: Some("session".into()),
            kind: AcpEventKind::SessionLoaded {
                response: serde_json::from_value(serde_json::json!({})).unwrap(),
            },
        },
    );
    assert!(!state.session("tab").unwrap().loading);
}

#[test]
fn acp_unidentified_chunks_do_not_merge_across_an_explicit_message_update() {
    use super::projection::{MessageRole, TimelineItem, project_timeline};
    let result = project_timeline(&[
        serde_json::json!({"sessionUpdate":"agent_message_chunk", "messageId":"answer", "content":{"type":"text","text":"a"}}),
        serde_json::json!({"sessionUpdate":"agent_thought_chunk", "content":{"type":"text","text":"first"}}),
        serde_json::json!({"sessionUpdate":"agent_message_chunk", "messageId":"answer", "content":{"type":"text","text":"b"}}),
        serde_json::json!({"sessionUpdate":"agent_thought_chunk", "content":{"type":"text","text":"second"}}),
    ]);
    assert_eq!(result.len(), 3);
    assert!(
        matches!(&result[1], TimelineItem::Message { role: MessageRole::Thought, text, .. } if text == "first")
    );
    assert!(
        matches!(&result[2], TimelineItem::Message { role: MessageRole::Thought, text, .. } if text == "second")
    );
}

#[test]
fn acp_tool_updates_preserve_message_chunk_boundaries() {
    use super::projection::{MessageRole, TimelineItem, project_timeline};
    let mut state = AcpFeatureState::default();
    state.start("tab".into(), 1);
    for update in [
        serde_json::json!({"sessionUpdate":"tool_call", "toolCallId":"t", "title":"Run", "status":"in_progress"}),
        serde_json::json!({"sessionUpdate":"agent_thought_chunk", "content":{"type":"text","text":"first"}}),
        serde_json::json!({"sessionUpdate":"tool_call_update", "toolCallId":"t", "status":"completed"}),
        serde_json::json!({"sessionUpdate":"agent_thought_chunk", "content":{"type":"text","text":"second"}}),
    ] {
        state.apply(
            "tab",
            AcpEvent {
                generation: 1,
                session_id: None,
                kind: AcpEventKind::SessionUpdate {
                    update: serde_json::from_value(update).unwrap(),
                },
            },
        );
    }
    let timeline = project_timeline(&state.session("tab").unwrap().timeline);
    assert_eq!(timeline.len(), 3);
    assert!(matches!(&timeline[0], TimelineItem::Tool { status, .. } if status == "completed"));
    assert!(
        matches!(&timeline[1], TimelineItem::Message { role: MessageRole::Thought, text, .. } if text == "first")
    );
    assert!(
        matches!(&timeline[2], TimelineItem::Message { role: MessageRole::Thought, text, .. } if text == "second")
    );
}

#[test]
fn acp_real_agent_ui_prompt_permission_and_response() {
    use crate::features::NyaTermApp;
    use crate::features::test_support::app_with_visible_local_session;
    use crate::test_support::{TestAppCleanup, TestConfigDir};
    use gpui::{
        AppContext as _, Entity, IntoElement, ParentElement as _, Render, Styled as _,
        TestAppContext, div,
    };
    use nyaterm_core::SavedConnection;
    use std::time::{Duration, Instant};
    struct Host {
        app: Entity<NyaTermApp>,
        id: String,
    }
    impl Render for Host {
        fn render(
            &mut self,
            _: &mut gpui::Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            let id = self.id.clone();
            div()
                .size_full()
                .child(self.app.update(cx, |app, cx| app.acp_view(id, cx)))
        }
    }
    let root = TestConfigDir::new("nyaterm-acp-real-page");
    let working = tempfile::tempdir().unwrap();
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("nyaterm-agent-client/tests/fixtures/acp_agent.py");
    let connection: SavedConnection = serde_json::from_value(serde_json::json!({
        "id":"acp-ui", "name":"ACP", "type":"acp",
        "command":format!("{} -u \"{}\"", if cfg!(windows) { "python" } else { "python3" }, script.display()),
        "working_dir":working.path().to_string_lossy()
    })).unwrap();
    let mut cx = TestAppContext::single();
    cx.update(nyaterm_ui::init_components);
    let _cleanup = TestAppCleanup::new(&cx);
    let app = app_with_visible_local_session(&mut cx, root.path(), "other");
    let id = cx.update_entity(&app, |app, cx| {
        app.sync_component_theme(cx);
        assert!(app.continue_saved_connection_start(connection, Default::default(), cx));
        app.session.active_id().unwrap().to_owned()
    });
    let host = app.clone();
    let target = id.clone();
    let (_, window) = cx.add_window_view(move |_, _| Host {
        app: host,
        id: target,
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        window.run_until_parked();
        let ready = window.update(|window, cx| {
            window.refresh();
            let _ = window.draw(cx);
            app.read(cx)
                .acp
                .session(&id)
                .unwrap()
                .active_session_id
                .is_some()
        });
        if ready {
            break;
        }
        assert!(Instant::now() < deadline, "ACP agent did not start");
        std::thread::sleep(Duration::from_millis(10));
    }
    window.update(|window, cx| {
        window.activate_window();
        app.update(cx, |app, cx| app.focus_acp_composer(&id, window, cx));
        let _ = window.draw(cx);
    });
    window.simulate_input("hello");
    window.simulate_keystrokes("secondary-enter");
    loop {
        window.run_until_parked();
        let ready = window.update(|window, cx| {
            window.refresh();
            let _ = window.draw(cx);
            !app.read(cx)
                .acp
                .session(&id)
                .unwrap()
                .pending_permissions
                .is_empty()
        });
        if ready {
            break;
        }
        assert!(Instant::now() < deadline, "ACP permission did not arrive");
        std::thread::sleep(Duration::from_millis(10));
    }
    let option = window
        .debug_bounds("acp-option-1-0")
        .expect("agent option visible");
    window.simulate_click(option.center(), gpui::Modifiers::default());
    loop {
        window.run_until_parked();
        let done = window.update(|window, cx| {
            window.refresh();
            let _ = window.draw(cx);
            let state = app.read(cx).acp.session(&id).unwrap();
            !state.running && state.pending_permissions.is_empty()
        });
        if done {
            break;
        }
        assert!(Instant::now() < deadline, "ACP prompt did not complete");
        std::thread::sleep(Duration::from_millis(10));
    }
    window.update(|_, cx| {
        let state = app.read(cx).acp.session(&id).unwrap();
        assert!(
            state
                .timeline
                .iter()
                .any(
                    |item| item.pointer("/content/text").and_then(|v| v.as_str()) == Some("hello")
                )
        );
        assert!(
            state
                .timeline
                .iter()
                .any(
                    |item| item.pointer("/content/text").and_then(|v| v.as_str()) == Some("answer")
                )
        );
        assert!(state.error.is_none(), "{:?}", state.error);
    });
    window.update(|_, cx| app.update(cx, |app, cx| app.close_session(id.clone(), cx)));
}

fn acp_history_fixture() -> (
    AcpFeatureState,
    std::sync::mpsc::Receiver<super::runtime::AcpCommand>,
) {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut state = AcpFeatureState::default();
    state.start("history".into(), 11);
    let session = state.session_mut("history").unwrap();
    session.commands = Some(tx);
    session.active_session_id = Some("old".into());
    session.timeline.push(serde_json::json!({"sessionUpdate":"user_message_chunk", "content":{"type":"text","text":"old conversation"}}));
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: None,
            kind: AcpEventKind::Connected {
                capabilities: Box::new(
                    serde_json::from_value(
                        serde_json::json!({"loadSession":true, "sessionCapabilities":{"list":{}}}),
                    )
                    .unwrap(),
                ),
            },
        },
    );
    (state, rx)
}

#[test]
fn acp_history_replay_is_staged_until_success_and_empty_history_replaces_old() {
    let (mut state, _rx) = acp_history_fixture();
    state.load_session("history", "target".into()).unwrap();
    assert_eq!(
        state
            .session("history")
            .unwrap()
            .active_session_id
            .as_deref(),
        Some("old")
    );
    assert_eq!(
        state.session("history").unwrap().timeline[0]["content"]["text"],
        "old conversation"
    );
    for update in [
        serde_json::json!({"sessionUpdate":"user_message_chunk", "content":{"type":"text","text":"replay"}}),
        serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text","text":"answer"}}),
        serde_json::json!({"sessionUpdate":"tool_call", "toolCallId":"t", "title":"Read", "status":"completed"}),
        serde_json::json!({"sessionUpdate":"plan", "entries":[]}),
    ] {
        state.apply(
            "history",
            AcpEvent {
                generation: 11,
                session_id: Some("target".into()),
                kind: AcpEventKind::SessionUpdate {
                    update: serde_json::from_value(update).unwrap(),
                },
            },
        );
    }
    assert_eq!(
        state.session("history").unwrap().timeline[0]["content"]["text"],
        "old conversation"
    );
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: Some("target".into()),
            kind: AcpEventKind::SessionLoaded {
                response: serde_json::from_value(serde_json::json!({})).unwrap(),
            },
        },
    );
    assert_eq!(
        state
            .session("history")
            .unwrap()
            .active_session_id
            .as_deref(),
        Some("target")
    );
    assert_eq!(
        state.session("history").unwrap().timeline[0]["content"]["text"],
        "replay"
    );
    let kinds: Vec<_> = state
        .session("history")
        .unwrap()
        .timeline
        .iter()
        .filter_map(|value| value.get("sessionUpdate").and_then(|kind| kind.as_str()))
        .collect();
    assert_eq!(
        kinds,
        [
            "user_message_chunk",
            "agent_message_chunk",
            "tool_call",
            "plan"
        ]
    );
    state.load_session("history", "empty".into()).unwrap();
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: Some("empty".into()),
            kind: AcpEventKind::SessionLoaded {
                response: serde_json::from_value(serde_json::json!({})).unwrap(),
            },
        },
    );
    assert_eq!(
        state
            .session("history")
            .unwrap()
            .active_session_id
            .as_deref(),
        Some("empty")
    );
    assert!(state.session("history").unwrap().timeline.is_empty());
}

#[test]
fn acp_history_loading_blocks_prompt_new_and_second_load() {
    let (mut state, _rx) = acp_history_fixture();
    state.load_session("history", "target".into()).unwrap();
    assert!(state.prompt("history", "must not send".into()).is_err());
    assert!(state.new_session("history").is_err());
    assert!(state.load_session("history", "second".into()).is_err());
}

#[test]
fn acp_history_load_error_keeps_old_conversation_and_refreshes_list() {
    let (mut state, rx) = acp_history_fixture();
    state.load_session("history", "deleted".into()).unwrap();
    assert!(matches!(
        rx.try_recv().unwrap(),
        super::runtime::AcpCommand::Load(_)
    ));
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: Some("deleted".into()),
            kind: AcpEventKind::Error {
                message: "ACP session/load failed".into(),
            },
        },
    );
    let session = state.session("history").unwrap();
    assert_eq!(session.active_session_id.as_deref(), Some("old"));
    assert_eq!(session.timeline[0]["content"]["text"], "old conversation");
    assert!(session.error.is_some());
    assert!(matches!(
        rx.try_recv().unwrap(),
        super::runtime::AcpCommand::List(None)
    ));
}

#[test]
fn acp_history_requires_both_list_and_load_capabilities() {
    for caps in [
        serde_json::json!({}),
        serde_json::json!({"loadSession":true}),
        serde_json::json!({"sessionCapabilities":{"list":{}}}),
    ] {
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut state = AcpFeatureState::default();
        state.start("tab".into(), 1);
        state.session_mut("tab").unwrap().commands = Some(tx);
        state.apply(
            "tab",
            AcpEvent {
                generation: 1,
                session_id: None,
                kind: AcpEventKind::Connected {
                    capabilities: Box::new(serde_json::from_value(caps).unwrap()),
                },
            },
        );
        assert!(state.list_sessions("tab", None).is_err());
        assert!(state.load_session("tab", "old".into()).is_err());
        assert!(state.new_session("tab").is_ok());
    }
}

#[test]
fn acp_history_new_session_success_replaces_old_transcript() {
    let (mut state, _rx) = acp_history_fixture();
    state.new_session("history").unwrap();
    assert_eq!(state.session("history").unwrap().timeline.len(), 1);
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: Some("new".into()),
            kind: AcpEventKind::SessionCreated {
                response: serde_json::from_value(serde_json::json!({"sessionId":"new"})).unwrap(),
            },
        },
    );
    assert_eq!(
        state
            .session("history")
            .unwrap()
            .active_session_id
            .as_deref(),
        Some("new")
    );
    assert!(state.session("history").unwrap().timeline.is_empty());
}

#[test]
fn acp_history_pagination_preserves_opaque_cursor_and_appends_empty_pages() {
    let (mut state, rx) = acp_history_fixture();
    state.list_sessions("history", None).unwrap();
    assert!(matches!(
        rx.try_recv().unwrap(),
        super::runtime::AcpCommand::List(None)
    ));
    state.apply("history", AcpEvent { generation: 11, session_id: None, kind: AcpEventKind::SessionList { response: serde_json::from_value(serde_json::json!({"sessions":[{"sessionId":"s1", "cwd":"/"}], "nextCursor":"opaque/%2F?x=空"})).unwrap() } });
    state
        .list_sessions("history", Some("opaque/%2F?x=空".into()))
        .unwrap();
    assert!(
        matches!(rx.try_recv().unwrap(), super::runtime::AcpCommand::List(Some(cursor)) if cursor == "opaque/%2F?x=空")
    );
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: None,
            kind: AcpEventKind::SessionList {
                response: serde_json::from_value(serde_json::json!({"sessions":[]})).unwrap(),
            },
        },
    );
    let list = state
        .session("history")
        .unwrap()
        .session_list
        .as_ref()
        .unwrap();
    assert_eq!(list["sessions"].as_array().unwrap().len(), 1);
    assert!(list["nextCursor"].is_null());
}

#[test]
fn acp_history_closing_or_replacing_connection_discards_late_load_events() {
    let (mut state, _rx) = acp_history_fixture();
    state.load_session("history", "target".into()).unwrap();
    state.remove("history");
    state.start("history".into(), 12);
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: Some("target".into()),
            kind: AcpEventKind::SessionLoaded {
                response: serde_json::from_value(serde_json::json!({})).unwrap(),
            },
        },
    );
    assert_eq!(state.session("history").unwrap().active_session_id, None);
    assert!(state.session("history").unwrap().timeline.is_empty());
}

#[test]
fn acp_history_incomplete_replay_never_replaces_the_current_conversation() {
    let (mut state, _rx) = acp_history_fixture();
    state.load_session("history", "target".into()).unwrap();
    state.apply("history", AcpEvent { generation: 11, session_id: Some("target".into()), kind: AcpEventKind::SessionUpdate { update: serde_json::from_value(serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text","text":"partial"}})).unwrap() } });
    assert!(state.mark_stream_overflow("history"));
    assert!(state.load_session("history", "another".into()).is_err());
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: Some("target".into()),
            kind: AcpEventKind::SessionLoaded {
                response: serde_json::from_value(serde_json::json!({})).unwrap(),
            },
        },
    );
    let session = state.session("history").unwrap();
    assert_eq!(session.active_session_id.as_deref(), Some("old"));
    assert_eq!(session.timeline[0]["content"]["text"], "old conversation");
    assert!(session.error.is_some());
}

#[test]
fn acp_history_selector_respects_capabilities_and_paginates_from_ui() {
    use super::runtime::AcpCommand;
    use crate::features::NyaTermApp;
    use crate::features::test_support::app_with_visible_local_session;
    use crate::test_support::{TestAppCleanup, TestConfigDir};
    use gpui::{
        AppContext as _, Entity, IntoElement, ParentElement as _, Render, Styled as _,
        TestAppContext, div,
    };
    struct Host(Entity<NyaTermApp>);
    impl Render for Host {
        fn render(
            &mut self,
            _: &mut gpui::Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            div().size_full().child(
                self.0
                    .update(cx, |app, cx| app.acp_view("history-ui".into(), cx)),
            )
        }
    }
    let root = TestConfigDir::new("nyaterm-acp-history-ui");
    let mut cx = TestAppContext::single();
    cx.update(nyaterm_ui::init_components);
    let _cleanup = TestAppCleanup::new(&cx);
    let app = app_with_visible_local_session(&mut cx, root.path(), "other");
    let (tx, rx) = std::sync::mpsc::channel();
    cx.update_entity(&app, |app, cx| {
        app.sync_component_theme(cx);
        app.acp.start("history-ui".into(), 1);
        let session = app.acp.session_mut("history-ui").unwrap();
        session.commands = Some(tx);
        session.connected = true;
        session.active_session_id = Some("old".into());
        session.timeline.push(serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text","text":"old"}}));
    });
    let host = app.clone();
    let (_, window) = cx.add_window_view(move |_, _| Host(host));
    window.update(|window, cx| {
        window.activate_window();
        let _ = window.draw(cx);
    });
    window.run_until_parked();
    let new_button = window.debug_bounds("acp-new-session").unwrap();
    assert!(window.debug_bounds("acp-history-toggle").is_none());
    window.simulate_click(new_button.center(), gpui::Modifiers::default());
    assert!(matches!(rx.try_recv().unwrap(), AcpCommand::New));
    window.update(|_, cx| {
        let session = app.read(cx).acp.session("history-ui").unwrap();
        assert_eq!(session.active_session_id.as_deref(), Some("old"));
        assert_eq!(session.timeline.len(), 1);
        assert!(session.loading);
    });
    window.update(|window, cx| {
        app.update(cx, |app, cx| {
            app.acp.apply("history-ui", AcpEvent { generation: 1, session_id: Some("created".into()), kind: AcpEventKind::SessionCreated { response: serde_json::from_value(serde_json::json!({"sessionId":"created"})).unwrap() } });
            let session = app.acp.session_mut("history-ui").unwrap();
            assert!(session.timeline.is_empty());
            session.timeline.push(serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text","text":"old"}}));
            app.acp.apply("history-ui", AcpEvent { generation: 1, session_id: None, kind: AcpEventKind::Connected { capabilities: Box::new(serde_json::from_value(serde_json::json!({"loadSession":true,"sessionCapabilities":{"list":{}}})).unwrap()) } });
            cx.notify();
        });
        window.refresh(); let _ = window.draw(cx);
    });
    let button = window.debug_bounds("acp-history-toggle").unwrap();
    window.simulate_click(button.center(), gpui::Modifiers::default());
    assert!(matches!(rx.try_recv().unwrap(), AcpCommand::List(None)));
    window.update(|window, cx| {
        app.update(cx, |app, cx| {
            app.acp.apply("history-ui", AcpEvent { generation: 1, session_id: None, kind: AcpEventKind::SessionList { response: serde_json::from_value(serde_json::json!({"sessions":[{"sessionId":"first", "cwd":"/", "title":"First"}], "nextCursor":"opaque/%2F?x=空"})).unwrap() } });
            cx.notify();
        });
        window.refresh(); let _ = window.draw(cx);
    });
    assert!(window.debug_bounds("acp-history-entry-0").is_some());
    let more = window.debug_bounds("acp-history-more").unwrap();
    window.simulate_click(more.center(), gpui::Modifiers::default());
    assert!(
        matches!(rx.try_recv().unwrap(), AcpCommand::List(Some(cursor)) if cursor == "opaque/%2F?x=空")
    );
    window.update(|window, cx| {
        app.update(cx, |app, cx| {
            app.acp.apply("history-ui", AcpEvent { generation: 1, session_id: None, kind: AcpEventKind::SessionList { response: serde_json::from_value(serde_json::json!({"sessions":[{"sessionId":"second", "cwd":"/", "title":"Second"}]})).unwrap() } });
            cx.notify();
        });
        window.refresh(); let _ = window.draw(cx);
    });
    let second = window.debug_bounds("acp-history-entry-1").unwrap();
    window.simulate_click(second.center(), gpui::Modifiers::default());
    assert!(matches!(rx.try_recv().unwrap(), AcpCommand::Load(target) if target == "second"));
    window.update(|_, cx| {
        let session = app.read(cx).acp.session("history-ui").unwrap();
        assert_eq!(session.active_session_id.as_deref(), Some("created"));
        assert_eq!(session.timeline[0]["content"]["text"], "old");
        assert!(session.loading);
    });
    window.update(|window, cx| {
        app.update(cx, |app, cx| {
            let session = app.acp.session_mut("history-ui").unwrap();
            let rows = session.session_list.as_mut().unwrap()["sessions"].as_array_mut().unwrap();
            for index in 2..32 {
                rows.push(serde_json::json!({"sessionId": format!("session-{index}"), "title": format!("Session {index}"), "cwd":"/"}));
            }
            cx.notify();
        });
        window.refresh();
        let _ = window.draw(cx);
    });
    window.run_until_parked();
    let first = window.debug_bounds("acp-history-entry-0").unwrap();
    window.simulate_event(gpui::ScrollEvent {
        position: first.center(),
        delta: gpui::ScrollDelta::Pixels(gpui::point(gpui::px(0.), gpui::px(-150.))),
        modifiers: Default::default(),
        touch_phase: gpui::TouchPhase::Moved,
        source: gpui::ScrollSource::Pointer,
    });
    window.run_until_parked();
    window.update(|window, cx| {
        let _ = window.draw(cx);
    });
    let after = window.debug_bounds("acp-history-entry-0").unwrap();
    assert!(after.top() < first.top(), "history list should scroll");
}

#[test]
fn acp_history_sequential_switch_ignores_old_response_and_replay() {
    let (mut state, _rx) = acp_history_fixture();
    let load = |target: &str| AcpEvent {
        generation: 11,
        session_id: Some(target.into()),
        kind: AcpEventKind::SessionLoaded {
            response: serde_json::from_value(serde_json::json!({})).unwrap(),
        },
    };
    state.load_session("history", "first".into()).unwrap();
    state.apply("history", load("first"));
    state.load_session("history", "second".into()).unwrap();
    state.apply("history", load("first"));
    assert_eq!(
        state
            .session("history")
            .unwrap()
            .active_session_id
            .as_deref(),
        Some("first")
    );
    assert!(state.session("history").unwrap().loading);
    state.apply("history", AcpEvent { generation: 11, session_id: Some("second".into()), kind: AcpEventKind::SessionUpdate { update: serde_json::from_value(serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text","text":"second reply"}})).unwrap() } });
    state.apply("history", load("second"));
    state.apply("history", AcpEvent { generation: 11, session_id: Some("first".into()), kind: AcpEventKind::SessionUpdate { update: serde_json::from_value(serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text","text":"late first"}})).unwrap() } });
    assert_eq!(
        state
            .session("history")
            .unwrap()
            .active_session_id
            .as_deref(),
        Some("second")
    );
    assert_eq!(state.session("history").unwrap().timeline.len(), 1);
    assert_eq!(
        state.session("history").unwrap().timeline[0]["content"]["text"],
        "second reply"
    );
}

#[test]
fn acp_history_real_agent_deleted_target_preserves_conversation_and_refreshes() {
    use crate::features::test_support::app_with_visible_local_session;
    use crate::test_support::{TestAppCleanup, TestConfigDir};
    use gpui::{AppContext as _, TestAppContext};
    use nyaterm_core::SavedConnection;
    use std::time::{Duration, Instant};
    let root = TestConfigDir::new("nyaterm-acp-history-deleted");
    let working = tempfile::tempdir().unwrap();
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("nyaterm-agent-client/tests/fixtures/acp_agent.py");
    let connection: SavedConnection = serde_json::from_value(serde_json::json!({
        "id":"acp-history", "name":"ACP", "type":"acp",
        "command":format!("{} -u \"{}\"", if cfg!(windows) { "python" } else { "python3" }, script.display()),
        "working_dir":working.path().to_string_lossy()
    })).unwrap();
    let mut cx = TestAppContext::single();
    let _cleanup = TestAppCleanup::new(&cx);
    let app = app_with_visible_local_session(&mut cx, root.path(), "other");
    let id = cx.update_entity(&app, |app, cx| {
        assert!(app.continue_saved_connection_start(connection, Default::default(), cx));
        app.session.active_id().unwrap().to_owned()
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        cx.run_until_parked();
        if cx.update_entity(&app, |app, _| {
            app.acp.session(&id).unwrap().active_session_id.is_some()
        }) {
            break;
        }
        assert!(Instant::now() < deadline, "ACP agent did not start");
        std::thread::sleep(Duration::from_millis(10));
    }
    cx.update_entity(&app, |app, _| {
        let session = app.acp.session_mut(&id).unwrap();
        assert!(session.history_supported);
        session.timeline.push(serde_json::json!({"sessionUpdate":"user_message_chunk", "content":{"type":"text","text":"preserve me"}}));
        app.acp.load_session(&id, "deleted".into()).unwrap();
    });
    loop {
        cx.run_until_parked();
        let done = cx.update_entity(&app, |app, _| {
            let session = app.acp.session(&id).unwrap();
            !session.loading
                && !session.list_loading
                && session.session_list.is_some()
                && session.error.is_some()
        });
        if done {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "failed load did not refresh history"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    cx.update_entity(&app, |app, cx| {
        let session = app.acp.session(&id).unwrap();
        assert_eq!(session.active_session_id.as_deref(), Some("session-1"));
        assert_eq!(session.timeline[0]["content"]["text"], "preserve me");
        assert_eq!(
            session.session_list.as_ref().unwrap()["sessions"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        app.close_session(id.clone(), cx);
    });
}

#[test]
fn acp_history_switch_does_not_abandon_pending_permission() {
    let (mut state, _rx) = acp_history_fixture();
    state
        .session_mut("history")
        .unwrap()
        .pending_permissions
        .insert(7, serde_json::json!({"options":[{"optionId":"allow"}]}));
    assert!(state.load_session("history", "target".into()).is_err());
    assert!(state.new_session("history").is_err());
    let session = state.session("history").unwrap();
    assert_eq!(session.active_session_id.as_deref(), Some("old"));
    assert!(session.pending_permissions.contains_key(&7));
}

#[test]
fn acp_history_empty_first_page_keeps_new_session_available() {
    let (mut state, rx) = acp_history_fixture();
    state.list_sessions("history", None).unwrap();
    assert!(matches!(
        rx.try_recv().unwrap(),
        super::runtime::AcpCommand::List(None)
    ));
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: None,
            kind: AcpEventKind::SessionList {
                response: serde_json::from_value(serde_json::json!({"sessions":[]})).unwrap(),
            },
        },
    );
    let session = state.session("history").unwrap();
    assert!(!session.list_loading);
    assert!(
        session.session_list.as_ref().unwrap()["sessions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(session.session_list.as_ref().unwrap()["nextCursor"].is_null());
    assert!(state.new_session("history").is_ok());
}

#[test]
fn acp_late_permission_during_cancel_cannot_be_approved() {
    let (mut state, _rx) = acp_history_fixture();
    state.session_mut("history").unwrap().running = true;
    state.cancel("history").unwrap();
    state.apply("history", AcpEvent {
        generation: 11,
        session_id: Some("old".into()),
        kind: AcpEventKind::PermissionRequested {
            request_id: 42,
            request: serde_json::from_value(serde_json::json!({"sessionId":"old", "toolCall":{"toolCallId":"t"}, "options":[]})).unwrap(),
        },
    });
    assert!(
        state
            .session("history")
            .unwrap()
            .pending_permissions
            .is_empty()
    );
    assert!(state.respond_permission("history", 42, None).is_err());
}

#[test]
fn acp_structural_session_updates_survive_full_stream_buffer() {
    use super::runtime::AcpEventBuffer;
    let buffer = AcpEventBuffer::new(0);
    for value in [
        serde_json::json!({"sessionUpdate":"tool_call", "toolCallId":"t", "title":"Run", "status":"in_progress"}),
        serde_json::json!({"sessionUpdate":"tool_call_update", "toolCallId":"t", "status":"completed"}),
        serde_json::json!({"sessionUpdate":"plan", "entries":[]}),
    ] {
        buffer.push(AcpEvent {
            generation: 1,
            session_id: Some("s".into()),
            kind: AcpEventKind::SessionUpdate {
                update: serde_json::from_value(value).unwrap(),
            },
        });
    }
    let (events, overflowed) = buffer.drain();
    assert!(!overflowed);
    assert_eq!(events.len(), 3);
}

#[test]
fn acp_new_session_invalidates_cached_history() {
    let (mut state, _rx) = acp_history_fixture();
    state.session_mut("history").unwrap().session_list = Some(serde_json::json!({"sessions":[]}));
    state.new_session("history").unwrap();
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: Some("new".into()),
            kind: AcpEventKind::SessionCreated {
                response: serde_json::from_value(serde_json::json!({"sessionId":"new"})).unwrap(),
            },
        },
    );
    assert!(state.session("history").unwrap().session_list.is_none());
}

#[test]
fn acp_successful_load_invalidates_cached_history_for_reopen() {
    let (mut state, _rx) = acp_history_fixture();
    state.session_mut("history").unwrap().session_list = Some(serde_json::json!({"sessions":[]}));
    state.load_session("history", "target".into()).unwrap();
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: Some("target".into()),
            kind: AcpEventKind::SessionLoaded {
                response: serde_json::from_value(serde_json::json!({})).unwrap(),
            },
        },
    );
    assert!(state.session("history").unwrap().session_list.is_none());
}

#[test]
fn acp_large_replay_does_not_replace_current_conversation() {
    let (mut state, _rx) = acp_history_fixture();
    state.load_session("history", "target".into()).unwrap();
    state.apply("history", AcpEvent { generation: 11, session_id: Some("target".into()), kind: AcpEventKind::SessionUpdate { update: serde_json::from_value(serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text","text":"x".repeat(9 * 1024 * 1024)}})).unwrap() } });
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: Some("target".into()),
            kind: AcpEventKind::SessionLoaded {
                response: serde_json::from_value(serde_json::json!({})).unwrap(),
            },
        },
    );
    let session = state.session("history").unwrap();
    assert_eq!(session.active_session_id.as_deref(), Some("old"));
    assert_eq!(session.timeline[0]["content"]["text"], "old conversation");
    assert!(session.error.is_some());
}

#[test]
fn acp_large_live_update_is_bounded_and_reports_truncation() {
    let (mut state, _rx) = acp_history_fixture();
    state.apply("history", AcpEvent { generation: 11, session_id: Some("old".into()), kind: AcpEventKind::SessionUpdate { update: serde_json::from_value(serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text","text":"x".repeat(9 * 1024 * 1024)}})).unwrap() } });
    let session = state.session("history").unwrap();
    assert!(serde_json::to_vec(&session.timeline).unwrap().len() < 9 * 1024 * 1024);
    assert!(session.error.is_some());
}

#[test]
fn acp_concurrent_stream_publish_does_not_drop_under_lock_contention() {
    use super::runtime::AcpEventBuffer;
    use std::sync::{Arc, Barrier};
    let buffer = Arc::new(AcpEventBuffer::new(10000));
    let barrier = Arc::new(Barrier::new(9));
    let threads: Vec<_> = (0..8).map(|_| {
        let buffer = buffer.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..200 {
                buffer.push(AcpEvent { generation: 1, session_id: Some("s".into()), kind: AcpEventKind::SessionUpdate { update: serde_json::from_value(serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text", "text":"x"}})).unwrap() } });
            }
        })
    }).collect();
    barrier.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    let (events, overflowed) = buffer.drain();
    assert!(!overflowed);
    assert_eq!(events.len(), 1600);
}

#[test]
fn acp_unknown_update_is_a_safe_generic_timeline_item() {
    use super::projection::{TimelineItem, project_timeline};
    let (mut state, _rx) = acp_history_fixture();
    state.apply(
        "history",
        AcpEvent {
            generation: 11,
            session_id: Some("old".into()),
            kind: AcpEventKind::UnknownSessionUpdate {
                kind: "future_update".into(),
            },
        },
    );
    let timeline = project_timeline(&state.session("history").unwrap().timeline);
    assert!(
        matches!(timeline.last(), Some(TimelineItem::Unknown { kind }) if kind == "future_update")
    );
}

#[test]
fn acp_live_truncation_remains_visible_after_the_next_prompt() {
    let (mut state, _rx) = acp_history_fixture();
    state.apply("history", AcpEvent { generation: 11, session_id: Some("old".into()), kind: AcpEventKind::SessionUpdate { update: serde_json::from_value(serde_json::json!({"sessionUpdate":"agent_message_chunk", "content":{"type":"text", "text":"x".repeat(9 * 1024 * 1024)}})).unwrap() } });
    state.prompt("history", "continue".into()).unwrap();
    let session = state.session("history").unwrap();
    assert!(
        session
            .error
            .as_deref()
            .is_some_and(|error| error.contains("truncated"))
            || session
                .timeline
                .iter()
                .any(|item| item["sessionUpdate"] == "timeline_truncated")
    );
}
