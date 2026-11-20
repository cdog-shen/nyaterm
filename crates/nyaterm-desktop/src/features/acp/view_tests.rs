use gpui::{
    AppContext as _, Entity, IntoElement, ParentElement as _, Render, Styled as _, TestAppContext,
    div,
};
use nyaterm_ui::NyaInputState;

use super::projection::{TimelineItem, project_timeline};
use super::runtime::AcpCommand;
use crate::features::NyaTermApp;
use crate::features::test_support::app_with_visible_local_session;
use crate::test_support::{TestAppCleanup, TestConfigDir};

struct Host(Entity<NyaTermApp>);

impl Render for Host {
    fn render(&mut self, _: &mut gpui::Window, cx: &mut gpui::Context<Self>) -> impl IntoElement {
        div().size_full().child(
            self.0
                .update(cx, |app, cx| app.acp_view("fixture".into(), cx)),
        )
    }
}

#[test]
fn tool_card_projects_standard_text_and_locations() {
    let items = project_timeline(&[serde_json::json!({
        "sessionUpdate":"tool_call", "toolCallId":"t", "title":"Read",
        "locations":[{"path":"/tmp/file.rs", "line":7}],
        "content":[{"type":"content", "content":{"type":"text", "text":"file content"}}]
    })]);
    let TimelineItem::Tool { details, .. } = &items[0] else {
        panic!("missing tool")
    };
    assert!(
        details
            .iter()
            .any(|(_, value)| value.contains("/tmp/file.rs"))
    );
    assert!(
        details
            .iter()
            .any(|(_, value)| value.contains("file content"))
    );
}

#[test]
fn permission_only_tool_details_are_visible_before_decision() {
    let root = TestConfigDir::new("nyaterm-acp-permission-details");
    let mut cx = TestAppContext::single();
    cx.update(nyaterm_ui::init_components);
    let _cleanup = TestAppCleanup::new(&cx);
    let app = app_with_visible_local_session(&mut cx, root.path(), "other");
    cx.update_entity(&app, |app, cx| {
        app.sync_component_theme(cx);
        app.acp.start("fixture".into(), 1);
        let session = app.acp.session_mut("fixture").unwrap();
        session.connected = true;
        session.active_session_id = Some("session".into());
        session.pending_permissions.insert(5, serde_json::json!({
            "toolCall":{"toolCallId":"permission-only", "title":"Edit", "rawInput":{"command":"dangerous"}, "rawOutput":"result", "locations":[{"path":"/tmp/file.rs"}], "content":[{"type":"content", "content":{"type":"text", "text":"explanation"}}, {"type":"diff", "path":"/tmp/file.rs", "oldText":"before", "newText":"after"}]},
            "options":[{"optionId":"deny", "name":"Deny"}]
        }));
    });
    let host = app.clone();
    let (_, window) = cx.add_window_view(move |_, _| Host(host));
    window.update(|window, cx| {
        window.activate_window();
        let _ = window.draw(cx);
    });
    window.run_until_parked();
    for selector in [
        "acp-permission-detail-5-0",
        "acp-permission-detail-5-1",
        "acp-permission-detail-5-2",
        "acp-permission-detail-5-3",
        "acp-permission-detail-5-4",
    ] {
        assert!(
            window.debug_bounds(selector).is_some(),
            "permission detail visible"
        );
    }
    window.update(|_, cx| {
        let session = app.read(cx).acp.session("fixture").unwrap();
        let text = session
            .code_views
            .values()
            .map(|(text, _)| text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        for expected in [
            "dangerous",
            "result",
            "/tmp/file.rs",
            "explanation",
            "before",
            "after",
        ] {
            assert!(text.contains(expected), "missing {expected}: {text}");
        }
    });
}

#[test]
fn history_reopen_refreshes_cached_first_page() {
    let root = TestConfigDir::new("nyaterm-acp-history-reopen");
    let mut cx = TestAppContext::single();
    cx.update(nyaterm_ui::init_components);
    let _cleanup = TestAppCleanup::new(&cx);
    let app = app_with_visible_local_session(&mut cx, root.path(), "other");
    let (tx, rx) = std::sync::mpsc::channel();
    cx.update_entity(&app, |app, cx| {
        app.sync_component_theme(cx);
        app.acp.start("fixture".into(), 1);
        let session = app.acp.session_mut("fixture").unwrap();
        session.commands = Some(tx);
        session.connected = true;
        session.history_supported = true;
        session.active_session_id = Some("session".into());
        session.session_list = Some(serde_json::json!({"sessions":[]}));
    });
    let host = app.clone();
    let (_, window) = cx.add_window_view(move |_, _| Host(host));
    window.update(|window, cx| {
        window.activate_window();
        let _ = window.draw(cx);
    });
    let button = window.debug_bounds("acp-history-toggle").unwrap();
    window.simulate_click(button.center(), gpui::Modifiers::default());
    assert!(matches!(rx.try_recv(), Ok(AcpCommand::List(None))));
    window.update(|window, cx| {
        app.update(cx, |app, cx| {
            app.acp.apply(
                "fixture",
                nyaterm_agent_client::acp::AcpEvent {
                    generation: 1,
                    session_id: None,
                    kind: nyaterm_agent_client::acp::AcpEventKind::SessionList {
                        response: serde_json::from_value(serde_json::json!({"sessions":[]}))
                            .unwrap(),
                    },
                },
            );
            cx.notify();
        });
        window.refresh();
        let _ = window.draw(cx);
    });
    window.simulate_click(button.center(), gpui::Modifiers::default());
    window.update(|window, cx| {
        window.refresh();
        let _ = window.draw(cx);
    });
    window.simulate_click(button.center(), gpui::Modifiers::default());
    assert!(matches!(rx.try_recv(), Ok(AcpCommand::List(None))));
}

#[test]
fn connected_without_session_can_retry_new_but_cannot_prompt() {
    let root = TestConfigDir::new("nyaterm-acp-new-retry");
    let mut cx = TestAppContext::single();
    cx.update(nyaterm_ui::init_components);
    let _cleanup = TestAppCleanup::new(&cx);
    let app = app_with_visible_local_session(&mut cx, root.path(), "other");
    let (tx, rx) = std::sync::mpsc::channel();
    cx.update_entity(&app, |app, cx| {
        app.sync_component_theme(cx);
        app.acp.start("fixture".into(), 1);
        let input = cx.new(|cx| NyaInputState::new(cx, "prompt"));
        let session = app.acp.session_mut("fixture").unwrap();
        session.input = Some(input);
        session.commands = Some(tx);
        session.connected = true;
        session.error = Some("session/new failed".into());
    });
    let host = app.clone();
    let (_, window) = cx.add_window_view(move |_, _| Host(host));
    window.update(|window, cx| {
        window.activate_window();
        let _ = window.draw(cx);
    });
    window.update(|_, cx| app.update(cx, |app, cx| app.submit_acp_prompt("fixture", cx)));
    assert!(rx.try_recv().is_err());
    let button = window.debug_bounds("acp-new-session").unwrap();
    window.simulate_click(button.center(), gpui::Modifiers::default());
    assert!(matches!(rx.try_recv(), Ok(AcpCommand::New)));
}

#[test]
fn explicitly_opening_acp_focuses_composer_after_menu_focus_restore() {
    let root = TestConfigDir::new("nyaterm-acp-open-focus");
    let working = tempfile::tempdir().unwrap();
    let connection: nyaterm_core::SavedConnection = serde_json::from_value(serde_json::json!({
        "id":"acp-focus", "name":"ACP", "type":"acp", "command":"nyaterm-missing-acp-focus", "working_dir":working.path().to_string_lossy()
    })).unwrap();
    let mut cx = TestAppContext::single();
    cx.update(nyaterm_ui::init_components);
    let _cleanup = TestAppCleanup::new(&cx);
    let app = app_with_visible_local_session(&mut cx, root.path(), "other");
    let host = app.clone();
    let (_, window) = cx.add_window_view(move |_, _| Host(host));
    let id = window.update(|window, cx| {
        window.activate_window();
        app.update(cx, |app, cx| {
            app.sync_component_theme(cx);
            app.start_saved_connection(connection, window, cx);
            let id = app.session.active_id().unwrap().to_owned();
            window.focus(app.terminal.input_focus(), cx);
            id
        })
    });
    window.run_until_parked();
    window.update(|window, cx| {
        let input = app
            .read(cx)
            .acp
            .session(&id)
            .unwrap()
            .input
            .as_ref()
            .unwrap();
        assert!(
            input.read(cx).component_focus_handle(cx).is_focused(window)
                || input.read(cx).focus_handle().is_focused(window)
        );
        assert!(!app.read(cx).terminal.input_focus().is_focused(window));
    });
}
