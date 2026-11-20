use std::sync::Arc;

use gpui::{AnyElement, Context, IntoElement, Window, div, prelude::*, px, rgb};
use nyaterm_ui::chat::NyaDisclosure;
use nyaterm_ui::text::{NyaSelectableCode, NyaTextFormat, NyaTextState, NyaTextStyle};
use nyaterm_ui::{NyaButton, NyaButtonVariant, NyaInputShell, NyaMarkdown, NyaScrollable as _};
use rust_i18n::t;

use super::projection::{MessageRole, TimelineItem, project_timeline, tool_details};
use crate::features::NyaTermApp;

impl NyaTermApp {
    pub(in crate::features) fn focus_acp_composer(
        &mut self,
        id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(input) = self
            .acp
            .session(id)
            .and_then(|session| session.input.clone())
            && !input.read(cx).component_focus_handle(cx).is_focused(window)
            && !input.read(cx).focus_handle().is_focused(window)
        {
            input.update(cx, |input, cx| input.focus_with_cursor_at_end(window, cx));
        }
    }

    pub(in crate::features) fn submit_acp_prompt(&mut self, id: &str, cx: &mut Context<Self>) {
        let Some(input) = self
            .acp
            .session(id)
            .and_then(|session| session.input.clone())
        else {
            return;
        };
        if !self.acp.session(id).is_some_and(|session| {
            session.connected
                && !session.closed
                && session.active_session_id.is_some()
                && !session.loading
                && !session.running
        }) {
            return;
        }
        let text = input.read(cx).value(cx);
        if text.trim().is_empty() {
            return;
        }
        match self.acp.prompt(id, text) {
            Ok(()) => input.update(cx, |input, cx| input.clear(cx)),
            Err(error) => {
                if let Some(session) = self.acp.session_mut(id) {
                    session.error = Some(error);
                }
            }
        }
        cx.notify();
    }

    pub(in crate::features) fn acp_view(
        &mut self,
        id: String,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(session) = self.acp.session(&id) else {
            return div().into_any_element();
        };
        let timeline = project_timeline(&session.timeline);
        let mut permissions = session
            .pending_permissions
            .iter()
            .map(|(id, request)| (*id, request.clone()))
            .collect::<Vec<_>>();
        permissions.sort_by_key(|(id, _)| *id);
        let input = session.input.clone();
        let connected = session.connected && !session.closed;
        let ready = connected && session.active_session_id.is_some();
        let loading = session.loading;
        let running = session.running;
        let cancelling = session.cancelling;
        let closed = session.closed;
        let error = session.error.clone();
        let history_supported = session.history_supported;
        let history_open = session.history_open;
        let list_loading = session.list_loading;
        let history = session.session_list.clone();
        let expanded = session.expanded.clone();
        let palette = self.theme_palette();
        let status = if closed {
            t!("acpChat.closed")
        } else if !connected {
            t!("acpChat.connecting")
        } else if !ready && !loading && error.is_some() {
            t!("acpChat.newSession")
        } else if !ready && !loading {
            t!("acpChat.connecting")
        } else if loading {
            t!("acpChat.loading")
        } else if cancelling {
            t!("acpChat.cancelling")
        } else if running {
            t!("acpChat.running")
        } else {
            t!("acpChat.ready")
        };
        let busy = !connected || loading || running || list_loading || !permissions.is_empty();
        let new_target = id.clone();
        let mut header = div()
            .flex()
            .items_center()
            .gap_2()
            .child(div().flex_1().child(format!("ACP · {status}")))
            .child(
                div()
                    .debug_selector(|| "acp-new-session".to_string())
                    .child(
                        NyaButton::new(format!("acp-new-{id}"), t!("acpChat.newSession"))
                            .small()
                            .disabled(busy)
                            .on_click(cx.listener(move |app, _, _, cx| {
                                if let Err(error) = app.acp.new_session(&new_target)
                                    && let Some(session) = app.acp.session_mut(&new_target)
                                {
                                    session.error = Some(error);
                                }
                                cx.notify();
                            })),
                    ),
            );
        if history_supported {
            let target = id.clone();
            header = header.child(
                div()
                    .debug_selector(|| "acp-history-toggle".to_string())
                    .child(
                        NyaButton::new(format!("acp-history-{id}"), t!("acpChat.history"))
                            .small()
                            .disabled(!connected || loading || running)
                            .on_click(cx.listener(move |app, _, _, cx| {
                                let should_load =
                                    if let Some(session) = app.acp.session_mut(&target) {
                                        session.history_open = !session.history_open;
                                        session.history_open && !session.list_loading
                                    } else {
                                        false
                                    };
                                if should_load
                                    && let Err(error) = app.acp.list_sessions(&target, None)
                                    && let Some(session) = app.acp.session_mut(&target)
                                {
                                    session.error = Some(error);
                                }
                                cx.notify();
                            })),
                    ),
            );
        }
        let mut history_panel = div().w_full().flex().flex_col().gap_1().px_4().py_2();
        if history_open && history_supported {
            if list_loading {
                history_panel =
                    history_panel.child(div().text_sm().child(t!("acpChat.historyLoading")));
            }
            let sessions = history
                .as_ref()
                .and_then(|list| list.get("sessions"))
                .and_then(serde_json::Value::as_array);
            if let Some(sessions) = sessions {
                if sessions.is_empty() && !list_loading {
                    history_panel =
                        history_panel.child(div().text_sm().child(t!("acpChat.historyEmpty")));
                }
                for (index, item) in sessions.iter().enumerate() {
                    let Some(session_id) =
                        item.get("sessionId").and_then(serde_json::Value::as_str)
                    else {
                        continue;
                    };
                    let name = item
                        .get("title")
                        .and_then(serde_json::Value::as_str)
                        .filter(|title| !title.is_empty())
                        .unwrap_or(session_id)
                        .to_string();
                    let session_id = session_id.to_string();
                    let target = id.clone();
                    history_panel = history_panel.child(
                        div()
                            .debug_selector(move || format!("acp-history-entry-{index}"))
                            .child(
                                NyaButton::new(format!("acp-history-entry-{id}-{index}"), name)
                                    .small()
                                    .label_left()
                                    .full_width()
                                    .disabled(busy)
                                    .on_click(cx.listener(move |app, _, _, cx| {
                                        if let Err(error) =
                                            app.acp.load_session(&target, session_id.clone())
                                            && let Some(session) = app.acp.session_mut(&target)
                                        {
                                            session.error = Some(error);
                                        }
                                        cx.notify();
                                    })),
                            ),
                    );
                }
            }
            if let Some(cursor) = history
                .as_ref()
                .and_then(|list| list.get("nextCursor"))
                .and_then(serde_json::Value::as_str)
            {
                let cursor = cursor.to_string();
                let target = id.clone();
                history_panel = history_panel.child(
                    div()
                        .debug_selector(|| "acp-history-more".to_string())
                        .child(
                            NyaButton::new(
                                format!("acp-history-more-{id}"),
                                t!("acpChat.historyMore"),
                            )
                            .small()
                            .disabled(busy)
                            .on_click(cx.listener(
                                move |app, _, _, cx| {
                                    if let Err(error) =
                                        app.acp.list_sessions(&target, Some(cursor.clone()))
                                        && let Some(session) = app.acp.session_mut(&target)
                                    {
                                        session.error = Some(error);
                                    }
                                    cx.notify();
                                },
                            )),
                        ),
                );
            }
        }
        let mut messages = div()
            .id(format!("acp-messages-{id}"))
            .w_full()
            .flex()
            .flex_col()
            .gap_3()
            .p_4();
        for (index, item) in timeline.into_iter().enumerate() {
            let content = match item {
                TimelineItem::Message { role, text, .. } => {
                    let label = match role {
                        MessageRole::User => t!("acpChat.user"),
                        MessageRole::Agent => t!("acpChat.agent"),
                        MessageRole::Thought => t!("acpChat.thought"),
                    };
                    div()
                        .debug_selector(move || format!("acp-message-{index}"))
                        .w_full()
                        .min_w_0()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .text_xs()
                                .text_color(rgb(palette.text_muted))
                                .child(label),
                        )
                        .child(NyaMarkdown::new(format!("acp-message-{id}-{index}"), text))
                        .into_any_element()
                }
                TimelineItem::Tool {
                    id: tool_id,
                    title,
                    status,
                    details,
                } => {
                    let mut card = div()
                        .debug_selector(move || format!("acp-tool-{index}"))
                        .w_full()
                        .min_w_0()
                        .p_3()
                        .rounded_md()
                        .border_1()
                        .border_color(rgb(palette.border))
                        .bg(rgb(palette.surface))
                        .flex()
                        .flex_col()
                        .gap_2()
                        .child(div().text_sm().child(format!("{title} · {status}")));
                    for (detail_index, (label, text)) in details.into_iter().enumerate() {
                        let key = format!("{tool_id}:{detail_index}:{label}");
                        let open = expanded.contains(&key);
                        let target = id.clone();
                        let toggle = key.clone();
                        card = card.child(
                            div()
                                .debug_selector(move || {
                                    format!("acp-detail-{index}-{detail_index}")
                                })
                                .child(
                                    NyaDisclosure::new(
                                        format!("acp-detail-{id}-{key}"),
                                        label,
                                        open,
                                    )
                                    .on_click(cx.listener(
                                        move |app, _, _, cx| {
                                            if let Some(session) = app.acp.session_mut(&target)
                                                && !session.expanded.remove(&toggle)
                                            {
                                                session.expanded.insert(toggle.clone());
                                            }
                                            cx.notify();
                                        },
                                    )),
                                ),
                        );
                        if open && let Some(session) = self.acp.session_mut(&id) {
                            let entry =
                                session.code_views.entry(key.clone()).or_insert_with(|| {
                                    (
                                        text.clone(),
                                        NyaTextState::new(NyaTextFormat::Code, &text, cx),
                                    )
                                });
                            if entry.0 != text {
                                entry.1.set_text(&text, cx);
                                entry.0 = text;
                            }
                            card = card.child(
                                div()
                                    .debug_selector(move || {
                                        format!("acp-code-{index}-{detail_index}")
                                    })
                                    .child(NyaSelectableCode::from_state(
                                        &entry.1,
                                        NyaTextStyle::transcript(palette, false),
                                        Arc::from([]),
                                    )),
                            );
                        }
                    }
                    card.into_any_element()
                }
                TimelineItem::Plan { entries } => {
                    let mut card = div()
                        .debug_selector(move || format!("acp-plan-{index}"))
                        .w_full()
                        .p_3()
                        .rounded_md()
                        .border_1()
                        .border_color(rgb(palette.border))
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(t!("acpChat.plan"));
                    for entry in entries {
                        card = card.child(div().text_sm().child(entry));
                    }
                    card.into_any_element()
                }
                TimelineItem::Unknown { kind } => div()
                    .text_xs()
                    .text_color(rgb(palette.text_muted))
                    .child(format!("{}: {kind}", t!("acpChat.unknown")))
                    .into_any_element(),
            };
            messages = messages.child(content);
        }
        for (request_id, request) in permissions {
            let mut card = div()
                .debug_selector(move || format!("acp-permission-{request_id}"))
                .id(format!("acp-permission-{id}-{request_id}"))
                .w_full()
                .p_3()
                .rounded_md()
                .border_1()
                .border_color(rgb(palette.warning))
                .flex()
                .flex_col()
                .gap_2()
                .child(t!("acpChat.permission"));
            if let Some(title) = request
                .pointer("/toolCall/title")
                .and_then(serde_json::Value::as_str)
            {
                card = card.child(title.to_string());
            }
            if let Some(tool) = request.get("toolCall") {
                for (detail_index, (label, text)) in tool_details(tool).into_iter().enumerate() {
                    if let Some(session) = self.acp.session_mut(&id) {
                        let key = format!("permission:{request_id}:{detail_index}:{label}");
                        let entry = session.code_views.entry(key).or_insert_with(|| {
                            (
                                text.clone(),
                                NyaTextState::new(NyaTextFormat::Code, &text, cx),
                            )
                        });
                        if entry.0 != text {
                            entry.1.set_text(&text, cx);
                            entry.0 = text;
                        }
                        card = card.child(
                            div()
                                .debug_selector(move || {
                                    format!("acp-permission-detail-{request_id}-{detail_index}")
                                })
                                .min_w_0()
                                .flex()
                                .flex_col()
                                .child(div().text_xs().child(label))
                                .child(NyaSelectableCode::from_state(
                                    &entry.1,
                                    NyaTextStyle::transcript(palette, false),
                                    Arc::from([]),
                                )),
                        );
                    }
                }
            }
            let mut actions = div().flex().flex_wrap().gap_2();
            if let Some(options) = request.get("options").and_then(serde_json::Value::as_array) {
                for (index, option) in options.iter().enumerate() {
                    let (Some(option_id), Some(name)) = (
                        option.get("optionId").and_then(serde_json::Value::as_str),
                        option.get("name").and_then(serde_json::Value::as_str),
                    ) else {
                        continue;
                    };
                    let option_id = option_id.to_string();
                    let target = id.clone();
                    actions = actions.child(
                        div()
                            .debug_selector(move || format!("acp-option-{request_id}-{index}"))
                            .child(
                                NyaButton::new(
                                    format!("acp-option-{id}-{request_id}-{index}"),
                                    name.to_string(),
                                )
                                .small()
                                .on_click(cx.listener(
                                    move |app, _, _, cx| {
                                        if let Err(error) = app.acp.respond_permission(
                                            &target,
                                            request_id,
                                            Some(option_id.clone()),
                                        ) && let Some(session) = app.acp.session_mut(&target)
                                        {
                                            session.error = Some(error);
                                        }
                                        cx.notify();
                                    },
                                )),
                            ),
                    );
                }
            }
            messages = messages.child(card.child(actions));
        }
        let target = id.clone();
        let send = NyaButton::new(format!("acp-send-{id}"), t!("acpChat.send"))
            .variant(NyaButtonVariant::Primary)
            .disabled(!ready || loading || running)
            .on_click(cx.listener(move |app, _, _, cx| app.submit_acp_prompt(&target, cx)));
        let target = id.clone();
        let stop = NyaButton::new(format!("acp-stop-{id}"), t!("acpChat.stop"))
            .disabled(!running || cancelling)
            .on_click(cx.listener(move |app, _, _, cx| {
                if let Err(error) = app.acp.cancel(&target)
                    && let Some(session) = app.acp.session_mut(&target)
                {
                    session.error = Some(error);
                }
                cx.notify();
            }));
        let mut composer = div()
            .w_full()
            .flex()
            .flex_col()
            .gap_2()
            .p_3()
            .border_t_1()
            .border_color(rgb(palette.border));
        if let Some(input) = input {
            let app = cx.weak_entity();
            let target = id.clone();
            composer = composer.child(
                div()
                    .debug_selector(|| "acp-composer-input".to_string())
                    .w_full()
                    .h(px(96.))
                    .child(
                        NyaInputShell::new(format!("acp-input-{id}"), &input)
                            .multi_line()
                            .fill_height()
                            .on_secondary_enter(move |_, cx| {
                                let _ =
                                    app.update(cx, |app, cx| app.submit_acp_prompt(&target, cx));
                            }),
                    ),
            );
        }
        composer = composer.child(div().flex().gap_2().child(send).child(stop));
        div()
            .debug_selector(|| "acp-page".to_string())
            .size_full()
            .min_h_0()
            .min_w_0()
            .flex()
            .flex_col()
            .bg(rgb(palette.bg))
            .text_color(rgb(palette.text))
            .child(
                div()
                    .flex_none()
                    .px_4()
                    .py_2()
                    .border_b_1()
                    .border_color(rgb(palette.border))
                    .child(header),
            )
            .when(history_open && history_supported, |view| {
                view.child(
                    div()
                        .w_full()
                        .max_h(px(220.))
                        .overflow_y_scrollbar()
                        .child(history_panel),
                )
            })
            .when_some(error, |view, error| {
                view.child(
                    div()
                        .px_4()
                        .py_2()
                        .text_color(rgb(palette.danger))
                        .child(error),
                )
            })
            .child(
                div()
                    .debug_selector(|| "acp-transcript-viewport".to_string())
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .child(div().size_full().overflow_y_scrollbar().child(messages)),
            )
            .child(composer)
            .into_any_element()
    }
}
