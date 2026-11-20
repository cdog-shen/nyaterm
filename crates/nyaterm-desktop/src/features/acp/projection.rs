use std::collections::HashMap;

use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum MessageRole {
    User,
    Agent,
    Thought,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum TimelineItem {
    Message {
        role: MessageRole,
        id: Option<String>,
        text: String,
    },
    Tool {
        id: String,
        title: String,
        status: String,
        details: Vec<(String, String)>,
    },
    Plan {
        entries: Vec<String>,
    },
    Unknown {
        kind: String,
    },
}

pub(super) fn project_timeline(values: &[Value]) -> Vec<TimelineItem> {
    let mut result: Vec<TimelineItem> = Vec::new();
    let mut messages = HashMap::new();
    let mut tools = HashMap::new();
    let mut previous_unidentified = None;
    for value in values {
        let kind = value
            .get("sessionUpdate")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let role = match kind {
            "user_message_chunk" => Some(MessageRole::User),
            "agent_message_chunk" => Some(MessageRole::Agent),
            "agent_thought_chunk" => Some(MessageRole::Thought),
            _ => None,
        };
        if let Some(role) = role {
            let text = value
                .pointer("/content/text")
                .and_then(Value::as_str)
                .unwrap_or("");
            let id = value
                .get("messageId")
                .or_else(|| value.pointer("/_meta/messageId"))
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_owned);
            let index = if let Some(id) = &id {
                previous_unidentified = None;
                messages.get(&(role, id.clone())).copied()
            } else {
                previous_unidentified
                    .and_then(|(previous_role, index)| (previous_role == role).then_some(index))
            };
            if let Some(index) = index {
                if let TimelineItem::Message { text: existing, .. } = &mut result[index] {
                    existing.push_str(text);
                }
            } else {
                if let Some(id) = &id {
                    messages.insert((role, id.clone()), result.len());
                } else {
                    previous_unidentified = Some((role, result.len()));
                }
                result.push(TimelineItem::Message {
                    role,
                    id,
                    text: text.to_string(),
                });
            }
            continue;
        }
        previous_unidentified = None;
        match kind {
            "tool_call" | "tool_call_update" => {
                let id = value
                    .get("toolCallId")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let index = if id.is_empty() {
                    None
                } else {
                    tools.get(&id).copied()
                };
                let details = tool_details(value);
                if let Some(index) = index {
                    if let TimelineItem::Tool {
                        title,
                        status,
                        details: existing,
                        ..
                    } = &mut result[index]
                    {
                        if let Some(value) = value.get("title").and_then(Value::as_str) {
                            *title = value.to_string();
                        }
                        if let Some(value) = value.get("status").and_then(Value::as_str) {
                            *status = value.to_string();
                        }
                        for (key, value) in details {
                            if let Some((_, old)) = existing.iter_mut().find(|(old, _)| old == &key)
                            {
                                *old = value;
                            } else {
                                existing.push((key, value));
                            }
                        }
                    }
                } else {
                    if !id.is_empty() {
                        tools.insert(id.clone(), result.len());
                    }
                    result.push(TimelineItem::Tool {
                        id,
                        title: value
                            .get("title")
                            .and_then(Value::as_str)
                            .unwrap_or("Tool")
                            .to_string(),
                        status: value
                            .get("status")
                            .and_then(Value::as_str)
                            .unwrap_or("pending")
                            .to_string(),
                        details,
                    });
                }
            }
            "plan" => {
                let entries = value
                    .get("entries")
                    .and_then(Value::as_array)
                    .map(|entries| {
                        entries
                            .iter()
                            .filter_map(|entry| {
                                let text = entry.get("content").and_then(Value::as_str)?;
                                let status =
                                    entry.get("status").and_then(Value::as_str).unwrap_or("");
                                Some(if status.is_empty() {
                                    text.to_string()
                                } else {
                                    format!("{status}: {text}")
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                result.push(TimelineItem::Plan { entries });
            }
            _ => result.push(TimelineItem::Unknown {
                kind: kind
                    .chars()
                    .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '-')
                    .take(80)
                    .collect(),
            }),
        }
    }
    result
}

pub(super) fn tool_details(value: &Value) -> Vec<(String, String)> {
    let mut details = Vec::new();
    for key in ["rawInput", "rawOutput", "diff"] {
        if let Some(data) = value.get(key).filter(|data| !data.is_null()) {
            let text = data
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| serde_json::to_string_pretty(data).unwrap_or_default());
            details.push((key.to_string(), text));
        }
    }
    if let Some(locations) = value.get("locations").and_then(Value::as_array) {
        for (index, location) in locations.iter().enumerate() {
            if let Some(path) = location.get("path").and_then(Value::as_str) {
                let text = match location.get("line").and_then(Value::as_u64) {
                    Some(line) => format!("{path}:{line}"),
                    None => path.to_string(),
                };
                details.push((format!("path {index}"), text));
            }
        }
    }
    if let Some(contents) = value.get("content").and_then(Value::as_array) {
        for (index, content) in contents.iter().enumerate() {
            if content.get("type").and_then(Value::as_str) == Some("content") {
                if let Some(text) = content.pointer("/content/text").and_then(Value::as_str) {
                    details.push((format!("content {index}"), text.to_string()));
                }
            } else if content.get("type").and_then(Value::as_str) == Some("diff") {
                let old = content.get("oldText").and_then(Value::as_str).unwrap_or("");
                let new = content.get("newText").and_then(Value::as_str).unwrap_or("");
                let path = content.get("path").and_then(Value::as_str).unwrap_or("");
                details.push((
                    format!("diff {path} {index}"),
                    format!("--- old\n{old}\n+++ new\n{new}"),
                ));
            }
        }
    }
    details
}
