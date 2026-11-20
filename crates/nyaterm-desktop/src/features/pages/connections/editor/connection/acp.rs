use gpui::{
    Context, div,
    prelude::{InteractiveElement, ParentElement, Styled},
};
use rust_i18n::t;

use super::super::super::list::editor_field;
use super::ConnectionEditorSectionContext;
use crate::features::NyaTermApp;
use crate::models::ConnectionEditorField;

pub(super) fn connection_editor_acp_section(
    section: ConnectionEditorSectionContext<'_>,
    cx: &mut Context<NyaTermApp>,
) -> gpui::Div {
    div()
        .debug_selector(|| "connection-editor-acp-section".to_string())
        .flex()
        .flex_col()
        .gap_3()
        .child(editor_field(
            section.palette,
            t!("dialog.acpCommand"),
            ConnectionEditorField::ShellPath,
            section.fields,
            cx,
        ))
        .child(editor_field(
            section.palette,
            t!("dialog.workingDir"),
            ConnectionEditorField::WorkingDir,
            section.fields,
            cx,
        ))
}
