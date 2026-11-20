use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::pin::Pin;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use super::{AcpEventKind, AcpEventSink};
use agent_client_protocol::Lines;
use futures::{Sink, Stream, StreamExt, channel::mpsc, sink, task::AtomicWaker};

use crate::external_process::{ExternalAgentChild, spawn_reader};

pub(super) type LineSink = Pin<Box<dyn Sink<String, Error = io::Error> + Send>>;
pub(super) type LineStream = Pin<Box<dyn Stream<Item = io::Result<String>> + Send>>;
pub(super) type StderrTail = Arc<Mutex<VecDeque<u8>>>;
pub(super) type TransportError = Arc<Mutex<Option<String>>>;
const STDERR_LIMIT: usize = 8192;
const FRAME_LIMIT: usize = 1024 * 1024;
const LINE_QUEUE_LIMIT: usize = 128;

pub(super) fn spawn(
    command: &str,
    cwd: &std::path::Path,
    event_sink: AcpEventSink,
) -> io::Result<(
    ExternalAgentChild,
    Lines<LineSink, LineStream>,
    StderrTail,
    TransportError,
)> {
    let args = nyaterm_core::terminal::connection_input::parse_shell_args(command)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid ACP command quoting"))?;
    let (program, args) = args
        .split_first()
        .filter(|(program, _)| !program.is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty ACP command"))?;
    #[cfg(windows)]
    let executable = find_windows_executable(program);
    #[cfg(not(windows))]
    let executable = program;
    let mut process = Command::new(executable);
    process
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = ExternalAgentChild::spawn(&mut process)?;
    let stdin = child
        .take_stdin()
        .ok_or_else(|| io::Error::other("missing agent stdin"))?;
    let stdout = child
        .take_stdout()
        .ok_or_else(|| io::Error::other("missing agent stdout"))?;
    let mut stderr = child
        .take_stderr()
        .ok_or_else(|| io::Error::other("missing agent stderr"))?;
    let (mut line_tx, mut line_rx) = mpsc::channel(LINE_QUEUE_LIMIT);
    let transport_error: TransportError = Arc::new(Mutex::new(None));
    let reader_error = transport_error.clone();
    let waker = Arc::new(AtomicWaker::new());
    let reader_waker = waker.clone();
    child.add_reader(spawn_reader("nyaterm-acp-stdout", move || {
        let mut reader = BufReader::new(stdout);
        loop {
            let mut bytes = Vec::new();
            let result = (&mut reader)
                .take((FRAME_LIMIT + 1) as u64)
                .read_until(b'\n', &mut bytes);
            let line = result.and_then(|n| {
                if n == 0 {
                    return Ok(None);
                }
                if bytes.last() == Some(&b'\n') {
                    bytes.pop();
                }
                if bytes.len() > FRAME_LIMIT {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "ACP stdout frame exceeds 1 MiB",
                    ));
                }
                if bytes.last() == Some(&b'\r') {
                    bytes.pop();
                }
                let line = String::from_utf8(bytes)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid ACP UTF-8"))?;
                serde_json::from_str::<serde_json::Value>(&line)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid ACP JSON"))?;
                Ok(Some(line))
            });
            match line {
                Ok(None) => break,
                Ok(Some(line)) => {
                    if let Err(error) = line_tx.try_send(Ok(line)) {
                        if error.is_full() {
                            *reader_error.lock().unwrap() =
                                Some("ACP stdout line queue full".into());
                            reader_waker.wake();
                        }
                        break;
                    }
                }
                Err(error) => {
                    *reader_error.lock().unwrap() = Some(error.to_string());
                    reader_waker.wake();
                    break;
                }
            }
        }
    })?);
    let stderr_tail = Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_LIMIT)));
    let stderr_writer = stderr_tail.clone();
    child.add_reader(spawn_reader("nyaterm-acp-stderr", move || {
        let mut buffer = [0; 4096];
        while let Ok(n) = stderr.read(&mut buffer) {
            if n == 0 {
                break;
            }
            let mut tail = stderr_writer.lock().unwrap();
            let excess = (tail.len() + n).saturating_sub(STDERR_LIMIT);
            tail.drain(..excess);
            tail.extend(&buffer[..n]);
        }
    })?);
    let (write_tx, write_rx) = std::sync::mpsc::sync_channel::<String>(LINE_QUEUE_LIMIT);
    child.add_reader(spawn_reader("nyaterm-acp-stdin", move || {
        let mut writer = stdin;
        for line in write_rx {
            if writer
                .write_all(line.as_bytes())
                .and_then(|_| writer.write_all(b"\n"))
                .and_then(|_| writer.flush())
                .is_err()
            {
                break;
            }
        }
    })?);
    let writer_error = transport_error.clone();
    let writer_waker = waker.clone();
    let outgoing: LineSink = Box::pin(sink::unfold(write_tx, move |sender, line: String| {
        let writer_error = writer_error.clone();
        let writer_waker = writer_waker.clone();
        async move {
            if let Err(error) = sender.try_send(line) {
                let message = match error {
                    std::sync::mpsc::TrySendError::Full(_) => "ACP stdin line queue full",
                    std::sync::mpsc::TrySendError::Disconnected(_) => "ACP input closed",
                };
                *writer_error.lock().unwrap() = Some(message.into());
                writer_waker.wake();
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, message));
            }
            Ok(sender)
        }
    }));
    let stream_error = transport_error.clone();
    let mut error_sent = false;
    let input = futures::stream::poll_fn(move |cx| {
        waker.register(cx.waker());
        if !error_sent && let Some(message) = stream_error.lock().unwrap().as_ref() {
            error_sent = true;
            return std::task::Poll::Ready(Some(Err(io::Error::other(message.clone()))));
        }
        Pin::new(&mut line_rx).poll_next(cx)
    });
    let incoming: LineStream = Box::pin(input.filter_map(move |line| {
        let event_sink = event_sink.clone();
        async move {
            if let Ok(raw) = &line
                && let Ok(value) = serde_json::from_str::<serde_json::Value>(raw)
                && value.get("method").and_then(|v| v.as_str()) == Some("session/update")
                && value.get("id").is_none()
                && let Some(params) = value.get("params")
                && let Some(session) = params.get("sessionId").and_then(|v| v.as_str())
                && let Some(kind) = params
                    .get("update")
                    .and_then(|v| v.get("sessionUpdate"))
                    .and_then(|v| v.as_str())
                && !matches!(
                    kind,
                    "user_message_chunk"
                        | "agent_message_chunk"
                        | "agent_thought_chunk"
                        | "tool_call"
                        | "tool_call_update"
                        | "plan"
                        | "available_commands_update"
                        | "current_mode_update"
                        | "config_option_update"
                        | "session_info_update"
                        | "usage_update"
                        | "notice"
                        | "compaction_update"
                        | "compaction_summary_chunk"
                )
            {
                event_sink.emit(
                    Some(session.to_owned()),
                    AcpEventKind::UnknownSessionUpdate {
                        kind: kind.to_owned(),
                    },
                );
                return None;
            }
            Some(line)
        }
    }));
    Ok((
        child,
        Lines::new(outgoing, incoming),
        stderr_tail,
        transport_error,
    ))
}

#[cfg(windows)]
fn find_windows_executable(program: &str) -> std::path::PathBuf {
    use std::path::PathBuf;

    let mut dirs: Vec<PathBuf> = std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).collect())
        .unwrap_or_default();
    for name in ["PNPM_HOME", "NVM_SYMLINK", "NVM_HOME"] {
        if let Some(path) = std::env::var_os(name).filter(|path| !path.is_empty()) {
            dirs.push(PathBuf::from(path));
        }
    }
    if let Some(appdata) = std::env::var_os("APPDATA") {
        dirs.push(PathBuf::from(appdata).join("npm"));
    }
    if let Some(local) = std::env::var_os("LOCALAPPDATA") {
        dirs.push(PathBuf::from(local).join("pnpm"));
    }
    let mut extensions = Vec::new();
    if let Some(value) = std::env::var_os("PATHEXT") {
        for ext in value.to_string_lossy().split(';') {
            if [".COM", ".EXE", ".BAT", ".CMD"]
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(ext))
                && !extensions
                    .iter()
                    .any(|existing: &String| existing.eq_ignore_ascii_case(ext))
            {
                extensions.push(ext.to_owned());
            }
        }
    }
    for ext in [".COM", ".EXE", ".BAT", ".CMD"] {
        if !extensions
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(ext))
        {
            extensions.push(ext.to_owned());
        }
    }
    let extensions: Vec<&str> = extensions.iter().map(String::as_str).collect();
    resolve_windows_program(program, &dirs, &extensions, |candidate| candidate.is_file())
}

#[cfg(windows)]
fn resolve_windows_program(
    program: &str,
    dirs: &[std::path::PathBuf],
    extensions: &[&str],
    exists: impl Fn(&std::path::Path) -> bool,
) -> std::path::PathBuf {
    use std::path::{Path, PathBuf};
    if program.contains(['/', '\\', ':']) {
        return PathBuf::from(program);
    }
    let has_extension = Path::new(program).extension().is_some();
    for dir in dirs {
        if has_extension {
            let candidate = dir.join(program);
            if exists(&candidate) {
                return candidate;
            }
        } else {
            for extension in extensions {
                let candidate = dir.join(format!("{program}{extension}"));
                if exists(&candidate) {
                    return candidate;
                }
            }
        }
    }
    PathBuf::from(program)
}

#[cfg(all(test, windows))]
mod windows_resolution_tests {
    use super::resolve_windows_program;
    use std::path::{Path, PathBuf};

    #[test]
    fn resolves_path_extensions_in_directory_and_extension_order() {
        let dirs = [PathBuf::from(r"C:\first"), PathBuf::from(r"C:\npm")];
        let extensions = [".COM", ".EXE", ".BAT", ".CMD"];
        let existing = [
            PathBuf::from(r"C:\npm\npx.CMD"),
            PathBuf::from(r"C:\npm\npx.EXE"),
        ];
        let result = resolve_windows_program("npx", &dirs, &extensions, |p| {
            existing.iter().any(|file| file == p)
        });
        assert_eq!(result, Path::new(r"C:\npm\npx.EXE"));
    }

    #[test]
    fn resolves_cmd_shim_when_extensionless_file_also_exists() {
        let dirs = [PathBuf::from(r"C:\npm")];
        let existing = [
            PathBuf::from(r"C:\npm\npx"),
            PathBuf::from(r"C:\npm\npx.CMD"),
        ];
        let result = resolve_windows_program("npx", &dirs, &[".EXE", ".CMD"], |p| {
            existing.iter().any(|file| file == p)
        });
        assert_eq!(result, Path::new(r"C:\npm\npx.CMD"));
    }

    #[test]
    fn preserves_explicit_paths_and_finds_explicit_extensions() {
        let dirs = [PathBuf::from(r"C:\npm")];
        let path = r"C:\tools\agent.cmd";
        assert_eq!(
            resolve_windows_program(path, &dirs, &[".EXE", ".CMD"], |_| false),
            Path::new(path)
        );
        assert_eq!(
            resolve_windows_program("npx.cmd", &dirs, &[".EXE", ".CMD"], |p| p
                == Path::new(r"C:\npm\npx.cmd")),
            Path::new(r"C:\npm\npx.cmd")
        );
    }
}
