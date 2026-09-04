"""Explicit host shell selection and argv compilation."""
from dataclasses import dataclass
import os
from pathlib import Path


WINDOWS_BACKEND_ID = "windows_cmd_v1"
WINDOWS_DIALECT = "windows_cmd"
UNAVAILABLE_REASON = "shell_backend_unavailable"


@dataclass(frozen=True)
class ShellBackend:
    """Resolved shell capability without any tool or Runtime policy."""

    backend_id: str
    dialect: str
    executable: str
    available: bool
    reason: str = ""

    def build_argv(self, command):
        if not self.available:
            raise RuntimeError(self.reason or UNAVAILABLE_REASON)
        return [self.executable, "/d", "/s", "/c", str(command)]

    def build_command_line(self, command):
        """Compile cmd's raw command line without CRT list re-quoting."""

        argv = self.build_argv(command)
        return f'"{argv[0]}" /d /s /c "{argv[4]}"'


def _valid_cmd_executable(value, is_file):
    candidate = str(value or "").strip().strip('"')
    if not candidate:
        return ""
    path = Path(candidate)
    if not path.is_absolute() or path.name.casefold() != "cmd.exe":
        return ""
    try:
        if not is_file(path):
            return ""
        return str(path.resolve())
    except OSError:
        return ""


def resolve_shell_backend(*, os_name=None, environ=None, is_file=None):
    """Resolve the current host backend; unsupported hosts stay unavailable."""

    os_name = str(os_name if os_name is not None else os.name)
    environ = os.environ if environ is None else environ
    is_file = (lambda path: path.is_file()) if is_file is None else is_file
    if os_name != "nt":
        return ShellBackend("", "", "", False, UNAVAILABLE_REASON)

    candidates = [environ.get("COMSPEC", "")]
    system_root = str(environ.get("SystemRoot", "")).strip().strip('"')
    if system_root:
        candidates.append(str(Path(system_root) / "System32" / "cmd.exe"))
    for candidate in candidates:
        executable = _valid_cmd_executable(candidate, is_file)
        if executable:
            return ShellBackend(
                WINDOWS_BACKEND_ID,
                WINDOWS_DIALECT,
                executable,
                True,
            )
    return ShellBackend("", "", "", False, UNAVAILABLE_REASON)


def shell_model_contract(backend=None):
    """Return the model-visible dialect contract without exposing host paths."""

    backend = backend or resolve_shell_backend()
    if not backend.available:
        return {
            "available": False,
            "backend_id": "",
            "dialect": "",
            "description": "当前宿主没有已验收的 Shell 后端；shell_command 不可用。",
        }
    return {
        "available": True,
        "backend_id": backend.backend_id,
        "dialect": backend.dialect,
        "description": (
            f"shell_backend={backend.backend_id}；shell_dialect={backend.dialect}。"
            "Runtime 已通过 cmd.exe 执行命令，通常无需再包 cmd /c，但合法嵌套仍可执行。"
            "PowerShell 必须显式使用 powershell -NoProfile -Command \"...\"；"
            "command 必须是单行；Bash/POSIX here-doc 不可用。多步命令使用 && 或显式脚本文件，"
            "验证链需要前项失败即停止时使用 && 或显式传递退出码，"
            "不要用末尾 echo 掩盖前项失败。"
        ),
    }
