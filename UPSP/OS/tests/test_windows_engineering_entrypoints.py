import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows entrypoints")


def _run_powershell(script, environment, *, executable="powershell.exe"):
    return subprocess.run(
        [executable, "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command",
         "$ErrorActionPreference = 'Stop'; "
         "[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false); " + script],
        env={**os.environ, **environment}, capture_output=True,
        text=True, encoding="utf-8", timeout=30,
    )


def test_build_output_allocation_preserves_existing_candidates(tmp_path):
    repo = tmp_path / "repo"
    frozen = repo / ".tmp/spec705-build/release/candidate.exe"
    frozen.parent.mkdir(parents=True)
    frozen.write_bytes(b"frozen candidate")
    outside = tmp_path / "outside"
    outside.mkdir()
    result = _run_powershell(r"""
$tokens = $null; $errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($env:BUILD_SCRIPT, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw $errors[0] }
$fn = $ast.Find({ param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
    $node.Name -eq 'New-BuildOutputRoot'
}, $true)
Invoke-Expression $fn.Extent.Text
$first = New-BuildOutputRoot $env:BUILD_REPO ''
$second = New-BuildOutputRoot $env:BUILD_REPO ''
$explicit = New-BuildOutputRoot $env:BUILD_REPO '.tmp\explicit-output'
$errors = @()
foreach ($request in @($explicit, '.tmp', '..\escape', '.tmp\spec705-build\nested')) {
    try { New-BuildOutputRoot $env:BUILD_REPO $request | Out-Null; $errors += 'accepted' }
    catch { $errors += $_.Exception.Message }
}
$junction = Join-Path $env:BUILD_REPO '.tmp\linked'
New-Item -ItemType Junction -Path $junction -Target $env:BUILD_OUTSIDE | Out-Null
try {
    try { New-BuildOutputRoot $env:BUILD_REPO (Join-Path $junction 'output') | Out-Null; $errors += 'accepted' }
    catch { $errors += $_.Exception.Message }
} finally { [IO.Directory]::Delete($junction) }
@{first=$first; second=$second; explicit=$explicit; errors=$errors} | ConvertTo-Json -Compress
""", {
        "BUILD_SCRIPT": str(REPO_ROOT / "tools/build_windows_desktop.ps1"),
        "BUILD_REPO": str(repo), "BUILD_OUTSIDE": str(outside),
    })
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["first"] != payload["second"]
    for key in ("first", "second", "explicit"):
        assert Path(payload[key]).is_dir()
        assert Path(payload[key]).is_relative_to(repo / ".tmp")
    assert payload["errors"][0].startswith("build_output_exists:")
    assert all(error.startswith("unsafe_build_root:") for error in payload["errors"][1:4])
    assert payload["errors"][4].startswith("build_output_reparse_point:")
    assert frozen.read_bytes() == b"frozen candidate"
    assert not (outside / "output").exists()


def test_cli_menu_zero_returns_without_reentering_menu():
    result = _run_powershell(r"""
$global:menuReads = 0
function Read-Host {
    $global:menuReads++
    if ($global:menuReads -gt 1) { throw 'menu_reentered' }
    return '0'
}
function Clear-Host {}
function py { $global:LASTEXITCODE = 0 }
& $env:MENU_SCRIPT
if ($global:menuReads -ne 1) { throw 'menu_not_read' }
""", {"MENU_SCRIPT": str(REPO_ROOT / "tools/upsp_cli_menu.ps1")})
    assert result.returncode == 0, result.stderr


def test_cli_smoke_propagates_powershell_exit_code(tmp_path):
    launcher = tmp_path / "UPSP_CLI.cmd"
    shutil.copyfile(REPO_ROOT / "UPSP_CLI.cmd", launcher)
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools/upsp_cli_menu.ps1").write_text(
        "param([switch]$Smoke)\nif (-not $Smoke) { exit 99 }\nexit 7\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        [os.environ.get("COMSPEC", "cmd.exe"), "/d", "/c", "UPSP_CLI.cmd", "--smoke"],
        cwd=tmp_path, capture_output=True, timeout=15,
    )
    assert result.returncode == 7, result.stderr


def test_desktop_local_root_uses_override_and_rejects_relative_path(tmp_path):
    pwsh = shutil.which("pwsh")
    if not pwsh:
        installed = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "PowerShell/7/pwsh.exe"
        if not installed.is_file():
            pytest.skip("PowerShell 7 is needed to compile the current C# path helper")
        pwsh = str(installed)
    result = _run_powershell(r"""
$source = '#nullable enable' + "`n" + [IO.File]::ReadAllText($env:DESKTOP_PATHS)
$source += @'

public static class DesktopPathsProbe {
    public static string Resolve(string? configured, string local) {
        return DesktopPaths.ResolveLocalStateRoot(configured, local);
    }
}
'@
Add-Type -TypeDefinition $source
$errorText = ''
try { [UPSP.Desktop.DesktopPathsProbe]::Resolve('relative', $env:LOCAL_FIXTURE) | Out-Null }
catch { $errorText = $_.Exception.ToString() }
@{
    standard = [UPSP.Desktop.DesktopPathsProbe]::Resolve($null, $env:LOCAL_FIXTURE)
    isolated = [UPSP.Desktop.DesktopPathsProbe]::Resolve($env:ISOLATED_FIXTURE, $env:LOCAL_FIXTURE)
    error = $errorText
} | ConvertTo-Json -Compress
""", {
        "DESKTOP_PATHS": str(REPO_ROOT / "desktop/UPSP.Desktop/DesktopPaths.cs"),
        "LOCAL_FIXTURE": str(tmp_path / "local-app-data"),
        "ISOLATED_FIXTURE": str(tmp_path / "isolated-local"),
    }, executable=pwsh)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert Path(payload["standard"]) == tmp_path / "local-app-data/UPSP"
    assert Path(payload["isolated"]) == tmp_path / "isolated-local"
    assert "local_state_root_must_be_absolute" in payload["error"]
    assert not (tmp_path / "local-app-data").exists()
