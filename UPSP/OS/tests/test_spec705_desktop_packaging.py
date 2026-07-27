import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
DESKTOP_ROOT = REPO_ROOT / "desktop"


def test_spec705_build_inputs_and_desktop_project_are_exact():
    inputs = json.loads(
        (DESKTOP_ROOT / "build-inputs.json").read_text(encoding="utf-8"))
    assert inputs["schema_version"] == "upsp_windows_build_inputs.v1"
    assert "product_version" not in inputs
    assert inputs["architecture"] == "win-x64"
    assert inputs["inputs"]["dotnet_sdk"]["version"] == "10.0.302"
    assert inputs["inputs"]["python_embed"]["version"] == "3.13.14"
    assert inputs["inputs"]["nsis"]["version"] == "3.12"
    assert all(
        len(item["sha256"]) == 64
        for item in inputs["inputs"].values()
    )

    project = (
        DESKTOP_ROOT / "UPSP.Desktop" / "UPSP.Desktop.csproj"
    ).read_text(encoding="utf-8")
    assert "<TargetFramework>net10.0-windows</TargetFramework>" in project
    assert "<RuntimeIdentifier>win-x64</RuntimeIdentifier>" in project
    assert "<SelfContained>true</SelfContained>" in project
    assert "<PublishSingleFile>true</PublishSingleFile>" in project
    assert (
        "<IncludeNativeLibrariesForSelfExtract>true"
        "</IncludeNativeLibrariesForSelfExtract>"
    ) in project
    assert (
        "<EnableCompressionInSingleFile>true"
        "</EnableCompressionInSingleFile>"
    ) in project
    assert "<PublishTrimmed>false</PublishTrimmed>" in project
    assert (
        "<ApplicationIcon>..\\assets\\upsp-logo.ico</ApplicationIcon>"
        in project
    )
    assert (
        'PackageReference Include="Microsoft.Web.WebView2" '
        'Version="1.0.4078.44"'
    ) in project
    lock = json.loads(
        (DESKTOP_ROOT / "UPSP.Desktop" / "packages.lock.json").read_text(
            encoding="utf-8"))
    resolved_packages = {
        item["resolved"]
        for dependencies in lock["dependencies"].values()
        for item in dependencies.values()
    }
    assert resolved_packages == {"1.0.4078.44", "10.0.10"}


def test_spec705_shell_keeps_native_and_runtime_boundaries():
    backend = (
        DESKTOP_ROOT / "UPSP.Desktop" / "DesktopBackend.cs"
    ).read_text(encoding="utf-8")
    form = (
        DESKTOP_ROOT / "UPSP.Desktop" / "MainForm.cs"
    ).read_text(encoding="utf-8")
    program = (
        DESKTOP_ROOT / "UPSP.Desktop" / "Program.cs"
    ).read_text(encoding="utf-8")

    assert "http://127.0.0.1:8770/" in backend
    assert "UPSP_DESKTOP_CONTROL_TOKEN" in backend
    assert "UPSP_DESKTOP_SESSION_ID" in backend
    assert "ArgumentList.Add" in backend
    assert "JobObjectLimitKillOnJobClose" in backend
    assert "TerminateStartedProcess" in backend
    assert (
        "Graceful backend shutdown timed out; terminating the managed process tree."
        in backend
    )
    assert "catch (TimeoutException)" in backend
    assert "DrainOutputAsync" in backend
    assert 'Text(root, "product_version")' in backend
    assert "AssemblyInformationalVersionAttribute" in backend
    assert "WaitForIdleAsync" in backend
    assert "StopOutcomeSafe" in backend
    assert "Task<bool> RequestStopAsync" in backend
    assert (
        'Text(receipt.RootElement, "reason") != '
        '"local_cleanup_in_progress"'
    ) in backend
    assert "stopAccepted && !snapshot.StopOutcomeSafe" in form
    assert "AreDevToolsEnabled = false" in form
    assert "AreHostObjectsAllowed = false" in form
    assert "AddHostObjectToScript" not in form
    assert "Icon = _appIcon ?? SystemIcons.Application;" in form
    assert "Icon = Icon," in form
    assert "SaveFileDialog" in form
    assert "if (Visible)" in form
    assert "Hide();" in form
    assert "Local\\UPSP.Desktop.SingleInstance.v1" in program


def test_spec707_installer_supports_guarded_upgrade_and_manifest_only_uninstall():
    installer = (
        DESKTOP_ROOT / "installer" / "UPSP.nsi"
    ).read_text(encoding="utf-8")
    uninstall = installer.split('Section "Uninstall"', 1)[1]

    assert "RequestExecutionLevel admin" in installer
    assert 'Icon "${PRODUCT_ICON}"' in installer
    assert 'UninstallIcon "${PRODUCT_ICON}"' in installer
    assert '!define MUI_ICON "${PRODUCT_ICON}"' in installer
    assert '!define MUI_UNICON "${PRODUCT_ICON}"' in installer
    assert 'InstallDir "$PROGRAMFILES64\\UPSP"' in installer
    assert "ValidateInstallDirectory" in installer
    assert "kernel32::GetFullPathNameW" in installer
    assert "GetFullPathName $INSTDIR" not in installer
    assert '${GetRoot} "$INSTDIR" $0' in installer
    assert '${GetRoot} $0 "$INSTDIR"' not in installer
    assert installer.index("Call ValidateInstallDirectory") < installer.index(
        "Call EnsureWebView2")
    assert installer.count('StrCpy $2 $0 $3') == 3
    assert installer.count('StrCpy $2 $0 1 $3') == 3
    assert installer.count('${If} $1 != ""') == 3
    assert '${StrStr} $2 "$0" "$1"' not in installer
    assert "MUI_FINISHPAGE_RUN" not in installer
    assert "安装目录必须是 UPSP 专用空目录" in installer
    assert "VersionNumeric" in installer
    assert 'VIProductVersion "${PRODUCT_NUMERIC_VERSION}"' in installer
    assert '$0 == "0.8.5"' in installer
    assert 'StrCpy $ExistingNumericVersion "0.0.0.0"' in installer
    assert "VersionCompare" in installer
    assert "OpenMutexW" in installer
    assert "不允许降级覆盖" in installer
    assert 'ExecWait \'"$INSTDIR\\Uninstall.exe" /S _?=$INSTDIR\'' in installer
    assert "MicrosoftEdgeWebview2Setup.exe" in installer
    assert "/silent /install" in installer
    assert '!include "${UNINSTALL_INCLUDE}"' in uninstall
    assert "RMDir /r" not in uninstall
    assert "$DOCUMENTS" not in uninstall
    assert "$LOCALAPPDATA" not in uninstall


def test_spec705_build_payload_has_an_explicit_production_allowlist():
    build = (REPO_ROOT / "tools" / "build_windows_desktop.ps1").read_text(
        encoding="utf-8")
    assert "'serve_seed_gui.py', 'serve_round_live.py', 'upsp_cli.py'" in build
    assert "'assembly', 'audit', 'data', 'engines', 'logic', 'schemas', 'utils'" in build
    assert "'index.html', 'styles.css', 'app.js', 'markdown.css', 'markdown-mermaid.js'" in build
    assert "UPSP\\gui\\src" not in build
    assert "UPSP\\OS\\tests" not in build
    assert "payload-manifest.json" in build
    assert "excludes = @('metadata/payload-manifest.json')" in build
    assert "manifested_file_count" in build
    assert "artifact-receipt.json" in build
    assert "desktop_single_file_publish_violation" in build
    assert "Where-Object Name -ne 'UPSP.exe'" in build
    assert "payload_root_file_violation" in build
    assert "Join-Path $payloadRoot 'metadata'" in build
    assert "UPSP\\product.json" in build
    assert "THIRD_PARTY_NOTICES.md" in build
    assert "nodeModulesQueue" in build
    assert "moduleRoots" in build
    assert "GUI_DEPENDENCY_LICENSE_INDEX.tsv" in build
    assert "gui_dependency_license_missing" in build
    assert "SHA256SUMS.txt" in build
    assert 'docs\\public\\releases\\$($product.version).md' in build
    assert "curl.exe" in build
    assert "--proto '=https'" in build
    assert "Invoke-WebRequest" not in build
    assert "IncludeSourceRevisionInInformationalVersion=false" in build
    assert (
        '"/DPRODUCT_ICON=$(Join-Path $repoRoot '
        "'desktop\\assets\\upsp-logo.ico')\""
        in build
    )


def test_spec707_product_manifest_is_the_release_version_truth():
    product = json.loads(
        (REPO_ROOT / "UPSP" / "product.json").read_text(encoding="utf-8"))
    assert product == {
        "schema_version": "upsp_product_manifest.v1",
        "name": "UPSP",
        "version": "0.1.0-alpha.5",
        "windows_file_version": "0.1.0.5",
        "channel": "alpha",
        "build_number": 5,
        "author": {
            "zh-CN": "由 TzPzFMZ 发起、设计并与 AI 协作开发",
            "en-US": (
                "Initiated and designed by TzPzFMZ, "
                "developed in collaboration with AI"
            ),
        },
        "repository_url": "https://github.com/TzPzFMZ/UPSP",
        "releases_url": "https://github.com/TzPzFMZ/UPSP/releases",
        "license": "MIT",
        "copyright": "Copyright (c) 2026 TzPzFMZ",
    }
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'version = "0.1.0a5"' in pyproject
    manifest = (
        DESKTOP_ROOT / "UPSP.Desktop" / "app.manifest"
    ).read_text(encoding="utf-8")
    assert 'assemblyIdentity version="1.0.0.0"' in manifest
    assert "0.8.5" not in (
        DESKTOP_ROOT / "UPSP.Desktop" / "UPSP.Desktop.csproj"
    ).read_text(encoding="utf-8")
