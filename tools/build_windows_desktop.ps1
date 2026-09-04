[CmdletBinding()]
param(
    [switch]$SkipDownload,
    [switch]$SkipTests
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$inputsPath = Join-Path $repoRoot 'desktop\build-inputs.json'
$inputs = Get-Content -LiteralPath $inputsPath -Raw -Encoding UTF8 | ConvertFrom-Json
$productPath = Join-Path $repoRoot 'UPSP\product.json'
$product = Get-Content -LiteralPath $productPath -Raw -Encoding UTF8 | ConvertFrom-Json
$productKeys = @($product.PSObject.Properties.Name | Sort-Object)
$stableProductVersion = $product.version -match '^\d+\.\d+\.\d+$' -and $product.channel -eq 'stable'
$alphaProductVersion = $product.version -match '^\d+\.\d+\.\d+-alpha\.\d+$' -and $product.channel -eq 'alpha'
$expectedProductKeys = @(
    'author', 'build_number', 'channel', 'copyright', 'license', 'name',
    'releases_url', 'repository_url', 'schema_version', 'version',
    'windows_file_version'
) | Sort-Object
if ($product.schema_version -ne 'upsp_product_manifest.v1' -or
    (Compare-Object $productKeys $expectedProductKeys) -or
    (-not $stableProductVersion -and -not $alphaProductVersion) -or
    $product.windows_file_version -notmatch '^\d+\.\d+\.\d+\.\d+$' -or
    -not ($product.build_number -is [int] -or $product.build_number -is [long]) -or
    $product.build_number -lt 1 -or
    -not ([string]$product.repository_url).StartsWith('https://') -or
    -not ([string]$product.releases_url).StartsWith('https://')) {
    throw 'product_manifest_invalid'
}
$workRoot = Join-Path $repoRoot '.tmp\spec705-build'
$downloadRoot = Join-Path $workRoot 'downloads'
$toolRoot = Join-Path $workRoot 'toolchains'
$publishRoot = Join-Path $workRoot 'publish'
$payloadRoot = Join-Path $workRoot 'payload'
$releaseRoot = Join-Path $workRoot 'release'
$uninstallInclude = Join-Path $workRoot 'uninstall-files.nsh'
if (-not $workRoot.StartsWith(
    (Join-Path $repoRoot '.tmp') + '\',
    [StringComparison]::OrdinalIgnoreCase)) {
    throw "unsafe_build_root:$workRoot"
}

function Assert-Sha256([string]$Path, [string]$Expected) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "missing_build_input:$Path"
    }
    $actual = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actual -ne $Expected.ToLowerInvariant()) {
        throw "build_input_sha256_mismatch:$Path expected=$Expected actual=$actual"
    }
}

function Get-BuildInput($Artifact) {
    $target = Join-Path $downloadRoot $Artifact.file
    if (-not (Test-Path -LiteralPath $target)) {
        if ($SkipDownload) {
            throw "build_input_not_cached:$target"
        }
        $partial = "$target.download"
        $curl = Get-Command curl.exe -ErrorAction SilentlyContinue
        if ($null -eq $curl) {
            throw "curl_missing_for_build_input:$($Artifact.file)"
        }
        & $curl.Source --location --fail --silent --show-error `
            --proto '=https' --tlsv1.2 --output $partial $Artifact.url
        if ($LASTEXITCODE -ne 0) {
            throw "build_input_download_failed:$($Artifact.file)"
        }
        Assert-Sha256 $partial $Artifact.sha256
        Move-Item -LiteralPath $partial -Destination $target
    }
    Assert-Sha256 $target $Artifact.sha256
    return $target
}

function Copy-ProductionTree([string]$Source, [string]$Destination) {
    $prefix = [IO.Path]::GetFullPath($Source).TrimEnd('\') + '\'
    Get-ChildItem -LiteralPath $Source -Recurse -File |
        Where-Object {
            ($_.FullName -notmatch '\\__pycache__\\') -and
            ($_.Extension -notin '.pyc', '.pyo')
        } |
        ForEach-Object {
        $relative = $_.FullName.Substring($prefix.Length)
        $target = Join-Path $Destination $relative
        New-Item -ItemType Directory -Force -Path (Split-Path $target) | Out-Null
        Copy-Item -LiteralPath $_.FullName -Destination $target
        }
}

function Write-JsonFile([string]$Path, $Value) {
    $json = $Value | ConvertTo-Json -Depth 8
    [IO.File]::WriteAllText($Path, $json + "`n", [Text.UTF8Encoding]::new($false))
}

New-Item -ItemType Directory -Force -Path $downloadRoot, $toolRoot | Out-Null
if (Test-Path -LiteralPath $releaseRoot) {
    Remove-Item -LiteralPath $releaseRoot -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $releaseRoot | Out-Null
$dotnetArchive = Get-BuildInput $inputs.inputs.dotnet_sdk
$pythonArchive = Get-BuildInput $inputs.inputs.python_embed
$nsisArchive = Get-BuildInput $inputs.inputs.nsis
$webViewBootstrapper = Get-BuildInput $inputs.inputs.webview2_bootstrapper

$dotnetRoot = Join-Path $toolRoot 'dotnet'
$nsisRoot = Join-Path $toolRoot 'nsis'
if (-not (Test-Path -LiteralPath (Join-Path $dotnetRoot 'dotnet.exe'))) {
    New-Item -ItemType Directory -Force -Path $dotnetRoot | Out-Null
    Expand-Archive -LiteralPath $dotnetArchive -DestinationPath $dotnetRoot
}
if (-not (Test-Path -LiteralPath (Join-Path $nsisRoot 'makensis.exe'))) {
    $nsisExtract = Join-Path $toolRoot 'nsis-extract'
    New-Item -ItemType Directory -Force -Path $nsisExtract | Out-Null
    Expand-Archive -LiteralPath $nsisArchive -DestinationPath $nsisExtract
    $resolvedNsis = Get-ChildItem -LiteralPath $nsisExtract -Filter makensis.exe -Recurse -File |
        Select-Object -First 1
    if ($null -eq $resolvedNsis) {
        throw 'nsis_makensis_missing'
    }
    Move-Item -LiteralPath $resolvedNsis.Directory.FullName -Destination $nsisRoot
}

$dotnet = Join-Path $dotnetRoot 'dotnet.exe'
$makensis = Join-Path $nsisRoot 'makensis.exe'
$env:DOTNET_CLI_HOME = $workRoot
$env:DOTNET_CLI_TELEMETRY_OPTOUT = '1'
$env:NUGET_PACKAGES = Join-Path $workRoot 'nuget'

if (-not $SkipTests) {
    Push-Location (Join-Path $repoRoot 'UPSP\gui')
    try {
        & npm ci
        if ($LASTEXITCODE -ne 0) { throw 'npm_ci_failed' }
        & npm run typecheck
        if ($LASTEXITCODE -ne 0) { throw 'npm_typecheck_failed' }
        & npm run build:check
        if ($LASTEXITCODE -ne 0) { throw 'npm_bundle_check_failed' }
    }
    finally {
        Pop-Location
    }
}

& $dotnet restore (Join-Path $repoRoot 'desktop\UPSP.Desktop\UPSP.Desktop.csproj') --locked-mode
if ($LASTEXITCODE -ne 0) { throw 'dotnet_restore_failed' }
if (Test-Path -LiteralPath $publishRoot) {
    Remove-Item -LiteralPath $publishRoot -Recurse -Force
}
& $dotnet publish (Join-Path $repoRoot 'desktop\UPSP.Desktop\UPSP.Desktop.csproj') `
    -c Release --no-restore --self-contained true -r win-x64 -o $publishRoot `
    "-p:Version=$($product.version.Split('-')[0])" `
    "-p:AssemblyVersion=$($product.version.Split('-')[0]).0" `
    "-p:FileVersion=$($product.windows_file_version)" `
    "-p:InformationalVersion=$($product.version)" `
    "-p:IncludeSourceRevisionInInformationalVersion=false"
if ($LASTEXITCODE -ne 0) { throw 'dotnet_publish_failed' }

$publishRuntimeFiles = @(
    Get-ChildItem -LiteralPath $publishRoot -File |
        Where-Object Extension -notin '.pdb', '.xml'
)
$unexpectedPublishFiles = @(
    $publishRuntimeFiles | Where-Object Name -ne 'UPSP.exe'
)
if ($publishRuntimeFiles.Count -ne 1 -or $unexpectedPublishFiles.Count -ne 0) {
    $names = ($publishRuntimeFiles.Name | Sort-Object) -join ','
    throw "desktop_single_file_publish_violation:$names"
}

if (Test-Path -LiteralPath $payloadRoot) {
    Remove-Item -LiteralPath $payloadRoot -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $payloadRoot | Out-Null
Copy-Item -LiteralPath $publishRuntimeFiles[0].FullName -Destination $payloadRoot

$pythonRoot = Join-Path $payloadRoot 'runtime\python'
New-Item -ItemType Directory -Force -Path $pythonRoot | Out-Null
Expand-Archive -LiteralPath $pythonArchive -DestinationPath $pythonRoot

$programOs = Join-Path $payloadRoot 'UPSP\OS'
foreach ($name in @('assembly', 'audit', 'data', 'engines', 'logic', 'schemas', 'utils')) {
    Copy-ProductionTree (Join-Path $repoRoot "UPSP\OS\$name") (Join-Path $programOs $name)
}
foreach ($name in @('constants.py', 'errors.py', 'main.py', 'paths.py')) {
    $target = Join-Path $programOs $name
    New-Item -ItemType Directory -Force -Path (Split-Path $target) | Out-Null
    Copy-Item -LiteralPath (Join-Path $repoRoot "UPSP\OS\$name") -Destination $target
}

Copy-ProductionTree (Join-Path $repoRoot 'UPSP\initialization') (Join-Path $payloadRoot 'UPSP\initialization')
Copy-Item -LiteralPath $productPath -Destination (Join-Path $payloadRoot 'UPSP\product.json')
foreach ($name in @('index.html', 'styles.css', 'app.js', 'markdown.css', 'markdown-mermaid.js')) {
    $target = Join-Path $payloadRoot "UPSP\gui\$name"
    New-Item -ItemType Directory -Force -Path (Split-Path $target) | Out-Null
    Copy-Item -LiteralPath (Join-Path $repoRoot "UPSP\gui\$name") -Destination $target
}
Copy-ProductionTree (Join-Path $repoRoot 'UPSP\gui\assets') (Join-Path $payloadRoot 'UPSP\gui\assets')
Copy-ProductionTree (Join-Path $repoRoot 'UPSP\gui\manual') (Join-Path $payloadRoot 'UPSP\gui\manual')

$licensesRoot = Join-Path $payloadRoot 'licenses'
$guiLicensesRoot = Join-Path $licensesRoot 'gui'
New-Item -ItemType Directory -Force -Path $guiLicensesRoot | Out-Null
Copy-Item -LiteralPath (Join-Path $repoRoot 'LICENSE') -Destination (Join-Path $licensesRoot 'LICENSE.txt')
Copy-Item -LiteralPath (Join-Path $repoRoot 'THIRD_PARTY_NOTICES.md') -Destination $licensesRoot
$fontLicensesRoot = Join-Path $repoRoot 'UPSP\gui\assets\fonts\licenses'
Copy-ProductionTree $fontLicensesRoot (Join-Path $licensesRoot 'fonts')
$guiRoot = Join-Path $repoRoot 'UPSP\gui'
$guiLockPath = Join-Path $guiRoot 'package-lock.json'
Copy-Item -LiteralPath $guiLockPath -Destination (Join-Path $guiLicensesRoot 'package-lock.json')
$nodeModulesQueue = [Collections.Generic.Queue[string]]::new()
$nodeModulesQueue.Enqueue((Join-Path $guiRoot 'node_modules'))
$moduleRoots = [Collections.Generic.List[string]]::new()
while ($nodeModulesQueue.Count -gt 0) {
    $nodeModulesRoot = $nodeModulesQueue.Dequeue()
    foreach ($entry in Get-ChildItem -LiteralPath $nodeModulesRoot -Directory) {
        $packages = if ($entry.Name.StartsWith('@')) {
            @(Get-ChildItem -LiteralPath $entry.FullName -Directory)
        } else {
            @($entry)
        }
        foreach ($package in $packages) {
            if (-not (Test-Path -LiteralPath (Join-Path $package.FullName 'package.json') -PathType Leaf)) {
                continue
            }
            $moduleRoots.Add($package.FullName)
            $nestedNodeModules = Join-Path $package.FullName 'node_modules'
            if (Test-Path -LiteralPath $nestedNodeModules -PathType Container) {
                $nodeModulesQueue.Enqueue($nestedNodeModules)
            }
        }
    }
}
if ($moduleRoots.Count -eq 0) {
    throw 'gui_dependency_lock_invalid'
}
$licenseIndex = @("package`tversion`tlicense")
foreach ($moduleRoot in $moduleRoots) {
    $modulePackagePath = Join-Path $moduleRoot 'package.json'
    if (-not (Test-Path -LiteralPath $modulePackagePath -PathType Leaf)) {
        throw "gui_dependency_missing:$moduleRoot"
    }
    $modulePackage = Get-Content -LiteralPath $modulePackagePath -Raw -Encoding UTF8 | ConvertFrom-Json
    $moduleLicenses = @(Get-ChildItem -LiteralPath $moduleRoot -File |
        Where-Object Name -Match '^(LICENSE|LICENCE|COPYING)([._-].*)?$')
    if (-not $modulePackage.name -or -not $modulePackage.version -or
        (-not $modulePackage.license -and $moduleLicenses.Count -eq 0)) {
        throw "gui_dependency_license_missing:$moduleRoot"
    }
    $licenseId = if ($modulePackage.license) { [string]$modulePackage.license } else { 'license-file-only' }
    $licenseIndex += "$($modulePackage.name)`t$($modulePackage.version)`t$licenseId"
    $safeName = ([string]$modulePackage.name) -replace '[^A-Za-z0-9._-]', '-'
    foreach ($moduleLicense in $moduleLicenses) {
        $safeLicenseName = $moduleLicense.Name -replace '[^A-Za-z0-9._-]', '-'
        Copy-Item -LiteralPath $moduleLicense.FullName -Force `
            -Destination (Join-Path $guiLicensesRoot "$safeName-$($modulePackage.version)-$safeLicenseName.txt")
    }
}
[IO.File]::WriteAllText(
    (Join-Path $guiLicensesRoot 'GUI_DEPENDENCY_LICENSE_INDEX.tsv'),
    (($licenseIndex -join "`n") + "`n"),
    [Text.UTF8Encoding]::new($false))
Copy-Item -LiteralPath (Join-Path $pythonRoot 'LICENSE.txt') -Destination (Join-Path $licensesRoot 'PYTHON_LICENSE.txt')
Copy-Item -LiteralPath (Join-Path $dotnetRoot 'LICENSE.txt') -Destination (Join-Path $licensesRoot 'DOTNET_LICENSE.txt')
Copy-Item -LiteralPath (Join-Path $dotnetRoot 'ThirdPartyNotices.txt') -Destination (Join-Path $licensesRoot 'DOTNET_THIRD_PARTY_NOTICES.txt')
Copy-Item -LiteralPath (Join-Path $nsisRoot 'COPYING') -Destination (Join-Path $licensesRoot 'NSIS_COPYING.txt')
$webViewLicenseRoot = Join-Path $env:NUGET_PACKAGES 'microsoft.web.webview2\1.0.4078.44'
Copy-Item -LiteralPath (Join-Path $webViewLicenseRoot 'LICENSE.txt') -Destination (Join-Path $licensesRoot 'WEBVIEW2_LICENSE.txt')
Copy-Item -LiteralPath (Join-Path $webViewLicenseRoot 'NOTICE.txt') -Destination (Join-Path $licensesRoot 'WEBVIEW2_NOTICE.txt')

$payloadTools = Join-Path $payloadRoot 'tools'
New-Item -ItemType Directory -Force -Path $payloadTools | Out-Null
foreach ($name in @('serve_seed_gui.py', 'serve_round_live.py', 'upsp_cli.py')) {
    Copy-Item -LiteralPath (Join-Path $repoRoot "tools\$name") -Destination (Join-Path $payloadTools $name)
}

# This smoke is part of the Windows payload contract and must also run with
# -SkipTests. It imports the copied production handler with the bundled Python.
$payloadPython = Join-Path $pythonRoot 'python.exe'
$shellSmokeRoot = Join-Path $workRoot 'shell-smoke'
$shellSmokeScript = Join-Path $workRoot 'shell-smoke.py'
if (Test-Path -LiteralPath $shellSmokeRoot) {
    Remove-Item -LiteralPath $shellSmokeRoot -Recurse -Force
}
$shellSmoke = @'
import sys
from pathlib import Path

program_os = Path(sys.argv[1]).resolve()
smoke_root = Path(sys.argv[2]).resolve()
cwd = smoke_root / "shell smoke 中文"
cwd.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(program_os))

from logic.general_tools import execute_general_tool_call


def run(command):
    return execute_general_tool_call(
        {
            "tool_id": "shell_command",
            "command": command,
            "purpose": "desktop payload shell smoke",
            "cwd": str(cwd),
            "timeout_ms": 10000,
        },
        allowed_roots=[smoke_root],
    )


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


echo = run("echo UPSP_SHELL_SMOKE")
require(echo.get("status") == "ok", "echo_failed")
require("UPSP_SHELL_SMOKE" in echo.get("stdout", ""), "echo_output_missing")
require(echo.get("shell_backend") == "windows_cmd_v1", "backend_mismatch")
require(echo.get("shell_dialect") == "windows_cmd", "dialect_mismatch")

nested = run("cmd /c ver")
require(nested.get("status") == "ok", "nested_cmd_failed")
require("Windows" in nested.get("stdout", ""), "nested_cmd_output_missing")

powershell = run('powershell -NoProfile -Command "Write-Output \'UPSP_PS_SMOKE\'"')
require(powershell.get("status") == "ok", "powershell_failed")
require("UPSP_PS_SMOKE" in powershell.get("stdout", ""), "powershell_output_missing")

streams = run("echo UPSP_STDOUT_SMOKE & echo UPSP_STDERR_SMOKE 1>&2")
require(streams.get("status") == "ok", "stream_split_failed")
require("UPSP_STDOUT_SMOKE" in streams.get("stdout", ""), "stdout_missing")
require("UPSP_STDERR_SMOKE" not in streams.get("stdout", ""), "stderr_leaked_stdout")
require("UPSP_STDERR_SMOKE" in streams.get("stderr", ""), "stderr_missing")
require("UPSP_STDOUT_SMOKE" not in streams.get("stderr", ""), "stdout_leaked_stderr")

nonzero = run("exit /b 7")
require(nonzero.get("status") == "failed", "nonzero_status_mismatch")
require(nonzero.get("reason") == "nonzero_exit", "nonzero_reason_mismatch")
require(nonzero.get("exit_code") == 7, "nonzero_exit_code_mismatch")
'@
try {
    [System.IO.File]::WriteAllText($shellSmokeScript, $shellSmoke, [System.Text.UTF8Encoding]::new($false))
    & $payloadPython -I -X utf8 $shellSmokeScript $programOs $shellSmokeRoot
    if ($LASTEXITCODE -ne 0) { throw 'payload_shell_backend_smoke_failed' }
}
finally {
    if (Test-Path -LiteralPath $shellSmokeRoot) {
        Remove-Item -LiteralPath $shellSmokeRoot -Recurse -Force
    }
    if (Test-Path -LiteralPath $shellSmokeScript) {
        Remove-Item -LiteralPath $shellSmokeScript -Force
    }
}

$head = (& git -C $repoRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or -not $head) { throw 'git_head_unavailable' }
$sourceDirty = [bool](& git -C $repoRoot status --porcelain --untracked-files=normal)
$buildInfo = [ordered]@{
    schema_version = 'upsp_desktop_build_info.v1'
    product_version = $product.version
    git_head = $head
    source_dirty = $sourceDirty
    architecture = $inputs.architecture
    dotnet_sdk = $inputs.inputs.dotnet_sdk.version
    python = $inputs.inputs.python_embed.version
    webview2_sdk = '1.0.4078.44'
    nsis = $inputs.inputs.nsis.version
    signature_status = 'unsigned'
}
$metadataRoot = Join-Path $payloadRoot 'metadata'
New-Item -ItemType Directory -Force -Path $metadataRoot | Out-Null
Write-JsonFile (Join-Path $metadataRoot 'build-info.json') $buildInfo

$manifestItems = Get-ChildItem -LiteralPath $payloadRoot -Recurse -File |
    Sort-Object FullName |
    ForEach-Object {
        [ordered]@{
            path = $_.FullName.Substring($payloadRoot.Length + 1).Replace('\', '/')
            bytes = $_.Length
            sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        }
    }
$manifest = [ordered]@{
    schema_version = 'upsp_desktop_payload_manifest.v1'
    product_version = $product.version
    git_head = $head
    source_dirty = $sourceDirty
    file_count = @($manifestItems).Count
    excludes = @('metadata/payload-manifest.json')
    files = @($manifestItems)
}
Write-JsonFile (Join-Path $metadataRoot 'payload-manifest.json') $manifest

$unexpectedPayloadRootFiles = @(
    Get-ChildItem -LiteralPath $payloadRoot -File |
        Where-Object Name -ne 'UPSP.exe'
)
if ($unexpectedPayloadRootFiles.Count -ne 0) {
    $names = ($unexpectedPayloadRootFiles.Name | Sort-Object) -join ','
    throw "payload_root_file_violation:$names"
}

$deleteLines = Get-ChildItem -LiteralPath $payloadRoot -Recurse -File |
    Sort-Object { $_.FullName.Length } -Descending |
    ForEach-Object {
        $relative = $_.FullName.Substring($payloadRoot.Length + 1)
        'Delete "$INSTDIR\' + $relative + '"'
    }
$directoryLines = Get-ChildItem -LiteralPath $payloadRoot -Recurse -Directory |
    Sort-Object { $_.FullName.Length } -Descending |
    ForEach-Object {
        $relative = $_.FullName.Substring($payloadRoot.Length + 1)
        'RMDir "$INSTDIR\' + $relative + '"'
    }
[IO.File]::WriteAllLines(
    $uninstallInclude,
    @($deleteLines) + @($directoryLines),
    [Text.UTF8Encoding]::new($false))

& $makensis `
    '/INPUTCHARSET' 'UTF8' `
    "/DPAYLOAD_ROOT=$payloadRoot" `
    "/DOUTPUT_DIR=$releaseRoot" `
    "/DWEBVIEW_BOOTSTRAPPER=$webViewBootstrapper" `
    "/DUNINSTALL_INCLUDE=$uninstallInclude" `
    "/DPRODUCT_ICON=$(Join-Path $repoRoot 'desktop\assets\upsp-logo.ico')" `
    "/DPRODUCT_VERSION=$($product.version)" `
    "/DPRODUCT_NUMERIC_VERSION=$($product.windows_file_version)" `
    "/DOUTPUT_NAME=UPSP-Setup-$($product.version)-win-x64.exe" `
    (Join-Path $repoRoot 'desktop\installer\UPSP.nsi')
if ($LASTEXITCODE -ne 0) { throw 'nsis_build_failed' }

$installer = Join-Path $releaseRoot "UPSP-Setup-$($product.version)-win-x64.exe"
$receipt = [ordered]@{
    schema_version = 'upsp_desktop_artifact_receipt.v1'
    product_version = $product.version
    git_head = $head
    source_dirty = $sourceDirty
    installer = [ordered]@{
        path = $installer
        bytes = (Get-Item -LiteralPath $installer).Length
        sha256 = (Get-FileHash -LiteralPath $installer -Algorithm SHA256).Hash.ToLowerInvariant()
    }
    signature_status = 'unsigned'
    payload = [ordered]@{
        path = $payloadRoot
        file_count = (Get-ChildItem -LiteralPath $payloadRoot -Recurse -File).Count
        manifested_file_count = @($manifestItems).Count
        manifest_sha256 = (Get-FileHash -LiteralPath (Join-Path $metadataRoot 'payload-manifest.json') -Algorithm SHA256).Hash.ToLowerInvariant()
    }
}
Write-JsonFile (Join-Path $releaseRoot 'artifact-receipt.json') $receipt
Copy-Item -LiteralPath (Join-Path $metadataRoot 'build-info.json') -Destination $releaseRoot
Copy-Item -LiteralPath (Join-Path $metadataRoot 'payload-manifest.json') -Destination $releaseRoot
Copy-Item -LiteralPath (Join-Path $repoRoot "docs\public\releases\$($product.version).md") `
    -Destination (Join-Path $releaseRoot 'RELEASE_NOTES.md')
$releaseHashes = Get-ChildItem -LiteralPath $releaseRoot -File |
    Where-Object Name -ne 'SHA256SUMS.txt' |
    Sort-Object Name |
    ForEach-Object {
        "$((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant())  $($_.Name)"
    }
[IO.File]::WriteAllText(
    (Join-Path $releaseRoot 'SHA256SUMS.txt'),
    (($releaseHashes -join "`n") + "`n"),
    [Text.UTF8Encoding]::new($false))
$receipt | ConvertTo-Json -Depth 8
