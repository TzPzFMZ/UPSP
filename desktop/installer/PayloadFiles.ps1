[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Root,
    [string]$ManifestSha256,
    [switch]$RemovePrevious
)
$ErrorActionPreference = 'Stop'
# A caller running PowerShell 7 can inherit an incompatible module search path.
$env:PSModulePath = Join-Path $PSHOME 'Modules'
try {
    $payloadRoot = [IO.Path]::GetFullPath($Root).TrimEnd('\')
    if ($payloadRoot -eq [IO.Path]::GetPathRoot($payloadRoot).TrimEnd('\')) { throw 'payload_root_invalid' }
    $manifestPath = Join-Path $payloadRoot 'metadata\payload-manifest.json'
    if (-not $RemovePrevious -and (Get-FileHash -LiteralPath $manifestPath -Algorithm SHA256).Hash -ne $ManifestSha256) {
        throw 'payload_manifest_hash_mismatch'
    }
    $manifest = Get-Content -LiteralPath $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($manifest.schema_version -ne 'upsp_desktop_payload_manifest.v1' -or
        $manifest.file_count -ne @($manifest.files).Count -or $manifest.file_count -lt 1) { throw 'payload_manifest_invalid' }
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($directory in Get-ChildItem -LiteralPath $payloadRoot -Directory -Recurse -Force) {
        if ($directory.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'payload_reparse_point' }
    }
    $files = @($manifest.files) + @(@{path='metadata/payload-manifest.json'})
    foreach ($entry in $files) {
        if (-not $entry.path -or $entry.path -match '(^[/\\]|[:\x00]|(^|[/\\])\.\.([/\\]|$))') { throw 'payload_path_invalid' }
        $target = [IO.Path]::GetFullPath((Join-Path $payloadRoot $entry.path))
        if (-not $target.StartsWith($payloadRoot + '\', [StringComparison]::OrdinalIgnoreCase) -or
            -not $seen.Add($target)) { throw 'payload_path_invalid' }
        for ($part = $target; $part.Length -ge $payloadRoot.Length; $part = Split-Path $part) {
            $item = Get-Item -LiteralPath $part -Force -ErrorAction SilentlyContinue
            if ($null -ne $item -and ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) { throw 'payload_reparse_point' }
        }
        if ($entry.sha256) {
            $item = Get-Item -LiteralPath $target -Force -ErrorAction SilentlyContinue
            $matches = $null -ne $item -and $item.Length -eq $entry.bytes -and
                (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash -eq $entry.sha256
            if (-not $matches) {
                if ($RemovePrevious) { continue } # Preserve files changed outside this installer.
                throw "payload_file_mismatch:$($entry.path)"
            }
        }
        if ($RemovePrevious) { Remove-Item -LiteralPath $target -Force -ErrorAction Stop }
    }
    if ($RemovePrevious) {
        $uninstaller = Join-Path $payloadRoot 'Uninstall.exe'
        if (Test-Path -LiteralPath $uninstaller) { Remove-Item -LiteralPath $uninstaller -Force }
        # Only empty directories are removed; unowned files remain with the recovery record.
        Get-ChildItem -LiteralPath $payloadRoot -Directory -Recurse -Force |
            Sort-Object { $_.FullName.Length } -Descending |
            ForEach-Object { if (-not (Get-ChildItem -LiteralPath $_.FullName -Force)) { Remove-Item -LiteralPath $_.FullName } }
        if (-not (Get-ChildItem -LiteralPath $payloadRoot -Force)) { Remove-Item -LiteralPath $payloadRoot }
    } else {
        foreach ($file in Get-ChildItem -LiteralPath $payloadRoot -File -Recurse -Force) {
            if (-not $seen.Contains($file.FullName) -and $file.FullName -ne (Join-Path $payloadRoot 'Uninstall.exe')) {
                throw "payload_unlisted_file:$($file.FullName)"
            }
        }
    }
    exit 0
} catch {
    Write-Output $_.Exception.ToString()
    exit 1
}
