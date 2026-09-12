Unicode True
!ifdef ISOLATED_TEST
  RequestExecutionLevel user
  !define REGISTRY_ROOT HKCU
  !define UNINSTALL_KEY "Software\UPSP-Spec808-Isolated\${ISOLATED_TEST}"
!else
  RequestExecutionLevel admin
  !define REGISTRY_ROOT HKLM
  !define UNINSTALL_KEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\UPSP"
!endif
ManifestDPIAware true
SetCompressor /SOLID lzma

!include "MUI2.nsh"
!include "LogicLib.nsh"
!include "FileFunc.nsh"
!include "StrFunc.nsh"
!include "WordFunc.nsh"
${Using:StrFunc} StrCase

!ifndef PAYLOAD_ROOT
  !error "PAYLOAD_ROOT is required"
!endif
!ifndef PAYLOAD_MANIFEST_SHA256
  !error "PAYLOAD_MANIFEST_SHA256 is required"
!endif
!ifndef OUTPUT_DIR
  !error "OUTPUT_DIR is required"
!endif
!ifndef WEBVIEW_BOOTSTRAPPER
  !error "WEBVIEW_BOOTSTRAPPER is required"
!endif
!ifndef UNINSTALL_INCLUDE
  !error "UNINSTALL_INCLUDE is required"
!endif
!ifndef PRODUCT_ICON
  !error "PRODUCT_ICON is required"
!endif
!ifndef PRODUCT_VERSION
  !error "PRODUCT_VERSION is required"
!endif
!ifndef PRODUCT_NUMERIC_VERSION
  !error "PRODUCT_NUMERIC_VERSION is required"
!endif
!ifndef OUTPUT_NAME
  !error "OUTPUT_NAME is required"
!endif

!define PRODUCT_NAME "UPSP"
!define WEBVIEW_CLIENT "Software\WOW6432Node\Microsoft\EdgeUpdate\Clients\{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}"
!define WEBVIEW_CLIENT_USER "Software\Microsoft\EdgeUpdate\Clients\{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}"
!define MUI_ICON "${PRODUCT_ICON}"
!define MUI_UNICON "${PRODUCT_ICON}"

Name "${PRODUCT_NAME}"
Icon "${PRODUCT_ICON}"
UninstallIcon "${PRODUCT_ICON}"
Caption "${PRODUCT_NAME} ${PRODUCT_VERSION} 安装程序"
OutFile "${OUTPUT_DIR}\${OUTPUT_NAME}"
VIProductVersion "${PRODUCT_NUMERIC_VERSION}"
VIAddVersionKey /LANG=2052 "FileDescription" "UPSP Windows Installer"
VIAddVersionKey /LANG=2052 "ProductName" "${PRODUCT_NAME}"
VIAddVersionKey /LANG=2052 "ProductVersion" "${PRODUCT_VERSION}"
VIAddVersionKey /LANG=2052 "FileVersion" "${PRODUCT_NUMERIC_VERSION}"
VIAddVersionKey /LANG=2052 "CompanyName" "TzPzFMZ"
VIAddVersionKey /LANG=2052 "LegalCopyright" "Copyright (c) 2026 TzPzFMZ"
InstallDir "$PROGRAMFILES64\UPSP"
InstallDirRegKey ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "InstallLocation"

Var UpgradeMode
Var ExistingNumericVersion
Var StageDir
Var BackupDir
Var UpgradeRecord
Var PreviousMoved
Var NewInstalled
Var RegistryChanged
Var ShortcutDir
Var DesktopShortcutPath
Var ShortcutsChanged

!define MUI_ABORTWARNING
!insertmacro MUI_PAGE_WELCOME
PageEx directory
  PageCallbacks SkipDirectoryForUpgrade "" ValidateInstallDirectory
PageExEnd
!insertmacro MUI_PAGE_COMPONENTS
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES

!insertmacro MUI_LANGUAGE "SimpChinese"
!insertmacro MUI_LANGUAGE "English"

Function .onInit
  !ifdef ISOLATED_TEST
    SetShellVarContext current
  !else
    SetShellVarContext all
  !endif
  SetRegView 64
  StrCpy $UpgradeMode "0"
  ReadRegStr $0 ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "DisplayVersion"
  ${If} $0 == ""
    Return
  ${EndIf}

  ReadRegStr $ExistingNumericVersion ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "VersionNumeric"
  ${If} $ExistingNumericVersion == ""
  ${AndIf} $0 == "0.8.5"
    ; Legacy package/DDS-derived label predates the product version sequence.
    StrCpy $ExistingNumericVersion "0.0.0.0"
  ${EndIf}
  ${If} $ExistingNumericVersion == ""
    MessageBox MB_ICONSTOP|MB_OK "现有 UPSP 安装缺少可识别的版本记录。为保护程序目录，本安装器不会覆盖它。" /SD IDOK
    Abort
  ${EndIf}

  ReadRegStr $INSTDIR ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "InstallLocation"
  ReadRegStr $1 ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "UninstallString"
  ${If} $INSTDIR == ""
  ${OrIfNot} ${FileExists} "$INSTDIR\UPSP.exe"
  ${OrIfNot} ${FileExists} "$INSTDIR\Uninstall.exe"
  ${OrIfNot} ${FileExists} "$INSTDIR\metadata\payload-manifest.json"
    MessageBox MB_ICONSTOP|MB_OK "现有 UPSP 的安装目录或程序清单不完整。覆盖升级已停止。" /SD IDOK
    Abort
  ${EndIf}
  StrCpy $2 '"$INSTDIR\Uninstall.exe"'
  ${If} $1 != $2
    MessageBox MB_ICONSTOP|MB_OK "现有 UPSP 的卸载登记与安装目录不一致。覆盖升级已停止。" /SD IDOK
    Abort
  ${EndIf}

  System::Call 'kernel32::OpenMutexW(i 0x100000, i 0, w "Local\UPSP.Desktop.SingleInstance.v1") p.r3'
  ${If} $3 != 0
    System::Call 'kernel32::CloseHandle(p r3)'
    MessageBox MB_ICONSTOP|MB_OK "UPSP 仍在运行。请从系统托盘选择“退出”，等待程序完全关闭后重试。" /SD IDOK
    Abort
  ${EndIf}

  ${VersionCompare} "$ExistingNumericVersion" "${PRODUCT_NUMERIC_VERSION}" $4
  ${If} $4 == 1
    MessageBox MB_ICONSTOP|MB_OK "已安装版本 $0 高于当前安装包 ${PRODUCT_VERSION}。不允许降级覆盖。" /SD IDOK
    Abort
  ${EndIf}
  StrCpy $UpgradeMode "1"
FunctionEnd

Function un.onInit
  !ifdef ISOLATED_TEST
    SetShellVarContext current
  !else
    SetShellVarContext all
  !endif
  SetRegView 64
FunctionEnd

Function ValidateInstallDirectory
  StrCpy $4 "$INSTDIR"
  System::Call 'kernel32::GetFullPathNameW(w r4, i ${NSIS_MAX_STRLEN}, w .r0, p 0) i.r5'
  ${If} $5 = 0
  ${OrIf} $5 >= ${NSIS_MAX_STRLEN}
    Goto invalid
  ${EndIf}
  StrCpy $INSTDIR "$0"
  ${GetRoot} "$INSTDIR" $0
  ${If} $INSTDIR == $0
    Goto invalid
  ${EndIf}

  ${StrCase} $0 "$INSTDIR" "L"
  ${StrCase} $1 "$WINDIR" "L"
  ${If} $1 != ""
    StrLen $3 $1
    StrCpy $2 $0 $3
    ${If} $2 == $1
      StrCpy $2 $0 1 $3
      ${If} $2 == ""
      ${OrIf} $2 == "\"
        Goto invalid
      ${EndIf}
    ${EndIf}
  ${EndIf}
  ${StrCase} $1 "$DOCUMENTS\UPSP" "L"
  ${If} $1 != ""
    StrLen $3 $1
    StrCpy $2 $0 $3
    ${If} $2 == $1
      StrCpy $2 $0 1 $3
      ${If} $2 == ""
      ${OrIf} $2 == "\"
        Goto invalid
      ${EndIf}
    ${EndIf}
  ${EndIf}
  ${StrCase} $1 "$LOCALAPPDATA\UPSP" "L"
  ${If} $1 != ""
    StrLen $3 $1
    StrCpy $2 $0 $3
    ${If} $2 == $1
      StrCpy $2 $0 1 $3
      ${If} $2 == ""
      ${OrIf} $2 == "\"
        Goto invalid
      ${EndIf}
    ${EndIf}
  ${EndIf}

  StrCpy $0 "$INSTDIR"
  check_ancestor:
    System::Call 'kernel32::GetFileAttributesW(w r0) i.r1'
    ${If} $1 != -1
      IntOp $2 $1 & 0x400
      ${If} $2 != 0
        Goto invalid
      ${EndIf}
    ${EndIf}
    ${GetParent} "$0" $0
    StrCmp $0 "" ancestors_checked check_ancestor
  ancestors_checked:
  ${If} $UpgradeMode == "1"
    Return
  ${EndIf}
  IfFileExists "$INSTDIR\*.*" 0 valid
  FindFirst $0 $1 "$INSTDIR\*.*"
  loop:
    StrCmp $1 "" valid
    StrCmp $1 "." next
    StrCmp $1 ".." next
    FindClose $0
    MessageBox MB_ICONSTOP|MB_OK "安装目录必须是 UPSP 专用空目录。请选择一个新的空目录。" /SD IDOK
    Abort
  next:
    FindNext $0 $1
    Goto loop

  invalid:
    MessageBox MB_ICONSTOP|MB_OK "不能安装到磁盘根目录、Windows 目录或 UPSP 用户数据目录。" /SD IDOK
    Abort
  valid:
FunctionEnd

Function SkipDirectoryForUpgrade
  ${If} $UpgradeMode == "1"
    Abort
  ${EndIf}
FunctionEnd

Function EnsureWebView2
  !ifdef ISOLATED_TEST
    Return
  !endif
  SetRegView 64
  ReadRegStr $0 HKLM "${WEBVIEW_CLIENT}" "pv"
  ${If} $0 == ""
    ReadRegStr $0 HKCU "${WEBVIEW_CLIENT_USER}" "pv"
  ${EndIf}
  ${If} $0 == ""
  ${OrIf} $0 == "0.0.0.0"
    InitPluginsDir
    SetOutPath "$PLUGINSDIR"
    File /oname=MicrosoftEdgeWebview2Setup.exe "${WEBVIEW_BOOTSTRAPPER}"
    ExecWait '"$PLUGINSDIR\MicrosoftEdgeWebview2Setup.exe" /silent /install' $1
    ${If} $1 != 0
      MessageBox MB_ICONSTOP|MB_OK "Microsoft Edge WebView2 Runtime 安装失败（错误码 $1）。UPSP 尚未写入程序目录。" /SD IDOK
      Abort
    ${EndIf}
  ${EndIf}
FunctionEnd

Section "UPSP 程序文件" SEC_MAIN
  SectionIn RO
  Call ValidateInstallDirectory
  Call EnsureWebView2
  StrCpy $PreviousMoved "0"
  StrCpy $NewInstalled "0"
  StrCpy $RegistryChanged "0"
  StrCpy $UpgradeRecord "$INSTDIR.upsp-upgrade.ini"
  ReadINIStr $0 "$UpgradeRecord" "upgrade" "phase"
  ${If} $0 != ""
  ${AndIf} $0 != "completed"
  ${AndIf} $0 != "rolled_back"
  ${AndIf} $0 != "aborted"
    MessageBox MB_ICONSTOP|MB_OK "上次更新尚未确认恢复。请依据 $UpgradeRecord 核对程序目录后再更新。" /SD IDOK
    SetErrorLevel 1
    Abort
  ${EndIf}
  ${GetParent} "$INSTDIR" $0
  ClearErrors
  CreateDirectory "$0"
  IfErrors stage_failed
  GetTempFileName $StageDir "$0"
  Delete "$StageDir"
  StrCpy $StageDir "$StageDir.upsp-stage"
  GetTempFileName $BackupDir "$0"
  Delete "$BackupDir"
  StrCpy $BackupDir "$BackupDir.upsp-previous"
  ClearErrors
  WriteINIStr "$UpgradeRecord" "upgrade" "install_dir" "$INSTDIR"
  WriteINIStr "$UpgradeRecord" "upgrade" "stage_dir" "$StageDir"
  WriteINIStr "$UpgradeRecord" "upgrade" "backup_dir" "$BackupDir"
  WriteINIStr "$UpgradeRecord" "upgrade" "manifest_sha256" "${PAYLOAD_MANIFEST_SHA256}"
  WriteINIStr "$UpgradeRecord" "upgrade" "phase" "preparing"
  IfErrors stage_failed
  ${If} $UpgradeMode == "1"
    ExecWait '"$SYSDIR\reg.exe" export "${REGISTRY_ROOT}\${UNINSTALL_KEY}" "$UpgradeRecord.reg" /y /reg:64' $0
    ${If} $0 != 0
      Goto stage_failed
    ${EndIf}
  ${EndIf}
  InitPluginsDir
  SetOutPath "$PLUGINSDIR"
  File /oname=PayloadFiles.ps1 "PayloadFiles.ps1"
  SetOutPath "$StageDir"
  ClearErrors
  SetOverwrite on
  File /r "${PAYLOAD_ROOT}\*"
  IfErrors stage_failed
  nsExec::ExecToStack /TIMEOUT=60000 '"$SYSDIR\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "$PLUGINSDIR\PayloadFiles.ps1" -Root "$StageDir" -ManifestSha256 "${PAYLOAD_MANIFEST_SHA256}"'
  Pop $0
  Pop $1
  WriteINIStr "$UpgradeRecord" "upgrade" "verification_exit" "$0"
  WriteINIStr "$UpgradeRecord" "upgrade" "verification_detail" "$1"
  ${If} $0 != 0
    Goto stage_failed
  ${EndIf}
  ClearErrors
  WriteUninstaller "$StageDir\Uninstall.exe"
  IfErrors stage_failed
  SetOutPath "$TEMP"
  ClearErrors
  WriteINIStr "$UpgradeRecord" "upgrade" "phase" "replacing"
  IfErrors stage_failed
  ${If} ${FileExists} "$INSTDIR"
    ClearErrors
    Rename "$INSTDIR" "$BackupDir"
    IfErrors stage_failed
    StrCpy $PreviousMoved "1"
  ${EndIf}
  !ifdef ISOLATED_TEST
    !ifdef FAIL_AFTER_BACKUP
      Goto rollback
    !endif
  !endif
  ClearErrors
  Rename "$StageDir" "$INSTDIR"
  IfErrors rollback
  StrCpy $NewInstalled "1"
  nsExec::ExecToStack /TIMEOUT=60000 '"$SYSDIR\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "$PLUGINSDIR\PayloadFiles.ps1" -Root "$INSTDIR" -ManifestSha256 "${PAYLOAD_MANIFEST_SHA256}"'
  Pop $0
  Pop $1
  WriteINIStr "$UpgradeRecord" "upgrade" "verification_exit" "$0"
  WriteINIStr "$UpgradeRecord" "upgrade" "verification_detail" "$1"
  ${If} $0 != 0
    Goto rollback
  ${EndIf}
  !ifdef ISOLATED_TEST
    !ifdef FAIL_AFTER_REPLACE
      Goto rollback
    !endif
  !endif
  StrCpy $RegistryChanged "1"
  ClearErrors
  Call CreateRegisteredShortcuts
  IfErrors rollback
  WriteRegStr ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "DisplayName" "UPSP"
  WriteRegStr ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "DisplayVersion" "${PRODUCT_VERSION}"
  WriteRegStr ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "VersionNumeric" "${PRODUCT_NUMERIC_VERSION}"
  WriteRegStr ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "Publisher" "TzPzFMZ"
  WriteRegStr ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "InstallLocation" "$INSTDIR"
  WriteRegStr ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "DisplayIcon" "$INSTDIR\UPSP.exe"
  WriteRegStr ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "UninstallString" '"$INSTDIR\Uninstall.exe"'
  WriteRegStr ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "QuietUninstallString" '"$INSTDIR\Uninstall.exe" /S'
  WriteRegDWORD ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "NoModify" 1
  WriteRegDWORD ${REGISTRY_ROOT} "${UNINSTALL_KEY}" "NoRepair" 1
  IfErrors rollback
  ClearErrors
  WriteINIStr "$UpgradeRecord" "upgrade" "phase" "completed"
  IfErrors rollback
  ${If} $PreviousMoved == "1"
    nsExec::ExecToStack /TIMEOUT=60000 '"$SYSDIR\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "$PLUGINSDIR\PayloadFiles.ps1" -Root "$BackupDir" -RemovePrevious'
    Pop $0
    Pop $1
    WriteINIStr "$UpgradeRecord" "upgrade" "backup_cleanup_exit" "$0"
    WriteINIStr "$UpgradeRecord" "upgrade" "backup_cleanup_detail" "$1"
  ${EndIf}
  Goto done

  rollback:
    ${If} $RegistryChanged == "1"
      ${If} $UpgradeMode == "1"
        ExecWait '"$SYSDIR\reg.exe" import "$UpgradeRecord.reg" /reg:64' $0
        ${If} $0 != 0
          Goto recovery_required
        ${EndIf}
      ${Else}
        ClearErrors
        DeleteRegKey ${REGISTRY_ROOT} "${UNINSTALL_KEY}"
        IfErrors recovery_required
      ${EndIf}
    ${EndIf}
    ${If} $NewInstalled == "1"
      ClearErrors
      Rename "$INSTDIR" "$StageDir"
      IfErrors recovery_required
    ${EndIf}
    ${If} $PreviousMoved == "1"
      !ifdef ISOLATED_TEST
        !ifdef FAIL_ROLLBACK
          Goto recovery_required
        !endif
      !endif
      ClearErrors
      Rename "$BackupDir" "$INSTDIR"
      IfErrors recovery_required
    ${EndIf}
    ${If} $UpgradeMode == "0"
    ${AndIf} $ShortcutsChanged == "1"
      ClearErrors
      Delete "$DesktopShortcutPath"
      Delete "$ShortcutDir\UPSP.lnk"
      Delete "$ShortcutDir\卸载 UPSP.lnk"
      RMDir "$ShortcutDir"
      IfErrors recovery_required
    ${EndIf}
    WriteINIStr "$UpgradeRecord" "upgrade" "phase" "rolled_back"
    MessageBox MB_ICONSTOP|MB_OK "更新失败，已恢复更新前的程序目录。失败载荷与恢复记录保留在 $UpgradeRecord。用户数据未改动。" /SD IDOK
    SetErrorLevel 1
    Abort

  stage_failed:
    WriteINIStr "$UpgradeRecord" "upgrade" "phase" "aborted"
    MessageBox MB_ICONSTOP|MB_OK "更新准备失败，程序目录尚未替换。失败现场见 $UpgradeRecord。" /SD IDOK
    SetErrorLevel 1
    Abort

  recovery_required:
    WriteINIStr "$UpgradeRecord" "upgrade" "phase" "recovery_required"
    MessageBox MB_ICONSTOP|MB_OK "更新和恢复均未完成。请保留程序目录、新载荷及旧程序副本，按 $UpgradeRecord 核对恢复。用户数据未改动。" /SD IDOK
    SetErrorLevel 1
    Abort
  done:

SectionEnd

Section "桌面快捷方式" SEC_DESKTOP
  ; Selection is consumed before registration and the success marker in SEC_MAIN.
SectionEnd

Function CreateRegisteredShortcuts
  !ifdef ISOLATED_TEST
    StrCpy $ShortcutDir "$INSTDIR.shortcuts"
    StrCpy $DesktopShortcutPath "$INSTDIR.desktop.lnk"
  !else
    StrCpy $ShortcutDir "$SMPROGRAMS\UPSP"
    StrCpy $DesktopShortcutPath "$DESKTOP\UPSP.lnk"
  !endif
  StrCpy $ShortcutsChanged "1"
  CreateDirectory "$ShortcutDir"
  IfErrors shortcut_failed
  CreateShortCut "$ShortcutDir\UPSP.lnk" "$INSTDIR\UPSP.exe"
  IfErrors shortcut_failed
  CreateShortCut "$ShortcutDir\卸载 UPSP.lnk" "$INSTDIR\Uninstall.exe"
  IfErrors shortcut_failed
  SectionGetFlags ${SEC_DESKTOP} $0
  IntOp $0 $0 & 1
  ${If} $0 != 0
    CreateShortCut "$DesktopShortcutPath" "$INSTDIR\UPSP.exe"
    IfErrors shortcut_failed
  ${EndIf}
  !ifdef ISOLATED_TEST
    !ifdef FAIL_SHORTCUT_WRITE
      Goto shortcut_failed
    !endif
  !endif
  ClearErrors
  Return
  shortcut_failed:
    SetErrors
FunctionEnd

LangString DESC_SEC_MAIN ${LANG_SIMPCHINESE} "安装 UPSP 桌面程序和内置运行时。"
LangString DESC_SEC_MAIN ${LANG_ENGLISH} "Install UPSP Desktop and its bundled runtimes."
LangString DESC_SEC_DESKTOP ${LANG_SIMPCHINESE} "在所有用户桌面创建 UPSP 快捷方式。"
LangString DESC_SEC_DESKTOP ${LANG_ENGLISH} "Create an UPSP shortcut on the shared desktop."
!insertmacro MUI_FUNCTION_DESCRIPTION_BEGIN
  !insertmacro MUI_DESCRIPTION_TEXT ${SEC_MAIN} $(DESC_SEC_MAIN)
  !insertmacro MUI_DESCRIPTION_TEXT ${SEC_DESKTOP} $(DESC_SEC_DESKTOP)
!insertmacro MUI_FUNCTION_DESCRIPTION_END

Section "Uninstall"
  !ifdef ISOLATED_TEST
    SetShellVarContext current
  !else
    SetShellVarContext all
  !endif
  !ifndef ISOLATED_TEST
  Delete "$DESKTOP\UPSP.lnk"
  Delete "$SMPROGRAMS\UPSP\UPSP.lnk"
  Delete "$SMPROGRAMS\UPSP\卸载 UPSP.lnk"
  RMDir "$SMPROGRAMS\UPSP"
  !endif
  DeleteRegKey ${REGISTRY_ROOT} "${UNINSTALL_KEY}"
  !include "${UNINSTALL_INCLUDE}"
  Delete "$INSTDIR\Uninstall.exe"
  RMDir "$INSTDIR"
SectionEnd
