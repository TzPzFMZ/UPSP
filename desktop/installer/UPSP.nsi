Unicode True
RequestExecutionLevel admin
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
!define UNINSTALL_KEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\UPSP"
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
InstallDirRegKey HKLM "${UNINSTALL_KEY}" "InstallLocation"

Var UpgradeMode
Var ExistingNumericVersion

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
  SetShellVarContext all
  SetRegView 64
  StrCpy $UpgradeMode "0"
  ReadRegStr $0 HKLM "${UNINSTALL_KEY}" "DisplayVersion"
  ${If} $0 == ""
    Return
  ${EndIf}

  ReadRegStr $ExistingNumericVersion HKLM "${UNINSTALL_KEY}" "VersionNumeric"
  ${If} $ExistingNumericVersion == ""
  ${AndIf} $0 == "0.8.5"
    ; Legacy package/DDS-derived label predates the product version sequence.
    StrCpy $ExistingNumericVersion "0.0.0.0"
  ${EndIf}
  ${If} $ExistingNumericVersion == ""
    MessageBox MB_ICONSTOP|MB_OK "现有 UPSP 安装缺少可识别的版本记录。为保护程序目录，本安装器不会覆盖它。" /SD IDOK
    Abort
  ${EndIf}

  ReadRegStr $INSTDIR HKLM "${UNINSTALL_KEY}" "InstallLocation"
  ReadRegStr $1 HKLM "${UNINSTALL_KEY}" "UninstallString"
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
  SetShellVarContext all
  SetRegView 64
FunctionEnd

Function ValidateInstallDirectory
  ${If} $UpgradeMode == "1"
    Return
  ${EndIf}
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
  ${If} $UpgradeMode == "1"
    ExecWait '"$INSTDIR\Uninstall.exe" /S _?=$INSTDIR' $0
    ${If} $0 != 0
      MessageBox MB_ICONSTOP|MB_OK "旧版本卸载程序未能正常完成（错误码 $0）。用户数据未受影响，升级已停止。" /SD IDOK
      Abort
    ${EndIf}
  ${EndIf}
  SetOutPath "$INSTDIR"
  File /r "${PAYLOAD_ROOT}\*"
  WriteUninstaller "$INSTDIR\Uninstall.exe"

  CreateDirectory "$SMPROGRAMS\UPSP"
  CreateShortCut "$SMPROGRAMS\UPSP\UPSP.lnk" "$INSTDIR\UPSP.exe"
  CreateShortCut "$SMPROGRAMS\UPSP\卸载 UPSP.lnk" "$INSTDIR\Uninstall.exe"

  WriteRegStr HKLM "${UNINSTALL_KEY}" "DisplayName" "UPSP"
  WriteRegStr HKLM "${UNINSTALL_KEY}" "DisplayVersion" "${PRODUCT_VERSION}"
  WriteRegStr HKLM "${UNINSTALL_KEY}" "VersionNumeric" "${PRODUCT_NUMERIC_VERSION}"
  WriteRegStr HKLM "${UNINSTALL_KEY}" "Publisher" "TzPzFMZ"
  WriteRegStr HKLM "${UNINSTALL_KEY}" "InstallLocation" "$INSTDIR"
  WriteRegStr HKLM "${UNINSTALL_KEY}" "DisplayIcon" "$INSTDIR\UPSP.exe"
  WriteRegStr HKLM "${UNINSTALL_KEY}" "UninstallString" '"$INSTDIR\Uninstall.exe"'
  WriteRegStr HKLM "${UNINSTALL_KEY}" "QuietUninstallString" '"$INSTDIR\Uninstall.exe" /S'
  WriteRegDWORD HKLM "${UNINSTALL_KEY}" "NoModify" 1
  WriteRegDWORD HKLM "${UNINSTALL_KEY}" "NoRepair" 1
SectionEnd

Section "桌面快捷方式" SEC_DESKTOP
  CreateShortCut "$DESKTOP\UPSP.lnk" "$INSTDIR\UPSP.exe"
SectionEnd

LangString DESC_SEC_MAIN ${LANG_SIMPCHINESE} "安装 UPSP 桌面程序和内置运行时。"
LangString DESC_SEC_MAIN ${LANG_ENGLISH} "Install UPSP Desktop and its bundled runtimes."
LangString DESC_SEC_DESKTOP ${LANG_SIMPCHINESE} "在所有用户桌面创建 UPSP 快捷方式。"
LangString DESC_SEC_DESKTOP ${LANG_ENGLISH} "Create an UPSP shortcut on the shared desktop."
!insertmacro MUI_FUNCTION_DESCRIPTION_BEGIN
  !insertmacro MUI_DESCRIPTION_TEXT ${SEC_MAIN} $(DESC_SEC_MAIN)
  !insertmacro MUI_DESCRIPTION_TEXT ${SEC_DESKTOP} $(DESC_SEC_DESKTOP)
!insertmacro MUI_FUNCTION_DESCRIPTION_END

Section "Uninstall"
  SetShellVarContext all
  Delete "$DESKTOP\UPSP.lnk"
  Delete "$SMPROGRAMS\UPSP\UPSP.lnk"
  Delete "$SMPROGRAMS\UPSP\卸载 UPSP.lnk"
  RMDir "$SMPROGRAMS\UPSP"
  DeleteRegKey HKLM "${UNINSTALL_KEY}"
  !include "${UNINSTALL_INCLUDE}"
  Delete "$INSTDIR\Uninstall.exe"
  RMDir "$INSTDIR"
SectionEnd
