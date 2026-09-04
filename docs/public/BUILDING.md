# 从源码构建 UPSP

## 环境

- Windows 11 x64
- Git
- Python 3.10 或更高版本
- Node.js 与 npm
- PowerShell
- 首次构建需要联网下载锁定版本的 .NET SDK、Python 嵌入式运行时、NSIS、WebView2 Bootstrapper 与 npm 依赖

产品运行时不要求用户另行安装 Python、Node、Git 或 .NET。

## 前端检查

```powershell
Set-Location UPSP\gui
npm ci
npm run check
```

`app.js`、`markdown.css` 与 `markdown-mermaid.js` 是随仓生成产物。修改 `src/*.ts` 或 Markdown 样式后应运行 `npm run build`，禁止直接编辑生成文件。

## Python 测试

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install ".[test]"
.\.venv\Scripts\python.exe -m pytest
```

公开源码不包含私有 Spec、dogfood、persona 或历史 Round，因此只收录不依赖这些材料的产品测试。

## Windows 安装器

```powershell
powershell -ExecutionPolicy Bypass -File tools\build_windows_desktop.ps1
```

构建脚本会：

1. 按 `desktop/build-inputs.json` 下载并校验锁定工具链；
2. 检查 TypeScript 和 GUI bundle；
3. 构建 self-contained WinForms x64 壳；
4. 组装只读程序 payload 与第三方许可证；
5. 使用 NSIS 生成安装器、manifest、receipt 和 SHA-256。

产物位于：

```text
.tmp\spec705-build\release\
```

`.tmp/`、安装后的 `Program Files\UPSP`、`文档\UPSP` 和 `LocalAppData\UPSP` 都不是源码编辑位置。

## 凭据与测试边界

- 构建和本地测试不需要 API Key。
- 不要把 `models.json`、persona、Round 或任何真实密钥复制进仓库。
- 真实模型 canary 会产生外部请求和费用，应由测试者明确触发。
