# UPSP 第三方许可说明

UPSP 的 Windows 发行包包含或使用以下第三方组件。各组件仍适用其原始许可；构建产物的 `licenses/` 目录保存可随包分发的许可正文。

## 桌面与运行时

| 组件 | 版本 | 许可 |
| --- | --- | --- |
| Microsoft .NET | 10.0.302 | MIT 及随 SDK 提供的第三方许可 |
| Microsoft WebView2 SDK | 1.0.4078.44 | Microsoft 软件许可 |
| Python | 3.13.14 | Python Software Foundation License |
| NSIS | 3.12 | zlib/libpng License |

## GUI 构建与随包代码

GUI 的完整锁定依赖见 `UPSP/gui/package-lock.json`。当前直接依赖包括：

| 组件 | 版本 | 许可 |
| --- | --- | --- |
| highlight.js | 11.11.1 | BSD-3-Clause |
| KaTeX | 0.18.1 | MIT |
| Mermaid | 11.16.0 | MIT |
| unified | 11.0.5 | MIT |
| remark-parse | 11.0.0 | MIT |
| remark-gfm | 4.0.1 | MIT |
| remark-math | 6.0.0 | MIT |
| remark-rehype | 11.1.2 | MIT |
| rehype-highlight | 7.0.2 | MIT |
| rehype-katex | 7.0.1 | MIT |
| rehype-sanitize | 6.0.0 | MIT |
| rehype-stringify | 10.0.1 | MIT |

界面随包提供以下 Google Fonts 字体文件，均适用 SIL Open Font License 1.1；固定来源提交、源文件与随包文件 SHA-256 记录在 `UPSP/gui/assets/fonts/font-manifest.json`，许可正文同时进入发行包的 `licenses/fonts/`。

| 字体 | 随包字重 | 许可 |
| --- | --- | --- |
| Noto Sans SC | 100–900 | OFL-1.1 |
| Orbitron | 400–900 | OFL-1.1 |
| M PLUS 1 Code | 100–700 | OFL-1.1 |

esbuild、TypeScript 与 type-fest 仅参与构建，不作为独立运行时服务安装。构建逐项核对 `npm ci` 从锁文件实际安装出的全部直接与传递依赖，在 `GUI_DEPENDENCY_LICENSE_INDEX.tsv` 中登记包名、版本和许可标识，并归集安装包中可取得的许可正文；缺失包身份或许可依据时构建失败。
