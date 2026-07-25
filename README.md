# UPSP

**极简 AI 主体动态记忆协议与可审计 Runtime。**

UPSP（Universal Persona Substrate Protocol）把位格档案、动态状态、记忆、关系、上下文装配、工具回执与运行审计保存为可迁移的本地文件，使模型服务可以更换，而位格的历史不必随一次会话消失。

当前 `0.1.0-alpha.4` 是 Windows Alpha：它已经能够完成位格初始化、模型配置、连续对话、真实流式输出、记忆与关系落账、十层上下文审阅、工具／回执审计、停止生成和崩溃恢复。它仍处于早期阶段，不应被当作无人值守的生产系统。

> 由 TzPzFMZ 发起、设计并与 AI 协作开发。

![UPSP 初始化](docs/public/assets/onboarding.png)

![UPSP 主界面](docs/public/assets/main-interface.png)

## 快速开始

1. 从 [GitHub Releases](https://github.com/TzPzFMZ/UPSP/releases) 下载 `UPSP-Setup-0.1.0-alpha.4-win-x64.exe` 和 `SHA256SUMS.txt`。
2. 核对安装器 SHA-256。
3. 安装并启动 UPSP。
4. 使用“阿廖沙”快速开始，或创建自己的位格。
5. 在“模型服务”中添加自己的模型接口和密钥；也可以先跳过模型，只查看本地界面。

当前安装器未经代码签名。Windows 可能显示“未知发布者”或 SmartScreen 警告；请只从本仓库 Releases 下载，并先核对 SHA-256。

### 当前验证环境

- Windows 11 x64
- 系统 Evergreen WebView2
- OpenAI Chat Completions 兼容协议
- OpenAI Responses 兼容协议
- Anthropic Messages 兼容协议

Windows 10、企业策略环境和所有第三方兼容接口尚未逐一正验。

## UPSP 保存什么

```text
文档\UPSP\
└─ personas\<PID>\OS\       位格、记忆、关系、Round 与单位格设置

LocalAppData\UPSP\
├─ config\                  界面设置、模型服务与密钥
└─ cache\                   WebView2、审计投影与可重建缓存
```

- 卸载或覆盖安装不会删除以上用户数据。
- 模型请求直接发往用户配置的服务；UPSP 当前不提供中转账户。
- 密钥保存在本机 ignored JSON 或进程环境变量中，当前未使用 Windows 加密存储。
- persona、Round、密钥和本机配置不属于公开源码，也不会进入 Git。

## 当前产品边界

- 只有一个活动位格和一个主对话线程。
- 多位格、分身、多线程对话和器官系统仍在开发中。
- Runtime 当前严格串行执行 `setup → reaction(0..N) → cleanup`。
- 没有自动更新、云同步、遥测或后台上传。
- “停止生成”会终止当前模型请求并执行本地结算；不支持暂停后从半截继续。
- GUI、流式排版与动效仍会继续打磨。

完整限制与本版变化见 [Alpha4 Release Notes](docs/public/releases/0.1.0-alpha.4.md)。

## 从源码构建

完整产品源码，包括 Python Runtime、TypeScript GUI、WinForms 壳和 NSIS 安装器，均在本仓库以 MIT 发布。

构建步骤见 [BUILDING.md](docs/public/BUILDING.md)。

## 旧自动版

首次公开的自动版 v1.6 已冻结归档在 [`legacy/automatic-v1.6/`](legacy/automatic-v1.6/)。它只用于历史追溯，不代表当前 Runtime、GUI 或数据合同。

## License

[MIT](LICENSE)。第三方组件及其许可证见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) 和安装目录中的 `licenses/`。

---

## English summary

UPSP is a local-first, auditable persona substrate and runtime for persistent AI identity, memory, relationships, context assembly, tool receipts, and round-level evidence.

The current `0.1.0-alpha.4` release is an early Windows 11 x64 Alpha. It ships the complete MIT-licensed source for the Python runtime, TypeScript GUI, WinForms desktop shell, and NSIS installer. Users bring their own model service and API key. Persona data stays under Documents, while machine-local settings and cache stay under LocalAppData.

The installer is currently unsigned. Single-persona and single-thread operation, incomplete provider coverage, and ongoing UI polish are known Alpha limitations. See [BUILDING.md](docs/public/BUILDING.md) and the [release notes](docs/public/releases/0.1.0-alpha.4.md).
