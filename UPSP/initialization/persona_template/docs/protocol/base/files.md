# 文件系统参数表

> 消费方式：脚本查表——文件操作脚本查本文件取命名规则、压缩参数、大小限制、生命周期事件、索引格式
> 注入模块：不直接注入（脚本内部消费）
> 触发：文件创建/升降格/节律点清理

---

## 一、目录结构

persona/ 是内环境运行时目录。core.md 为身份锚点，state.json 为体征表。STM 下分 context（三步装配区，每步一个子目录含 step.json、step.md 和 manifest.json；cache/ 下有 now_cache.jsonl 与 lately_cache.jsonl 主源；round/ 下有 round_{N}.jsonl 轮审计事件流）、workbench（WB 调度台，含 status.json 和三区流转目录）、buffer（缓存区，含 raw_log 原始语料缓冲、感受缓冲、安全粗筛暂存和 state 热备）、health（健康监测，按版本分层）以及尚未接入自动生命周期的 media 目标目录。A 轨语料被 lately 接纳时镜像进 `STM/buffer/raw_log.jsonl`；主轴节律轮把它归档进 `LTM/Corpus/public/rhythms/*.jsonl`，同名 `.md` 为派生阅读副本。程序目录中的 `OS/audit/round.html` 是轮审计 HTML 入口，由本地只读服务加载位格 Round JSONL 或 LocalAppData 中的 `round-index.js` 与 `round-data/round_{N}.js` 可再生投影；投影不是运行依赖，也不写回安装目录。

LTM 下分 container_registry.json（容器类型注册表）、index.md（工作容器总索引）、Memory（分 Full/Summary/Abstract/Pinned/Backup 五层，每层含 index.md、meta.json、media/）以及 9 个工作容器目录（DC/EC/PRJ/SKL/IMM/CHR/COR/FUT/ITR）。relation 按对象分目录。

`UPSP/initialization/persona_template/` 与活体目录结构对应，包含完整但未绑定身份的 `core.md` 表单、中性 `state.json`、空白 birth、通用 Registry、空账本、规则文档原件和空目录占位，可进 Git；它不含自分关系或运行证据。初始化器只在同卷临时副本中填充这些模板，完整校验后原子落位为 Windows“文档”已知文件夹下当前 PID 的 `OS/persona/`（即 `paths.PERSONA_DIR`）。同一活动实例的 `OS/files` 是 persona/ 外部资料暂存区，固定子目录为 `raw/`、`media_raw/`、`clips/`、`archive/`，不建立全局索引。

---

## 二、命名规则

记忆条目 ID 为 8 位十六进制 TTTTTNNN——前 5 位是当日零点起秒数，后 3 位是随机数。工作容器 ID 必须来自容器索引中真实列出的实例；DC、EC、PRJ、FUT 等只是类型前缀。CHR 和 COR 无注册表无 ID 无状态机。WB 工单编号为 T-YYYYMMDD-序号。媒体文件为条目 ID 前 4 位加序号加扩展名（如 0E6F_01.jpg）。节志为年份加周期号。

---

## 三、大小限制

记忆条目正文按层级有字符上限：F 级≤2048，S 级≤512，A 级≤128，P 级单条≤8192 总量≤65536。`resident_list` / 常驻清单字符上限 65536，挂接项由反应步只读内容工具声明；`instant_list` / 即时清单只在本轮或本步窗口保留。Dreams 上限 8192。now_cache 使用 `now.budget_chars=65536` 高水位和 `now.trim_chars=16384` 批量回落；lately_cache 使用 `lately.budget_chars=262144` 高水位和 `lately.trim_chars=65536` 批量删除。两者都按完整语料块处理，最新 now 块不硬截断；旧 8/32/8 轮窗口与 40/8 轮履带只作历史迁移说明。quarantine_buffer FIFO 32 条。raw_log 不随热缓存淘汰，只在主轴节律轮完成 Corpus 节归档后清空。

---

## 四、媒体压缩

本节是未来 GUI／多模态入口接通后的目标标准。当前 Seed 没有图片输入、provider-native image block、图片转换或随记忆升降格的媒体事务，也不依赖 Pillow。目标仍是图片片段默认 JPG，并跟随记忆层级压缩；在整条链路实现前不得声称图片交互已经可用。

---

## 五、生命周期

记忆条目创建时在对应层级写入正文、meta.json 并更新 index.md。升格、降格、钉选、召回、冷备和删除的文本记忆生命周期按当前实现执行；本节出现的媒体搬运、重压缩和同步删除均为待实现目标，不属于当前事务。

缓冲文件各有独立的写入和清理时机。A 轨语料沿 now→lately 滚动，并在被 lately 接纳时镜像进 raw_log；B 轨 material 只按 now→lately 滚动；now 触顶后最早完整 A/B 块立即滚入 lately，lately 触顶后删除最旧完整块但不回删 raw_log。C 轨只在目标调用期间保留，不参与任一缓存水位。`context_buffer.json`、`near_cache.*`、`remote_index.json` 和 `remote_blocks/` 已退役；`STM/buffer/raw_log.jsonl/.md` 是活动原始语料缓冲。quarantine_buffer 安全粗筛标记时写入。安全裁决与拒绝进入 processor receipt、tool transaction audit 和 Round JSONL，不另建 `security_events.md`；系统健康故障走 health/base/alerts.md，同时可写 A 轨 `kind=fault_note` 语料块进入履带。只读工具结果按两条链路进入 now：执行状态、范围、游标和失败原因写 A 轨 `kind=tool_fact`，允许水位后进入 lately 与 raw_log；文件正文、网页正文、候选列表和索引展开内容写 B 轨 `kind=material` 或既有 CONTENT 挂载。C 轨临时材料在目标调用完成后清除；不删除 A/B 工具事实，也不为 material 生成 `kind=cache_summary` 历史工具摘要。当前执行/读取/写入仍以本轮工具结果、processor receipt 或 round audit 为准。不再维护独立工具调用台账文件。state_backups.jsonl 由善后步成功收尾后追加完整 state 快照，默认 FIFO 8。LocalAppData 审计缓存中的 `round-index.js` 与 `round-data/` 跟随 round JSONL 生成和裁剪，可删除后重建，不入 Git。

OS/files 生命周期：`raw/` 与 `media_raw/` 跟资料输入 FIFO 删除联动；`clips/` 与 `archive/` 不挂记忆元数据，由月度节律整理清理。路径、URL 和外部文件引用写在记忆正文或工作容器正文里，不新增记忆条目 metadata 字段。
