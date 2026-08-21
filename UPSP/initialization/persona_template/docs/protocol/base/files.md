# 文件系统参数表

> 消费方式：脚本查表——文件操作脚本查本文件取命名规则、压缩参数、大小限制、生命周期事件、索引格式
> 注入模块：不直接注入（脚本内部消费）
> 触发：文件创建/升降格/节律点清理

---

## 一、目录结构

persona/ 是内环境运行时目录。core.md 为身份锚点，state.json 为体征表。STM 下分 context（三步装配区，每步一个子目录含 step.json、step.md 和 manifest.json；cache/ 下有 now_cache.jsonl 与 lately_cache.jsonl 主源；round/ 下有 round_{N}.jsonl 轮审计事件流）、workbench（WB 调度台，含 status.json 和三区流转目录）、buffer（缓存区，含 raw_log 原始语料缓冲、感受缓冲、安全粗筛暂存和 state 热备）、health（健康监测，按版本分层）以及尚未接入自动生命周期的 media 目标目录。A 轨语料被 lately 接纳时镜像进 `STM/buffer/raw_log.jsonl`；主轴节律轮把它归档进 `LTM/Corpus/public/rhythms/*.jsonl`，同名 `.md` 为派生阅读副本。程序目录中的 `OS/audit/round.html` 是轮审计 HTML 入口，由本地只读服务加载位格 Round JSONL 或 LocalAppData 中的 `round-index.js` 与 `round-data/round_{N}.js` 可再生投影；投影不是运行依赖，也不写回安装目录。

LTM 下分 container_registry.json（容器类型注册表）、index.md（工作容器总索引）、Memory（分 Full/Summary/Abstract/Pinned/Backup 五层，每层含 index.md、meta.json、media/）以及 9 个工作容器目录（DC/EC/PRJ/SKL/IMM/CHR/COR/FUT/ITR）。relation 按对象分目录。

`UPSP/initialization/persona_template/` 与活体目录结构对应，包含完整但未绑定身份的 `core.md` 表单、中性 `state.json`、空白 birth、通用 Registry、空账本、规则文档原件和空目录占位，可进 Git；它不含自分关系或运行证据。初始化器只在同卷临时副本中填充这些模板，完整校验后原子落位为 Windows“文档”已知文件夹下当前 PID 的 `OS/persona/`（即 `paths.PERSONA_DIR`）。同一活动实例的 `OS/files` 是 persona/ 外部资料暂存区，固定子目录为 `raw/`、`media_raw/`、`clips/`、`archive/`，不建立全局索引。

模型侧只读虚拟根为 `persona://`：根层列出 PID，`persona://active/...` 指向当前 PID，`persona://<PID>/<instance_id>/...` 指向指定分身或 `meta` 共享目录。只允许 `file_read/file_glob/file_grep` 与只读子 agent 范围解析；返回模型的路径保持别名，宿主绝对路径只进私有审计。显式工程 sandbox 的 `read_paths` 仍是更外层约束。`*.private.md`、含 private entry 的记忆 `meta.json`、密钥类路径及 `.git` 不可读；搜索跳过受限项并把覆盖标记为不完整。

---

## 二、命名规则

记忆条目 ID 为 8 位十六进制 TTTTTNNN——前 5 位是当日零点起秒数，后 3 位是随机数。工作容器 ID 必须来自容器索引中真实列出的实例；DC、EC、PRJ、FUT 等只是类型前缀。CHR 和 COR 无注册表无 ID 无状态机。WB 工单编号为 T-YYYYMMDD-序号。媒体文件为条目 ID 前 4 位加序号加扩展名（如 0E6F_01.jpg）。节志为年份加周期号。

---

## 三、大小限制

记忆条目正文按层级有字符上限：F 级≤2048，S 级≤512，A 级≤128，P 级单条≤8192 总量≤65536。`resident_list` / 常驻清单字符上限 65536，挂接项由反应步只读内容工具声明；`instant_list` / 即时清单只在本轮或本步窗口保留。Dreams 上限 8192。now_cache 是下一 provider Frame 的完整待消费包，不设字符水位或裁剪量；lately_cache 以三步主模型共同逻辑窗口 90% 为压力线，善后只冻结 v3 账本，下一自然轮 Reaction 渐进压缩，整周期结束后才原子改写。默认保护最近16次用户输入原文，分片/周期默认目标为12.5%/25%。旧 now 水位、字符兜底、Cleanup FIFO 与 8/32/8、40/8 轮履带只作历史迁移说明。quarantine_buffer FIFO 32 条。raw_log 不随热缓存压缩，只在主轴节律轮完成 Corpus 节归档后清空。

当前真源参数为 `lately.pressure_ratio=0.9`、`lately.protected_interaction_count=16`、`lately.semantic_summary_ratio=0.125`、`lately.cycle_target_ratio=0.25`、`lately.batch_source_chars=65536`。

---

## 四、媒体压缩

本节是未来 GUI／多模态入口接通后的目标标准。当前 Seed 没有图片输入、provider-native image block、图片转换或随记忆升降格的媒体事务，也不依赖 Pillow。目标仍是图片片段默认 JPG，并跟随记忆层级压缩；在整条链路实现前不得声称图片交互已经可用。

---

## 五、生命周期

记忆条目创建时在对应层级写入正文、meta.json 并更新 index.md。升格、降格、钉选、召回、冷备和删除的文本记忆生命周期按当前实现执行；本节出现的媒体搬运、重压缩和同步删除均为待实现目标，不属于当前事务。

缓冲文件各有独立的写入和清理时机。ABC 轨只决定被消费后的去向：A 轨随成功 provider Frame 从 now 迁入 lately 并镜像 raw；B 轨 material 只从 now 迁入 lately；C 轨只在目标调用期间保留并在调用后清除。provider 失败不推进，Round closeout 排空残余 A/B 并清除 C。lately 达到真实窗口压力线后，善后只冻结 v3 压缩账本；下一自然轮 Reaction 分批处理，达到周期目标后才原子改写，受保护用户输入不可强删。`context_buffer.json`、`near_cache.*`、`remote_index.json` 和 `remote_blocks/` 已退役；`STM/buffer/raw_log.jsonl/.md` 是活动原始语料缓冲。quarantine_buffer 安全粗筛标记时写入。安全裁决与拒绝进入 processor receipt、tool transaction audit 和 Round JSONL，不另建 `security_events.md`；系统健康故障走 health/base/alerts.md，同时可写 A 轨 `kind=fault_note` 语料块进入履带。只读工具结果按两条链路进入 now：执行状态、范围、游标和失败原因写 A 轨 `kind=tool_fact`，下一成功 Frame 后进入 lately 与 raw_log；文件正文、网页正文、候选列表和索引展开内容写 B 轨 `kind=material` 或既有 CONTENT 挂载。C 轨临时材料在目标调用完成后清除；A/B 工具事实不会因材料处理被删除，历史 B 轨 material 则可随所属交互段压入 `interaction_summary`，无交互前缀才生成 `cache_summary`。当前执行/读取/写入仍以本轮工具结果、processor receipt 或 round audit 为准。不再维护独立工具调用台账文件。state_backups.jsonl 由善后步成功收尾后追加完整 state 快照，默认 FIFO 8。LocalAppData 审计缓存中的 `round-index.js` 与 `round-data/` 跟随 round JSONL 生成和裁剪，可删除后重建，不入 Git。

OS/files 生命周期：`raw/` 与 `media_raw/` 跟资料输入 FIFO 删除联动；`clips/` 与 `archive/` 不挂记忆元数据，由月度节律整理清理。路径、URL 和外部文件引用写在记忆正文或工作容器正文里，不新增记忆条目 metadata 字段。
