# 容器系统参数表·协议层

> 消费方式：脚本查表——容器创建/状态变更/索引刷新时查本文件取参数
> 注入模块：不直接注入（脚本+LLM参考）
> 触发：容器生命周期事件

---

## 一、九容器总览

| 容器 | 前缀 | 位置 | 注册表 | 状态机 | 挂靠 | 说明 |
|------|------|------|--------|--------|------|------|
| 辩证链 | DC- | LTM/Dialectics/ | ✅ registry.json | ongoing→suspended→concluded | **必挂** | 推理链，链内笔记必须挂靠记忆 |
| 事件链 | EC- | LTM/Events/ | ✅ registry.json | active→interrupted→restarted→ended→cancelled | **必挂** | 事件追踪，链内笔记必须挂靠记忆 |
| 项目 | PRJ- | LTM/Projects/ | ✅ 每项目独立 | active→paused→ended | **必挂** | 阶段文件必须挂靠记忆条目 |
| 技能 | SKL- | LTM/Skills/ | ✅ registry.json | active→expired→planned | **必挂** | 通过记忆条目桥接链/项目 |
| 免疫 | IMM- | LTM/Immune/ | ✅ registry.json | active_threat→monitoring→resolved→acquired | 可挂 | 大部分内容脚本自动写入 |
| 编年史 | CHR- | LTM/Chronicle/ | ❌ 无 | — | 不挂 | 纯目录，无ID |
| 语料库 | COR- | LTM/Corpus/ | ❌ 无 | — | 不挂 | 纯目录，无ID |
| 未来 | FUT- | LTM/Future/ | ✅ registry.json | planned→in_progress→completed→abandoned | **必挂** | 内容条目必须链接记忆 |
| 迭代 | ITR- | LTM/Iteration/ | ✅ registry.json | collecting→planned→training→deployed→retired | 可挂 | 训练材料脚本自动写入 |

WB（工作台）是调度台非容器，不进 container_registry。

### 主动建链判定

当证据已经形成持久关系时，模型应按永久 `containers.md` 主动创建、复用或续写：可复用推演与判断修正进 DC，同一事件的有序状态变化进 EC，跨轮目标与交付进 PRJ，待未来核验的预测、承诺或计划进 FUT。单一孤立事实、一次性草稿推理、临时任务步骤和无证据关系不得建链。同一种语义职责复用同类型主链；不同职责可由不同类型并存，同一 `MEM-*` 可桥接多种真实关系。`memory_route_pending` 可 `deferred/open` 只是 Runtime 防死循环边界，不改变永久行为合同。

---

## 二、注册表必选字段（8）

```json
{
  "id": "DC-3",
  "type": "dialectic",
  "title": "Metabolism ≠ Labor",
  "status": "ongoing",
  "created_at": "2026-04-13T01:04:15+08:00",
  "updated_at": "2026-04-13T01:44:22+08:00",
  "entries": [],
  "tags": []
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | 容器编号（前缀+序号） |
| type | string | 容器类型英文标识 |
| title | string | 标题（可中文） |
| status | string | 状态机当前状态（英文） |
| created_at | ISO8601+偏移 | 创建时间 |
| updated_at | ISO8601+偏移 | 最近更新时间 |
| entries | string[] | 关联记忆条目ID列表 |
| tags | string[] | 语义标签 |

字段英文化规范：字段名英文、系统字段值英文（status:"ongoing" 而非"继续"）、title等自然语言可用中文。

---

## 三、各容器特有参数

### DC- 辩证链

| 参数 | 值 |
|------|-----|
| ID格式 | DC-{链序号} / DC-{链序号}-{笔记号} |
| 特有字段 | source_round（开链轮次）、conclusion（链结论） |
| 文件结构 | registry.json + open.md + closed.md |
| 状态机 | ongoing → suspended → concluded |

### EC- 事件链

| 参数 | 值 |
|------|-----|
| ID格式 | EC-{链序号} / EC-{链序号}-{笔记号} |
| 特有字段 | severity（1-5）、resolution（解决记录） |
| 文件结构 | registry.json + open.md + closed.md |
| 状态机 | active → interrupted → restarted → ended → cancelled |

### PRJ- 项目

| 参数 | 值 |
|------|-----|
| ID格式 | PRJ-{date}-{seq}（如PRJ-20260417-01） |
| 特有字段 | deadline、milestones、progress |
| 文件结构 | 每项目独立文件夹（registry.json + plan.md + phases/_index.md + notes.md + materials/ + drafts/） |
| 状态机 | active → paused → ended |

### SKL- 技能

| 参数 | 值 |
|------|-----|
| ID格式 | SKL-{category}-{skill-name} |
| 当前字段 | id/type/prefix/name/title/status/category/created_at/updated_at/entries/tags/linked_memories/path |
| 五分类 | habits / procedures / licenses / patterns / reflexes |
| 文件结构 | registry.json + keywords.json + 各分类/{skill-name}/(card.md+changelog.md) |
| 状态机 | active → expired → planned |
| 当前创建分类 | procedures / patterns；licenses/habits/reflexes 只保留目录兼容，不开放创建 |
| 创建入口 | 真实公共 MEM 回执后调用 memory_container_create(container_type=SKL, target_file=card.md) |
| 排序字段 | Skills registry 当前顺序 |
| 当前装配 | 进入技能/容器索引；`container_read` 后目标文件进入统一常驻正文；不进定期层 |

Seed 不规定技能卡成熟度模板、自动采用结算或投影生命周期。`card.md` 保存当前可复用方法，`changelog.md` 保存通用写入账本；更细的技能代谢等待 Arbor 器官另行设计。

### IMM- 免疫

| 参数 | 值 |
|------|-----|
| ID格式 | IMM-{文件名}-{序号}（如IMM-active-3） |
| 特有字段 | severity(1-5)、category(threat/health/maintenance) |
| 8个固定md | birth/chronic/transplant/surgery/active/resolved/acquired/alerts |
| 状态机 | active_threat → monitoring → resolved → acquired |

### CHR- 编年史

| 参数 | 值 |
|------|-----|
| 注册表 | 无（纯目录） |
| 状态机 | 无 |
| 子目录 | rhythms/ daily/ weekly/ monthly/ quarterly/ yearly/ |
| 命名 | R-{日期}-{序号}.md / D-{日期}.md / W-{周标识}.md / M-{月份}.md / Q-{季度}.md / Y-{年份}.md |
| 压缩链路 | 节志(512字)→日志(×128字)→周志(×0.3)→月志(×0.3)→季志(×0.3)→年志(×0.3) |

### COR- 语料库

| 参数 | 值 |
|------|-----|
| 注册表 | 无（纯目录） |
| 状态机 | 无 |
| 二分区 | public/（公开语料） + private/（隐私语料） |
| public子目录 | rhythms/ daily/ weekly/ monthly/ quarterly/ yearly/ |
| 冷备 | Attic/（yearly 满3年后按 `{年份}/attic-{年份}.{jsonl,md}` 成对迁入） |

### FUT- 未来

| 参数 | 值 |
|------|-----|
| ID格式 | FUT-{category}-{seq} |
| 特有字段 | subtype(objective/ideal/dream/plan/prediction)、source（来源链ID） |
| 三个平级md | objectives.md / plans.md / predictions.md |
| 状态机 | planned → in_progress → completed / abandoned |

### ITR- 迭代

| 参数 | 值 |
|------|-----|
| ID格式 | ITR-{date}-{seq} |
| 子目录 | Lineage/ Blueprints/ Raw/ Materials/ Logs/ |
| Raw三子集 | Tacit/(pending+processed) + Association/(五张计数表) + Connection/(pending+processed) |
| Materials | Evolution/（历史进化集，只读保留；当前 Seed 不再生成） |
| 状态机 | collecting → planned → training → deployed → retired |
| Base版默认 | collecting 状态（正在收集训练材料） |

---

## 四、联想集五张计数表

| 表名 | 维度 |
|------|------|
| assoc_kw_kw.json | 关键词 × 关键词 |
| assoc_kw_ifeel.json | 关键词 × 交互感受词 |
| assoc_kw_rfeel.json | 关键词 × 关系感受词 |
| assoc_ifeel_rfeel.json | 交互感受词 × 关系感受词 |
| assoc_object_rfeel.json | 交互对象 × 关系感受词 |

联想集无 pending/processed——五张计数表本身是累加的。默契集和联系集需要 pending/processed 分离。

---

## 五、索引体系

### 三层同构

| | 记忆条目 | 工作容器 | 技能 |
|---|---|---|---|
| 索引层 | index.md | LTM/index.md | Skills/index.md |
| 元数据层 | meta.json | container_registry.json（9类类型注册表）+ 各类型/实例 registry/meta | Skills/registry.json |
| 正文层 | full.md/summary.md/abstract.md | notes.md/plan.md等 | card.md |

统一生成流程：类型注册表确定 9 类容器边界，各类型/实例 registry/meta 记录实例事实，维护脚本刷新 LTM/index.md（物化视图）→ 提取脚本渲染 → 模块注入。`LTM/container_registry.json` 不再混入容器实例条目。

### 总索引排序

- 容器总索引：按类型注册表声明顺序与各类型 registry 实例事实生成（稳定）
- 技能索引：按熟练度排序
- 记忆条目：按热度排序

### 总索引EXPLORER加载

| 场景 | 加载内容 | 触发条件 |
|------|---------|---------|
| 浏览容器 | 舱段一级视图（各容器概览+最新实例预览） | 皮层默认可见 |
| 读取容器（有注册表） | 舱段二级视图（单容器全部条目索引）+ 按状态加载正文 | 使用 `container_read`；成功后目标文件加入统一常驻清单，下一 Frame 读取完整当前真源 |
| 浏览 CHR/COR | 列目录——文件夹本身就是索引 | 当前不作为普通 `container_read` 或同步写入目标，按各自归档流程处理 |

---

## 六、容器与记忆的关联

- 记忆条目通过 linked_containers 字段引用容器编号（单向硬链接）
- 容器注册表通过 entries 字段列出关联记忆条目ID
- ×条目进 Backup 不删除——链上记忆条目ID保留
- 关系是主体，节点不是
