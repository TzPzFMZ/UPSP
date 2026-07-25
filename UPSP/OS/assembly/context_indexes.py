"""ContextAssembler 的索引装配 helper。

本模块只负责把容器、关键词、关系图和 STM/LTM 热度等索引渲染成文本。
函数继续通过 assembler 访问 adapter 与 store，不在 helper 中新增状态语义。
"""
import json
import os
import re

from constants import DREAMS_DISPLAY_LIMIT, STM_INDEX_DISPLAY_LIMIT
from assembly.context_helpers import fold_marker, slice_entries
from paths import CORE_MD, DREAMS_MD


def _stm_projection_visible(assembler, mem_id, meta):
    checker = getattr(assembler, "_memory_visible_in_stm_projection", None)
    if callable(checker):
        return checker(mem_id, meta)
    return assembler._memory_meta_visible(meta)


def build_container_index(assembler):
    """容器索引：显示容器类型与真实可打开实例。"""
    parts = ["## 容器索引"]
    try:
        from data.container_store import ContainerStore
        cs = ContainerStore()
        reg = cs.load_registry()
        for c in reg.get("containers", []):
            cid = c.get("prefix", "?")
            name = c.get("name", "")
            parts.append(f"- [{cid}] {name}（类型，不是可打开实例）")
            instances = cs.list_containers(prefix=cid)
            if not instances:
                parts.append("  - 暂无可打开实例；如需新建，请调用 memory_container_create。")
            else:
                for item in instances[:5]:
                    instance_id = item.get("id") or ""
                    title = item.get("title") or item.get("name") or ""
                    status = item.get("status") or ""
                    suffix = f" — {title}" if title else ""
                    if status:
                        suffix += f"（{status}）"
                    parts.append(f"  - 可打开实例：{instance_id}{suffix}")
                if len(instances) > 5:
                    parts.append(f"  - （另有 {len(instances) - 5} 个实例已折叠）")
    except Exception:
        parts.append("（容器注册表读取失败）")
    return "\n".join(parts)


def find_recent_child(cdir, prefix):
    """在容器目录中找最近修改的子条目。返回描述文本或空字符串"""
    import os as _os, json as _json
    # 1. 先看注册表
    reg_file = _os.path.join(cdir, "registry.json")
    children = []
    if _os.path.isfile(reg_file):
        try:
            with open(reg_file, "r", encoding="utf-8") as f:
                cr = _json.load(f)
            children = cr.get("chains") or cr.get("items") or cr.get("records") or []
        except Exception:
            pass
    if children:
        latest = children[-1]
        if isinstance(latest, dict):
            title = latest.get("title", latest.get("id", "?"))
            status = latest.get("status", "")
            return f"{title} ({status})" if status else title
        return str(latest)[:60]

    # 2. DC/EC: 检查子目录（如 DC-R34-001/）
    if prefix in ("DC", "EC"):
        try:
            subdirs = []
            for item in _os.listdir(cdir):
                ipath = _os.path.join(cdir, item)
                if _os.path.isdir(ipath) and item.startswith(prefix + "-"):
                    mtime = _os.path.getmtime(ipath)
                    subdirs.append((mtime, item))
            if subdirs:
                subdirs.sort(reverse=True)
                latest_name = subdirs[0][1]
                # 尝试读子目录内的 open.md 第一行标题
                open_md = _os.path.join(cdir, latest_name, "open.md")
                if _os.path.isfile(open_md):
                    try:
                        with open(open_md, "r", encoding="utf-8") as f:
                            first_line = f.readline().strip().lstrip("#").strip()
                        if first_line:
                            return f"{latest_name} → {first_line[:50]}"
                    except Exception:
                        pass
                return latest_name
        except Exception:
            pass

    # 3. IMM/CHR/COR/FUT: 列出目录内的 .md 文件（排除 registry/index）
    try:
        md_files = []
        for item in _os.listdir(cdir):
            ipath = _os.path.join(cdir, item)
            if _os.path.isfile(ipath) and item.endswith(".md") and item not in ("index.md", "registry.md"):
                mtime = _os.path.getmtime(ipath)
                md_files.append((mtime, item))
        if md_files:
            md_files.sort(reverse=True)
            return md_files[0][1].replace(".md", "")
    except Exception:
        pass

    # 4. 递归检查子目录中有无 .md 文件
    try:
        for item in _os.listdir(cdir):
            subpath = _os.path.join(cdir, item)
            if _os.path.isdir(subpath):
                for subitem in _os.listdir(subpath):
                    if subitem.endswith(".md"):
                        return f"{item}/{subitem}"[:60]
    except Exception:
        pass

    return ""


def build_ltm_heat_index(assembler, limit=16, offset=0):
    """LTM 热度索引：按 last_recalled_at 排序，分钟粒度相对时间"""
    parts = ["## LTM 热度索引"]
    try:
        import json as _json
        from datetime import datetime as _dt, timezone as _tz
        from paths import (LTM_FULL_META_JSON, LTM_SUMMARY_META_JSON,
                         LTM_ABSTRACT_META_JSON, LTM_PINNED_META_JSON)
        entries = []
        for meta_path in (LTM_FULL_META_JSON, LTM_SUMMARY_META_JSON,
                        LTM_ABSTRACT_META_JSON, LTM_PINNED_META_JSON):
            if os.path.isfile(meta_path):
                try:
                    with open(meta_path, "r", encoding="utf-8") as f:
                        tm = _json.load(f)
                    for mem_id, info in tm.items():
                        if str(mem_id).startswith("_") or not isinstance(info, dict):
                            continue
                        if not assembler._memory_meta_visible(info):
                            continue
                        entries.append((mem_id, info))
                except Exception:
                    pass
        if entries:
            entries.sort(key=lambda x: x[1].get("last_recalled_round") or 0, reverse=True)
            now = _dt.now(_tz.utc)
            for mem_id, info in slice_entries(entries, offset, limit):
                title = info.get("title", mem_id)
                # 优先用 last_recalled_at 时间戳，兜底用 last_recalled_round
                recalled_at = info.get("last_recalled_at")
                if recalled_at:
                    try:
                        # 支持 ISO 格式或 Unix 时间戳
                        if isinstance(recalled_at, str):
                            t = _dt.fromisoformat(recalled_at.replace("Z", "+00:00"))
                        elif isinstance(recalled_at, (int, float)):
                            t = _dt.fromtimestamp(recalled_at, tz=_tz.utc)
                        else:
                            t = None
                        if t:
                            delta = now - t
                            days = delta.days
                            if days < 0:
                                time_label = "未来"
                            elif days == 0:
                                hrs = delta.seconds // 3600
                                if hrs == 0:
                                    mins = delta.seconds // 60
                                    time_label = f"{mins}分钟前" if mins > 0 else "刚刚"
                                else:
                                    time_label = f"{hrs}小时前"
                            elif days == 1:
                                time_label = "1天前"
                            elif days < 7:
                                time_label = f"{days}天前"
                            elif days < 30:
                                time_label = f"{days // 7}周前"
                            elif days < 365:
                                time_label = f"{days // 30}月前"
                            else:
                                time_label = f"{days // 365}年前"
                        else:
                            time_label = None
                    except Exception:
                        time_label = None
                else:
                    time_label = None
                # 兜底：无时间戳则用轮号
                if time_label:
                    parts.append(f"- {mem_id} [{info.get('type', '?')}] {title} ({time_label})")
                else:
                    recalled = info.get("last_recalled_round", 0)
                    parts.append(f"- {mem_id} [{info.get('type', '?')}] {title} (R{recalled})")
            marker = fold_marker("ltm_heat", len(entries), offset, limit)
            if marker:
                parts.append(marker)
        else:
            parts.append("（无 LTM 条目）")
    except Exception:
        parts.append("（LTM 索引读取失败）")
    return "\n".join(parts)


def build_keyword_index(assembler, source, limit=8, offset=0):
    """倒排索引：从 keywords.json 读取，显示关键词→命中的记忆条目（标题+编号）"""
    parts = [f"## {source.upper()} 倒排索引"]
    try:
        import json as _json
        from paths import LTM_KEYWORDS_JSON, LTM_DIR
        from paths import STM_MEMORY_DIR, LTM_FULL_META_JSON, LTM_SUMMARY_META_JSON
        from paths import LTM_ABSTRACT_META_JSON, LTM_PINNED_META_JSON
        import os as _os

        kw_paths = {
            "stm": os.path.join(STM_MEMORY_DIR, "keywords.json"),
            "ltm": LTM_KEYWORDS_JSON,
            "skills": os.path.join(os.path.dirname(LTM_KEYWORDS_JSON), "..", "Skills", "keywords.json"),
            "relation": os.path.join(os.path.dirname(LTM_DIR), "relation", "_index", "keywords.json"),
        }
        kw_path = kw_paths.get(source)
        if not kw_path or not _os.path.isfile(kw_path):
            parts.append("（无倒排索引文件）")
            return "\n".join(parts)

        # 建立 MEM-ID → 标题 的合并查找表（STM+所有LTM层）
        title_map = {}
        meta_map = {}
        meta_files = [
            os.path.join(STM_MEMORY_DIR, "meta.json"),
            LTM_FULL_META_JSON,
            LTM_SUMMARY_META_JSON,
            LTM_ABSTRACT_META_JSON,
            LTM_PINNED_META_JSON,
        ]
        for mp in meta_files:
            if _os.path.isfile(mp):
                try:
                    with open(mp, "r", encoding="utf-8") as f:
                        tm = _json.load(f)
                    for mem_id, info in tm.items():
                        if str(mem_id).startswith("_") or not isinstance(info, dict):
                            continue
                        meta_map[mem_id] = info
                        title = info.get("title", "")
                        if title and mem_id not in title_map:
                            title_map[mem_id] = title
                except Exception:
                    pass

        with open(kw_path, "r", encoding="utf-8") as f:
            kw_data = _json.load(f)
        index_data = kw_data.get("index", kw_data)
        if isinstance(index_data, dict):
            if not index_data:
                parts.append("（无索引数据）")
            else:
                valid_relation_subjects = None
                if source == "relation":
                    valid_relation_subjects = set()
                    relation_visible_titles = {}
                    try:
                        from data.relation_store import RelationStore, relation_card_label
                        for card in RelationStore().load_registry().get("cards", []):
                            if card.get("status") == "archived":
                                continue
                            card_id = card.get("id", "")
                            card_name = card.get("name") or card_id
                            valid_relation_subjects.update(
                                x for x in (card_id, card_name) if x)
                            visible = relation_card_label(card)
                            for key in (card_id, card_name):
                                if key and visible:
                                    relation_visible_titles[key] = visible
                    except Exception:
                        valid_relation_subjects = set()
                        relation_visible_titles = {}
                # P1-9 修复：按"条目被多少关键词命中"排序（搜索引擎逻辑）
                entry_hits = {}  # mem_id → (hit_count, keyword_list)
                for kw, ids in index_data.items():
                    if not isinstance(ids, list):
                        continue
                    for mid in ids:
                        if (valid_relation_subjects is not None and
                                mid not in valid_relation_subjects):
                            continue
                        clean_mid = mid.split("[")[0] if "[" in mid else mid
                        if str(clean_mid).startswith("MEM-"):
                            mem_meta = meta_map.get(mid, meta_map.get(clean_mid, {}))
                            if mem_meta and source == "stm" and not _stm_projection_visible(
                                    assembler, clean_mid, mem_meta):
                                continue
                            if mem_meta and source != "stm" and not assembler._memory_meta_visible(mem_meta):
                                continue
                        if mid not in entry_hits:
                            entry_hits[mid] = (0, [])
                        cnt, kwlist = entry_hits[mid]
                        entry_hits[mid] = (cnt + 1, kwlist + [kw])
                sorted_entries = sorted(entry_hits.items(),
                    key=lambda x: x[1][0], reverse=True)
                if not sorted_entries:
                    parts.append("（无索引数据）")
                for mem_id, (hit_count, _keywords) in slice_entries(
                        sorted_entries, offset, limit):
                    clean_id = mem_id.split("[")[0] if "[" in mem_id else mem_id
                    title = title_map.get(mem_id, title_map.get(clean_id, ""))
                    if source == "relation":
                        clean_id = relation_visible_titles.get(
                            mem_id,
                            relation_visible_titles.get(clean_id, clean_id),
                        )
                    if title:
                        parts.append(f"- {clean_id} [{hit_count}词] {title}")
                    else:
                        parts.append(f"- {clean_id} [{hit_count}词]")
                scope_map = {
                    "skills": "skills_inverted",
                    "ltm": "ltm_inverted",
                    "stm": "stm_inverted",
                    "relation": "relation_inverted",
                }
                marker = fold_marker(
                    scope_map.get(source, f"{source}_inverted"),
                    len(sorted_entries),
                    offset,
                    limit,
                )
                if marker:
                    parts.append(marker)
    except Exception:
        parts.append("（倒排索引读取失败）")
    return "\n".join(parts)


def derive_input_keywords(assembler, state, step, mount_ids):
    """从挂载信息中推导当前轮的"焦点关键词"，供联想索引查询用。
    设计来源: 备忘录_联想联系索引体系_20260502.md §五"""
    keywords = []
    if step == "reaction" and mount_ids:
        for req in mount_ids:
            # 防御：mount_ids 可能含 str 而非 dict
            if isinstance(req, str):
                continue
            if req.get("type") == "memory":
                ids = req.get("ids", "")
                if isinstance(ids, (list, tuple)):
                    id_list = ids
                else:
                    id_list = str(ids).split(",")
                for mem_id in id_list:
                    mem_id = mem_id.strip()
                    if mem_id:
                        keywords.extend(assembler._get_keywords_for_mem_id(mem_id))
    if not keywords:
        # 兜底：从 state 中取最近记忆条目的关键词
        try:
            recent_ids = state.get("base", {}).get("meta", {}).get("recent_memory_ids", [])
            for mem_id in recent_ids[:3]:
                keywords.extend(assembler._get_keywords_for_mem_id(mem_id))
        except Exception:
            pass
    # 去重保序，上限 8 个
    seen = set()
    result = []
    for kw in keywords:
        if kw not in seen:
            seen.add(kw)
            result.append(kw)
    return result[:8]


def get_keywords_for_mem_id(mem_id):
    """反查：某个记忆条目被哪些关键词标记。"""
    try:
        import json as _json, os as _os
        from paths import KEYWORDS_JSON
        if not _os.path.isfile(KEYWORDS_JSON):
            return []
        with open(KEYWORDS_JSON, "r", encoding="utf-8") as f:
            kw_data = _json.load(f)
        index = kw_data.get("index", {})
        result = []
        for kw, ids in index.items():
            if isinstance(ids, list) and mem_id in ids:
                result.append(kw)
        return result
    except Exception:
        return []


def build_association_index(assembler, limit=16, input_keywords=None, offset=0):
    """联想索引：倒排索引的下游重排序器。
    输入关键词 → 联想集五表 + 联系集 → 合并排序 → 匹配记忆条目 → top N。
    设计来源: 备忘录_联想联系索引体系_20260502.md §五"""
    parts = ["## 联想索引"]
    try:
        import json as _json, os as _os
        from paths import (ASSOCIATION_SET_DIR, CONNECTION_SET_DIR,
                         KEYWORDS_JSON, LTM_KEYWORDS_JSON,
                         STM_MEMORY_DIR, LTM_FULL_META_JSON, LTM_SUMMARY_META_JSON,
                         LTM_ABSTRACT_META_JSON, LTM_PINNED_META_JSON)

        # 联想集表名映射
        table_files = {
            "assoc_kw_kw.json": "kw_kw",
            "assoc_kw_ifeel.json": "kw_ifeel",
            "assoc_kw_rfeel.json": "kw_rfeel",
            "assoc_ifeel_rfeel.json": "ifeel_rfeel",
            "assoc_object_rfeel.json": "object_rfeel",
        }

        # —— Step 1: 从联想集五表收集候选关键词 ——
        assoc_candidates = {}  # keyword → total_count
        for fname, src_label in table_files.items():
            fpath = _os.path.join(ASSOCIATION_SET_DIR, fname)
            if not _os.path.isfile(fpath):
                continue
            with open(fpath, "r", encoding="utf-8") as f:
                data = _json.load(f)
            for pair_key, count in data.items():
                parts_kv = pair_key.split("|||")
                if len(parts_kv) < 2:
                    continue
                a, b = parts_kv[0], parts_kv[1]
                if input_keywords:
                    # 输入驱动：只收集与输入关键词共现的词
                    if a in input_keywords and b not in input_keywords:
                        assoc_candidates[b] = assoc_candidates.get(b, 0) + count
                    elif b in input_keywords and a not in input_keywords:
                        assoc_candidates[a] = assoc_candidates.get(a, 0) + count
                else:
                    # 无输入关键词：收集全部词对（兜底）
                    assoc_candidates[a] = assoc_candidates.get(a, 0) + count
                    assoc_candidates[b] = assoc_candidates.get(b, 0) + count

        def add_mem_match(mem_id, keyword, score):
            mem_id = str(mem_id or "").strip()
            if not mem_id:
                return
            existing_kws, existing_score = mem_matches.get(mem_id, ([], 0))
            if keyword and keyword not in existing_kws:
                existing_kws.append(keyword)
            mem_matches[mem_id] = (existing_kws, existing_score + score)

        # —— Step 2: 从联系集收集候选关键词与直接记忆落点 ——
        contact_candidates = {}  # keyword → contact_count
        mem_matches = {}  # mem_id → (matched_keywords, total_score)
        input_set = set(input_keywords or [])
        contact_path = _os.path.join(CONNECTION_SET_DIR, "pending.jsonl")
        if _os.path.isfile(contact_path):
            with open(contact_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = _json.loads(line)
                    except _json.JSONDecodeError:
                        continue
                    wa = entry.get("word_a", "")
                    wb = entry.get("word_b", "")
                    ea = entry.get("entry_a", "")
                    eb = entry.get("entry_b", "")
                    if not wa or not wb or not ea or not eb:
                        continue
                    if input_set:
                        if wa in input_set and wb not in input_set:
                            contact_candidates[wb] = contact_candidates.get(wb, 0) + 1
                            add_mem_match(eb, wb, 2)
                        elif wb in input_set and wa not in input_set:
                            contact_candidates[wa] = contact_candidates.get(wa, 0) + 1
                            add_mem_match(ea, wa, 2)
                        elif wa in input_set and wb in input_set:
                            add_mem_match(ea, wa, 2)
                            add_mem_match(eb, wb, 2)
                    else:
                        contact_candidates[wa] = contact_candidates.get(wa, 0) + 1
                        contact_candidates[wb] = contact_candidates.get(wb, 0) + 1
                        add_mem_match(ea, wa, 2)
                        add_mem_match(eb, wb, 2)

        # —— Step 3: 合并排序（联想集分 + 联系集分×2）——
        scored_keywords = {}
        for kw, score in assoc_candidates.items():
            if kw:  # 过滤空关键词
                scored_keywords[kw] = score
        for kw, contact_score in contact_candidates.items():
            if kw:
                scored_keywords[kw] = scored_keywords.get(kw, 0) + contact_score * 2

        sorted_kws = sorted(scored_keywords.items(), key=lambda x: -x[1])

        # —— Step 4: 用排好序的关键词匹配记忆倒排索引 → 记忆条目 ——
        kw_sources = [KEYWORDS_JSON, LTM_KEYWORDS_JSON]

        for kw_path in kw_sources:
            if not _os.path.isfile(kw_path):
                continue
            with open(kw_path, "r", encoding="utf-8") as f:
                idx = _json.load(f)
            index_data = idx.get("index", idx)
            for kw, ids in index_data.items():
                if not isinstance(ids, list):
                    continue
                if kw in scored_keywords:
                    score = scored_keywords[kw]
                    for mid in ids:
                        add_mem_match(mid, kw, score)

        # —— Step 5: 按总分排序，输出记忆条目 ——
        sorted_mems = sorted(mem_matches.items(), key=lambda x: -x[1][1])

        # 标题查找表
        title_map = {}
        meta_map = {}
        meta_files = [
            _os.path.join(STM_MEMORY_DIR, "meta.json"),
            LTM_FULL_META_JSON, LTM_SUMMARY_META_JSON,
            LTM_ABSTRACT_META_JSON, LTM_PINNED_META_JSON,
        ]
        for mp in meta_files:
            if _os.path.isfile(mp):
                with open(mp, "r", encoding="utf-8") as f:
                    tm = _json.load(f)
                for mid, info in tm.items():
                    if str(mid).startswith("_") or not isinstance(info, dict):
                        continue
                    meta_map[mid] = info
                    title = info.get("title", "")
                    if title and mid not in title_map:
                        title_map[mid] = title

        shown = 0
        for mem_id, (matched_kws, total_score) in slice_entries(
                sorted_mems, offset, limit):
            if shown >= limit:
                break
            clean_id = mem_id.split("[")[0] if "[" in mem_id else mem_id
            if str(clean_id).startswith("MEM-"):
                mem_meta = meta_map.get(mem_id, meta_map.get(clean_id, {}))
                if mem_meta and not _stm_projection_visible(
                        assembler, clean_id, mem_meta):
                    continue
            title = title_map.get(mem_id, title_map.get(clean_id, ""))
            kw_str = ", ".join(matched_kws[:3])
            if title:
                parts.append(f"- {clean_id} [{total_score}] {title} (联想: {kw_str})")
            else:
                parts.append(f"- {clean_id} [{total_score}] (联想: {kw_str})")
            shown += 1

        if shown == 0 and (sorted_kws or mem_matches):
            parts.append("（无高置信记忆条目）")

        marker = fold_marker(
            "association", len(sorted_mems), offset, limit)
        if marker:
            parts.append(marker)
        if not sorted_kws and not mem_matches:
            parts.append("（联想集/联系集数据为空，冷启动中）")
        return "\n".join(parts)
    except Exception:
        return ""


def build_explorer_index(assembler):
    parts = ["## EXPLORER（索引区）"]
    try:
        from data.container_store import ContainerStore
        reg = ContainerStore().load_registry()
        parts.append("### 容器")
        for i, c in enumerate(reg.get("containers", [])):
            if i >= 16:
                parts.append(f"（另有 {len(reg['containers']) - 16} 个容器已折叠）")
                break
            cid = c.get("id", c.get("prefix", "?"))
            parts.append(
                f"- [{cid}] {c.get('name', '')} "
                f"({c.get('status', 'open')})")
    except Exception:
        parts.append("（容器注册表读取失败）")
    try:
        from data.relation_store import RelationStore
        reg = RelationStore().load_registry()
        parts.append("### 关系")
        for card in reg.get("cards", []):
            parts.append(
                f"- [{card.get('category', '?')}] {card.get('id', '')} "
                f"— {card.get('name', '')}")
    except Exception:
        pass
    return "\n".join(parts)

# ==============================================================
# STM 热度索引（高频层）
# ==============================================================


def build_stm_heat_index(assembler, limit=STM_INDEX_DISPLAY_LIMIT, offset=0):
    parts = ["## STM 索引"]
    heat, meta = {}, {}
    try:
        from data.memory_heat import MemoryHeat
        heat = MemoryHeat().load_heat()
    except Exception:
        pass
    try:
        from data.memory_store import MemoryStore
        meta = MemoryStore().load_meta()
    except Exception:
        pass
    entries = heat.get("entries", {})
    if entries:
        visible_entries = [
            (mem_id, info)
            for mem_id, info in entries.items()
            if _stm_projection_visible(assembler, mem_id, meta.get(mem_id, {}))
        ]
        sorted_entries = sorted(visible_entries,
                                key=lambda x: x[1].get("H", 0), reverse=True)
        displayed = slice_entries(sorted_entries, offset, limit)
        parts.append("### 热度索引")
        for mem_id, info in displayed:
            h = info.get("H", 0)
            zone = info.get("zone", "衰减")
            title = meta.get(mem_id, {}).get("title", "")
            heat_lock = " [锁热]" if info.get("heat_locked") else ""
            heat_label = "热" if h >= 70 else ("温" if h >= 40 else "凉")
            parts.append(f"- {mem_id} [{zone}|{heat_label}]{heat_lock} {title}")
        marker = fold_marker("stm_heat", len(sorted_entries), offset, limit)
        if marker:
            parts.append(marker)
    else:
        parts.append("### 热度索引\n（无 STM 条目）")
    parts.append("### 梦境索引")
    if os.path.isfile(DREAMS_MD):
        try:
            with open(DREAMS_MD, "r", encoding="utf-8") as f:
                content = f.read()
            dream_titles = re.findall(r'^## (.+)$', content, re.MULTILINE)
            if dream_titles:
                recent = dream_titles[-DREAMS_DISPLAY_LIMIT:]
                recent.reverse()
                for t in recent:
                    parts.append(f"- {t}")
            else:
                parts.append("（无梦境记录）")
        except Exception:
            parts.append("（读取失败）")
    else:
        parts.append("（无梦境文件）")
    return "\n".join(parts)

# ==============================================================
# 已挂载 CONTENT（反应步）
# ==============================================================
