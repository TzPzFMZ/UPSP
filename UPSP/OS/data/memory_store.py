"""
记忆条目正文读写 — memory.md / meta.json / index.md
DDS §9 记忆体系

职责：
  - 记忆条目 CRUD（memory.md 追加式写入）
  - 元数据管理（meta.json）
  - 索引行维护（index.md）

注意：heat.json 在 memory_heat.py，倒排索引在 memory_index.py
"""
import json
import os
import re
from datetime import datetime

from data.atomic_write import atomic_write_json
from utils.content_ranges import apply_explicit_range, range_kwargs_from_request
from paths import (
    MEMORY_MD, META_JSON, INDEX_MD,
    LTM_FULL_FULL_MD, LTM_FULL_META_JSON,
    LTM_SUMMARY_SUMMARY_MD, LTM_SUMMARY_META_JSON,
    LTM_ABSTRACT_ABSTRACT_MD, LTM_ABSTRACT_META_JSON,
    LTM_PINNED_PINNED_MD, LTM_PINNED_META_JSON,
)
from schemas.memory import (
    default_meta_entry, default_meta_json,
    MEMORY_ENTRY_TEMPLATE, INDEX_HEADER, INDEX_SEPARATOR,
)
from errors import EntryNotFoundError, WriteError, ReadError
from constants import TZ_SHANGHAI


def _overview_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if len(text) > 128:
        raise ValueError("current_overview_too_long")
    return text


def _dream_text(value):
    return "是" if bool(value) else "否"


def _body_limit_for_weight(weight):
    if weight >= 5:
        return 2048
    if weight >= 3:
        return 512
    return 128


class MemoryStore:
    """记忆条目正文 + 元数据 + 索引 的读写管理"""

    def __init__(self):
        pass

    def _active_read_layers(self):
        """STM + LTM active layers visible to memory_content_read."""
        return [
            ("STM", META_JSON, MEMORY_MD),
            ("LTM/Full", LTM_FULL_META_JSON, LTM_FULL_FULL_MD),
            ("LTM/Summary", LTM_SUMMARY_META_JSON, LTM_SUMMARY_SUMMARY_MD),
            ("LTM/Abstract", LTM_ABSTRACT_META_JSON, LTM_ABSTRACT_ABSTRACT_MD),
            ("LTM/Pinned", LTM_PINNED_META_JSON, LTM_PINNED_PINNED_MD),
        ]

    def _read_json_file(self, path):
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(path, cause=e)
        return data if isinstance(data, dict) else {}

    def _read_entry_from_file(self, path, mem_id):
        if not os.path.isfile(path):
            raise EntryNotFoundError(mem_id, f"{os.path.basename(path)} 不存在")
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError as e:
            raise ReadError(path, cause=e)

        clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id
        marker = f"\n## MEM-{clean_id}"
        start = content.find(marker)
        if start == -1:
            if content.startswith(f"## MEM-{clean_id}"):
                start = 0
            else:
                raise EntryNotFoundError(mem_id)
        else:
            start += 1

        end = content.find("\n## ", start + 1)
        if end == -1:
            end = len(content)
        return content[start:end].strip()

    def _private_entry_paths(self, mem_id, directory=None):
        """Return private files that actually contain mem_id."""
        if directory is None:
            directories = list(dict.fromkeys(
                os.path.dirname(body_path)
                for _layer, _meta_path, body_path in self._active_read_layers()
            ))
        else:
            directories = [directory]
        matches = []
        # ponytail: linear file scan is enough for Seed; add a runtime cache only
        # if profiling shows private-file lookup is material.
        for root in directories:
            if not os.path.isdir(root):
                continue
            for name in sorted(os.listdir(root)):
                if not name.endswith(".private.md"):
                    continue
                path = os.path.join(root, name)
                try:
                    self._read_entry_from_file(path, mem_id)
                except Exception:
                    continue
                matches.append(path)
        return matches

    def private_subjects_for_memory(self, mem_id):
        """Derive privacy owners from the private files containing mem_id."""
        suffix = ".private.md"
        return list(dict.fromkeys(
            os.path.basename(path)[:-len(suffix)]
            for path in self._private_entry_paths(mem_id)
        ))

    def _private_entry_path(self, mem_id, directory=None):
        matches = self._private_entry_paths(mem_id, directory=directory)
        if not matches:
            return None
        if len(matches) != 1:
            raise ValueError("ambiguous_private_memory_owner")
        return matches[0]

    def _resolve_read_target(self, mem_id):
        for layer, meta_path, body_path in self._active_read_layers():
            meta = self._read_json_file(meta_path)
            entry = meta.get(mem_id)
            if isinstance(entry, dict):
                if str(entry.get("access") or "public").strip().lower() == "private":
                    private_path = self._private_entry_path(
                        mem_id, directory=os.path.dirname(body_path))
                    if private_path is None:
                        raise EntryNotFoundError(
                            mem_id, "private memory body not found")
                    body_path = private_path
                return layer, dict(entry), body_path
        try:
            self._read_entry_from_file(MEMORY_MD, mem_id)
        except Exception:
            pass
        else:
            return "STM", default_meta_entry(mem_id), MEMORY_MD
        raise EntryNotFoundError(mem_id)

    def read_meta_by_id(self, mem_id):
        """Return metadata for read tools across STM and active LTM layers."""
        layer, meta, _body_path = self._resolve_read_target(mem_id)
        meta["_memory_layer"] = layer
        return meta

    # ==============================================================
    # memory.md 读写
    # ==============================================================

    def read_entry(self, mem_id):
        """按公开/私密归属读取指定条目正文。"""
        _layer, _meta, body_path = self._resolve_read_target(mem_id)
        return self._read_entry_from_file(body_path, mem_id)

    def write_entry(self, mem_id, title, summary="", weight=2,
                    tags=None, linked_containers=None,
                    feelings=None, delta_desc="", subject=None,
                    round_num=None, last_recalled_round=None,
                    dream=False, current_overview=""):
        """追加一条新记忆条目到 memory.md（原子写）
        F(5): 正文 ≤2048字 / S(4,3): 摘要 ≤512字 / A(2,1): 正文 ≤128字。
        超过当前权重上限时拒绝写入，不静默截断。"""
        now = datetime.now(TZ_SHANGHAI).isoformat()
        clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id
        # morph 不带括号，模板自己包
        morph = {5: "F", 4: "S", 3: "S", 2: "A", 1: "A"}.get(weight, "A")
        max_len = _body_limit_for_weight(weight)
        source_text = summary.strip() if summary else ""
        if source_text and len(source_text) > max_len:
            raise ValueError(
                f"memory_body_too_long:max={max_len};actual={len(source_text)}")
        content = source_text
        overview = _overview_text(current_overview)

        if weight >= 5:
            content_line = f"**内容**（≤2048字）：{content}" if content else ""
        elif weight >= 3:
            content_line = f"**摘要**（≤512字）：{content}" if content else ""
        else:
            content_line = f"**正文**（≤128字）：{content}" if content else ""

        def _round_text(value):
            if value is None:
                return "未知"
            return f"第{value}轮"

        last_round = last_recalled_round if last_recalled_round is not None else round_num
        entry_text = MEMORY_ENTRY_TEMPLATE.format(
            mem_id=clean_id,
            morph=morph,
            weight=weight,
            subject=subject or "—",
            created_round_text=_round_text(round_num),
            last_recalled_round_text=_round_text(last_round),
            title=title,
            dream_text=_dream_text(dream),
            current_overview=overview,
            content_line=content_line,
            created_at=now,
            tags=", ".join(tags) if tags else "",
            feelings=", ".join(feelings) if feelings else "无",
            delta_desc=delta_desc if delta_desc else "",
            linked_containers=", ".join(linked_containers) if linked_containers else "",
        )

        # 原子追加
        os.makedirs(os.path.dirname(MEMORY_MD), exist_ok=True)
        if os.path.isfile(MEMORY_MD):
            with open(MEMORY_MD, "r", encoding="utf-8") as f:
                existing = f.read()
        else:
            existing = "<!-- STM 记忆条目正文 -->\n"

        tmp = MEMORY_MD + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(existing.rstrip() + "\n\n" + entry_text + "\n")
            os.replace(tmp, MEMORY_MD)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(MEMORY_MD, cause=e)

    def list_entries(self):
        """列出 STM 公开与私密正文中实际存在的活动条目 ID。"""
        result = []
        body_paths = [MEMORY_MD]
        root = os.path.dirname(MEMORY_MD)
        if os.path.isdir(root):
            body_paths.extend(
                os.path.join(root, name)
                for name in sorted(os.listdir(root))
                if name.endswith(".private.md")
            )
        for body_path in body_paths:
            if not os.path.isfile(body_path):
                continue
            try:
                with open(body_path, "r", encoding="utf-8") as f:
                    result.extend(re.findall(
                        r"^## (MEM-[0-9A-F]{8})", f.read(), re.MULTILINE))
            except OSError:
                continue
        return list(dict.fromkeys(result))

    def list_public_entries(self):
        """List public entries across the active STM/LTM read layers."""
        entries = []
        seen = set()
        registered = set()
        for layer, meta_path, body_path in self._active_read_layers():
            meta = self._read_json_file(meta_path)
            for mem_id, raw in meta.items():
                if mem_id in registered or not isinstance(raw, dict):
                    continue
                registered.add(mem_id)
                if str(raw.get("access") or "public").strip().lower() != "public":
                    continue
                try:
                    self._read_entry_from_file(body_path, mem_id)
                except EntryNotFoundError:
                    continue
                entry = dict(raw)
                entry["id"] = mem_id
                entry["memory_layer"] = layer
                entries.append(entry)
                seen.add(mem_id)

        if os.path.isfile(MEMORY_MD):
            try:
                with open(MEMORY_MD, "r", encoding="utf-8") as handle:
                    fallback_ids = re.findall(
                        r"^## (MEM-[0-9A-F]{8})", handle.read(), re.MULTILINE)
            except OSError:
                fallback_ids = []
            for mem_id in fallback_ids:
                if mem_id in registered or mem_id in seen:
                    continue
                entry = default_meta_entry(mem_id)
                entry["id"] = mem_id
                entry["memory_layer"] = "STM"
                entries.append(entry)
                seen.add(mem_id)

        return sorted(
            entries,
            key=lambda item: (
                str(item.get("created_at") or ""),
                str(item.get("id") or ""),
            ),
            reverse=True,
        )


    def remove_entry(self, mem_id):
        """从当前公开/私密正文文件中物理移除指定条目块。"""
        _layer, _meta, body_path = self._resolve_read_target(mem_id)
        self._remove_entry_from_file(body_path, mem_id)

    def _remove_entry_from_file(self, body_path, mem_id):
        if not os.path.isfile(body_path):
            raise EntryNotFoundError(mem_id)
        try:
            with open(body_path, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError as e:
            raise ReadError(body_path, cause=e)

        import re
        pattern = re.compile(
            rf"(?ms)^##\s+{re.escape(mem_id)}\b.*?(?=^##\s+MEM-[0-9A-F]{{8}}\b|\Z)"
        )
        new_content, count = pattern.subn("", content)
        if count == 0:
            raise EntryNotFoundError(mem_id)

        if body_path.endswith(".private.md") and not re.search(
                r"(?m)^##\s+MEM-[0-9A-F]{8}\b", new_content):
            os.remove(body_path)
            return

        tmp = body_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(new_content.rstrip() + "\n")
            os.replace(tmp, body_path)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(body_path, cause=e)

    # ==============================================================
    # meta.json 读写
    # ==============================================================

    def load_meta(self):
        """读取 meta.json 全量"""
        if not os.path.isfile(META_JSON):
            return default_meta_json()
        try:
            with open(META_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(META_JSON, cause=e)

    def save_meta(self, meta):
        """写入 meta.json（原子）"""
        atomic_write_json(META_JSON, meta)

    def get_meta(self, mem_id):
        """获取单条记忆的元数据"""
        meta = self.load_meta()
        if mem_id not in meta:
            return default_meta_entry(mem_id)
        return meta[mem_id]

    def set_meta(self, mem_id, entry):
        """写入单条记忆元数据"""
        meta = self.load_meta()
        meta[mem_id] = entry
        self.save_meta(meta)


    def delete_meta(self, mem_id):
        """删除单条记忆元数据"""
        meta = self.load_meta()
        if mem_id in meta:
            del meta[mem_id]
            self.save_meta(meta)

    # ==============================================================
    # index.md 读写
    # ==============================================================

    def append_index(self, mem_id, entry_type, weight, title,
                     subject="", round_num=0, dream=False,
                     current_overview=""):
        """追加一条索引行（原子写）"""
        os.makedirs(os.path.dirname(INDEX_MD), exist_ok=True)
        default_index = f"<!-- STM 索引行 -->\n\n{INDEX_HEADER}\n{INDEX_SEPARATOR}\n"
        overview = _overview_text(current_overview)

        line = (f"| {mem_id} | [{entry_type}] | {weight} "
                f"| {title} | {_dream_text(dream)} | {subject or '—'} "
                f"| {round_num:05d} | {overview} |\n")

        tmp = INDEX_MD + ".tmp"
        try:
            if os.path.isfile(INDEX_MD):
                with open(INDEX_MD, "r", encoding="utf-8") as f:
                    existing = f.read()
            else:
                existing = default_index
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(existing.rstrip() + "\n" + line)
            os.replace(tmp, INDEX_MD)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(INDEX_MD, cause=e)

    def read_index(self):
        """读取 index.md 全部行"""
        if not os.path.isfile(INDEX_MD):
            return []
        try:
            with open(INDEX_MD, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except OSError:
            return []
        # 过滤表头，只返回数据行
        return [l.strip() for l in lines
                if l.startswith("| MEM-") and "|---" not in l]


    def update_entry_title_and_body(self, mem_id, title, body):
        """同步更新 memory.md 正文标题/正文行与 index.md 标题列。"""
        clean_title = str(title or "").strip()
        clean_body = str(body or "").strip()
        if not clean_title or not clean_body:
            raise ValueError("missing_title_or_body")
        self._update_memory_title_and_body(mem_id, clean_title, clean_body)
        self._update_index_title(mem_id, clean_title)

    def read_body_by_id(self, mem_id, **range_request):
        """Return one memory entry body and metadata for protocol read tools."""
        layer, meta, body_path = self._resolve_read_target(mem_id)
        body = self._read_entry_from_file(body_path, mem_id)
        ranged = apply_explicit_range(body, range_kwargs_from_request(range_request))
        return {
            "mem_id": mem_id,
            "memory_layer": layer,
            "meta": meta,
            "body": ranged["content"],
            "read_mode": ranged["read_mode"],
            "range_requested": ranged["range_requested"],
            "range_applied": ranged["range_applied"],
            "total_lines": ranged["total_lines"],
            "total_chars": ranged["total_chars"],
        }

    def update_linked_containers(self, mem_id, operation, container_refs,
                                 current_overview=None):
        """Atomically update linked_containers in meta.json and memory.md."""
        refs = []
        for ref in container_refs or []:
            text = str(ref or "").strip()
            if text and text not in refs:
                refs.append(text)
        op = str(operation or "add").strip().lower()
        if op not in {"add", "remove", "set"}:
            raise ValueError("invalid_operation")

        meta = self.load_meta()
        if mem_id not in meta or not isinstance(meta.get(mem_id), dict):
            raise EntryNotFoundError(mem_id)
        entry = dict(meta[mem_id])
        current = []
        for ref in entry.get("linked_containers") or []:
            text = str(ref or "").strip()
            if text and text not in current:
                current.append(text)

        if op == "set":
            updated_refs = refs
        elif op == "add":
            updated_refs = current + [ref for ref in refs if ref not in current]
        else:
            updated_refs = [ref for ref in current if ref not in refs]

        entry["linked_containers"] = updated_refs
        if current_overview is not None:
            entry["current_overview"] = _overview_text(current_overview)
        meta[mem_id] = entry
        self.save_meta(meta)
        self._update_memory_linked_containers(mem_id, updated_refs)
        if current_overview is not None:
            self._update_memory_current_overview(mem_id, entry["current_overview"])
            self._update_index_current_overview(mem_id, entry["current_overview"])
        return entry

    def mark_private(self, mem_id, privacy_subject, body_action="move_private"):
        """Move one public memory into its relation-owned private file."""
        subject = str(privacy_subject or "").strip()
        if not subject:
            raise ValueError("missing_privacy_subject")
        action = str(body_action or "move_private").strip().lower()
        if action != "move_private":
            raise ValueError("invalid_body_action")

        meta = self.load_meta()
        if mem_id not in meta or not isinstance(meta.get(mem_id), dict):
            raise EntryNotFoundError(mem_id)
        entry = dict(meta[mem_id])
        original_entry = dict(entry)
        private_path = self._private_memory_path(subject)
        owners = self.private_subjects_for_memory(mem_id)
        if owners and owners != [subject]:
            raise ValueError("privacy_subject_conflict")

        if owners == [subject]:
            try:
                self._read_entry_from_file(MEMORY_MD, mem_id)
            except Exception:
                pass
            else:
                self._remove_entry_from_file(MEMORY_MD, mem_id)
            entry["access"] = "private"
            meta[mem_id] = entry
            self.save_meta(meta)
            result = dict(entry)
            result["private_path"] = private_path
            return result

        try:
            body = self._read_entry_from_file(MEMORY_MD, mem_id)
        except Exception as exc:
            raise EntryNotFoundError(mem_id, "public memory body not found") from exc
        if not body.strip():
            raise ValueError("empty_memory_body")

        self._append_entry_to_file(
            private_path,
            body,
            header=f"<!-- private memory: {subject} -->",
        )
        entry["access"] = "private"
        meta[mem_id] = entry
        try:
            self.save_meta(meta)
            self._remove_entry_from_file(MEMORY_MD, mem_id)
        except Exception:
            try:
                self._remove_entry_from_file(private_path, mem_id)
            except Exception:
                pass
            try:
                meta[mem_id] = original_entry
                self.save_meta(meta)
            except Exception:
                pass
            raise

        result = dict(entry)
        result["private_path"] = private_path
        return result

    def _append_entry_to_file(self, body_path, body, header=""):
        """Atomically append one complete memory block to body_path."""
        existing = ""
        if os.path.isfile(body_path):
            try:
                with open(body_path, "r", encoding="utf-8") as f:
                    existing = f.read()
            except OSError as exc:
                raise ReadError(body_path, cause=exc)
        os.makedirs(os.path.dirname(body_path), exist_ok=True)
        content = existing.rstrip()
        if not content:
            content = header.strip()
        content = content + "\n\n" + str(body or "").strip() + "\n"
        tmp = body_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(content)
            os.replace(tmp, body_path)
        except OSError as exc:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(body_path, cause=exc)

    def declassify_private_memory(self, mem_id, mode, redacted_body="", reason=""):
        """Apply privacy declassification modes to a private memory entry."""
        action = str(mode or "").strip().lower()
        if action not in {"declassify", "redact", "delete", "keep_private"}:
            raise ValueError("invalid_declassify_mode")
        meta = self.load_meta()
        if mem_id not in meta or not isinstance(meta.get(mem_id), dict):
            raise EntryNotFoundError(mem_id)
        entry = dict(meta[mem_id])
        private_path = self._private_entry_path(mem_id)
        if private_path is None:
            raise EntryNotFoundError(mem_id, "private memory body not found")

        if action == "keep_private":
            entry["access"] = "private"
            meta[mem_id] = entry
            self.save_meta(meta)
            result = dict(entry)
            result["mode"] = action
            return result

        if action == "delete":
            self._remove_entry_from_file(private_path, mem_id)
            self.delete_meta(mem_id)
            self.remove_index(mem_id)
            return {"mem_id": mem_id, "mode": action, "deleted": True}

        if action == "redact":
            clean_body = str(redacted_body or "").strip()
            if not clean_body:
                raise ValueError("missing_redacted_body")
            self.update_entry_title_and_body(
                mem_id,
                entry.get("title") or mem_id,
                clean_body,
            )

        body = self._read_entry_from_file(private_path, mem_id)
        try:
            self._remove_entry_from_file(MEMORY_MD, mem_id)
        except Exception:
            pass
        self._append_entry_to_file(
            MEMORY_MD,
            body,
            header="<!-- STM 记忆条目正文 -->",
        )
        self._remove_entry_from_file(private_path, mem_id)
        entry["access"] = "public"
        meta[mem_id] = entry
        self.save_meta(meta)

        result = dict(entry)
        result["mode"] = action
        result["reason"] = str(reason or "").strip()
        result["private_path"] = private_path
        return result

    @staticmethod
    def _private_memory_path(subject):
        safe_subject = re.sub(r'[<>:"/\\\\|?*]+', "_", str(subject or "private").strip())
        safe_subject = safe_subject or "private"
        return os.path.join(os.path.dirname(MEMORY_MD), f"{safe_subject}.private.md")

    def _entry_bounds(self, content, mem_id):
        clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id
        patterns = (f"## MEM-{clean_id}", f"## {mem_id}")
        start = -1
        for pattern in patterns:
            match = re.search(rf"(?m)^{re.escape(pattern)}\b", content)
            if match:
                start = match.start()
                break
        if start == -1:
            raise EntryNotFoundError(mem_id)
        end_match = re.search(r"(?m)^##\s+MEM-[0-9A-FA-Z-]+\b", content[start + 1:])
        end = start + 1 + end_match.start() if end_match else len(content)
        return start, end

    def _update_memory_current_overview(self, mem_id, overview):
        _layer, _meta, body_path = self._resolve_read_target(mem_id)
        try:
            with open(body_path, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError as e:
            raise ReadError(body_path, cause=e)

        start, end = self._entry_bounds(content, mem_id)
        block = content[start:end]
        replacement = f"现状概况：{overview}"
        if re.search(r"(?m)^现状概况：", block):
            block = re.sub(r"(?m)^现状概况：.*$", replacement, block, count=1)
        elif re.search(r"(?m)^梦源：", block):
            block = re.sub(r"(?m)^(梦源：.*)$", rf"\1\n{replacement}", block, count=1)
        else:
            block = re.sub(r"(?m)^(\*\*标题\*\*：.*)$", rf"\1\n{replacement}", block, count=1)

        new_content = content[:start] + block + content[end:]
        tmp = body_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(new_content)
            os.replace(tmp, body_path)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(body_path, cause=e)

    def _update_index_current_overview(self, mem_id, overview):
        if not os.path.isfile(INDEX_MD):
            return
        try:
            with open(INDEX_MD, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except OSError as e:
            raise ReadError(INDEX_MD, cause=e)

        changed = False
        new_lines = []
        for line in lines:
            if line.lstrip().startswith(f"| {mem_id} |"):
                cells = [part.strip() for part in line.strip().strip("|").split("|")]
                if len(cells) >= 8:
                    cells[7] = overview
                else:
                    while len(cells) < 7:
                        cells.append("")
                    cells.append(overview)
                line = "| " + " | ".join(cells) + " |\n"
                changed = True
            new_lines.append(line)
        if not changed:
            return

        tmp = INDEX_MD + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
            os.replace(tmp, INDEX_MD)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(INDEX_MD, cause=e)

    def _update_memory_title_and_body(self, mem_id, title, body):
        _layer, _meta, body_path = self._resolve_read_target(mem_id)
        try:
            with open(body_path, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError as e:
            raise ReadError(body_path, cause=e)

        start, end = self._entry_bounds(content, mem_id)
        block = content[start:end]
        if re.search(r"(?m)^\*\*标题\*\*：.*$", block):
            block = re.sub(
                r"(?m)^\*\*标题\*\*：.*$",
                lambda _m: f"**标题**：{title}",
                block,
                count=1,
            )
        else:
            block = re.sub(
                r"(?m)^(##\s+MEM-[^\n]+)$",
                lambda m: f"{m.group(1)}\n**标题**：{title}",
                block,
                count=1,
            )

        body_pattern = r"(?m)^(\*\*(?:内容|摘要)\*\*[^：]*：).*$"
        if re.search(body_pattern, block):
            block = re.sub(
                body_pattern,
                lambda m: f"{m.group(1)}{body}",
                block,
                count=1,
            )
        elif re.search(r"(?m)^\*\*梗概\*\*.*$", block):
            block = re.sub(
                r"(?m)^(\*\*梗概\*\*.*)$",
                lambda m: f"{m.group(1)}\n**内容**（召回补全）：{body}",
                block,
                count=1,
            )
        else:
            block = block.rstrip() + f"\n**内容**（召回补全）：{body}\n"

        new_content = content[:start] + block + content[end:]
        tmp = body_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(new_content)
            os.replace(tmp, body_path)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(body_path, cause=e)

    def _update_index_title(self, mem_id, title):
        if not os.path.isfile(INDEX_MD):
            return
        try:
            with open(INDEX_MD, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except OSError as e:
            raise ReadError(INDEX_MD, cause=e)

        changed = False
        new_lines = []
        for line in lines:
            if line.lstrip().startswith(f"| {mem_id} |"):
                cells = [part.strip() for part in line.strip().strip("|").split("|")]
                if len(cells) >= 4:
                    cells[3] = title
                    line = "| " + " | ".join(cells) + " |\n"
                    changed = True
            new_lines.append(line)
        if not changed:
            return

        tmp = INDEX_MD + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
            os.replace(tmp, INDEX_MD)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(INDEX_MD, cause=e)

    def _update_memory_linked_containers(self, mem_id, linked_containers):
        _layer, _meta, body_path = self._resolve_read_target(mem_id)
        try:
            with open(body_path, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError as e:
            raise ReadError(body_path, cause=e)

        start, end = self._entry_bounds(content, mem_id)
        block = content[start:end]
        replacement = f"关联容器：{', '.join(linked_containers)}"
        if re.search(r"(?m)^关联容器：.*$", block):
            block = re.sub(r"(?m)^关联容器：.*$", replacement, block, count=1)
        else:
            block = block.rstrip() + "\n" + replacement + "\n"
        new_content = content[:start] + block + content[end:]

        tmp = body_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(new_content)
            os.replace(tmp, body_path)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(body_path, cause=e)

    def remove_index(self, mem_id):
        """从 index.md 中移除指定条目行"""
        if not os.path.isfile(INDEX_MD):
            return
        try:
            with open(INDEX_MD, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except OSError as e:
            raise ReadError(INDEX_MD, cause=e)

        new_lines = [
            line for line in lines
            if not line.lstrip().startswith(f"| {mem_id} |")
        ]
        if new_lines == lines:
            return

        tmp = INDEX_MD + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
            os.replace(tmp, INDEX_MD)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(INDEX_MD, cause=e)
