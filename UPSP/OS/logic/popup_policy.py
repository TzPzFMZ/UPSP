"""
POPUP 层内排序策略。

POPUP 仍是 messages 绝对末位；本模块只负责 POPUP 内部片段的分层、
拆块与稳定排序，不写缓存、不生成事实语料块。
"""
import re

from assembly.context_helpers import join_layer_blocks


POPUP_TIER_ORDER = ("guide", "reminder", "warning")
POPUP_TIER_TITLES = {
    "guide": "GUIDE｜指南",
    "reminder": "REMINDER｜提醒",
    "warning": "WARNING｜警告",
}
POPUP_TIER_BODY_LABELS = {
    "guide": "行动内容：",
    "reminder": "提醒内容：",
    "warning": "纠偏动作：",
}

MAX_POPUP_CHARS = 12000
MAX_POPUP_ITEM_MESSAGE_CHARS = 3600

WARNING_KINDS = {
    "native_tool_result",
    "security_review",
    "structure_warning",
}

RETIRED_POPUP_KINDS = {
    "received_handoff",
}

WARNING_MARKERS = (
    "[! 警告",
    "[!! 紧急",
    "[!!! 安全",
)

_KIND_RE = re.compile(r"(?m)^-\s*kind:\s*([A-Za-z0-9_:-]+)")
_TIER_RE = re.compile(r"(?m)^\s{2}tier:\s*([A-Za-z0-9_:-]+)\s*$")
_FIELD_RE = re.compile(r"^\s{2}([A-Za-z0-9_:-]+):\s*(.*)$")

HIDDEN_VISIBLE_FIELDS = {
    "kind",
    "tier",
    "decision_required",
    "source",
    "call_id",
    "tool_id",
    "tool_family",
    "tool_class",
    "field",
    "expected",
    "actual",
    "next_action",
    "fields",
    "step",
    "phase",
    "native_terminal_tool",
    "reason",
    "provider",
    "response_id",
    "provider_item_id",
    "index",
    "backend_type",
    "permission_scope",
    "risk",
    "handler",
    "result_kind",
    "subject",
    "submitted_subject",
    "confirmed_subject",
    "feedback_signature",
}

KIND_TITLES = {
    "identity_prompt": "身份提醒",
    "identity_resolution_card": "身份确认",
    "relation_registration_reminder": "关系登记提醒",
    "protocol_tool_reminder": "协议工具提醒",
    "tool_request_card": "工具唤醒提醒",
    "tool_family_reminder": "工具族提醒",
    "reaction_step_guide": "反应步指南",
    "relation_update_reminder": "关系更新提醒",
    "stance_consistency_reminder": "立场一致性提醒",
    "memory_settlement_reminder": "记忆提醒",
    "relay_target_card": "中继目标账本",
    "setup_handoff": "起手步交接",
    "standby_setup_handoff": "待命起手交接",
    "reaction_loop": "反应循环指南",
    "reaction_closeout": "反应循环指南",
    "final_reply": "最终回复指南",
    "cleanup_handoff": "善后步交接",
    "security_review": "安全裁决",
    "structure_warning": "结构警告",
}


class PopupPolicy:
    """POPUP 片段按 guide -> reminder -> warning 稳定排序。"""

    def split_fragments(self, text):
        stripped = str(text or "").strip()
        if not stripped:
            return []

        fragments = []
        current = []
        for line in stripped.splitlines():
            if (
                (line.startswith("- kind:") or re.match(r"^【[^】]+】\s*$", line.strip()))
                and current
            ):
                fragments.append("\n".join(current).strip())
                current = []
            current.append(line)
        if current:
            fragments.append("\n".join(current).strip())
        return [fragment for fragment in fragments if fragment]

    def combine(self, fragments):
        return self.combine_with_block_index(fragments)[0]

    def combine_with_block_index(self, fragments):
        groups = {tier: [] for tier in POPUP_TIER_ORDER}
        for item in self._dedupe_items(
                self._parse_fragment(fragment)
                for fragment in self.sort_fragments(fragments)):
            groups.setdefault(item["tier"], []).append(item)

        reminder_items = groups.get("reminder") or []
        reminder_items.sort(
            key=lambda item: 0
            if item.get("kind") == "memory_settlement_reminder"
            else 1
        )

        blocks = []
        block_number = 0
        for tier in POPUP_TIER_ORDER:
            items = groups.get(tier) or []
            if not items:
                continue
            for index, item in enumerate(items, 1):
                block_number += 1
                title = self._item_title(tier, item)
                content = self._render_item(tier, item)
                if index == 1:
                    content = f"## {POPUP_TIER_TITLES.get(tier, tier.upper())}\n\n{content}"
                blocks.append({
                    "block_id": f"popup:{tier}:{item.get('kind') or 'item'}:{block_number}",
                    "title": title,
                    "kind": str(item.get("kind") or tier),
                    "content": content,
                })
        rendered, block_index = join_layer_blocks(blocks)
        protected = ""
        memory_items = [
            item for item in reminder_items
            if item.get("kind") == "memory_settlement_reminder"
        ]
        if memory_items:
            protected = self._render_tier("reminder", [memory_items[0]])
        capped = self._cap_rendered_popup(
            rendered,
            protected_block=protected,
        )
        if capped == rendered:
            return capped, block_index
        return join_layer_blocks([{
            "block_id": "popup:budget_capped",
            "title": "POPUP 内容预算",
            "kind": "popup_budget_capped",
            "content": capped,
        }])

    def _dedupe_items(self, items):
        deduped = []
        by_signature = {}
        for item in items:
            signature = self._dedupe_signature(item)
            if signature and signature in by_signature:
                original = by_signature[signature]
                original["duplicate_count"] = int(
                    original.get("duplicate_count") or 0) + 1
                continue
            if signature:
                by_signature[signature] = item
            deduped.append(item)
        return deduped

    @staticmethod
    def _dedupe_signature(item):
        fields = item.get("fields") or {}
        kind = str(item.get("kind") or fields.get("kind") or "").strip()
        if not kind:
            return ""
        parts = [
            kind,
            str(fields.get("tool_id") or "").strip(),
            str(fields.get("reason") or "").strip(),
            str(fields.get("pending_id") or "").strip(),
            str(
                fields.get("subject")
                or fields.get("submitted_subject")
                or fields.get("feedback_signature")
                or ""
            ).strip(),
        ]
        if kind == "native_tool_result":
            return "|".join(parts)
        if kind in {"memory_settlement_reminder", "identity_prompt",
                    "identity_resolution_card", "relation_registration_reminder"}:
            return kind
        return ""

    @staticmethod
    def _cap_rendered_popup(text, protected_block=""):
        rendered = str(text or "")
        if len(rendered) <= MAX_POPUP_CHARS:
            return rendered
        budget_notice = (
            "\n\n### POPUP 内容预算\n提醒内容：\n"
            "POPUP 已按 Spec598 截断超预算内容；同类重复提醒不再展开历史全文。"
        )
        protected = str(protected_block or "")
        if not protected or protected not in rendered:
            return (
                rendered[:MAX_POPUP_CHARS - len(budget_notice)].rstrip()
                + budget_notice
            )
        prefix, tail = rendered.split(protected, 1)
        available = MAX_POPUP_CHARS - len(protected) - len(budget_notice)
        if available <= 0:
            return protected[:MAX_POPUP_CHARS]
        if len(prefix) <= available:
            kept_tail = tail[:available - len(prefix)].rstrip()
            return prefix + protected + kept_tail + budget_notice
        kept_prefix = prefix[:available].rstrip()
        return kept_prefix + protected + budget_notice

    def sort_fragments(self, fragments):
        clean = [
            str(fragment).strip()
            for fragment in fragments
            if str(fragment or "").strip()
            and self._extract(_KIND_RE, str(fragment or "")) not in RETIRED_POPUP_KINDS
        ]
        return [
            fragment for _, _, fragment in sorted(
                (self._tier_rank(fragment), idx, fragment)
                for idx, fragment in enumerate(clean)
            )
        ]

    def classify(self, fragment):
        text = str(fragment or "")
        kind = self._extract(_KIND_RE, text)
        tier = self._extract(_TIER_RE, text)
        plain_title = self._plain_title(text)

        if kind in WARNING_KINDS or any(marker in text for marker in WARNING_MARKERS):
            return "warning"
        if plain_title:
            if "警告" in plain_title or "失败" in plain_title:
                return "warning"
            if "指南" in plain_title:
                return "guide"
            if "提醒" in plain_title or "身份" in plain_title:
                return "reminder"
        if tier in POPUP_TIER_ORDER:
            return tier
        if kind.endswith("_guide"):
            return "guide"
        if kind.endswith("_reminder") or kind in {"identity_prompt", "tool_request_card"}:
            return "reminder"
        return "reminder"

    def _tier_rank(self, fragment):
        tier = self.classify(fragment)
        try:
            return POPUP_TIER_ORDER.index(tier)
        except ValueError:
            return POPUP_TIER_ORDER.index("reminder")

    @staticmethod
    def _extract(pattern, text):
        match = pattern.search(text)
        return match.group(1).strip() if match else ""

    @staticmethod
    def _plain_title(text):
        match = re.search(r"(?m)^【([^】]+)】\s*$", str(text or ""))
        return match.group(1).strip() if match else ""

    def _parse_fragment(self, fragment):
        text = str(fragment or "").strip()
        fields = {}
        message_lines = []
        in_message_block = False
        plain_title = self._plain_title(text)
        if plain_title:
            fields["title"] = plain_title
        for raw_line in text.splitlines():
            if plain_title and raw_line.strip() == f"【{plain_title}】":
                continue
            kind_match = re.match(r"^-\s*kind:\s*(.+?)\s*$", raw_line)
            if kind_match:
                fields["kind"] = kind_match.group(1).strip()
                continue

            field_match = _FIELD_RE.match(raw_line)
            if field_match:
                key = field_match.group(1).strip()
                value = field_match.group(2).strip()
                if key == "message":
                    if value == "|":
                        in_message_block = True
                    else:
                        fields["message"] = value
                        in_message_block = False
                    continue
                fields[key] = value
                if in_message_block:
                    continue
                continue

            if in_message_block:
                message_lines.append(raw_line)
            elif plain_title:
                message_lines.append(raw_line)

        tier = self.classify(text)
        message = self._message_from_fields(text, fields, message_lines)
        return {
            "tier": tier if tier in POPUP_TIER_ORDER else "reminder",
            "kind": fields.get("kind", ""),
            "fields": fields,
            "message": message,
        }

    def _message_from_fields(self, text, fields, message_lines):
        if message_lines:
            return self._clean_message_lines(message_lines)
        message = str(fields.get("message") or "").strip()
        if message:
            return message
        return self._clean_unstructured_text(text)

    def _clean_message_lines(self, lines):
        cleaned = []
        for raw_line in lines:
            line = raw_line[4:] if raw_line.startswith("    ") else raw_line.strip()
            stripped = line.strip()
            if self._is_hidden_visible_line(stripped):
                continue
            cleaned.append(line.rstrip())

        while cleaned and not cleaned[0].strip():
            cleaned.pop(0)
        while cleaned and not cleaned[-1].strip():
            cleaned.pop()

        nonblank = [line for line in cleaned if line.strip()]
        if nonblank:
            common_indent = min(len(line) - len(line.lstrip(" ")) for line in nonblank)
            if common_indent:
                cleaned = [
                    line[common_indent:] if line.strip() else line
                    for line in cleaned
                ]
        return "\n".join(cleaned).strip()

    def _clean_unstructured_text(self, text):
        lines = []
        for raw_line in str(text or "").splitlines():
            stripped = raw_line.strip()
            if self._is_hidden_visible_line(stripped):
                continue
            lines.append(raw_line.rstrip())
        return "\n".join(lines).strip()

    @staticmethod
    def _is_hidden_visible_line(line):
        if not line:
            return False
        if re.match(r"^-?\s*source=", line):
            return True
        field_match = re.match(r"^-?\s*([A-Za-z0-9_:-]+):", line)
        return bool(field_match and field_match.group(1) in HIDDEN_VISIBLE_FIELDS)

    def _render_tier(self, tier, items):
        title = POPUP_TIER_TITLES.get(tier, tier.upper())
        blocks = [f"## {title}"]
        for item in items:
            blocks.append(self._render_item(tier, item))
        return "\n\n".join(blocks)

    def _render_item(self, tier, item):
        fields = item["fields"]
        title = self._item_title(tier, item)
        label = POPUP_TIER_BODY_LABELS.get(tier, "提醒内容：")
        message = item["message"].strip()
        if not message:
            message = "请按当前上下文处理该提示。"
        if len(message) > MAX_POPUP_ITEM_MESSAGE_CHARS:
            message = (
                message[:MAX_POPUP_ITEM_MESSAGE_CHARS].rstrip()
                + "\n（本条 POPUP 内容已截断；不要展开重复历史全文。）"
            )
        duplicate_count = int(item.get("duplicate_count") or 0)
        if duplicate_count:
            message = (
                message
                + f"\n已压缩 {duplicate_count} 条同类重复提醒；"
                "不要原样重试，改为停止重试、自然收束或请求用户确认。"
            )
        return f"### {title}\n{label}\n{message}"

    def _item_title(self, tier, item):
        kind = item.get("kind") or ""
        fields = item.get("fields") or {}
        title = str(fields.get("title") or "").strip()
        if title:
            return title
        tool_id = str(fields.get("tool_id") or "").strip()
        if kind in {"protocol_tool_guide", "general_tool_guide"}:
            return f"工具指南：`{tool_id}`" if tool_id else "工具指南"
        if kind == "native_tool_result":
            return "原生工具调用警告"
        if kind in KIND_TITLES:
            return KIND_TITLES[kind]
        return {
            "guide": "指南内容",
            "reminder": "提醒内容",
            "warning": "警告内容",
        }.get(tier, "提示内容")
