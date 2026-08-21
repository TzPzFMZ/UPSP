"""
POPUP 弹窗 + 插话机制 — 兼容层
DDS §24 插话机制

文件 I/O 已下沉到 data/popup_store.py。
本模块保留 PopupManager 接口兼容旧调用方。
"""
from datetime import datetime
import os

from paths import CONTEXT_POPUP, DOCS_POPUP_TEMPLATE
from constants import local_now
from data.popup_store import PopupStore

POPUP_TEMPLATE_MD = DOCS_POPUP_TEMPLATE

GUIDE_TEMPLATE_TITLES = {
    "setup": "起手步指南",
    "standby_setup": "待命起手指南",
    "reaction_loop": "反应循环指南",
    "reaction_closeout": "反应循环指南",
    "final_reply": "最终回复指南",
    "cleanup": "善后步指南",
    "memory_reconsolidation": "回忆重整指南",
    "memory_write_rewrite": "记忆写入重写指南",
    "cache_compaction": "最近缓存压缩指南",
}


class PopupManager:
    """POPUP 管理器（兼容层，委托 data/popup_store.PopupStore）"""

    IDENTITY_RESOLUTION_MESSAGE = (
        "身份未知或超时时，先基于本轮上下文自然确认对象；无法确认时，"
        "先不要做写记忆、挂容器、写关系、调用外部工具等高影响动作，"
        "在最终回复中简短询问或说明等待身份确认。"
    )

    IDENTITY_PROMPT_MESSAGE = (
        "本轮外部输入没有明确自己的身份，请根据上下文自行决定是否进行询问或确认。"
    )

    RELATION_REGISTRATION_MESSAGE = (
        "当前交互对象为陌生关系；如需沉淀关系或记忆，请优先创建新的关系卡。"
    )

    FALLBACK_TEMPLATES = {
        "identity_prompt": {
            "tier": "reminder",
            "decision_required": "false",
            "message": IDENTITY_PROMPT_MESSAGE,
        },
        "identity_resolution_card": {
            "tier": "reminder",
            "decision_required": "true",
            "message": IDENTITY_RESOLUTION_MESSAGE,
        },
        "relation_registration_reminder": {
            "tier": "reminder",
            "decision_required": "false",
            "message": RELATION_REGISTRATION_MESSAGE,
        },
        "step_output_schema_attention": {
            "tier": "guide",
            "decision_required": "false",
            "message": (
                "当前步必须使用 provider-native terminal tool 收束；"
                "裸文本是非法输出，必须提交对应表单。"
            ),
        },
    }

    def __init__(self, popup_path=None, state_store=None):
        self.store = PopupStore(popup_path or CONTEXT_POPUP)
        self.popup_path = self.store.popup_path
        self.state_store = state_store

    def read_popup(self):
        content = self.store.read_popup()
        self._sync_popup_active(bool(content.strip()))
        return content

    def has_popup(self):
        return bool(self.read_popup())

    def inject_into_messages(self, messages):
        content = self.read_popup()
        if not content:
            return list(messages)
        result = list(messages)
        result.append({"role": "user", "content": content})
        return result

    def write_popup(self, content):
        self.store.write_popup(content)
        self._sync_popup_active(bool(str(content or "").strip()))

    def clear_popup(self):
        self.write_popup("")

    def _sync_popup_active(self, active):
        if self.state_store is None:
            return
        try:
            self.state_store.set("base.context_cache.popup_active", bool(active))
        except Exception:
            pass

    def emit_warning(self, message):
        now = local_now().isoformat()
        self.write_popup(f"[! 警告 {now}]\n{message}")

    def emit_urgent(self, message):
        now = local_now().isoformat()
        self.write_popup(f"[!! 紧急 {now}]\n{message}")

    def emit_secure(self, message):
        now = local_now().isoformat()
        self.write_popup(f"[!!! 安全 {now}]\n{message}")

    @staticmethod
    def format_event(kind, message, decision_required=False, tier=None, source=None):
        decision = "true" if decision_required else "false"
        lines = [
            f"- kind: {kind}",
        ]
        if tier:
            lines.append(f"  tier: {tier}")
        lines.extend([
            f"  decision_required: {decision}",
            f"  message: {message}",
        ])
        if source:
            lines.append(f"  source: {source}")
        return "\n".join(lines)

    @staticmethod
    def load_template(kind):
        """从 docs/protocol/base/popup.md 读取指定 kind 模板。"""
        try:
            if not POPUP_TEMPLATE_MD or not os.path.isfile(POPUP_TEMPLATE_MD):
                return None
            with open(POPUP_TEMPLATE_MD, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
        except Exception:
            return None

        in_section = False
        fields = {}
        multiline_key = None
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("### "):
                title = stripped[4:].strip()
                if in_section:
                    break
                in_section = title == kind
                multiline_key = None
                continue
            if not in_section:
                continue
            if multiline_key and not line.startswith("- "):
                if line.startswith("  ") or not stripped:
                    value_line = line[2:] if line.startswith("  ") else ""
                    fields[multiline_key] = (
                        f"{fields[multiline_key]}\n{value_line.rstrip()}"
                        if fields[multiline_key]
                        else value_line.rstrip()
                    )
                    continue
                break
            if not stripped.startswith("- "):
                continue
            item = stripped[2:]
            if ":" not in item:
                continue
            key, value = item.split(":", 1)
            key = key.strip()
            value = value.strip()
            if value == "|":
                fields[key] = ""
                multiline_key = key
            else:
                fields[key] = value
                multiline_key = None

        if not fields:
            return dict(PopupManager.FALLBACK_TEMPLATES.get(kind) or {})
        return fields

    @staticmethod
    def load_guide_template(kind):
        """从 docs/protocol/base/popup.md 读取当前阶段的可见指南正文。"""
        title = GUIDE_TEMPLATE_TITLES.get(str(kind or "").strip())
        if not title:
            return None
        try:
            if not POPUP_TEMPLATE_MD or not os.path.isfile(POPUP_TEMPLATE_MD):
                return None
            with open(POPUP_TEMPLATE_MD, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
        except Exception:
            return None

        label = f"{title}："
        found = False
        quote_lines = []
        for line in lines:
            stripped = line.strip()
            if not found:
                found = stripped == label
                continue
            if stripped.startswith(">"):
                quote_lines.append(stripped[1:].strip())
                continue
            if quote_lines and stripped:
                break
        message = "\n".join(line for line in quote_lines if line).strip()
        return message or None

    @classmethod
    def build_identity_prompt_event(cls):
        template = cls.load_template("identity_prompt") or {}
        message = template.get("message") or cls.IDENTITY_PROMPT_MESSAGE
        decision_required = str(
            template.get("decision_required", "false")
        ).strip().lower() == "true"
        return cls.format_event(
            kind="identity_prompt",
            tier=template.get("tier", "reminder"),
            decision_required=decision_required,
            message=message,
            source="docs/protocol/base/popup.md" if template else "fallback",
        )

    @classmethod
    def build_identity_resolution_event(cls):
        template = cls.load_template("identity_resolution_card") or {}
        message = template.get("message") or cls.IDENTITY_RESOLUTION_MESSAGE
        decision_required = str(
            template.get("decision_required", "true")
        ).strip().lower() == "true"
        return cls.format_event(
            kind="identity_resolution_card",
            tier=template.get("tier", "reminder"),
            decision_required=decision_required,
            message=message,
            source="docs/protocol/base/popup.md" if template else "fallback",
        )

    @classmethod
    def build_relation_registration_event(cls):
        template = cls.load_template("relation_registration_reminder") or {}
        return cls.format_event(
            kind="relation_registration_reminder",
            tier=template.get("tier", "reminder"),
            decision_required=False,
            message=(template.get("message") or cls.RELATION_REGISTRATION_MESSAGE),
            source="docs/protocol/base/popup.md" if template else "fallback",
        )
