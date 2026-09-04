"""Atomic creation of one runtime persona from the tracked clean skeleton."""

from __future__ import annotations

import json
import os
import re
import secrets
import shutil
from copy import deepcopy
from datetime import datetime
from pathlib import Path

from constants import TZ_SHANGHAI
from logic.workhood import compute_workhood, speed_wheel_max
from schemas.state import default_state
from .windows_data import generate_pid, validate_pid


AXES = (
    ("S", "结构", "体验", "E"),
    ("C", "收敛", "发散", "D"),
    ("V", "证据", "幻想", "F"),
    ("A", "分析", "直觉", "I"),
    ("R", "批判", "协作", "O"),
    ("B", "抽象", "具体", "K"),
)
REQUIRED_TEMPLATE_FILES = (
    "core.md",
    "state.json",
    "LTM/Immune/birth.md",
    "rules/rules_registry.json",
    "docs/docs_registry.json",
    "relation/relation_registry.json",
    "relation/_index/keywords.json",
    "STM/context/resident_list.json",
    "STM/buffer/raw_log.jsonl",
    "STM/buffer/raw_log.md",
    "LTM/container_registry.json",
)
REQUIRED_TEMPLATE_DIRS = (
    "STM/context/setup",
    "STM/context/reaction",
    "STM/context/cleanup",
    "STM/context/round",
    "STM/workbench/input",
    "STM/workbench/process",
    "STM/workbench/output",
    "LTM/Memory/Full",
    "LTM/Memory/Summary",
    "LTM/Memory/Abstract",
    "LTM/Memory/Backup",
    "LTM/Memory/Pinned",
    "LTM/Chronicle/rhythms",
    "LTM/Corpus/public/rhythms",
    "relation/self",
    "relation/ours",
    "relation/them",
    "relation/orgs",
)
REQUIRED_PERSONA_FILES = (
    "core.md",
    "state.json",
    "LTM/Immune/birth.md",
    "relation/relation_registry.json",
)
ABBREVIATION_RE = re.compile(r"^[A-Z][A-Z0-9]{1,7}$")
TEMPLATE_MARKER_RE = re.compile(r"@@UPSP_[A-Z0-9_]+@@")
CORE_TEMPLATE_MARKERS = (
    "@@UPSP_PID@@",
    "@@UPSP_NAME_ZH@@",
    "@@UPSP_NAME_EN@@",
    "@@UPSP_ABBREVIATION@@",
    "@@UPSP_ROLES@@",
    "@@UPSP_AXIS_S@@",
    "@@UPSP_AXIS_S_EXPLANATION@@",
    "@@UPSP_AXIS_C@@",
    "@@UPSP_AXIS_C_EXPLANATION@@",
    "@@UPSP_AXIS_V@@",
    "@@UPSP_AXIS_V_EXPLANATION@@",
    "@@UPSP_AXIS_A@@",
    "@@UPSP_AXIS_A_EXPLANATION@@",
    "@@UPSP_AXIS_R@@",
    "@@UPSP_AXIS_R_EXPLANATION@@",
    "@@UPSP_AXIS_B@@",
    "@@UPSP_AXIS_B_EXPLANATION@@",
    "@@UPSP_CODE_VALUES@@",
    "@@UPSP_PERSONA_CODE@@",
    "@@UPSP_MODEL_STAMP@@",
    "@@UPSP_SELF_DESCRIPTION@@",
    "@@UPSP_TRAITS@@",
    "@@UPSP_INSTANCE_NOTES@@",
)
BIRTH_TEMPLATE_MARKERS = (
    "@@UPSP_PID@@",
    "@@UPSP_NAME_ZH@@",
    "@@UPSP_NAME_EN@@",
    "@@UPSP_ABBREVIATION@@",
    "@@UPSP_PERSONA_CODE@@",
    "@@UPSP_CREATED_AT@@",
    "@@UPSP_MODEL_ALIAS@@",
    "@@UPSP_MODEL_PROFILE_ID@@",
    "@@UPSP_MODEL_ID@@",
)


class PersonaInitializationError(ValueError):
    """A stable, user-safe bootstrap validation or filesystem error."""


def persona_code(axes: dict[str, int]) -> str:
    return "".join(
        "X" if axes[left] == 50 else left if axes[left] > 50 else right
        for left, _ln, _rn, right in AXES
    )


def _axis_explanation(left: str, left_name: str, right_name: str, value: int) -> str:
    if value >= 80:
        return f"明显偏向{left_name}，同时保留必要的{right_name}能力。"
    if value >= 60:
        return f"偏向{left_name}，会在情境需要时调用{right_name}。"
    if value > 40:
        return f"在{left_name}与{right_name}之间保持相对均衡。"
    if value > 20:
        return f"偏向{right_name}，会在情境需要时调用{left_name}。"
    return f"明显偏向{right_name}，同时保留必要的{left_name}能力。"


def validate_profile(raw: object, *, require_budget: bool = True) -> dict:
    if not isinstance(raw, dict):
        raise PersonaInitializationError("persona_profile_invalid")
    profile = deepcopy(raw)
    name_zh = str(profile.get("name_zh") or "").strip()
    name_en = str(profile.get("name_en") or "").strip()
    abbreviation = str(profile.get("abbreviation") or "").strip().upper()
    roles = profile.get("roles")
    axes = profile.get("axes")
    traits = profile.get("traits")
    description = str(profile.get("self_description") or "").strip()
    notes = str(profile.get("instance_notes") or "").strip()
    if not name_zh and not name_en:
        raise PersonaInitializationError("persona_name_required")
    if not ABBREVIATION_RE.fullmatch(abbreviation):
        raise PersonaInitializationError("persona_abbreviation_invalid")
    if (
        not isinstance(roles, list)
        or not 1 <= len(roles) <= 3
        or any(not isinstance(item, str) or not item.strip() for item in roles)
    ):
        raise PersonaInitializationError("persona_roles_invalid")
    if not isinstance(axes, dict) or set(axes) != {item[0] for item in AXES}:
        raise PersonaInitializationError("persona_axes_invalid")
    normalized_axes = {}
    for key, _left_name, _right_name, _right in AXES:
        value = axes[key]
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 100:
            raise PersonaInitializationError("persona_axis_invalid")
        normalized_axes[key] = value
    if require_budget and sum(abs(value - 50) for value in normalized_axes.values()) != 60:
        raise PersonaInitializationError("persona_axis_budget_invalid")
    if (
        not isinstance(traits, list)
        or len(traits) != 3
        or any(not isinstance(item, str) or not item.strip() for item in traits)
    ):
        raise PersonaInitializationError("persona_traits_invalid")
    if len(description) > 200:
        raise PersonaInitializationError("persona_description_too_long")
    return {
        "id": str(profile.get("id") or "custom").strip() or "custom",
        "name_zh": name_zh,
        "name_en": name_en,
        "abbreviation": abbreviation,
        "roles": [item.strip() for item in roles],
        "axes": normalized_axes,
        "persona_code": persona_code(normalized_axes),
        "traits": [item.strip() for item in traits],
        "self_description": description,
        "instance_notes": notes,
    }


def load_preset(preset_root: str | Path, preset_id: str) -> dict:
    if preset_id != "alyosha":
        raise PersonaInitializationError("persona_preset_not_found")
    path = Path(preset_root) / preset_id / "profile.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PersonaInitializationError("persona_preset_not_found") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise PersonaInitializationError("persona_preset_invalid") from exc
    profile = validate_profile(raw)
    if raw.get("immutable") is not True or profile["persona_code"] != "SCVAOK":
        raise PersonaInitializationError("persona_preset_invalid")
    return profile


def _read_markdown_template(path: Path, expected_markers: tuple[str, ...]) -> str:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise PersonaInitializationError("persona_template_invalid") from exc
    found = TEMPLATE_MARKER_RE.findall(text)
    if (
        set(found) != set(expected_markers)
        or any(found.count(marker) != 1 for marker in expected_markers)
    ):
        raise PersonaInitializationError("persona_template_markers_invalid")
    return text


def _fill_template(text: str, values: dict[str, str]) -> str:
    rendered = text
    for marker, value in values.items():
        rendered = rendered.replace(marker, value)
    if TEMPLATE_MARKER_RE.search(rendered):
        raise PersonaInitializationError("persona_template_markers_unresolved")
    return rendered


def _render_core(template: str, profile: dict, pid: str, model_stamp: dict) -> str:
    display = profile["name_zh"] or profile["name_en"]
    values = {
        "@@UPSP_PID@@": pid,
        "@@UPSP_NAME_ZH@@": profile["name_zh"],
        "@@UPSP_NAME_EN@@": profile["name_en"],
        "@@UPSP_ABBREVIATION@@": profile["abbreviation"],
        "@@UPSP_ROLES@@": "\n".join(
            f"角色{index}：{role}"
            for index, role in enumerate(profile["roles"], 1)
        ),
        "@@UPSP_CODE_VALUES@@": " / ".join(
            f"{key}{profile['axes'][key]}" for key, *_ in AXES
        ),
        "@@UPSP_PERSONA_CODE@@": profile["persona_code"],
        "@@UPSP_MODEL_STAMP@@": (
            "第 0 轮"
            f"（{model_stamp['created_at']}，配置：{model_stamp['model_alias']}，"
            f"模型：{model_stamp['model']}，ID：{model_stamp['profile_id']}）"
        ),
        "@@UPSP_SELF_DESCRIPTION@@": (
            profile["self_description"]
            or f"我是{display}。我的具体实践将从真实交互中逐步形成。"
        ),
        "@@UPSP_TRAITS@@": "\n".join(
            f"{index}. {trait}"
            for index, trait in enumerate(profile["traits"], 1)
        ),
        "@@UPSP_INSTANCE_NOTES@@": profile["instance_notes"] or "暂无。",
    }
    for left, left_name, right_name, right in AXES:
        value = profile["axes"][left]
        values[f"@@UPSP_AXIS_{left}@@"] = (
            f"{left} {value}% / {right} {100 - value}%"
        )
        values[f"@@UPSP_AXIS_{left}_EXPLANATION@@"] = _axis_explanation(
            left, left_name, right_name, value
        )
    return _fill_template(template, values)


def _render_birth(template: str, profile: dict, pid: str, model_stamp: dict) -> str:
    return _fill_template(template, {
        "@@UPSP_PID@@": pid,
        "@@UPSP_NAME_ZH@@": profile["name_zh"],
        "@@UPSP_NAME_EN@@": profile["name_en"],
        "@@UPSP_ABBREVIATION@@": profile["abbreviation"],
        "@@UPSP_PERSONA_CODE@@": profile["persona_code"],
        "@@UPSP_CREATED_AT@@": model_stamp["created_at"],
        "@@UPSP_MODEL_ALIAS@@": model_stamp["model_alias"],
        "@@UPSP_MODEL_PROFILE_ID@@": model_stamp["profile_id"],
        "@@UPSP_MODEL_ID@@": model_stamp["model"],
    })


def _contains_shape(value: object, expected: object) -> bool:
    if not isinstance(expected, dict):
        return True
    return (
        isinstance(value, dict)
        and all(
            key in value and _contains_shape(value[key], child)
            for key, child in expected.items()
        )
    )


def _load_state_template(path: Path) -> dict:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PersonaInitializationError("persona_state_template_invalid") from exc
    if not _contains_shape(state, default_state()):
        raise PersonaInitializationError("persona_state_template_invalid")
    base = state["base"]
    neutral = (
        base.get("core_axes") == {key: 50 for key, *_ in AXES}
        and all(
            item == {"value": 0}
            for item in (base.get("dynamic_axes") or {}).values()
        )
        and set((base.get("dynamic_axes") or {})) == {
            "valence", "arousal", "focus", "mood", "humor", "safety",
        }
        and set((base.get("comfort_zone") or {}).values()) == {0}
        and all(value is False for value in (base.get("heartbeat_flags") or {}).values())
        and base.get("identity", {}).get("confirmed") is False
        and base.get("identity", {}).get("current_source") == "unbound"
        and base.get("meta", {}).get("total_round") == 0
        and base.get("meta", {}).get("daily_round") == 0
    )
    if not neutral:
        raise PersonaInitializationError("persona_state_template_not_neutral")
    return state


def _materialize_state(
    template: dict,
    axes: dict[str, int],
    context_window: int,
    created_at: str,
) -> dict:
    state = deepcopy(template)
    base = state["base"]
    base["meta"]["last_update"] = created_at
    base["core_axes"] = dict(axes)
    workhood = compute_workhood(axes, base["dynamic_axes"])
    base["workhood_index"] = workhood
    base["core_speed_wheel"] = {
        "current": 0,
        "max": speed_wheel_max(workhood["value"]),
    }
    base["fatigue"]["awake_since"] = created_at
    base["token_usage"]["window_size"] = context_window
    return state


def _render_self_card(profile: dict, pid: str, created_at: str) -> str:
    name = profile["name_zh"] or profile["name_en"]
    roles = " / ".join(profile["roles"])
    return "\n".join([
        f"# {name}",
        "",
        "## 基础信息",
        "- 类别：self",
        f"- 创建时间：{created_at}",
        "- 最后交互：尚未发生",
        "",
        "## 关系六轴",
        "- 信任：0",
        "- 安心：0",
        "- 重视：0",
        "- 投入：0",
        "- 坦诚：0",
        "- 共振：0",
        "",
        "## 备注",
        f"位格主体自身。{profile['persona_code']}。{roles}。",
        "",
        "- 状态：active",
        "",
    ])


class PersonaInitializer:
    def __init__(
        self,
        persona_dir: str | Path,
        template_dir: str | Path,
        preset_dir: str | Path,
        *,
        pid: str | None = None,
        shared_persona_dir: str | Path | None = None,
    ):
        self.persona_dir = Path(persona_dir)
        self.shared_persona_dir = Path(
            shared_persona_dir if shared_persona_dir is not None else persona_dir
        )
        self.template_dir = Path(template_dir)
        self.preset_dir = Path(preset_dir)
        self.pid = validate_pid(pid) if pid is not None else None

    def _status_path(self, relative: str) -> Path:
        shared = (
            relative == "core.md"
            or relative.startswith("rules/")
            or relative.startswith("docs/")
            or relative.startswith("LTM/Memory/")
        )
        return (self.shared_persona_dir if shared else self.persona_dir) / relative

    def status(self) -> dict:
        if not self.persona_dir.exists():
            return {"state": "missing", "ready": False, "missing": list(REQUIRED_PERSONA_FILES)}
        if not self.persona_dir.is_dir():
            return {"state": "incomplete", "ready": False, "missing": list(REQUIRED_PERSONA_FILES)}
        required_files = tuple(dict.fromkeys((*REQUIRED_TEMPLATE_FILES, *REQUIRED_PERSONA_FILES)))
        missing = [
            item for item in required_files
            if not self._status_path(item).is_file()
        ]
        missing.extend(
            item for item in REQUIRED_TEMPLATE_DIRS
            if not self._status_path(item).is_dir()
        )
        if missing:
            return {"state": "incomplete", "ready": False, "missing": missing}
        try:
            core = self._status_path("core.md").read_text(encoding="utf-8")
            birth = (
                self._status_path("LTM/Immune/birth.md")
            ).read_text(encoding="utf-8")
            parsed_json = {
                item: json.loads(self._status_path(item).read_text(encoding="utf-8"))
                for item in required_files
                if item.endswith(".json")
            }
            state = parsed_json["state.json"]
            registry = parsed_json["relation/relation_registry.json"]
        except (OSError, UnicodeError, json.JSONDecodeError):
            return {"state": "incomplete", "ready": False, "missing": []}
        pid_match = re.search(r"(?m)^PID：\s*(\S+)\s*$", core)
        abbreviation_match = re.search(r"(?m)^缩写：\s*(\S+)\s*$", core)
        has_name = bool(
            re.search(r"(?m)^(?:中文名|英文名)：\s*\S.*$", core)
        )
        if (
            not _contains_shape(state, default_state())
            or not pid_match
            or not abbreviation_match
            or not ABBREVIATION_RE.fullmatch(abbreviation_match.group(1))
            or not has_name
            or TEMPLATE_MARKER_RE.search(core)
            or TEMPLATE_MARKER_RE.search(birth)
        ):
            return {"state": "incomplete", "ready": False, "missing": []}
        if self.pid is not None and pid_match.group(1) != self.pid:
            return {"state": "incomplete", "ready": False, "missing": ["pid_mismatch"]}
        cards = registry.get("cards") if isinstance(registry, dict) else None
        self_cards = [
            item for item in cards or []
            if isinstance(item, dict)
            and item.get("category") == "self"
            and item.get("status", "active") == "active"
        ]
        if len(self_cards) != 1 or self_cards[0].get("id") != pid_match.group(1):
            return {"state": "incomplete", "ready": False, "missing": ["self_relation_card"]}
        card_path = str(self_cards[0].get("path") or "")
        if card_path != f"relation/self/{pid_match.group(1)}.md":
            return {"state": "incomplete", "ready": False, "missing": ["self_relation_card"]}
        if not (self.persona_dir / card_path).is_file():
            return {"state": "incomplete", "ready": False, "missing": ["self_relation_card"]}
        return {"state": "ready", "ready": True, "missing": []}

    def _validate_template(self) -> tuple[str, str, dict]:
        if not self.template_dir.is_dir():
            raise PersonaInitializationError("persona_template_missing")
        missing = [
            item for item in REQUIRED_TEMPLATE_FILES
            if not (self.template_dir / item).is_file()
        ]
        if missing:
            raise PersonaInitializationError("persona_template_incomplete")
        core = _read_markdown_template(
            self.template_dir / "core.md",
            CORE_TEMPLATE_MARKERS,
        )
        birth = _read_markdown_template(
            self.template_dir / "LTM/Immune/birth.md",
            BIRTH_TEMPLATE_MARKERS,
        )
        state = _load_state_template(self.template_dir / "state.json")
        try:
            registry = json.loads(
                (self.template_dir / "relation/relation_registry.json").read_text(
                    encoding="utf-8"
                )
            )
            keywords = json.loads(
                (self.template_dir / "relation/_index/keywords.json").read_text(
                    encoding="utf-8"
                )
            )
            for item in REQUIRED_TEMPLATE_FILES:
                if item.endswith(".json"):
                    json.loads((self.template_dir / item).read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise PersonaInitializationError("persona_template_invalid") from exc
        if (
            not isinstance(registry, dict)
            or registry.get("cards") != []
            or not isinstance(keywords, dict)
            or keywords.get("index") != {}
        ):
            raise PersonaInitializationError("persona_template_contains_live_identity")
        return core, birth, state

    def create(self, profile: object, model_stamp: object) -> dict:
        if self.persona_dir.exists():
            raise PersonaInitializationError("persona_already_exists")
        core_template, birth_template, state_template = self._validate_template()
        normalized = validate_profile(profile)
        if not isinstance(model_stamp, dict):
            raise PersonaInitializationError("model_stamp_invalid")
        required_stamp = {"profile_id", "model_alias", "model", "context_window"}
        if not required_stamp.issubset(model_stamp):
            raise PersonaInitializationError("model_stamp_invalid")
        try:
            context_window = int(model_stamp["context_window"] or 0)
        except (TypeError, ValueError) as exc:
            raise PersonaInitializationError("model_stamp_invalid") from exc
        if context_window < 0:
            raise PersonaInitializationError("model_stamp_invalid")
        stamp = {
            "profile_id": str(model_stamp["profile_id"]).strip(),
            "model_alias": str(model_stamp["model_alias"]).strip(),
            "model": str(model_stamp["model"]).strip(),
            "context_window": context_window,
            "created_at": datetime.now(TZ_SHANGHAI).isoformat(),
        }
        if not all(stamp[key] for key in ("profile_id", "model_alias", "model")):
            raise PersonaInitializationError("model_stamp_invalid")
        pid = self.pid or generate_pid()
        target_parent = self.persona_dir.parent
        target_parent.mkdir(parents=True, exist_ok=True)
        temp_dir = target_parent / f".persona-init-{secrets.token_hex(8)}"
        try:
            # 安装模板可能只读；活体副本必须保持可写且不回写安装树。
            shutil.copytree(
                self.template_dir,
                temp_dir,
                copy_function=shutil.copyfile,
            )
            for relative_dir in REQUIRED_TEMPLATE_DIRS:
                (temp_dir / relative_dir).mkdir(parents=True, exist_ok=True)
            (temp_dir / "core.md").write_text(
                _render_core(core_template, normalized, pid, stamp),
                encoding="utf-8",
                newline="\n",
            )
            state = _materialize_state(
                state_template,
                normalized["axes"],
                stamp["context_window"],
                stamp["created_at"],
            )
            (temp_dir / "state.json").write_text(
                json.dumps(state, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
                newline="\n",
            )
            birth_path = temp_dir / "LTM/Immune/birth.md"
            birth_path.parent.mkdir(parents=True, exist_ok=True)
            birth_path.write_text(
                _render_birth(birth_template, normalized, pid, stamp),
                encoding="utf-8",
                newline="\n",
            )
            self_card = temp_dir / "relation/self" / f"{pid}.md"
            self_card.parent.mkdir(parents=True, exist_ok=True)
            self_card.write_text(
                _render_self_card(normalized, pid, stamp["created_at"]),
                encoding="utf-8",
                newline="\n",
            )
            aliases = [
                value for value in (
                    normalized["name_zh"],
                    normalized["name_en"],
                    normalized["abbreviation"],
                    "我",
                )
                if value
            ]
            registry = {
                "_comment": "关系域注册表",
                "_version": "Base-0.46.9",
                "categories": ["self", "ours", "them", "orgs"],
                "cards": [{
                    "id": pid,
                    "name": normalized["name_zh"] or normalized["name_en"],
                    "category": "self",
                    "path": f"relation/self/{pid}.md",
                    "status": "active",
                    "updated_at": stamp["created_at"],
                    "summary_resident": False,
                    "aliases": aliases,
                    "tags": ["self"],
                }],
            }
            (temp_dir / "relation/relation_registry.json").write_text(
                json.dumps(registry, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
                newline="\n",
            )
            keywords = {"_comment": "关系域倒排索引", "index": {
                alias: [pid] for alias in aliases
            }}
            (temp_dir / "relation/_index/keywords.json").write_text(
                json.dumps(keywords, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
                newline="\n",
            )
            for item in REQUIRED_PERSONA_FILES:
                if not (temp_dir / item).is_file():
                    raise PersonaInitializationError("persona_generated_incomplete")
            generated_status = PersonaInitializer(
                temp_dir,
                self.template_dir,
                self.preset_dir,
                pid=pid,
            ).status()
            if not generated_status["ready"]:
                raise PersonaInitializationError("persona_generated_incomplete")
            os.replace(temp_dir, self.persona_dir)
        except Exception:
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
            raise
        return {
            "pid": pid,
            "preset_id": normalized["id"],
            "name_zh": normalized["name_zh"],
            "name_en": normalized["name_en"],
            "abbreviation": normalized["abbreviation"],
            "persona_code": normalized["persona_code"],
            "model_profile_id": stamp["profile_id"],
        }
