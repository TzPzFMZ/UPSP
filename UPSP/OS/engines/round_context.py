"""Round-scoped DTOs shared by Runtime and step runners."""
from dataclasses import dataclass, field
from typing import Any

from logic.memory_reconsolidation import MemoryReconsolidationTracker
from logic.memory_write_rewrite import MemoryWriteRewriteTracker


@dataclass(frozen=True)
class RuntimeTrigger:
    trigger_id: str
    trigger_seq: int
    observed_at: str
    round_type: str
    flags: dict[str, Any] = field(default_factory=dict)
    messages: tuple[Any, ...] = ()
    execution_permission_level: str = "guarded"
    final_response_max_chars: int | None = None
    final_response_length_rejections: int = 0
    response_contract: dict[str, Any] = field(default_factory=dict)
    task_guidance_enabled: bool = True
    context_window_tokens: int = 0

    def as_dict(self):
        result = {
            "trigger_id": self.trigger_id,
            "trigger_seq": self.trigger_seq,
            "observed_at": self.observed_at,
            "round_type": self.round_type,
            "flags": dict(self.flags),
            "messages": list(self.messages),
            "execution_permission_level": self.execution_permission_level,
        }
        if self.final_response_max_chars is not None:
            result["final_response_max_chars"] = self.final_response_max_chars
        if self.final_response_length_rejections:
            result["final_response_length_rejections"] = (
                self.final_response_length_rejections)
        if self.response_contract:
            result["response_contract"] = dict(self.response_contract)
        if not self.task_guidance_enabled:
            result["task_guidance_enabled"] = False
        return result


@dataclass(frozen=True)
class FrameRef:
    frame_id: str
    round_num: int
    axis: str
    sequence: int
    trigger_id: str = ""
    caused_by: str = ""
    topology_version: str = ""
    role_id: str = ""

    @classmethod
    def for_axis(
            cls,
            round_num,
            axis,
            sequence,
            *,
            trigger_id="",
            caused_by="",
            topology_version="",
            role_id=""):
        round_num = int(round_num)
        sequence = int(sequence)
        axis = str(axis or "").strip()
        return cls(
            frame_id=f"R{round_num:06d}:{axis}:{sequence}",
            round_num=round_num,
            axis=axis,
            sequence=sequence,
            trigger_id=str(trigger_id or ""),
            caused_by=str(caused_by or ""),
            topology_version=str(topology_version or ""),
            role_id=str(role_id or ""),
        )

    def as_dict(self):
        return {
            "frame_id": self.frame_id,
            "round_num": self.round_num,
            "axis": self.axis,
            "sequence": self.sequence,
            "trigger_id": self.trigger_id,
            "caused_by": self.caused_by,
            "topology_version": self.topology_version,
            "role_id": self.role_id,
        }


@dataclass
class RoundContext:
    round_num: int
    round_type: str
    state: dict[str, Any]
    flags: dict[str, Any]
    interaction_meta: dict[str, Any] = field(default_factory=dict)
    user_input_text: str = ""
    setup_messages: list[dict[str, Any]] = field(default_factory=list)
    trigger: RuntimeTrigger | None = None
    topology_version: str = ""
    setup_frame: FrameRef | None = None
    cleanup_frame: FrameRef | None = None
    execution_permission_level: str = "guarded"
    final_response_max_chars: int | None = None
    final_response_length_rejections: int = 0
    response_contract: dict[str, Any] = field(default_factory=dict)
    task_guidance_enabled: bool = True
    context_window_tokens: int = 0
    memory_heat_boosted_ids: set[str] = field(default_factory=set)
    memory_reconsolidation_tracker: Any = field(init=False)
    memory_write_rewrite_tracker: Any = field(init=False)

    def __post_init__(self):
        self.memory_reconsolidation_tracker = MemoryReconsolidationTracker(
            self.round_num
        )
        self.memory_write_rewrite_tracker = MemoryWriteRewriteTracker(
            self.round_num
        )


@dataclass
class SetupResult:
    raw_result: dict[str, Any]
    intent: dict[str, Any]
    interaction_meta: dict[str, Any]
    user_input_text: str = ""
    setup_messages: list[dict[str, Any]] = field(default_factory=list)
    internal_handoff: list[dict[str, Any]] = field(default_factory=list)
    frame_ref: FrameRef | None = None
