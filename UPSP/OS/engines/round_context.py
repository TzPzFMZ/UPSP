"""Round-scoped DTOs shared by Runtime and step runners."""
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RuntimeTrigger:
    trigger_id: str
    trigger_seq: int
    observed_at: str
    round_type: str
    flags: dict[str, Any] = field(default_factory=dict)
    messages: tuple[Any, ...] = ()

    def as_dict(self):
        return {
            "trigger_id": self.trigger_id,
            "trigger_seq": self.trigger_seq,
            "observed_at": self.observed_at,
            "round_type": self.round_type,
            "flags": dict(self.flags),
            "messages": list(self.messages),
        }


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


@dataclass
class SetupResult:
    raw_result: dict[str, Any]
    intent: dict[str, Any]
    interaction_meta: dict[str, Any]
    user_input_text: str = ""
    setup_messages: list[dict[str, Any]] = field(default_factory=list)
    internal_handoff: list[dict[str, Any]] = field(default_factory=list)
    frame_ref: FrameRef | None = None
