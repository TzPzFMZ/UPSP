"""Shared heartbeat flag constants for internal processors."""

from schemas.state import default_state


KNOWN_HEARTBEAT_FLAGS = frozenset(
    default_state().get("base", {}).get("heartbeat_flags", {}).keys()
)
