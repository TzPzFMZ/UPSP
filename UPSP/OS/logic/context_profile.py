"""Context profile compatibility guard.

Spec612 restores the DDS-defined full POPUP layer as the only live assembly
mode.  The former ``popup_exception_only`` experiment is deliberately rejected
instead of silently falling back to full, so stale launch commands cannot hide
which model-visible contract was used.
"""
from __future__ import annotations


CONTEXT_PROFILE_FULL = "full"
RETIRED_CONTEXT_PROFILE_POPUP_EXCEPTION_ONLY = "popup_exception_only"
CONTEXT_PROFILES = {CONTEXT_PROFILE_FULL}


def normalize_context_profile(value: str | None) -> str:
    profile = str(value or CONTEXT_PROFILE_FULL).strip().lower()
    if profile == RETIRED_CONTEXT_PROFILE_POPUP_EXCEPTION_ONLY:
        raise ValueError(f"retired_context_profile:{profile}")
    if profile not in CONTEXT_PROFILES:
        raise ValueError(f"unsupported_context_profile:{profile}")
    return profile
