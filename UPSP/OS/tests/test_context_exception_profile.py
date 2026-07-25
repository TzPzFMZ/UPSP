from pathlib import Path

import pytest
from UPSP.OS.tests.native_tool_test_helpers import _load_module_from_path


REPO_ROOT = Path(__file__).resolve().parents[3]
ACCEPTANCE_PATH = REPO_ROOT / "tools" / "round_context_acceptance.py"


def _load_acceptance():
    return _load_module_from_path('round_context_acceptance_spec612', ACCEPTANCE_PATH)


def _popup_texts(call):
    return [
        str(message.get("content") or "")
        for message in call.get("messages") or []
        if "<!-- POPUP" in str(message.get("content") or "")
    ]


def test_spec612_full_popup_is_present_on_every_provider_call(tmp_path):
    acceptance = _load_acceptance()
    book = tmp_path / "book.md"
    book.write_text("Spec612 full POPUP fixture.\n", encoding="utf-8")

    report = acceptance.run_acceptance(
        scenario="coalesced_calendar_book",
        mode="fake",
        output_dir=tmp_path / "full",
        book_path=book,
        strict=True,
    )

    assert report["summary"]["failed_checks"] == []
    assert report["context_profile"] == "full"
    assert all(len(_popup_texts(call)) == 1 for call in report["calls"])
    assert not any(
        message.get("kind") == "runtime_obligation"
        for call in report["calls"]
        for message in call.get("messages") or []
    )


def test_spec612_rhythm_required_field_contract_stays_in_last_popup(tmp_path):
    acceptance = _load_acceptance()
    book = tmp_path / "book.md"
    book.write_text("Spec612 rhythm guide fixture.\n", encoding="utf-8")

    report = acceptance.run_acceptance(
        scenario="coalesced_calendar_book",
        mode="fake",
        output_dir=tmp_path / "required-field",
        book_path=book,
        strict=True,
    )

    rhythm_popups = [
        popup
        for call in report["calls"]
        if call.get("channel") == "reaction.loop"
        for popup in _popup_texts(call)
        if "guide_id=rhythm:calendar_day:R000001" in popup
    ]
    assert rhythm_popups
    assert all("item_id=calendar_day_due" in popup for popup in rhythm_popups)
    assert all("option_id=write_chronicle" in popup for popup in rhythm_popups)
    assert all("fields.content" in popup for popup in rhythm_popups)
    assert all("需要填写：content（正文）" in popup for popup in rhythm_popups)


def test_spec612_retires_popup_exception_only_fail_closed(tmp_path):
    from assembly.context import ContextAssembler
    from logic.context_profile import normalize_context_profile

    with pytest.raises(
        ValueError,
        match="^retired_context_profile:popup_exception_only$",
    ):
        normalize_context_profile("popup_exception_only")
    with pytest.raises(
        ValueError,
        match="^retired_context_profile:popup_exception_only$",
    ):
        ContextAssembler(
            context_dir=str(tmp_path / "context"),
            context_profile="popup_exception_only",
        )


def test_spec612_rejects_unknown_context_profile():
    from logic.context_profile import normalize_context_profile

    with pytest.raises(ValueError, match="^unsupported_context_profile:mystery$"):
        normalize_context_profile("mystery")


@pytest.mark.parametrize(
    ("method", "args", "scope"),
    [
        ("_get_now_entries", (), "now_cache"),
        ("_get_call_transient_entries", (7, "reaction"), "call_transient"),
        ("_get_current_input_text", (), "current_input"),
        ("_task_board_recent_context_entries", (), "task_board_recent_context"),
    ],
)
def test_required_context_reads_distinguish_failure_from_empty(
        tmp_path, method, args, scope):
    from assembly.context import ContextAssembler
    from errors import RequiredContextError

    class BrokenContextStore:
        def get_now_entries(self):
            raise OSError("now unavailable")

        def get_call_transient_entries(self, *_args, **_kwargs):
            raise OSError("call transient unavailable")

        def get_current_input_text(self):
            raise OSError("current input unavailable")

        def get_lately_entries(self, *_args, **_kwargs):
            raise OSError("lately unavailable")

    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"),
        context_store=BrokenContextStore(),
    )

    with pytest.raises(RequiredContextError) as raised:
        getattr(assembler, method)(*args)

    assert raised.value.as_dict() == {
        "receipt_type": "required_context_failure.v1",
        "status": "failed",
        "stage": "read",
        "scope": scope,
        "reason": f"required_context_read_failed:{scope}:OSError",
        "error_type": "OSError",
    }


def test_required_context_natural_absence_remains_empty(tmp_path):
    from assembly.context import ContextAssembler

    class EmptyContextStore:
        @staticmethod
        def get_now_entries():
            return []

        @staticmethod
        def get_call_transient_entries(*_args, **_kwargs):
            return []

        @staticmethod
        def get_current_input_text():
            return None

        @staticmethod
        def get_lately_entries(*_args, **_kwargs):
            return []

    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"),
        context_store=EmptyContextStore(),
    )

    assert assembler._get_now_entries() == []
    assert assembler._get_call_transient_entries(7, "reaction") == []
    assert assembler._get_current_input_text() is None
    assert assembler._task_board_recent_context_entries() == []


def test_task_board_and_active_focus_read_fail_closed(tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from data import container_store as container_store_module
    from data import workbench as workbench_module
    from errors import RequiredContextError

    assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
    monkeypatch.setattr(
        workbench_module,
        "WorkbenchStore",
        lambda: type("BrokenWorkbench", (), {
            "load_status": lambda self: (_ for _ in ()).throw(
                OSError("workbench unavailable"))
        })(),
    )
    with pytest.raises(RequiredContextError, match="task_board"):
        assembler._build_task_board_projection()

    monkeypatch.setattr(
        workbench_module,
        "WorkbenchStore",
        lambda: type("FocusedWorkbench", (), {
            "load_status": lambda self: {"base": {"focus": "PRJ-1"}}
        })(),
    )
    monkeypatch.setattr(
        container_store_module,
        "ContainerStore",
        lambda: type("BrokenContainers", (), {
            "read_focus_projection": lambda self, focus: (
                (_ for _ in ()).throw(OSError("focus unavailable")))
        })(),
    )
    with pytest.raises(RequiredContextError, match="workbench_focus"):
        assembler._build_workbench_focus_projection()


def test_execution_permission_read_failure_never_falls_back_to_unlimited():
    from logic.execution_permission import load_execution_permission_level

    class BrokenConfig:
        @staticmethod
        def get_execution_permission_level():
            raise OSError("permission unavailable")

    with pytest.raises(OSError, match="permission unavailable"):
        load_execution_permission_level(BrokenConfig())
