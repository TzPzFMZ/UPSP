import os
import sys

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


CONTRACT = {
    "language": "en",
    "answer_scope": "conclusion_only",
    "max_sentences": 1,
}


def test_spec738_contract_is_deliberately_narrow_and_optional():
    from logic.response_contract import normalize_response_contract

    assert normalize_response_contract(None) == {}
    assert normalize_response_contract(CONTRACT) == CONTRACT
    with pytest.raises(ValueError, match="invalid_response_contract"):
        normalize_response_contract({**CONTRACT, "tone": "brief"})
    with pytest.raises(ValueError, match="invalid_response_contract_language"):
        normalize_response_contract({**CONTRACT, "language": "zh"})


def test_spec739_contract_only_renders_soft_statusbar_guidance():
    from logic.response_contract import render_response_contract

    assert render_response_contract(CONTRACT) == (
        "使用英文；只回答结论；限一句话。")
    assert render_response_contract(None) == ""
