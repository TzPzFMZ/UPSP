"""Transient final-response guidance rendered into STATUSBAR."""


_ALLOWED_KEYS = {"language", "answer_scope", "max_sentences"}


def normalize_response_contract(value):
    if value in (None, {}):
        return {}
    if not isinstance(value, dict) or set(value) - _ALLOWED_KEYS:
        raise ValueError("invalid_response_contract")
    if value.get("language") != "en":
        raise ValueError("invalid_response_contract_language")
    if value.get("answer_scope") != "conclusion_only":
        raise ValueError("invalid_response_contract_answer_scope")
    max_sentences = value.get("max_sentences")
    if type(max_sentences) is not int or max_sentences != 1:
        raise ValueError("invalid_response_contract_max_sentences")
    return {
        "language": "en",
        "answer_scope": "conclusion_only",
        "max_sentences": 1,
    }


def render_response_contract(value):
    if not normalize_response_contract(value):
        return ""
    return "使用英文；只回答结论；限一句话。"
