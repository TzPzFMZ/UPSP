import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

def test_web_fetch_general_tool_fetches_public_page_with_injected_backend():
    from logic.general_tools import (
        execute_general_tool_call,
        format_general_tool_fact,
        format_general_tool_material_entry,
    )

    calls = []

    def fake_fetch(url, timeout_ms):
        calls.append((url, timeout_ms))
        return {
            "status_code": 200,
            "content_type": "text/html; charset=utf-8",
            "final_url": url,
            "title": "Example Page",
            "content": "alpha beta gamma",
        }

    result = execute_general_tool_call(
        {
            "tool_id": "web_fetch",
            "url": "https://example.com/page",
            "reason": "unit test",
            "max_chars": "20",
            "timeout_ms": "1500",
        },
        web_fetch_fn=fake_fetch,
    )

    assert calls == [("https://example.com/page", 1500)]
    assert result["tool_id"] == "web_fetch"
    assert result["tool_family"] == "general_tool"
    assert result["tool_class"] == "read_tool"
    assert result["handler"] == "web_fetch_handler"
    assert result["permission_scope"] == "public_web_read"
    assert result["status"] == "ok"
    assert result["source_url"] == "https://example.com/page"
    assert result["title"] == "Example Page"
    assert result["content"] == "alpha beta gamma"
    assert result["read_mode"] == "bounded"
    assert result["window_kind"] == "web_fetch_bounded"
    assert result["returned_chars"] == len("alpha beta gamma")
    assert result["window_chars"] == 4096
    assert "max_chars" not in result
    assert result["has_more"] is False
    assert result["protocol_tool_receipt"] is False

    fact = format_general_tool_fact(result)
    material = format_general_tool_material_entry(result)
    assert "本轮已经成功读取网页：" in fact
    assert "窗口字符：16/4096。" in fact
    assert "这是网页正文窗口，不代表整页、整站或外部事实已经完整读取。" in fact
    assert "alpha beta gamma" not in fact
    assert "截断" not in fact
    assert material["kind"] == "material"
    assert material["tool_id"] == "web_fetch"
    assert material["content"] == "alpha beta gamma"
    assert material["title"] == "Example Page"


def test_spec457_web_fetch_injected_backend_ignores_persisted_health(tmp_path, monkeypatch):
    from logic.general_tools import execute_general_tool_call

    health_path = tmp_path / "web_backend_health.json"
    health_path.write_text(
        json.dumps(
            {
                "web_fetch": {
                    "direct_fetch": {"hard_fail_count": 9},
                    "jina_reader": {"hard_fail_count": 0},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("UPSP_WEB_BACKEND_HEALTH_PATH", str(health_path))
    calls = []

    def fake_fetch(url, timeout_ms):
        calls.append((url, timeout_ms))
        return {
            "status_code": 200,
            "content_type": "text/plain",
            "final_url": url,
            "title": "Injected Page",
            "content": "injected body",
        }

    result = execute_general_tool_call(
        {
            "tool_id": "web_fetch",
            "url": "https://example.com/injected",
            "timeout_ms": "1500",
        },
        web_fetch_fn=fake_fetch,
    )

    assert result["status"] == "ok"
    assert result["selected_backend"] == "direct_fetch"
    assert calls == [("https://example.com/injected", 1500)]
    health = json.loads(health_path.read_text(encoding="utf-8"))
    assert health["web_fetch"]["direct_fetch"]["hard_fail_count"] == 9


def test_web_fetch_general_tool_uses_config_window_and_char_cursor():
    from logic.general_tools import execute_general_tool_call, format_general_tool_fact

    def fake_fetch(url, timeout_ms):
        return {
            "status_code": 200,
            "content_type": "text/html; charset=utf-8",
            "final_url": url,
            "title": "Long Page",
            "content": "x" * 5000,
        }

    first = execute_general_tool_call(
        {
            "tool_id": "web_fetch",
            "url": "https://example.com/long",
            "reason": "Spec342 web first window",
            "max_chars": "20",
        },
        web_fetch_fn=fake_fetch,
    )
    second = execute_general_tool_call(
        {
            "tool_id": "web_fetch",
            "url": "https://example.com/long",
            "reason": "Spec342 web continuation",
            "char_start": first["next_char_start"],
        },
        web_fetch_fn=fake_fetch,
    )

    assert first["status"] == "ok"
    assert first["window_chars"] == 4096
    assert first["returned_chars"] == 4096
    assert first["window_boundary"] == "line_char"
    assert first["line_overlong"] is True
    assert first["next_char_start"] == 4097
    assert "max_chars" not in first

    assert second["status"] == "ok"
    assert second["char_start"] == 4097
    assert second["char_end"] == 5000
    assert second["returned_chars"] == 904
    assert second["has_more"] is False

    fact = format_general_tool_fact(first)
    assert "本轮如需继续，应调用 web_fetch 并传 char_start=4097。" in fact
    assert "截断" not in fact


def test_web_fetch_general_tool_rejects_non_public_or_download_urls():
    from logic.general_tools import execute_general_tool_call

    def should_not_fetch(url, timeout_ms):
        raise AssertionError("fetch backend should not run")

    bad_scheme = execute_general_tool_call(
        {"tool_id": "web_fetch", "url": "file:///etc/passwd"},
        web_fetch_fn=should_not_fetch,
    )
    assert bad_scheme["status"] == "rejected"
    assert bad_scheme["reason"] == "invalid_url_scheme"

    login_page = execute_general_tool_call(
        {"tool_id": "web_fetch", "url": "https://example.com/login"},
        web_fetch_fn=should_not_fetch,
    )
    assert login_page["status"] == "rejected"
    assert login_page["reason"] == "interactive_or_login_page_denied"

    download = execute_general_tool_call(
        {"tool_id": "web_fetch", "url": "https://example.com/report.zip"},
        web_fetch_fn=should_not_fetch,
    )
    assert download["status"] == "rejected"
    assert download["reason"] == "download_like_url"


def test_spec455_web_fetch_routes_to_jina_after_direct_hard_fail(
    tmp_path, monkeypatch
):
    from urllib.error import URLError
    from logic import general_tools

    health_path = tmp_path / "web_backend_health.json"
    monkeypatch.setenv("UPSP_WEB_BACKEND_HEALTH_PATH", str(health_path))
    calls = []

    def direct_fetch(url, timeout_ms):
        calls.append(("direct_fetch", url))
        raise URLError("direct fetch denied")

    def jina_reader(url, timeout_ms):
        calls.append(("jina_reader", url))
        return {
            "status_code": 200,
            "content_type": "text/plain; charset=utf-8",
            "final_url": url,
            "title": "Reader Page",
            "content": "reader body",
        }

    monkeypatch.setattr(general_tools, "_default_fetch_url", direct_fetch)
    monkeypatch.setattr(general_tools, "_jina_reader_fetch_url", jina_reader, raising=False)

    result = general_tools.execute_general_tool_call({
        "tool_id": "web_fetch",
        "url": "https://example.com/report",
        "timeout_ms": "2000",
    })

    assert result["status"] == "ok"
    assert result["selected_backend"] == "jina_reader"
    assert [item["backend_id"] for item in result["backend_attempts"]] == [
        "direct_fetch",
        "jina_reader",
    ]
    assert result["backend_attempts"][0]["status"] == "failed"
    assert calls == [
        ("direct_fetch", "https://example.com/report"),
        ("jina_reader", "https://example.com/report"),
    ]

    health = json.loads(health_path.read_text(encoding="utf-8"))
    assert health["web_fetch"]["direct_fetch"]["hard_fail_count"] == 1


def test_spec455_web_fetch_hard_fail_count_reorders_next_call(
    tmp_path, monkeypatch
):
    from logic import general_tools

    health_path = tmp_path / "web_backend_health.json"
    health_path.write_text(
        json.dumps(
            {
                "web_fetch": {
                    "direct_fetch": {"hard_fail_count": 4},
                    "jina_reader": {"hard_fail_count": 0},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("UPSP_WEB_BACKEND_HEALTH_PATH", str(health_path))
    calls = []

    def direct_fetch(url, timeout_ms):
        calls.append("direct_fetch")
        return {
            "status_code": 200,
            "content_type": "text/plain",
            "final_url": url,
            "content": "direct",
        }

    def jina_reader(url, timeout_ms):
        calls.append("jina_reader")
        return {
            "status_code": 200,
            "content_type": "text/plain",
            "final_url": url,
            "content": "reader",
        }

    monkeypatch.setattr(general_tools, "_default_fetch_url", direct_fetch)
    monkeypatch.setattr(general_tools, "_jina_reader_fetch_url", jina_reader, raising=False)

    result = general_tools.execute_general_tool_call({
        "tool_id": "web_fetch",
        "url": "https://example.com/report",
    })

    assert result["status"] == "ok"
    assert result["selected_backend"] == "jina_reader"
    assert calls == ["jina_reader"]


def test_web_search_general_tool_returns_candidate_sources():
    from logic.general_tools import (
        execute_general_tool_call,
        format_general_tool_fact,
        format_general_tool_material_entry,
    )

    calls = []

    def fake_search(query, max_results, timeout_ms):
        calls.append((query, max_results, timeout_ms))
        return [
            {
                "title": "UPSP source",
                "url": "https://example.com/upsp",
                "snippet": "candidate summary",
                "source": "fake-search",
            },
            {
                "title": "Second source",
                "url": "https://example.com/second",
                "snippet": "another candidate",
            },
        ]

    result = execute_general_tool_call(
        {
            "tool_id": "web_search",
            "query": "UPSP general_tool",
            "max_results": "1",
            "timeout_ms": "2000",
        },
        web_search_fn=fake_search,
    )

    assert calls == [("UPSP general_tool", 5, 2000)]
    assert result["tool_id"] == "web_search"
    assert result["handler"] == "web_search_handler"
    assert result["permission_scope"] == "public_web_read"
    assert result["status"] == "ok"
    assert result["query"] == "UPSP general_tool"
    assert result["result_count"] == 2
    assert result["read_mode"] == "bounded"
    assert result["window_kind"] == "web_search_bounded"
    assert result["window_results"] == 5
    assert "max_results" not in result
    assert result["has_more"] == "unknown"
    assert result["results"][0] == {
        "title": "UPSP source",
        "url": "https://example.com/upsp",
        "domain": "example.com",
        "snippet": "candidate summary",
        "source": "fake-search",
        "source_backend": "fake-search",
        "candidate_kind": "search_candidate",
        "official_candidate": False,
    }
    assert result["protocol_tool_receipt"] is False

    fact = format_general_tool_fact(result)
    material = format_general_tool_material_entry(result)
    assert "本轮已经完成网页搜索。" in fact
    assert "结果窗口：2/5。" in fact
    assert "搜索结果只是候选来源，不代表网页正文已读。" in fact
    assert "如需正文，应继续调用 web_fetch。" in fact
    assert "官方资料、价格或技术文档任务应优先对官方域名候选调用 web_fetch。" in fact
    assert "搜索不准时，换官方域名、已知 URL 或更具体查询重试。" in fact
    assert "UPSP source" not in fact
    assert "https://example.com/upsp" not in fact
    assert material["kind"] == "material"
    assert material["tool_id"] == "web_search"
    assert "候选来源（搜索结果不是正文）" in material["content"]
    assert "https://example.com/upsp" in material["content"]
    assert "domain=example.com" in material["content"]
    assert "kind=search_candidate" in material["content"]
    assert "source_backend=fake-search" in material["content"]


def test_spec517_web_search_ranks_and_labels_official_candidates():
    from logic.general_tools import (
        execute_general_tool_call,
        format_general_tool_fact,
        format_general_tool_material_entry,
    )

    def fake_search(query, max_results, timeout_ms):
        return [
            {
                "title": "OpenAI pricing mirror",
                "url": "https://example.net/openai-pricing",
                "snippet": "mirror summary",
                "source": "fake-search",
            },
            {
                "title": "OpenAI Pricing",
                "url": "https://openai.com/api/pricing",
                "snippet": "official pricing",
                "source": "fake-search",
            },
        ]

    result = execute_general_tool_call(
        {
            "tool_id": "web_search",
            "query": "OpenAI API pricing",
            "timeout_ms": "2000",
        },
        web_search_fn=fake_search,
    )

    assert result["status"] == "ok"
    assert result["results"][0]["domain"] == "openai.com"
    assert result["results"][0]["candidate_kind"] == "official_domain_candidate"
    assert result["results"][0]["official_candidate"] is True

    fact = format_general_tool_fact(result)
    material = format_general_tool_material_entry(result)
    assert "官方资料、价格或技术文档任务应优先对官方域名候选调用 web_fetch。" in fact
    assert "搜索不准时，换官方域名、已知 URL 或更具体查询重试。" in fact
    assert "domain=openai.com" in material["content"]
    assert "kind=official_domain_candidate" in material["content"]
    assert "source_backend=fake-search" in material["content"]


def test_spec517_web_fetch_marks_js_shell_not_body_evidence():
    from logic.general_tools import (
        execute_general_tool_call,
        format_general_tool_fact,
        format_general_tool_material_entry,
    )

    def fake_fetch(url, timeout_ms):
        return {
            "status_code": 200,
            "content_type": "text/html; charset=utf-8",
            "final_url": url,
            "title": "App Shell",
            "content": "You need to enable JavaScript to run this app.",
        }

    result = execute_general_tool_call(
        {
            "tool_id": "web_fetch",
            "url": "https://example.com/app",
            "timeout_ms": "2000",
        },
        web_fetch_fn=fake_fetch,
    )

    assert result["status"] == "ok"
    assert result["content_quality"] == "js_shell"
    assert result["content_quality_reason"] == "javascript_required_or_app_shell"
    assert result["evidence_refs"] == []
    assert "evidence_handle" not in result

    fact = format_general_tool_fact(result)
    material = format_general_tool_material_entry(result)
    assert "内容质量：js_shell（javascript_required_or_app_shell）。" in fact
    assert "这不能作为可靠网页正文证据" in fact
    assert "【网页正文质量提示】content_quality=js_shell; reason=javascript_required_or_app_shell。" in material["content"]


def test_spec522_web_fetch_short_body_with_title_is_not_body_evidence():
    from logic.general_tools import execute_general_tool_call, format_general_tool_fact

    def fake_fetch(url, timeout_ms):
        return {
            "status_code": 200,
            "content_type": "text/html; charset=utf-8",
            "final_url": url,
            "title": "Pricing",
            "content": "OK",
        }

    result = execute_general_tool_call(
        {
            "tool_id": "web_fetch",
            "url": "https://example.com/pricing",
            "timeout_ms": "2000",
        },
        web_fetch_fn=fake_fetch,
    )

    assert result["status"] == "ok"
    assert result["content_quality"] == "insufficient_text"
    assert result["content_quality_reason"] == "body_too_short"
    assert result["evidence_refs"] == []
    assert "evidence_handle" not in result
    assert "这不能作为可靠网页正文证据" in format_general_tool_fact(result)


def test_spec429_default_web_search_falls_back_from_duckduckgo_challenge(monkeypatch):
    from logic import general_tools

    calls = []

    def fake_fetch(url, timeout_ms):
        calls.append(url)
        if "duckduckgo.com" in url:
            body = (
                "<html><body>"
                "Unfortunately, bots use DuckDuckGo too. "
                "Please complete the following challenge."
                "</body></html>"
            ).encode("utf-8")
            return {
                "status_code": 202,
                "content_type": "text/html; charset=utf-8",
                "final_url": url,
                "body_bytes": body,
            }
        body = (
            "<html><body>"
            "<li class=\"b_algo\"><h2><a href=\"https://example.com/gaia\">"
            "GAIA benchmark</a></h2><div class=\"b_caption\"><p>"
            "Agent benchmark task source.</p></div></li>"
            "</body></html>"
        ).encode("utf-8")
        return {
            "status_code": 200,
            "content_type": "text/html; charset=utf-8",
            "final_url": url,
            "body_bytes": body,
        }

    monkeypatch.setattr(general_tools, "_default_fetch_url", fake_fetch)

    results = general_tools._default_search_web("GAIA agent benchmark", 5, 2000)

    assert len(calls) == 2
    assert "duckduckgo.com" in calls[0]
    assert "bing.com" in calls[1]
    assert results == [{
        "title": "GAIA benchmark",
        "url": "https://example.com/gaia",
        "snippet": "Agent benchmark task source.",
        "source": "bing_html",
    }]


def test_spec429_web_search_challenge_blocked_is_not_ok_zero(tmp_path, monkeypatch):
    from logic import general_tools

    monkeypatch.setenv("UPSP_WEB_BACKEND_HEALTH_PATH", str(tmp_path / "web_health.json"))

    def fake_fetch(url, timeout_ms):
        body = (
            "<html><body>"
            "Unfortunately, bots use DuckDuckGo too. "
            "Please complete the following challenge."
            "</body></html>"
        ).encode("utf-8")
        return {
            "status_code": 202,
            "content_type": "text/html; charset=utf-8",
            "final_url": url,
            "body_bytes": body,
        }

    monkeypatch.setattr(general_tools, "_default_fetch_url", fake_fetch)

    result = general_tools.execute_general_tool_call({
        "tool_id": "web_search",
        "query": "GAIA agent benchmark",
        "timeout_ms": "2000",
    })

    assert result["status"] == "failed"
    assert result["reason"] == "web_backend_exhausted"
    assert result["query"] == "GAIA agent benchmark"
    assert result["result_count"] == 0
    assert result["results"] == []
    assert any(
        item.get("reason") == "search_blocked"
        for item in result.get("backend_attempts", [])
    )


def test_spec429_web_search_empty_results_remain_ok():
    from logic.general_tools import execute_general_tool_call

    result = execute_general_tool_call(
        {
            "tool_id": "web_search",
            "query": "unlikely empty query",
            "timeout_ms": "2000",
        },
        web_search_fn=lambda query, max_results, timeout_ms: [],
    )

    assert result["status"] == "ok"
    assert result["result_count"] == 0
    assert result["results"] == []


def test_spec457_web_search_injected_backend_ignores_persisted_health(tmp_path, monkeypatch):
    from logic.general_tools import execute_general_tool_call

    health_path = tmp_path / "web_backend_health.json"
    health_path.write_text(
        json.dumps(
            {
                "web_search": {
                    "ddgs": {"hard_fail_count": 9},
                    "html_fallback": {"hard_fail_count": 0},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("UPSP_WEB_BACKEND_HEALTH_PATH", str(health_path))
    calls = []

    def fake_search(query, max_results, timeout_ms):
        calls.append((query, max_results, timeout_ms))
        return []

    result = execute_general_tool_call(
        {
            "tool_id": "web_search",
            "query": "empty injected search",
            "timeout_ms": "2000",
        },
        web_search_fn=fake_search,
    )

    assert result["status"] == "ok"
    assert result["selected_backend"] == "ddgs"
    assert result["result_count"] == 0
    assert calls == [("empty injected search", 5, 2000)]
    health = json.loads(health_path.read_text(encoding="utf-8"))
    assert health["web_search"]["ddgs"]["hard_fail_count"] == 9


def test_spec455_web_search_routes_to_html_after_ddgs_hard_fail(
    tmp_path, monkeypatch
):
    from urllib.error import URLError
    from logic import general_tools

    health_path = tmp_path / "web_backend_health.json"
    monkeypatch.setenv("UPSP_WEB_BACKEND_HEALTH_PATH", str(health_path))
    calls = []

    def ddgs_search(query, max_results, timeout_ms):
        calls.append(("ddgs", query))
        raise URLError("ddgs unavailable")

    def html_search(query, max_results, timeout_ms):
        calls.append(("html_fallback", query))
        return [{
            "title": "Official source",
            "url": "https://example.com/source",
            "snippet": "official page",
            "source": "bing_html",
        }]

    monkeypatch.setattr(general_tools, "_ddgs_search_web", ddgs_search, raising=False)
    monkeypatch.setattr(general_tools, "_html_search_web", html_search, raising=False)

    result = general_tools.execute_general_tool_call({
        "tool_id": "web_search",
        "query": "official pricing page",
        "timeout_ms": "2000",
    })

    assert result["status"] == "ok"
    assert result["selected_backend"] == "html_fallback"
    assert [item["backend_id"] for item in result["backend_attempts"]] == [
        "ddgs",
        "html_fallback",
    ]
    assert result["backend_attempts"][0]["status"] == "failed"
    assert result["result_count"] == 1
    assert calls == [
        ("ddgs", "official pricing page"),
        ("html_fallback", "official pricing page"),
    ]

    health = json.loads(health_path.read_text(encoding="utf-8"))
    assert health["web_search"]["ddgs"]["hard_fail_count"] == 1
