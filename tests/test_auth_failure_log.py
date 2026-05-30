"""Tests for the auth-failure classifier used in the agent-loop circuit
breaker.

When a broken session (zero tokens, non-zero exit) was caused by an
authentication problem — a logged-out / expired account — the loop logs
"authentication failed on account '<label>'" instead of a generic
backoff line, so the operator can see *which* account to re-login.
"""

from __future__ import annotations

from pod import cli
from pod.cli import _looks_like_auth_failure


def test_auth_markers_are_detected():
    for tail in [
        "error: invalid api key provided",
        "please run /login to authenticate",
        "oauth token has expired",
        "http 401 unauthorized",
        "authentication failed: not logged in",
    ]:
        assert _looks_like_auth_failure(tail) is True, tail


def test_non_auth_failures_are_not_flagged():
    for tail in [
        "hit your limit; resets at 5pm",        # quota, not auth
        "rate limit exceeded",                  # quota, not auth
        "segmentation fault (core dumped)",     # crash
        "traceback (most recent call last):",   # crash
        "",                                     # no output
    ]:
        assert _looks_like_auth_failure(tail) is False, tail


def test_read_session_stdout_tail_lowercases_and_matches(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "PROJECT_DIR", tmp_path)
    sessions = tmp_path / "sessions"
    sessions.mkdir()
    (sessions / "U123.stdout").write_text("startup...\nOAuth Token Has Expired\n")
    tail = cli._read_session_stdout_tail("U123", {})
    assert "oauth token has expired" in tail  # lowercased
    assert _looks_like_auth_failure(tail) is True


def test_read_session_stdout_tail_missing_file_is_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "PROJECT_DIR", tmp_path)
    assert cli._read_session_stdout_tail("nonexistent", {}) == ""
