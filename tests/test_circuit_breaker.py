"""Tests for cli._session_is_broken — the agent-loop circuit breaker.

The breaker decides whether a finished session "never reached the model"
and should trigger backoff instead of an immediate re-dispatch. The
regression these guard: a logged-out account fails auth ~20s into
startup with zero tokens and a non-zero exit, which the old
`elapsed < 15` bound let through, so the agent re-dispatched onto the
broken account forever.
"""

from __future__ import annotations

from pod.cli import _session_is_broken


def test_auth_failure_20s_nonzero_zero_tokens_is_broken():
    # The qim crash-loop signature: ~20s, exit 1, no tokens.
    assert _session_is_broken(exit_code=1, tokens_in=0, tokens_out=0,
                              elapsed=20.0) is True


def test_fast_zero_token_exit_is_broken_even_on_zero_exit():
    assert _session_is_broken(exit_code=0, tokens_in=0, tokens_out=0,
                              elapsed=4.0) is True


def test_long_zero_token_clean_exit_is_not_broken():
    # Exited cleanly after real wall-clock with no tokens — not a startup
    # failure; don't back off.
    assert _session_is_broken(exit_code=0, tokens_in=0, tokens_out=0,
                              elapsed=20.0) is False


def test_nonzero_exit_with_tokens_is_not_broken():
    # The model was reached and produced output before erroring — a real
    # session failure, handled elsewhere, not a startup crash loop.
    assert _session_is_broken(exit_code=1, tokens_in=1200, tokens_out=300,
                              elapsed=90.0) is False


def test_nonzero_exit_with_only_input_tokens_is_not_broken():
    assert _session_is_broken(exit_code=1, tokens_in=50, tokens_out=0,
                              elapsed=3.0) is False
