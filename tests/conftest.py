"""Pytest fixtures shared across the suite.

Most importantly: this installs a session-wide auto-use fixture that
swaps `pod.github.get_client()` to return a `_FakeClient` by default,
so any test path that triggers a layer-routed GitHub call doesn't
silently hit the real API. Tests that need a real client (or a custom
fake) can override via `with patch_client(...)`.
"""

from __future__ import annotations

import pytest
from unittest import mock

from pod import github as gh
from _gh_helpers import _FakeClient, fake_response


@pytest.fixture(autouse=True)
def _default_fake_gh_client():
    """Auto-use: every test starts with a no-op fake client. Tests that
    set up their own client via `patch_client(...)` will override this
    one (the inner patch returns the inner client; on exit, the outer
    fixture's client is restored)."""
    fake = _FakeClient(default=fake_response(
        503, body={"message": "test default — no route configured"}))
    p = mock.patch.object(gh, "get_client", return_value=fake)
    p.start()
    try:
        yield fake
    finally:
        p.stop()
