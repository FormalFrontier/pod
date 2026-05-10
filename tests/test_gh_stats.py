"""Tests for `pod gh-stats`."""

from __future__ import annotations

import datetime
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from pod import cli


def _now_iso(seconds_ago: int = 0) -> str:
    t = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        seconds=seconds_ago)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _row(**fields):
    base = {
        "ts": _now_iso(0),
        "verb": "GET",
        "url": "/repos/o/r/issues/1",
        "status": 200,
        "ms": 100,
        "cache_hit": False,
        "caller": "cli.py:foo:42",
        "bucket": "core",
        "transport": "httpx",
        "bytes": 1024,
    }
    base.update(fields)
    return json.dumps(base)


def _args(log: Path, *, since="1h", by="caller", top=20):
    return SimpleNamespace(log=str(log), since=since, by=by, top=top)


class GhStatsTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.log = Path(self._tmp.name) / "gh-access.log"

    def _run(self, args):
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf):
            cli.cmd_gh_stats({}, args)
        return buf.getvalue()

    def test_missing_log_exits(self):
        self.log.unlink(missing_ok=True)
        with self.assertRaises(SystemExit):
            cli.cmd_gh_stats({}, _args(self.log))

    def test_empty_window_reports_no_rows(self):
        self.log.write_text("")
        out = self._run(_args(self.log))
        self.assertIn("No log rows", out)

    def test_aggregates_by_caller(self):
        self.log.write_text("\n".join([
            _row(caller="cli.py:foo:1"),
            _row(caller="cli.py:foo:1", cache_hit=True),
            _row(caller="cli.py:bar:2"),
        ]))
        out = self._run(_args(self.log, by="caller"))
        # Top group (cli.py:foo:1) has 2 calls, 50% cache; bar has 1.
        self.assertIn("cli.py:foo:1", out)
        self.assertIn("cli.py:bar:2", out)
        # The two-call row appears before the one-call row in the output.
        idx_foo = out.index("cli.py:foo:1")
        idx_bar = out.index("cli.py:bar:2")
        self.assertLess(idx_foo, idx_bar)

    def test_aggregates_by_path(self):
        self.log.write_text("\n".join([
            _row(url="/a", caller="cli.py:foo:1"),
            _row(url="/b", caller="cli.py:foo:1"),
            _row(url="/a", caller="cli.py:bar:2"),
        ]))
        out = self._run(_args(self.log, by="url"))
        # /a appears twice, /b once.
        self.assertIn("/a", out)
        self.assertIn("/b", out)

    def test_filters_outside_window(self):
        self.log.write_text("\n".join([
            _row(ts=_now_iso(seconds_ago=120), caller="caller-recent"),
            _row(ts=_now_iso(seconds_ago=7200), caller="caller-old-now"),  # 2h ago
        ]))
        out = self._run(_args(self.log, since="1h"))
        self.assertIn("caller-recent", out)
        self.assertNotIn("caller-old-now", out)

    def test_top_limits_groups(self):
        self.log.write_text("\n".join([
            _row(caller=f"cli.py:fn:{i}") for i in range(50)
        ]))
        out = self._run(_args(self.log, top=3))
        # Total line says 50 calls; only 3 group lines below.
        self.assertIn("Total: 50 calls", out)
        # Crude: count grouped lines (lines that start with two spaces and a digit).
        group_lines = [ln for ln in out.splitlines()
                       if ln.startswith("    ") and ln.strip().split()[0].isdigit()]
        self.assertLessEqual(len(group_lines), 3)

    def test_invalid_since_exits(self):
        self.log.write_text(_row())
        with self.assertRaises(SystemExit):
            cli.cmd_gh_stats({}, _args(self.log, since="forever"))

    def test_unparseable_lines_are_skipped(self):
        self.log.write_text(_row() + "\n{not json}\n" + _row())
        out = self._run(_args(self.log))
        self.assertIn("Total: 2 calls", out)


if __name__ == "__main__":
    unittest.main()
