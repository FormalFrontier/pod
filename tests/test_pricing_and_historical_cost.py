import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pod import cli


class PricingResolverTests(unittest.TestCase):
    def test_specific_model_wins(self):
        cfg = {"pricing": {
            "codex": {
                "gpt-5.4": {"input": 2.5, "output": 15.0, "cache_read": 0.25, "cache_create": 0.0},
                "default": {"input": 99.0, "output": 99.0, "cache_read": 99.0, "cache_create": 0.0},
            },
            "claude": {"opus": {"input": 5.0, "output": 25.0, "cache_read": 0.5, "cache_create": 6.25}},
        }}
        p = cli._pricing_for(cfg, "codex", "gpt-5.4")
        self.assertEqual(p["input"], 2.5)

    def test_backend_default_when_model_unknown(self):
        cfg = {"pricing": {
            "codex": {
                "gpt-5.4": {"input": 2.5, "output": 15.0, "cache_read": 0.25, "cache_create": 0.0},
                "default": {"input": 3.0, "output": 16.0, "cache_read": 0.3, "cache_create": 0.0},
            },
        }}
        p = cli._pricing_for(cfg, "codex", "gpt-5.4-mini-future")
        self.assertEqual(p["input"], 3.0)

    def test_legacy_flat_pricing_still_honoured(self):
        cfg = {"pricing": {"input": 7.0, "output": 70.0, "cache_read": 0.7, "cache_create": 8.0}}
        p = cli._pricing_for(cfg, "claude", "opus")
        self.assertEqual(p["input"], 7.0)
        self.assertEqual(p["output"], 70.0)

    def test_baked_in_fallback_when_no_pricing_configured(self):
        p = cli._pricing_for({}, "claude", "opus")
        self.assertEqual(p["input"], 5.0)

    def test_missing_model_backend_default(self):
        cfg = {"pricing": {"claude": {"default": {"input": 1.0, "output": 2.0,
                                                      "cache_read": 0.1, "cache_create": 0.2}}}}
        p = cli._pricing_for(cfg, "claude", "")
        self.assertEqual(p["input"], 1.0)


class AgentStateCostTests(unittest.TestCase):
    def test_codex_state_priced_at_codex_rates(self):
        cfg = {"pricing": {
            "codex": {"gpt-5.4": {"input": 2.5, "output": 15.0, "cache_read": 0.25, "cache_create": 0.0}},
            "claude": {"opus": {"input": 5.0, "output": 25.0, "cache_read": 0.5, "cache_create": 6.25}},
        }}
        state = cli.AgentState(
            short_id="x", backend="codex", model="gpt-5.4",
            tokens_in=1_000_000, tokens_out=1_000_000,
            cache_read=1_000_000, cache_create=0,
        )
        # 1M non-cached in * 2.5 + 1M out * 15 + 1M cached * 0.25 = 2.5 + 15 + 0.25
        self.assertAlmostEqual(state.cost(cfg), 17.75, places=4)

    def test_claude_state_priced_at_claude_rates_even_with_codex_config_present(self):
        cfg = {"pricing": {
            "codex": {"gpt-5.4": {"input": 2.5, "output": 15.0, "cache_read": 0.25, "cache_create": 0.0}},
            "claude": {"opus": {"input": 5.0, "output": 25.0, "cache_read": 0.5, "cache_create": 6.25}},
        }}
        state = cli.AgentState(
            short_id="y", backend="claude", model="opus",
            tokens_in=1_000_000, tokens_out=1_000_000,
            cache_read=1_000_000, cache_create=1_000_000,
        )
        # 1M * 5 + 1M * 25 + 1M * 0.5 + 1M * 6.25 = 36.75
        self.assertAlmostEqual(state.cost(cfg), 36.75, places=4)


class CodexJsonlParserTests(unittest.TestCase):
    def test_codex_input_tokens_are_split_into_cached_and_non_cached(self):
        # Codex stdout reports `input_tokens` inclusive of cached; pod
        # must normalize so `tokens_in` holds only the non-cached portion.
        state = cli.AgentState(short_id="z")
        line = json.dumps({
            "type": "turn.completed",
            "usage": {"input_tokens": 10_000, "cached_input_tokens": 7_000, "output_tokens": 500},
        }).encode()
        cli._parse_codex_jsonl_line(line, state)
        self.assertEqual(state.tokens_in, 3_000)
        self.assertEqual(state.cache_read, 7_000)
        self.assertEqual(state.tokens_out, 500)
        self.assertEqual(state.cache_create, 0)


class CodexHistoricalCostTests(unittest.TestCase):
    def _cfg(self):
        return {
            "pricing": {
                "codex": {
                    "gpt-5.4": {"input": 2.5, "output": 15.0, "cache_read": 0.25, "cache_create": 0.0},
                    "default": {"input": 2.5, "output": 15.0, "cache_read": 0.25, "cache_create": 0.0},
                },
                "claude": {
                    "default": {"input": 5.0, "output": 25.0, "cache_read": 0.5, "cache_create": 6.25},
                },
            },
            "project": {"session_dir": "sessions"},
        }

    def _write_rollout(self, rollout_dir: Path, session_uuid: str,
                        input_tokens: int, cached: int, output_tokens: int):
        rollout_dir.mkdir(parents=True, exist_ok=True)
        path = rollout_dir / f"rollout-2026-04-22T03-00-00-{session_uuid}.jsonl"
        with open(path, "w") as fh:
            fh.write(json.dumps({"type": "session_meta"}) + "\n")
            fh.write(json.dumps({
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "total_token_usage": {
                            "input_tokens": input_tokens,
                            "cached_input_tokens": cached,
                            "output_tokens": output_tokens,
                        }
                    },
                },
            }) + "\n")
        return path

    def _write_manifest(self, project_dir: Path, session_uuid: str, model: str):
        mdir = project_dir / ".pod" / "codex-sessions" / "manifests"
        mdir.mkdir(parents=True, exist_ok=True)
        (mdir / f"{session_uuid}.json").write_text(json.dumps({
            "session_id": session_uuid,
            "backend": "codex",
            "model": model,
            "wt_dir": str(project_dir),
            "started_at": 0.0,
        }))

    def test_codex_rollout_priced_via_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_dir = Path(tmp)
            sid = "00000000-0000-0000-0000-000000000001"
            self._write_rollout(
                project_dir / ".pod" / "codex-sessions" / "2026" / "04" / "22",
                sid, input_tokens=2_000_000, cached=1_000_000, output_tokens=100_000,
            )
            self._write_manifest(project_dir, sid, "gpt-5.4")

            with mock.patch.object(cli, "PROJECT_DIR", project_dir):
                total = cli._codex_historical_cost(self._cfg())

            # non_cached = 1M * 2.5, out = 100k * 15 / 1M = 1.5, cached = 1M * 0.25
            # total = 2.5 + 1.5 + 0.25 = 4.25
            self.assertAlmostEqual(total, 4.25, places=4)

    def test_stdout_skipped_when_rollout_exists_for_same_uuid(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_dir = Path(tmp)
            sid = "00000000-0000-0000-0000-000000000002"
            # Both rollout and stdout for the same session
            self._write_rollout(
                project_dir / ".pod" / "codex-sessions" / "2026" / "04" / "22",
                sid, input_tokens=1_000_000, cached=0, output_tokens=0,
            )
            self._write_manifest(project_dir, sid, "gpt-5.4")
            (project_dir / "sessions").mkdir(parents=True, exist_ok=True)
            stdout_path = project_dir / "sessions" / f"{sid}.stdout"
            stdout_path.write_text(json.dumps({
                "type": "turn.completed",
                "usage": {"input_tokens": 9_999_999, "cached_input_tokens": 0, "output_tokens": 9_999_999},
            }) + "\n")

            with mock.patch.object(cli, "PROJECT_DIR", project_dir):
                total = cli._codex_historical_cost(self._cfg())

            # Rollout only: 1M * 2.5 = 2.5. Stdout must NOT be double-counted.
            self.assertAlmostEqual(total, 2.5, places=4)

    def test_pre_relocation_stdout_still_counted(self):
        # No rollout exists; old .stdout must still contribute.
        with tempfile.TemporaryDirectory() as tmp:
            project_dir = Path(tmp)
            sid = "00000000-0000-0000-0000-000000000003"
            (project_dir / "sessions").mkdir(parents=True, exist_ok=True)
            stdout_path = project_dir / "sessions" / f"{sid}.stdout"
            lines = [
                json.dumps({
                    "type": "turn.completed",
                    "usage": {"input_tokens": 500_000, "cached_input_tokens": 100_000, "output_tokens": 50_000},
                }),
                json.dumps({
                    "type": "turn.completed",
                    "usage": {"input_tokens": 500_000, "cached_input_tokens": 100_000, "output_tokens": 50_000},
                }),
            ]
            stdout_path.write_text("\n".join(lines) + "\n")
            # No manifest → falls back to codex.default pricing

            with mock.patch.object(cli, "PROJECT_DIR", project_dir):
                total = cli._codex_historical_cost(self._cfg())

            # Summed: in=1M, cached=200k, out=100k
            # non_cached_in = 800k * 2.5 = 2.0
            # cached 200k * 0.25 = 0.05
            # out 100k * 15.0 = 1.5
            # total = 3.55
            self.assertAlmostEqual(total, 3.55, places=4)


class CodexManifestTests(unittest.TestCase):
    def test_write_and_read_manifest_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_dir = Path(tmp)
            with mock.patch.object(cli, "PROJECT_DIR", project_dir):
                cli._write_codex_manifest("abc-123", "codex", "gpt-5.4", "/tmp/wt/abc")
                manifest = cli._read_codex_manifest("abc-123")
            self.assertIsNotNone(manifest)
            self.assertEqual(manifest["backend"], "codex")
            self.assertEqual(manifest["model"], "gpt-5.4")
            self.assertEqual(manifest["session_id"], "abc-123")


class RolloutUuidExtractionTests(unittest.TestCase):
    def test_extract_session_uuid_from_rollout_filename(self):
        p = Path("rollout-2026-04-22T03-00-00-019db237-5424-74c1-a3fe-cb090997a4b3.jsonl")
        self.assertEqual(
            cli._session_uuid_from_rollout(p),
            "019db237-5424-74c1-a3fe-cb090997a4b3",
        )

    def test_non_rollout_filename_returns_none(self):
        self.assertIsNone(cli._session_uuid_from_rollout(Path("something-else.jsonl")))


if __name__ == "__main__":
    unittest.main()
