"""pod — Multi-agent manager for Claude Code.

Manages concurrent autonomous Claude sessions with a TUI and CLI interface.
Each agent runs as an independent background process; the TUI is just a viewer.

Usage:
    pod                  # Interactive TUI
    pod init             # Bootstrap .pod/ in current git repo
    pod update           # Re-populate agent config from package
    pod add [N]          # Launch N new agents (default 1); updates target
    pod target N         # Set target agent count (auto-spawn/cap enforced)
    pod list             # Show running agents
    pod finish [ID|all]  # Signal agent(s) to finish after current work
    pod kill [ID|all]    # Kill agent(s) immediately (unclaims issues)
    pod status           # Queue depth, agent count, total cost
    pod cleanup          # Remove stale worktrees not owned by any agent
    pod log [ID]         # Tail agent's session stdout
    pod config           # Print current config
    pod coordination ... # Run bundled coordination script
"""

from __future__ import annotations

import argparse
import contextlib
import curses
import dataclasses
import datetime
import fcntl
import hashlib
import json
import os
import random
import re
import signal
import shutil
import subprocess
import sys
import threading
import time
import tomllib
import uuid
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------


def _find_project_dir() -> Path:
    """Walk up from cwd to find a directory containing .pod/."""
    d = Path.cwd().resolve()
    while True:
        if (d / ".pod").is_dir():
            return d
        if d.parent == d:
            break
        d = d.parent
    # Fallback: git root
    try:
        r = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                           capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            return Path(r.stdout.strip())
    except Exception:
        pass
    return Path.cwd()


def _data_dir() -> Path:
    """Locate bundled data files from the installed package."""
    return Path(__file__).parent / "data"


PROJECT_DIR = _find_project_dir()
POD_DIR = PROJECT_DIR / ".pod"
AGENTS_DIR = POD_DIR / "agents"
CONFIG_PATH = POD_DIR / "config.toml"
LOG_PATH = POD_DIR / "pod.log"
CLAIM_HISTORY_PATH = POD_DIR / "claim-history.json"
PR_CLAIM_HISTORY_PATH = POD_DIR / "pr-claim-history.json"
ISOLATED_CONFIG_DIR = POD_DIR / "claude-config"
TARGET_FILE = POD_DIR / "target"  # Target agent count (int, one per line)
PLANNER_TARGET_FILE = POD_DIR / "planner-target"  # Planner-recommended target agent count
PLANNER_MIN_QUEUE_FILE = POD_DIR / "planner-min-queue"  # Planner-recommended min_queue
FORCE_QUOTA_FILE = POD_DIR / "force-quota"  # If exists, skip quota checks globally

# ---------------------------------------------------------------------------
# Default configuration (written on first run)
# ---------------------------------------------------------------------------

DEFAULT_CONFIG = """\
# pod configuration — all values have sensible defaults.
# Edit this file to customise agent behaviour.

[project]
worktree_base = "worktrees"        # Where git worktrees are created
session_dir = "sessions"           # Session stdout capture directory
build_cache_dir = ".lake"          # Build cache to rsync into worktrees
protected_files = ["PLAN.md"]      # Files agents may not modify in PRs

[merge]
# Status check contexts that must pass before a PR can auto-merge.
# These are the `name:` field of jobs in .github/workflows/*.yml.
# Leave empty to skip branch-protection setup.
#
# Why this matters: `coordination create-pr` runs `gh pr merge --auto` on
# every PR it opens, but GitHub only honours --auto when the default branch
# has branch protection with at least one required status check. Without
# that, PRs go green but never merge.
#
# Example: required_checks = ["build-and-test"]
required_checks = []

[agent]
backend = "claude"                 # "claude" or "codex"

[agent.claude]
model = "opus"                     # Claude model to use
quota_check = "~/.claude/skills/claude-usage/claude-available-model"
quota_check_required = true        # Hard-fail if quota unavailable
quota_retry_seconds = 60           # Sleep duration when quota unavailable
isolated_config = true             # Use pod-managed isolated CLAUDE_CONFIG_DIR for agents

[agent.codex]
model = "gpt-5.4"                  # Codex model to use
quota_check = "~/.claude/skills/claude-usage/codex-available-model"
quota_check_required = false       # Proceed if quota cache missing/stale
quota_retry_seconds = 60           # Sleep duration when quota unavailable
isolated_config = true             # Use strict pod-managed CODEX_HOME (no ~/.codex state except auth.json)

# Dollars per million tokens.
# Resolution order when pricing an agent (see `_pricing_for`):
#   [pricing.<backend>.<model>]
#   → [pricing.<backend>.default]
#   → legacy flat [pricing]
#   → baked-in Claude Opus constants
# Legacy flat [pricing] remains honoured for backward compatibility.

[pricing.claude.opus]
input = 5.00
output = 25.00
cache_read = 0.50
cache_create = 6.25

[pricing.claude.default]
input = 5.00
output = 25.00
cache_read = 0.50
cache_create = 6.25

[pricing.codex."gpt-5.4"]
input = 2.50
output = 15.00
cache_read = 0.25
cache_create = 0.0

[pricing.codex.default]
input = 2.50
output = 15.00
cache_read = 0.25
cache_create = 0.0

[dispatch]
# Built-in strategies: "queue_balance", "round_robin"
# Or a path to a custom script (receives env vars, prints worker type name)
strategy = "queue_balance"
min_queue = 3                      # queue_balance: below this → planner-type

[repair]
# A PR is considered "stuck" if a required check has been IN_PROGRESS
# longer than this many minutes. Used by `coordination list-pr-repair`.
stuck_ci_minutes = 120
# Maximum concurrent repair agents (spawned regardless of planner_target).
concurrency_cap = 2

[monitor]
poll_interval = 2                  # Seconds between status updates
jsonl_stale_warning = 300          # Warn if JSONL unchanged for this many seconds
jsonl_missing_warning = 60         # Warn if JSONL not created after this many seconds
max_claim_restarts = 1             # Max times to auto-restart a dead session before releasing claim
show_costs = true                  # Show estimated API costs in TUI and status
stuck_initial_timeout = 3600       # Seconds since last assistant output before first stuck check
stuck_confirm_timeout = 1200       # Seconds to wait before confirming kill (after first detection)
stuck_check_interval = 30          # Seconds between process health checks during stuck detection

# --- Worker Types ---
# Each [worker_types.<name>] defines a type of agent session.
# The dispatch strategy chooses among these.

[worker_types.plan]
prompt = "/plan"
lock = "planner"                   # Acquire this lock before running
copy_build_cache = false

[worker_types.work]
prompt = "/work"
copy_build_cache = true

# Repair agent: handles unhealthy PRs (merge conflicts, failed CI, stuck CI).
# Only selected when `coordination list-pr-repair` reports candidates (see
# `dispatch_queue_balance`). `copy_build_cache` is enabled so repair agents
# can run the project's verification before pushing.
[worker_types.repair]
prompt = "/repair"
copy_build_cache = true

# Example additional worker type:
# [worker_types.review]
# prompt = "/review"
# copy_build_cache = true
"""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _agent_config_sync_check():
    """Compare pod template commands/skills with the project .claude/ and report/update.

    Only runs for Claude backend — Codex config is installed per-session
    into CODEX_HOME, not into the project tree.

    Uses .claude/.pod-checksums to track what pod last installed, enabling
    three-way detection:
      - pod updated, project unchanged  → auto-overwrite
      - project customised, pod unchanged → print note (user customisation)
      - both changed independently       → warn, don't overwrite
      - no checksums file yet            → bootstrap: record current hashes,
                                           warn on any differences
    """
    # Only meaningful for Claude backend — Codex installs per-session
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "rb") as f:
                cfg = tomllib.load(f)
            if cfg.get("agent", {}).get("backend", "claude") != "claude":
                return
        except (OSError, tomllib.TOMLDecodeError):
            pass
    data_config = _data_dir() / "agent-config" / "claude"
    proj_claude = PROJECT_DIR / ".claude"
    checksums_file = proj_claude / ".pod-checksums"

    # Collect all template files under commands/ and skills/
    template_files: list[Path] = []
    for subdir in ("commands", "skills"):
        src = data_config / subdir
        if src.is_dir():
            for f in src.rglob("*"):
                if f.is_file():
                    template_files.append(f.relative_to(data_config))

    if not template_files:
        return

    stored: dict[str, str] = {}
    parse_ok = False
    if checksums_file.exists():
        try:
            stored = json.loads(checksums_file.read_text())
            parse_ok = True
        except Exception:
            stored = {}

    first_run = not checksums_file.exists() or not parse_ok
    updated: list[str] = []
    custom: list[str] = []
    conflicts: list[str] = []

    for rel in template_files:
        key = str(rel)
        pkg_file = data_config / rel
        proj_file = proj_claude / rel

        pkg_hash = _sha256(pkg_file)
        proj_hash = _sha256(proj_file) if proj_file.exists() else None
        inst_hash = stored.get(key)

        if pkg_hash == proj_hash:
            continue  # identical, nothing to do

        if first_run or inst_hash is None:
            # No tracking history yet — can't distinguish customisation from old version
            if proj_file.exists():
                conflicts.append(key)  # report as unknown difference
            else:
                updated.append(key)    # new file in template, install it
        elif proj_hash == inst_hash:
            # Project unchanged since install, template has been updated
            updated.append(key)
        elif pkg_hash == inst_hash:
            # Template unchanged, project has been customised
            custom.append(key)
        else:
            # Both changed independently
            conflicts.append(key)

    # Apply auto-updates
    for key in updated:
        src = data_config / key
        dst = proj_claude / key
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(dst))

    # Write checksums: record the pod-template hash for every file we installed or
    # that is already in sync.  Do NOT record first-run conflicts — leave their
    # inst_hash absent so they continue to be reported as unknown on future runs.
    skip_keys = set(conflicts) if first_run else set()
    new_checksums = dict(stored)
    for rel in template_files:
        key = str(rel)
        if key in skip_keys:
            continue
        pkg_file = data_config / rel
        # After any copy the project file matches the template; record pkg hash
        # so future runs can distinguish "user edited" from "pod updated".
        new_checksums[key] = _sha256(pkg_file)
    checksums_file.write_text(json.dumps(new_checksums, sort_keys=True, indent=2) + "\n")

    if updated:
        print(f"pod: updated {len(updated)} file(s) from new pod version: "
              f"{', '.join(updated)}")
    if custom:
        print(f"pod: {len(custom)} project-customised file(s) differ from pod template: "
              f"{', '.join(custom)}")
    if conflicts and not first_run:
        print(f"pod: WARNING: {len(conflicts)} file(s) modified in both pod template and "
              f"project .claude/ — not auto-updating: {', '.join(conflicts)}")
    elif conflicts and first_run:
        print(f"pod: {len(conflicts)} file(s) differ from pod template "
              f"(origin unknown, tracking from now): {', '.join(conflicts)}")


def ensure_config() -> dict:
    """Load config, requiring pod init to have been run."""
    if not CONFIG_PATH.exists():
        print(f"No .pod/config.toml found. Run 'pod init' first.", file=sys.stderr)
        sys.exit(1)
    AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    _agent_config_sync_check()
    with open(CONFIG_PATH, "rb") as f:
        cfg = tomllib.load(f)
    return _migrate_legacy_config(cfg)


def get_isolated_config_dir(config: dict) -> Path | None:
    """Return isolated CLAUDE_CONFIG_DIR path, or None if disabled/not Claude.

    Only used for Claude backend. Codex isolation is handled by
    _setup_codex_home() inside launch_agent() via CODEX_HOME env var.
    """
    if _backend(config) != "claude":
        return None
    if not _backend_cfg(config, "isolated_config", default=False):
        return None
    return ISOLATED_CONFIG_DIR


def ensure_isolated_config(config: dict) -> Path | None:
    """Set up isolated CLAUDE_CONFIG_DIR for agents. Returns path or None if disabled."""
    config_dir = get_isolated_config_dir(config)
    if config_dir is None:
        return None

    config_dir.mkdir(parents=True, exist_ok=True)

    # Minimal settings — no hooks, no plugins, no global CLAUDE.md
    settings_path = config_dir / "settings.json"
    settings_path.write_text('{"skipDangerousModePermissionPrompt": true}\n')

    # Symlink credentials from ~/.claude/ so subscription auth works.
    # Race-safe: use try/except since multiple agents may run this concurrently.
    real_claude = Path.home() / ".claude"
    cred_link = config_dir / ".credentials.json"
    cred_target = real_claude / ".credentials.json"
    if cred_target.exists():
        try:
            cred_link.unlink(missing_ok=True)
            cred_link.symlink_to(cred_target)
        except FileExistsError:
            pass  # Another process created it first — fine
    else:
        # Clean up stale symlink if source credential file is gone
        cred_link.unlink(missing_ok=True)

    # JSONL session storage
    (config_dir / "projects").mkdir(exist_ok=True)

    return config_dir


def _instruction_file_warning(git_root: Path, backend: str) -> str | None:
    """Return a warning if instruction files are inconsistent for the backend."""
    agents = git_root / "AGENTS.md"
    claude = git_root / ".claude" / "CLAUDE.md"

    agents_exists = agents.exists() or agents.is_symlink()
    claude_exists = claude.exists() or claude.is_symlink()

    if not agents_exists and not claude_exists:
        return None
    if agents_exists and not claude_exists:
        if backend == "claude":
            return ("warning: repo has AGENTS.md but no .claude/CLAUDE.md, "
                    "while pod is configured for Claude\n"
                    "    → Prefer AGENTS.md -> .claude/CLAUDE.md for cross-backend parity.")
        else:
            return None
    if claude_exists and not agents_exists:
        if backend == "codex":
            return ("warning: repo has .claude/CLAUDE.md but no AGENTS.md, "
                    "while pod is configured for Codex\n"
                    "    → Prefer AGENTS.md -> .claude/CLAUDE.md for cross-backend parity.")
        else:
            return None

    try:
        if agents.is_symlink() and agents.resolve() == claude.resolve():
            return None
    except OSError:
        pass
    try:
        if claude.is_symlink() and claude.resolve() == agents.resolve():
            return None
    except OSError:
        pass

    return ("warning: AGENTS.md and .claude/CLAUDE.md both exist but are not symlinked together\n"
            "    → Prefer AGENTS.md -> .claude/CLAUDE.md for cross-backend parity.")


def _claude_projects_dir(claude_config_dir: Path | None = None) -> Path:
    """Return the directory containing JSONL project subdirs."""
    if claude_config_dir is not None:
        return claude_config_dir / "projects"
    return Path.home() / ".claude" / "projects"


def cfg_get(config: dict, *keys, default=None):
    """Nested dict lookup with default."""
    d = config
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k, default)
        else:
            return default
    return d


def _backend(config: dict) -> str:
    """Return the agent backend name ('claude' or 'codex')."""
    return cfg_get(config, "agent", "backend", default="claude")


def _backend_cfg(config: dict, *keys, default=None):
    """Read a backend-specific config value.
    e.g. _backend_cfg(config, 'model') reads agent.<backend>.model."""
    return cfg_get(config, "agent", _backend(config), *keys, default=default)


def _migrate_legacy_config(cfg: dict) -> dict:
    """Migrate legacy top-level [claude] section to [agent.claude]."""
    if "claude" not in cfg:
        return cfg
    agent = cfg.get("agent", {})
    if "claude" in agent:
        raise SystemExit(
            "Config error: both [claude] and [agent.claude] exist — "
            "remove the legacy [claude] section"
        )
    log("Deprecation: [claude] section is now [agent.claude]; migrating in memory")
    cfg.setdefault("agent", {}).setdefault("backend", "claude")
    cfg["agent"]["claude"] = cfg.pop("claude")
    return cfg


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_log_lock = threading.Lock()


def log(msg: str):
    """Append timestamped message to pod.log."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}\n"
    with _log_lock:
        try:
            with open(LOG_PATH, "a") as f:
                f.write(line)
        except OSError:
            pass


def say(msg: str):
    """Print to stderr and log."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, file=sys.stderr)
    log(msg)


# ---------------------------------------------------------------------------
# Claim history — persists which issues our local sessions have claimed
# ---------------------------------------------------------------------------

CLAIM_HISTORY_LOCK_PATH = POD_DIR / "claim-history.lock"


@contextlib.contextmanager
def _claim_history_filelock():
    """Cross-process exclusive lock for claim-history.json via fcntl.flock."""
    CLAIM_HISTORY_LOCK_PATH.touch()
    fd = open(CLAIM_HISTORY_LOCK_PATH)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()


def load_claim_history() -> dict:
    """Load {issue_num_str -> {session_uuid, short_id, restart_count}}."""
    try:
        return json.loads(CLAIM_HISTORY_PATH.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def _save_claim_history(history: dict):
    """Atomically write claim history."""
    tmp = CLAIM_HISTORY_PATH.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(history, indent=2) + "\n")
        tmp.rename(CLAIM_HISTORY_PATH)
    except OSError as e:
        log(f"Failed to save claim history: {e}")


def record_claim(issue: int, session_uuid: str, short_id: str):
    """Record that our session claimed this issue. Preserves restart_count.

    If a *different* session re-claims an issue that was previously released,
    the new claim overwrites the old released entry (with restart_count=0).
    If the *same* session tries to re-add its own released claim, we skip
    to avoid a feedback loop where a resumed session triggers infinite
    restart spawning.
    """
    with _claim_history_filelock():
        history = load_claim_history()
        key = str(issue)
        existing = history.get(key, {})
        if existing.get("released"):
            if existing.get("session_uuid") == session_uuid:
                return  # Don't re-add our own released claim (prevents restart loops)
            # Different session re-claimed this issue — allow tracking the new claim
        history[key] = {
            "session_uuid": session_uuid,
            "short_id": short_id,
            "restart_count": existing.get("restart_count", 0) if not existing.get("released") else 0,
        }
        _save_claim_history(history)


def clear_claim(issue: int, session_uuid: str | None = None):
    """Mark issue as released in claim history, but only if the entry belongs
    to session_uuid (compare-and-swap).  If session_uuid is None, releases
    unconditionally (legacy callers / PR-created path)."""
    with _claim_history_filelock():
        history = load_claim_history()
        key = str(issue)
        entry = history.get(key)
        if not entry:
            return
        if session_uuid and entry.get("session_uuid") != session_uuid:
            return  # Entry belongs to a different session — don't clobber
        entry["released"] = True
        _save_claim_history(history)


# ---------------------------------------------------------------------------
# PR claim history (for repair agents)
# ---------------------------------------------------------------------------
# Mirrors issue-claim-history but keyed by PR number. A repair agent claims
# a PR via `coordination claim-pr-repair`, which records an entry here. The
# housekeeping loop (check_dead_pr_claimed_prs) releases stale claims whose
# owning session has died, to keep the `repair-claimed` label from leaking.

def load_pr_claim_history() -> dict:
    """Load {pr_num_str -> {session_uuid, short_id, claimed_at}}."""
    try:
        return json.loads(PR_CLAIM_HISTORY_PATH.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def _save_pr_claim_history(history: dict):
    tmp = PR_CLAIM_HISTORY_PATH.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(history, indent=2) + "\n")
        tmp.rename(PR_CLAIM_HISTORY_PATH)
    except OSError as e:
        log(f"Failed to save PR claim history: {e}")


def record_pr_claim(pr: int, session_uuid: str, short_id: str):
    """Record that our session is repairing PR #pr. Reuses the issue-claim
    filelock since writes are rare and concurrency is low."""
    with _claim_history_filelock():
        history = load_pr_claim_history()
        history[str(pr)] = {
            "session_uuid": session_uuid,
            "short_id": short_id,
            "claimed_at": time.time(),
        }
        _save_pr_claim_history(history)


def clear_pr_claim(pr: int, session_uuid: str | None = None):
    """Remove the PR-claim entry, compare-and-swap on session_uuid if given."""
    with _claim_history_filelock():
        history = load_pr_claim_history()
        key = str(pr)
        entry = history.get(key)
        if not entry:
            return
        if session_uuid and entry.get("session_uuid") != session_uuid:
            return
        history.pop(key, None)
        _save_pr_claim_history(history)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class AgentState:
    """Mutable state for one agent, serialised to .pod/agents/<id>.json."""
    short_id: str = ""
    uuid: str = ""
    backend_session_id: str = ""  # Backend-native id (Codex thread_id, or same as uuid for Claude)
    pid: int = 0
    pid_start_time: float = 0.0    # /proc start time — detects PID reuse
    worker_type: str = ""          # e.g. "work", "plan"
    status: str = "starting"       # starting, running, waiting_quota, finishing, stopped
    session_start: float = 0.0
    claimed_issue: int = 0
    pr_number: int = 0
    repair_pr: int = 0             # PR currently claimed by this agent for repair
    git_start: str = ""
    tokens_in: int = 0
    tokens_out: int = 0
    cache_read: int = 0
    cache_create: int = 0
    last_text: str = ""
    last_activity: float = 0.0
    finishing: bool = False
    force_quota: bool = False      # Skip quota check for this agent
    lock_held: str = ""            # Name of lock held (e.g. "planner"), or ""
    loop_iteration: int = 0
    worktree: str = ""
    branch: str = ""
    resume_session_uuid: str = ""  # If set, first iteration uses this UUID (to resume conversation)
    backend: str = "claude"        # "claude" or "codex" (for per-backend pricing lookup)
    model: str = ""                # e.g. "opus", "gpt-5.4" (for per-model pricing lookup)

    def cost(self, config: dict) -> float:
        """Calculate cost in dollars using backend/model-aware pricing."""
        pricing = _pricing_for(config, self.backend, self.model)
        return (
            self.tokens_in * pricing.get("input", 5.0) / 1e6
            + self.cache_create * pricing.get("cache_create", 6.25) / 1e6
            + self.cache_read * pricing.get("cache_read", 0.50) / 1e6
            + self.tokens_out * pricing.get("output", 25.0) / 1e6
        )

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> AgentState:
        known = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in known})

    def write(self):
        """Atomically write state to .pod/agents/<id>.json."""
        path = AGENTS_DIR / f"{self.short_id}.json"
        tmp = path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(self.to_dict(), indent=2) + "\n")
            tmp.rename(path)
        except OSError as e:
            log(f"Failed to write state for {self.short_id}: {e}")

    def remove_file(self):
        """Remove the state file."""
        path = AGENTS_DIR / f"{self.short_id}.json"
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def read_all_agents() -> list[AgentState]:
    """Read all agent state files, filtering out stale (dead process) ones."""
    agents = []
    if not AGENTS_DIR.exists():
        return agents
    for p in sorted(AGENTS_DIR.glob("*.json")):
        if p.suffix != ".json" or p.name.endswith(".tmp"):
            continue
        try:
            d = json.loads(p.read_text())
            agent = AgentState.from_dict(d)
            # Check if process is still alive (and not a reused PID)
            if agent.pid > 0:
                if not _pid_is_valid(agent.pid, agent.pid_start_time):
                    agent.status = "dead"
            agents.append(agent)
        except (json.JSONDecodeError, OSError):
            continue
    return agents


def _read_commented_int(path: Path) -> int | None:
    """Read the first non-comment integer from a small state file."""
    try:
        for line in path.read_text().splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            return int(stripped)
    except (OSError, ValueError):
        return None
    return None


def _write_commented_int(path: Path, value: int, comments: list[str]):
    """Write a small state file with comment headers plus a numeric payload."""
    header = "\n".join(f"# {comment}" for comment in comments)
    path.write_text(f"{header}\n{max(0, value)}\n")


def read_target() -> int | None:
    """Read target agent count from .pod/target, or None if not set."""
    return _read_commented_int(TARGET_FILE)


def write_target(n: int):
    """Write target agent count to .pod/target."""
    _write_commented_int(TARGET_FILE, n, [
        "User target agent count.",
        "pod tries to keep this many agents running.",
    ])


def read_planner_target() -> int | None:
    """Read planner-recommended target from .pod/planner-target, or None if not set."""
    return _read_commented_int(PLANNER_TARGET_FILE)


def read_planner_min_queue() -> int | None:
    """Read planner-recommended min_queue from .pod/planner-min-queue, or None if not set."""
    return _read_commented_int(PLANNER_MIN_QUEUE_FILE)


def get_effective_target() -> int | None:
    """Effective target = min(user_target, planner_target). User target is the ceiling."""
    user_target = read_target()
    planner_target = read_planner_target()
    if user_target is not None and planner_target is not None:
        return min(user_target, planner_target)
    return user_target  # planner_target alone doesn't create a target; user must set one


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _get_pid_start_time(pid: int) -> float:
    """Get process start time from /proc (Linux). Returns 0 if unavailable."""
    try:
        with open(f"/proc/{pid}/stat", "r") as f:
            # Field 22 (1-indexed) is starttime; split past last ')' for comm
            parts = f.read().split(")")[-1].split()
            return float(parts[19])  # starttime = field 22 - 2 (pid,comm) = index 19
    except (OSError, IndexError, ValueError):
        return 0.0


def _pid_is_valid(pid: int, expected_start: float) -> bool:
    """Check if PID is alive AND belongs to the expected process."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass  # Exists but we can't signal it — still valid
    if expected_start > 0:
        actual = _get_pid_start_time(pid)
        if actual > 0 and actual != expected_start:
            return False  # PID reused by a different process
    return True


def _check_process_stuck(pid: int) -> tuple[bool, str]:
    """Check if a claude subprocess (process group leader) appears stuck.

    Checks two hard signals for the entire process group:
      1. CPU usage: sum of %CPU across all group members must be < 0.5%
      2. No network connections: no TCP/UDP sockets open by any group member

    Child process state is logged for diagnostics but not used as a gate.

    Returns (is_stuck, detail) where detail is a human-readable string.
    Both hard signals must indicate stuck for is_stuck=True.
    If any check fails, conservatively returns (False, ...).
    """
    details = []

    # --- Get all processes in the process group ---
    try:
        result = subprocess.run(
            ["ps", "ax", "-o", "pid=,pgid=,%cpu=,state="],
            capture_output=True, text=True, timeout=10,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        return (False, f"ps failed: {e}")

    group_procs: list[tuple[int, float, str]] = []  # (pid, cpu, state)
    for line in result.stdout.strip().splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        try:
            p_pid = int(parts[0])
            p_pgid = int(parts[1])
            p_cpu = float(parts[2])
            p_state = parts[3]
        except (ValueError, IndexError):
            continue
        if p_pgid == pid:
            group_procs.append((p_pid, p_cpu, p_state))

    if not group_procs:
        return (True, "process group empty (all processes gone)")

    # --- CPU check (hard signal) ---
    total_cpu = sum(cpu for _, cpu, _ in group_procs)
    if total_cpu >= 0.5:
        return (False, f"CPU active: {total_cpu:.1f}% across {len(group_procs)} processes")
    details.append(f"cpu={total_cpu:.1f}%")

    # --- Child state (diagnostic only, not a gate) ---
    children = [(p, s) for p, _, s in group_procs if p != pid]
    zombie_children = [(p, s) for p, s in children if s.startswith("Z")]
    active_children = [(p, s) for p, s in children if not s.startswith("Z")]
    if children:
        if active_children:
            details.append(f"children={len(children)} ({len(active_children)} active, "
                           f"{len(zombie_children)} zombie)")
        else:
            details.append(f"children={len(children)} all zombie")
    else:
        details.append("no children")

    # --- Network connections (hard signal) ---
    all_pids = [str(p) for p, _, _ in group_procs]
    pid_list_str = ",".join(all_pids)
    try:
        result = subprocess.run(
            ["lsof", "-i", "-a", "-p", pid_list_str],
            capture_output=True, text=True, timeout=10,
        )
        # lsof returns exit 1 when no matches found (normal for "no connections")
        net_lines = [l for l in result.stdout.strip().splitlines()
                     if l and not l.startswith("COMMAND")]
        if net_lines:
            return (False, f"network connections: {len(net_lines)} open sockets")
        details.append("no network")
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        return (False, f"lsof failed: {e}")

    # --- Both hard signals indicate stuck ---
    return (True, "; ".join(details))


def _kill_stuck_subprocess(proc: subprocess.Popen) -> None:
    """Kill a stuck claude subprocess and its entire process group.

    Uses SIGTERM then SIGKILL escalation (same as _sigterm_handler).
    Does NOT exit the agent process or do cleanup — the caller's monitor
    loop detects proc.poll() != None and proceeds through normal
    session-end logic (unclaiming, worktree cleanup, etc.).
    """
    try:
        pgid = os.getpgid(proc.pid)
    except (ProcessLookupError, OSError):
        return  # Already dead

    # SIGTERM first
    try:
        os.killpg(pgid, signal.SIGTERM)
    except (ProcessLookupError, OSError):
        return

    # Wait briefly for graceful shutdown
    try:
        proc.wait(timeout=5)
        return  # Exited cleanly
    except subprocess.TimeoutExpired:
        pass

    # SIGKILL — force kill
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, OSError):
        pass

    # Final wait to reap
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        log(f"WARNING: failed to reap stuck subprocess PID {proc.pid} after SIGKILL")


def human_size(n: int) -> str:
    if n >= 1048576:
        return f"{n / 1048576:.1f}MB"
    if n >= 1024:
        return f"{n / 1024:.1f}KB"
    return f"{n}B"


def human_duration(secs: int | float) -> str:
    secs = int(secs)
    if secs >= 3600:
        return f"{secs // 3600}h{secs % 3600 // 60:02d}m"
    if secs >= 60:
        return f"{secs // 60}m{secs % 60:02d}s"
    return f"{secs}s"


def timeago(iso_ts: str) -> str:
    """Convert ISO 8601 timestamp to relative time string like '2h ago'."""
    if not iso_ts:
        return ""
    try:
        dt = datetime.datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        delta = datetime.datetime.now(datetime.timezone.utc) - dt
        secs = int(delta.total_seconds())
        if secs < 0:
            return ""
        if secs < 60:
            return f"{secs}s ago"
        if secs < 3600:
            return f"{secs // 60}m ago"
        if secs < 86400:
            return f"{secs // 3600}h ago"
        return f"{secs // 86400}d ago"
    except (ValueError, TypeError):
        return ""


def fmt_tokens(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1000:
        return f"{n / 1000:.0f}k"
    return str(n)


_BAKED_IN_PRICING = {
    "input": 5.00,
    "output": 25.00,
    "cache_read": 0.50,
    "cache_create": 6.25,
}


def _pricing_for(config: dict, backend: str, model: str) -> dict:
    """Resolve the pricing dict for a (backend, model) pair.

    Fallback order:
      pricing.<backend>.<model>
        → pricing.<backend>.default
        → legacy flat [pricing]
        → baked-in Claude Opus constants
    """
    pricing_root = (config or {}).get("pricing") or {}
    backend_table = pricing_root.get(backend) if isinstance(pricing_root, dict) else None
    if isinstance(backend_table, dict):
        specific = backend_table.get(model) if model else None
        if isinstance(specific, dict):
            return specific
        default = backend_table.get("default")
        if isinstance(default, dict):
            return default
    # Legacy flat [pricing] = {"input": ..., "output": ..., ...}
    if isinstance(pricing_root, dict) and all(
        isinstance(v, (int, float)) for v in pricing_root.values()
    ) and pricing_root:
        return pricing_root
    return _BAKED_IN_PRICING


def token_summary(state: AgentState, config: dict, show_costs: bool = True) -> str:
    total_in = state.tokens_in + state.cache_read + state.cache_create
    if total_in == 0 and state.tokens_out == 0:
        return ""
    if show_costs:
        cost = state.cost(config)
        return f"{fmt_tokens(total_in)}/{fmt_tokens(state.tokens_out)}~${cost:.2f}"
    return f"{fmt_tokens(total_in)}/{fmt_tokens(state.tokens_out)}"


def _price_tokens(pricing: dict, tokens_in: int, tokens_out: int,
                  cache_read: int, cache_create: int) -> float:
    return (
        tokens_in * pricing.get("input", 5.0) / 1e6
        + cache_create * pricing.get("cache_create", 6.25) / 1e6
        + cache_read * pricing.get("cache_read", 0.50) / 1e6
        + tokens_out * pricing.get("output", 25.0) / 1e6
    )


def _claude_historical_cost(config: dict,
                             claude_config_dir: Path | None) -> float:
    """Scan Claude JSONL rollouts for this project and return total $."""
    # Compute the Claude projects-dir prefix for this project.
    # Claude Code munges paths: replace / with -, strip leading dots from components.
    def _claude_dir_name(p: Path) -> str:
        parts = str(p).split("/")
        cleaned = [part.lstrip(".") for part in parts]
        return "-".join(cleaned)

    project_prefix = _claude_dir_name(PROJECT_DIR)

    # Scan both ~/.claude/projects (historical) and isolated config (new sessions)
    projects_dirs = []
    real_projects = Path.home() / ".claude" / "projects"
    if real_projects.is_dir():
        projects_dirs.append(real_projects)
    if claude_config_dir is not None:
        iso_projects = claude_config_dir / "projects"
        if iso_projects.is_dir():
            projects_dirs.append(iso_projects)

    total_in = 0
    total_out = 0
    total_cache_read = 0
    total_cache_create = 0

    for projects_dir in projects_dirs:
        for d in projects_dir.iterdir():
            if not d.is_dir():
                continue
            name = d.name
            # Match this project's dirs (main + worktrees)
            if not (name == project_prefix or name.startswith(project_prefix + "-")):
                continue
            for f in d.glob("*.jsonl"):
                try:
                    with open(f) as fh:
                        for line in fh:
                            if '"usage"' not in line or '"assistant"' not in line:
                                continue
                            try:
                                rec = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            if rec.get("type") != "assistant":
                                continue
                            usage = rec.get("message", {}).get("usage", {})
                            total_in += usage.get("input_tokens", 0)
                            total_out += usage.get("output_tokens", 0)
                            total_cache_read += usage.get("cache_read_input_tokens", 0)
                            total_cache_create += usage.get("cache_creation_input_tokens", 0)
                except OSError:
                    continue

    # Historical Claude runs predate the per-session backend/model manifest,
    # so price them at the Claude default tier.
    pricing = _pricing_for(config, "claude", "")
    return _price_tokens(pricing, total_in, total_out, total_cache_read, total_cache_create)


def _read_codex_manifest(session_uuid: str) -> dict | None:
    path = _codex_manifest_dir() / f"{session_uuid}.json"
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def _session_uuid_from_rollout(rollout_path: Path) -> str | None:
    """Extract the session UUID from a Codex rollout filename.

    Codex names rollouts `rollout-<ISO-timestamp>-<session-uuid>.jsonl`.
    When pod is the launcher, the session-uuid matches the pod session_uuid
    used for the captured stdout file and the manifest sidecar.
    """
    name = rollout_path.name
    # Expected form: rollout-2026-04-22T02-30-18-<uuid>.jsonl
    if not name.startswith("rollout-") or not name.endswith(".jsonl"):
        return None
    stem = name[len("rollout-"):-len(".jsonl")]
    # The UUID is the last 5 dash-separated groups (8-4-4-4-12).
    parts = stem.rsplit("-", 5)
    if len(parts) < 6:
        return None
    return "-".join(parts[1:])


def _rollout_final_token_count(path: Path) -> tuple[int, int, int]:
    """Return (input_tokens, cached_input_tokens, output_tokens) from the
    last token_count event in a Codex rollout.

    `total_token_usage` is cumulative, so the last value is the full-session
    total. Returns (0, 0, 0) if the file has no token_count events.
    """
    last = (0, 0, 0)
    try:
        with open(path) as fh:
            for line in fh:
                if '"token_count"' not in line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                payload = rec.get("payload", {})
                if payload.get("type") != "token_count":
                    continue
                info = payload.get("info") or {}
                total = info.get("total_token_usage") or {}
                last = (
                    total.get("input_tokens", 0),
                    total.get("cached_input_tokens", 0),
                    total.get("output_tokens", 0),
                )
    except OSError:
        return (0, 0, 0)
    return last


def _codex_historical_cost(config: dict) -> float:
    """Scan project-local Codex rollouts and stdout captures.

    Rollouts are authoritative; for any session_uuid with both a rollout
    and a .stdout, the .stdout is skipped to avoid double counting. This
    covers pre-relocation sessions (where only .stdout exists) and
    post-relocation sessions (where the rollout exists).
    """
    # 1. Rollouts (post-relocation, plus any legacy files that got moved in).
    rollout_root = _codex_rollouts_dir()
    seen_uuids: set[str] = set()
    total = 0.0
    if rollout_root.is_dir():
        for rollout in rollout_root.rglob("rollout-*.jsonl"):
            sid = _session_uuid_from_rollout(rollout)
            if sid:
                seen_uuids.add(sid)
            tokens_in, cached, tokens_out = _rollout_final_token_count(rollout)
            if tokens_in == 0 and tokens_out == 0:
                continue
            manifest = _read_codex_manifest(sid) if sid else None
            model = (manifest or {}).get("model", "")
            pricing = _pricing_for(config, "codex", model)
            # Codex's `input_tokens` is total input; `cached_input_tokens`
            # is a subset that is also counted under input. Price the
            # cached portion at the cache_read rate and the rest at the
            # full input rate.
            non_cached_in = max(0, tokens_in - cached)
            total += _price_tokens(pricing, non_cached_in, tokens_out, cached, 0)

    # 2. Pre-relocation `.stdout` captures: sum per-turn deltas, skip any
    # uuid already covered by a rollout.
    stdout_dir = PROJECT_DIR / cfg_get(config, "project", "session_dir", default="sessions")
    if stdout_dir.is_dir():
        for stdout_path in stdout_dir.glob("*.stdout"):
            sid = stdout_path.stem
            if sid in seen_uuids:
                continue
            s_in = s_cached = s_out = 0
            try:
                with open(stdout_path) as fh:
                    for line in fh:
                        if '"turn.completed"' not in line:
                            continue
                        try:
                            rec = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if rec.get("type") != "turn.completed":
                            continue
                        usage = rec.get("usage", {})
                        s_in += usage.get("input_tokens", 0)
                        s_cached += usage.get("cached_input_tokens", 0)
                        s_out += usage.get("output_tokens", 0)
            except OSError:
                continue
            if s_in == 0 and s_out == 0:
                continue
            manifest = _read_codex_manifest(sid)
            model = (manifest or {}).get("model", "")
            pricing = _pricing_for(config, "codex", model)
            non_cached_in = max(0, s_in - s_cached)
            total += _price_tokens(pricing, non_cached_in, s_out, s_cached, 0)

    return total


def compute_historical_cost(config: dict,
                            claude_config_dir: Path | None = None) -> float:
    """Scan project rollouts (Claude + Codex) and return total estimated cost.

    Uses the same counting methodology as the JSONL monitor (sum all
    assistant records for Claude; sum final token_count for Codex rollouts
    or per-turn `turn.completed` for legacy stdout captures).
    """
    return (
        _claude_historical_cost(config, claude_config_dir)
        + _codex_historical_cost(config)
    )


def _reload_config_value(*keys, default=None):
    """Re-read a single value from config.toml on disk (hot-reload safe)."""
    try:
        with open(CONFIG_PATH, "rb") as f:
            cfg = tomllib.load(f)
        # Support legacy [claude] config
        if "claude" in cfg and "agent" not in cfg:
            cfg.setdefault("agent", {})["claude"] = cfg.pop("claude")
            cfg["agent"].setdefault("backend", "claude")
        for k in keys:
            cfg = cfg[k]
        return cfg
    except (OSError, KeyError, tomllib.TOMLDecodeError):
        return default


_MODEL_TIER: dict[str, dict[str, int]] = {
    "claude": {"opus": 2, "sonnet": 1, "haiku": 0},
    "codex": {"gpt-5.4": 2, "gpt-5.3-codex": 2, "gpt-5.4-mini": 1, "gpt-5": 2},
}


def _model_tier(backend: str, model: str) -> int:
    """Return the capability tier (higher = more capable) for a model."""
    return _MODEL_TIER.get(backend, {}).get(model, 0)


def _claude_keychain_service(claude_config_dir: Path | None) -> str:
    """Compute the macOS keychain service name Claude Code uses for credentials.

    Mirrors @anthropic-ai/claude-code's lookup: the service is
    "Claude Code-credentials" for the default ~/.claude config dir, or
    "Claude Code-credentials-<sha256(NFC(config_dir))[:8]>" when
    CLAUDE_CONFIG_DIR is set.
    """
    import hashlib
    import unicodedata
    if claude_config_dir is None:
        return "Claude Code-credentials"
    normalized = unicodedata.normalize("NFC", str(claude_config_dir))
    suffix = hashlib.sha256(normalized.encode()).hexdigest()[:8]
    return f"Claude Code-credentials-{suffix}"


def resolve_claude_credential(claude_config_dir: Path | None) -> dict:
    """Resolve the credential Claude Code will actually use at launch time.

    Mirrors Claude Code's lookup order: the macOS keychain entry keyed by
    sha256(CLAUDE_CONFIG_DIR) first, then the plaintext
    ``$CLAUDE_CONFIG_DIR/.credentials.json`` fallback.

    Returns {accountLabel, tokenPrefix, source}.
    """
    import json as _json
    service = _claude_keychain_service(claude_config_dir)

    try:
        r = subprocess.run(
            ["security", "find-generic-password",
             "-s", service, "-a", os.getenv("USER", ""), "-w"],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode == 0 and r.stdout.strip():
            kc = _json.loads(r.stdout.strip())
            return {
                "accountLabel": kc.get("accountLabel", "?"),
                "tokenPrefix": kc.get("claudeAiOauth", {}).get("accessToken", "")[:20],
                "source": f"keychain[{service}]",
            }
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError, ValueError):
        pass

    if claude_config_dir is not None:
        creds_path = claude_config_dir / ".credentials.json"
    else:
        creds_path = Path.home() / ".claude" / ".credentials.json"
    try:
        with open(creds_path) as f:
            creds = _json.load(f)
        symlink = ""
        if creds_path.is_symlink():
            symlink = f" -> {os.readlink(creds_path)}"
        return {
            "accountLabel": creds.get("accountLabel", "?"),
            "tokenPrefix": creds.get("claudeAiOauth", {}).get("accessToken", "")[:20],
            "source": f"file[{creds_path}]{symlink}",
        }
    except (OSError, ValueError):
        pass

    return {"accountLabel": "?", "tokenPrefix": "", "source": "none"}


def check_quota(config: dict, force: bool = False,
                claude_config_dir: Path | None = None) -> bool:
    """Check if agent quota is available. Returns True if OK.

    For the Claude backend, resolves the credential Claude Code will actually
    use for ``claude_config_dir`` and asks the quota helper about *that*
    account specifically — otherwise the helper inspects whichever account
    owns the default keychain entry, which may differ from the one launched
    sessions actually hit.
    """
    if force or FORCE_QUOTA_FILE.exists():
        return True
    cmd = os.path.expanduser(_backend_cfg(config, "quota_check", default=""))
    if not cmd:
        return True
    quota_required = _backend_cfg(config, "quota_check_required", default=True)
    args = [cmd]
    if _backend(config) == "claude":
        resolved = resolve_claude_credential(claude_config_dir)
        label = resolved.get("accountLabel", "")
        if label and label != "?":
            args += ["--account", label]
    try:
        result = subprocess.run(
            args, capture_output=True, text=True, timeout=30
        )
        if result.returncode == 2 and not quota_required:
            # Exit 2 = cache missing/stale; proceed if quota check is optional
            log("quota_check returned 2 (unknown); proceeding because quota_check_required=false")
            return True
        if result.returncode != 0:
            return False
        available = result.stdout.strip()
        # Re-read model from disk so config.toml edits take effect without restart.
        backend = _backend(config)
        required = _reload_config_value("agent", backend, "model", default="opus")
        # Model tier: higher-tier availability satisfies lower-tier requirements.
        return _model_tier(backend, available) >= _model_tier(backend, required)
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


def coordination(config: dict, *args, env_extra: dict | None = None,
                 stdin_data: str | None = None) -> subprocess.CompletedProcess:
    """Run a coordination subcommand."""
    script = str(_data_dir() / "coordination")
    env = dict(os.environ)
    # Pass protected-files list so coordination can enforce it.
    pf = cfg_get(config, "project", "protected_files", default=["PLAN.md"])
    if isinstance(pf, list):
        pf = list(pf)
    else:
        pf = pf.split(":")
    # Auto-protect files installed by pod
    for rel in _pod_installed_files():
        if rel not in pf:
            pf.append(rel)
    env["POD_PROTECTED_FILES"] = ":".join(pf)
    # Pass repair-agent thresholds so the coordination script doesn't have to
    # re-parse config.toml. Treated as advisory defaults — script may override
    # for local testing by exporting these before calling.
    stuck_ci_minutes = cfg_get(config, "repair", "stuck_ci_minutes", default=120)
    env.setdefault("POD_STUCK_CI_MINUTES", str(stuck_ci_minutes))
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [script, *args],
        capture_output=True, text=True, timeout=60,
        cwd=str(PROJECT_DIR), env=env,
        input=stdin_data,
    )


def _gh_rate_limit_wait() -> int:
    """Seconds until GH GraphQL rate limit resets, or 0 if plenty remaining.

    Uses the REST API (separate bucket) to check the GraphQL limit.
    Returns 0 on error or if remaining >= 100.
    """
    try:
        r = subprocess.run(
            ["gh", "api", "rate_limit", "--jq",
             '.resources.graphql | if .remaining < 100 then (.reset - now | ceil) else 0 end'],
            capture_output=True, text=True, timeout=10, cwd=str(PROJECT_DIR))
        if r.returncode == 0:
            return max(0, int(float(r.stdout.strip())))
    except Exception:
        pass
    return 0


def _clear_gh_cache():
    """Clear the gh CLI HTTP cache to avoid stale rate-limit headers.

    After rate limit exhaustion, gh caches introspection query responses
    (with X-Gh-Cache-Ttl: 24h) that carry X-Ratelimit-Remaining: 0.
    Even after the rate limit resets, gh reads the cached headers and
    refuses to make requests. Clearing the cache forces fresh requests.
    """
    import pathlib
    cache_dir = pathlib.Path.home() / ".cache" / "gh"
    if not cache_dir.is_dir():
        return
    for f in cache_dir.rglob("*"):
        if f.is_file() and not f.name.endswith(".zip"):
            try:
                f.unlink()
            except OSError:
                pass


_queue_depth_cache: tuple[float, int] = (0.0, 0)  # (timestamp, depth)
_QUEUE_DEPTH_TTL = 30  # seconds — avoid redundant calls within same dispatch cycle


def get_queue_depth(config: dict) -> int:
    """Get number of unclaimed issues (cached for 30s to reduce API calls)."""
    global _queue_depth_cache
    now = time.time()
    if now - _queue_depth_cache[0] < _QUEUE_DEPTH_TTL:
        return _queue_depth_cache[1]
    try:
        r = coordination(config, "queue-depth")
        depth = int(r.stdout.strip())
        _queue_depth_cache = (now, depth)
        return depth
    except (ValueError, subprocess.TimeoutExpired):
        # On failure, return last known good value (don't cache failure as 0)
        if _queue_depth_cache[0] > 0:
            return _queue_depth_cache[1]
        return 0


def get_return_to_human(config: dict) -> bool:
    """Check whether a planner has signalled return-to-human on the sentinel issue."""
    try:
        r = coordination(config, "check-return-to-human")
        return r.stdout.strip() == "true"
    except subprocess.TimeoutExpired:
        return False


def clear_return_to_human(config: dict) -> None:
    """Remove the return-to-human signal from the sentinel issue."""
    try:
        coordination(config, "clear-return-to-human")
    except subprocess.TimeoutExpired:
        pass


@dataclasses.dataclass
class GHItem:
    """An issue or PR for TUI display."""
    kind: str           # "issue" or "pr"
    number: int
    title: str
    labels: list[str]
    ci_status: str      # "" (unknown/none), "pass", "fail"
    state: str          # "open", "closed", "merged"
    timestamp: str      # ISO 8601 timestamp for the current state


def fetch_issues_and_prs() -> list[GHItem]:
    """Fetch issues (agent-plan or human-oversight label, all states) and recent PRs from GitHub."""
    items: list[GHItem] = []
    seen_open: set[int] = set()
    cwd = str(PROJECT_DIR)

    issue_json = "--json=number,title,labels,state,createdAt,updatedAt,closedAt"
    # All open issues (there should never be thousands of open ones)
    for label in ("agent-plan", "human-oversight"):
        try:
            r = subprocess.run(
                ["gh", "issue", "list", "--label", label, "--state", "open",
                 "--limit", "500", issue_json],
                capture_output=True, text=True, timeout=30, cwd=cwd,
            )
            if r.returncode == 0:
                for iss in json.loads(r.stdout):
                    if iss["number"] in seen_open:
                        continue
                    seen_open.add(iss["number"])
                    labels = [l["name"] for l in iss.get("labels", [])]
                    ts = iss.get("updatedAt") or iss.get("createdAt", "")
                    items.append(GHItem(
                        kind="issue", number=iss["number"], title=iss["title"],
                        labels=labels, ci_status="", state="open", timestamp=ts,
                    ))
        except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
            pass
    # Recent closed issues (just enough for context; display logic drops these first anyway)
    seen_closed: set[int] = set()
    for label in ("agent-plan", "human-oversight"):
        try:
            r = subprocess.run(
                ["gh", "issue", "list", "--label", label, "--state", "closed",
                 "--limit", "30", issue_json],
                capture_output=True, text=True, timeout=30, cwd=cwd,
            )
            if r.returncode == 0:
                for iss in json.loads(r.stdout):
                    if iss["number"] in seen_closed or iss["number"] in seen_open:
                        continue
                    seen_closed.add(iss["number"])
                    labels = [l["name"] for l in iss.get("labels", [])]
                    ts = iss.get("closedAt") or iss.get("updatedAt") or iss.get("createdAt", "")
                    items.append(GHItem(
                        kind="issue", number=iss["number"], title=iss["title"],
                        labels=labels, ci_status="", state="closed", timestamp=ts,
                    ))
        except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
            pass

    # PRs (open + recently closed/merged)
    try:
        r = subprocess.run(
            ["gh", "pr", "list", "--state", "all", "--limit", "15",
             "--json", "number,title,labels,statusCheckRollup,state,createdAt,updatedAt,closedAt,mergedAt"],
            capture_output=True, text=True, timeout=30, cwd=cwd,
        )
        if r.returncode == 0:
            for pr in json.loads(r.stdout):
                labels = [l["name"] for l in pr.get("labels", [])]
                # CI status from statusCheckRollup
                ci = ""
                checks = pr.get("statusCheckRollup", []) or []
                if checks:
                    if any(c.get("conclusion") == "FAILURE" for c in checks):
                        ci = "fail"
                    elif (any(c.get("conclusion") == "SUCCESS" for c in checks) and
                          all(c.get("conclusion") == "SUCCESS" for c in checks if c.get("conclusion"))):
                        ci = "pass"
                pr_state = pr.get("state", "OPEN").lower()
                ts = pr.get("mergedAt") or pr.get("closedAt") or "" if pr_state in ("merged", "closed") else pr.get("updatedAt") or pr.get("createdAt", "")
                items.append(GHItem(
                    kind="pr", number=pr["number"], title=pr["title"],
                    labels=labels, ci_status=ci, state=pr_state, timestamp=ts,
                ))
    except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
        pass

    # Sort by number descending (newest first), issues before PRs at same number
    items.sort(key=lambda x: (-x.number, x.kind))

    # Deduplicate: if an issue and PR share the same number, keep both
    # (they're different GitHub objects)
    return items


def fetch_blocked_deps() -> dict[int, list[int]]:
    """Fetch open depends-on dependencies for blocked issues (closed deps filtered out)."""
    import re as _re
    cwd = str(PROJECT_DIR)
    try:
        r = subprocess.run(
            ["gh", "issue", "list", "--label", "agent-plan", "--label", "blocked",
             "--state", "open", "--limit", "20", "--json", "number,body"],
            capture_output=True, text=True, timeout=30, cwd=cwd,
        )
        if r.returncode != 0:
            return {}

        raw: dict[int, list[int]] = {}
        for iss in json.loads(r.stdout):
            deps = [int(d) for d in _re.findall(r"depends-on: #(\d+)", iss.get("body", ""))]
            if deps:
                raw[iss["number"]] = deps
        if not raw:
            return {}

        # Fetch open issue numbers to filter out closed deps
        r2 = subprocess.run(
            ["gh", "issue", "list", "--state", "open", "--limit", "100",
             "--json", "number", "--jq", "[.[].number]"],
            capture_output=True, text=True, timeout=30, cwd=cwd,
        )
        open_nums: set[int] = set(json.loads(r2.stdout)) if r2.returncode == 0 else set()

        return {num: filtered for num, deps in raw.items()
                if (filtered := [d for d in deps if d in open_nums])}
    except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
        return {}


# ---------------------------------------------------------------------------
# JSONL Monitor (runs as a thread inside agent process)
# ---------------------------------------------------------------------------

def jsonl_monitor(jsonl_path: str, state: AgentState, stop: threading.Event,
                  backend: str = "claude"):
    """Poll JSONL file and update agent state. Runs in a daemon thread."""
    pos = 0
    while True:
        try:
            if not os.path.exists(jsonl_path):
                if stop.is_set():
                    break
                stop.wait(1)
                continue
            with open(jsonl_path, "rb") as f:
                f.seek(pos)
                while True:
                    line = f.readline()
                    if not line:
                        break
                    if not line.endswith(b"\n"):
                        break  # Partial line — retry next poll
                    pos += len(line)
                    _parse_jsonl_line(line, state, backend)
        except OSError:
            pass
        if stop.is_set():
            break  # Final read done — exit
        stop.wait(1)


def _parse_jsonl_line(line: bytes, state: AgentState, backend: str = "claude"):
    """Dispatch JSONL parsing to the appropriate backend parser."""
    if backend == "codex":
        _parse_codex_jsonl_line(line, state)
    else:
        _parse_claude_jsonl_line(line, state)


def _parse_codex_jsonl_line(line: bytes, state: AgentState):
    """Parse one JSONL line from Codex --json stdout and update state.

    Codex stdout event types (verified from codex exec --json v0.104.0):
      thread.started   → capture backend_session_id for resume
      turn.completed   → token usage
      item.completed   → agent_message (text), command_execution (tool output)
    """
    try:
        d = json.loads(line)
    except json.JSONDecodeError:
        return

    t = d.get("type")

    if t == "thread.started":
        # Capture Codex thread id for resume; do NOT overwrite state.uuid
        if not state.backend_session_id:
            state.backend_session_id = d.get("thread_id", "")
        state.last_activity = time.time()

    elif t == "turn.completed":
        usage = d.get("usage", {})
        # Codex reports `input_tokens` inclusive of the cached portion
        # (verified: input_tokens + output_tokens == total_tokens in
        # Codex rollouts). Normalize to the same convention as Claude:
        # `tokens_in` = non-cached input, `cache_read` = cached input.
        in_total = usage.get("input_tokens", 0)
        cached = usage.get("cached_input_tokens", 0)
        state.tokens_in += max(0, in_total - cached)
        state.cache_read += cached
        state.tokens_out += usage.get("output_tokens", 0)
        # Codex has no cache_creation_input_tokens equivalent
        state.last_activity = time.time()

    elif t == "item.completed":
        item = d.get("item", {})
        itype = item.get("type")
        if itype == "agent_message":
            text = item.get("text", "").strip()
            if text:
                state.last_text = text[:200]
                state.last_activity = time.time()
        elif itype == "command_execution":
            cmd = item.get("command", "")[:120]
            state.last_text = f"[exec] {cmd}"
            state.last_activity = time.time()
            # Detect issue claim from coordination script output
            output = item.get("aggregated_output", "")
            m = re.search(r"Claimed issue #(\d+)", output)
            if m:
                state.claimed_issue = int(m.group(1))
            # Detect PR repair claim from output (only on success — the command
            # emits "Claimed PR #N for repair" only after race-detect passes)
            m_pr_claim = re.search(r"Claimed PR #(\d+) for repair", output)
            if m_pr_claim:
                state.repair_pr = int(m_pr_claim.group(1))
            # Detect PR creation from coordination create-pr
            m2 = re.search(r"(?:coordination\s+create-pr\s+(?:--\S+\s+)*)(\d+)", cmd)
            if m2:
                state.pr_number = int(m2.group(1))
            # Detect repair-claim release (mark-pr-salvaged / close-pr-unsalvageable)
            m_rel = re.search(
                r"coordination\s+(?:mark-pr-salvaged|close-pr-unsalvageable)\s+(\d+)",
                cmd,
            )
            if m_rel:
                state.repair_pr = 0


def _parse_claude_jsonl_line(line: bytes, state: AgentState):
    """Parse one JSONL line and update state."""
    try:
        d = json.loads(line)
    except json.JSONDecodeError:
        return

    msg_type = d.get("type")

    # Process tool results (type: "user") to detect successful claims
    if msg_type == "user":
        content = d.get("message", {}).get("content", [])
        if isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("type") == "tool_result":
                    result_text = block.get("content", "")
                    if isinstance(result_text, str):
                        m = re.search(r"Claimed issue #(\d+)", result_text)
                        if m:
                            state.claimed_issue = int(m.group(1))
                        m_pr = re.search(r"Claimed PR #(\d+) for repair", result_text)
                        if m_pr:
                            state.repair_pr = int(m_pr.group(1))
        return

    if msg_type != "assistant":
        return

    usage = d.get("message", {}).get("usage", {})
    state.tokens_in += usage.get("input_tokens", 0)
    state.cache_create += usage.get("cache_creation_input_tokens", 0)
    state.cache_read += usage.get("cache_read_input_tokens", 0)
    state.tokens_out += usage.get("output_tokens", 0)
    state.last_activity = time.time()

    for block in d.get("message", {}).get("content", []):
        btype = block.get("type")
        if btype == "text" and block.get("text", "").strip():
            state.last_text = block["text"].strip()
        elif btype == "tool_use":
            name = block.get("name", "")
            inp = block.get("input", {})
            detail = _tool_detail(name, inp, state)
            state.last_text = f"[{name}] {detail}" if detail else f"[{name}]"


def _tool_detail(name: str, inp: dict, state: AgentState) -> str:
    """Extract a display-friendly detail string from a tool invocation."""
    if name == "Bash":
        desc = inp.get("description", "")
        cmd = inp.get("command", "")
        # Detect coordination create-pr (issue-claim is detected from tool results instead)
        m = re.search(r"(?:^|&&\s*|;\s*)(?:\./)?coordination\s+create-pr\s+(?:--\S+\s+)*(\d+)", cmd)
        if m:
            state.pr_number = int(m.group(1))
        # Repair-claim release: mark-pr-salvaged / close-pr-unsalvageable
        # clear the repair-claimed label, so we should clear our own tracking
        # as soon as we see the command fire.
        m_rel = re.search(
            r"(?:^|&&\s*|;\s*)(?:\./)?coordination\s+(?:mark-pr-salvaged|close-pr-unsalvageable)\s+(\d+)",
            cmd,
        )
        if m_rel:
            state.repair_pr = 0
        if desc and cmd:
            return f"{desc}: {cmd}"
        return desc or cmd
    elif name == "Edit":
        p = inp.get("file_path", "")
        return p.split("/")[-1] if p else ""
    elif name in ("Read", "Write"):
        p = inp.get("file_path", "")
        return p.split("/")[-1] if p else ""
    elif name in ("Grep", "Glob"):
        return inp.get("pattern", "")
    elif name == "TodoWrite":
        todos = inp.get("todos", [])
        active = [t for t in todos if t.get("status") == "in_progress"]
        return active[0].get("activeForm", "") if active else ""
    elif name == "Task":
        return inp.get("description", "")
    return name


# ---------------------------------------------------------------------------
# Dispatch Strategies
# ---------------------------------------------------------------------------

def _count_running_repair_agents(agents: list | None = None) -> int:
    """Count live agents whose worker_type is 'repair'."""
    if agents is None:
        agents = read_all_agents()
    return sum(1 for a in agents
               if a.worker_type == "repair"
               and a.status not in ("dead", "stopped", "killed"))


def _list_pr_repair_count(config: dict) -> int:
    """Return the number of PR-repair candidates, or 0 on error.

    Calls `coordination list-pr-repair` and counts output lines.
    """
    try:
        r = coordination(config, "list-pr-repair")
    except (subprocess.TimeoutExpired, OSError):
        return 0
    if r.returncode != 0:
        return 0
    return sum(1 for line in r.stdout.splitlines() if line.strip())


def _choose_draining(config: dict, draining: dict) -> str | None:
    """Pick a random draining worker type that has available work.

    For types with issue_label, check per-label queue depth via
    `coordination queue-depth <label>`. Shuffled so no type is
    systematically starved.
    For types without issue_label, always consider them available — except
    `repair`, which is excluded here; repair dispatch is handled by the
    short-circuit at the top of dispatch_queue_balance so that a generic
    draining pass does not pick `repair` when no PRs need it.
    Returns None if no draining type has work.
    """
    items = list(draining.items())
    random.shuffle(items)
    for name, wt in items:
        if name == "repair":
            log("_choose_draining: skipping repair (handled by top-level short-circuit)")
            continue
        issue_label = wt.get("issue_label", "")
        if issue_label:
            try:
                r = coordination(config, "queue-depth", issue_label)
                depth = int(r.stdout.strip())
            except (ValueError, subprocess.TimeoutExpired):
                depth = 0
            if depth > 0:
                log(f"_choose_draining: {name} (label={issue_label}) has depth={depth}")
                return name
            else:
                log(f"_choose_draining: {name} (label={issue_label}) has depth=0, skipping")
        else:
            # No label filter — treat as always having work
            log(f"_choose_draining: {name} (no label filter) → selected")
            return name
    log(f"_choose_draining: no type had work out of {[n for n,_ in items]}")
    return None


def _get_critical_path_depth(config: dict, label: str = "") -> int:
    """Check for unclaimed critical-path issues, optionally filtered by label."""
    try:
        args = ["critical-path-depth"]
        if label:
            args.append(label)
        r = coordination(config, *args)
        return int(r.stdout.strip())
    except (ValueError, subprocess.TimeoutExpired):
        return 0


def _choose_critical_path_draining(config: dict, draining: dict) -> str | None:
    """Pick a draining worker type that has unclaimed critical-path work.

    Like _choose_draining but only considers critical-path issues.
    For typed workers (with issue_label), checks per-label critical-path depth.
    For untyped workers, checks global critical-path depth.
    `repair` is excluded — it handles PRs, not critical-path issues.
    """
    items = list(draining.items())
    random.shuffle(items)
    for name, wt in items:
        if name == "repair":
            continue
        issue_label = wt.get("issue_label", "")
        if issue_label:
            if _get_critical_path_depth(config, issue_label) > 0:
                return name
        else:
            # No label filter — handles all issue types
            if _get_critical_path_depth(config) > 0:
                return name
    return None


def dispatch_queue_balance(config: dict, queue_depth: int,
                           worker_types: dict,
                           state: AgentState | None = None) -> str | None:
    """Low queue → locked types (queue-filling), high queue → unlocked types (queue-draining).

    **Repair short-circuit**: before any filling/draining logic, if the
    `repair` worker type is configured and `coordination list-pr-repair`
    reports PR candidates with spare repair concurrency, dispatch `repair`.
    This keeps repair orthogonal to `planner_target` and to queue depth.
    """
    # Repair short-circuit. Runs first so that unhealthy PRs are always
    # addressed before new feature work is planned or drained.
    if "repair" in worker_types:
        candidates = _list_pr_repair_count(config)
        running_repair = _count_running_repair_agents()
        cap = cfg_get(config, "repair", "concurrency_cap", default=2)
        if candidates > running_repair and running_repair < cap:
            log(f"dispatch: repair short-circuit (candidates={candidates}, "
                f"running_repair={running_repair}, cap={cap})")
            return "repair"
        elif candidates > 0:
            log(f"dispatch: repair candidates={candidates} but running_repair={running_repair} "
                f"already at/near cap={cap}, falling through")

    config_min_queue = cfg_get(config, "dispatch", "min_queue", default=3)
    planner_min_queue = read_planner_min_queue()
    if planner_min_queue is not None:
        min_queue = max(1, min(config_min_queue, planner_min_queue))
    else:
        min_queue = max(1, config_min_queue)

    # Separate types into queue-filling (have locks) and queue-draining (no locks).
    # `repair` is draining in principle but is handled exclusively by the
    # short-circuit above, so _choose_draining skips it.
    filling = {k: v for k, v in worker_types.items() if v.get("lock")}
    draining = {k: v for k, v in worker_types.items() if not v.get("lock")}

    log(f"dispatch: queue_depth={queue_depth} min_queue={min_queue} "
        f"(config={config_min_queue} planner={planner_min_queue}) "
        f"filling={list(filling)} draining={list(draining)}")

    # Critical-path override: if there are unclaimed critical-path issues,
    # always dispatch a matching worker regardless of min_queue threshold.
    # This prevents the startup stall where the pipeline is bottlenecked on
    # a single issue but the queue is too small to trigger worker dispatch.
    if queue_depth > 0 and draining:
        chosen = _choose_critical_path_draining(config, draining)
        if chosen is not None:
            log(f"dispatch: critical-path override → {chosen}")
            return chosen

    if queue_depth < min_queue and filling:
        log(f"dispatch: queue low ({queue_depth} < {min_queue}), trying filling types")
        # Try to acquire lock for a filling type
        for name, wt in filling.items():
            lock_name = wt["lock"]
            r = coordination(config, f"lock-{lock_name}")
            if r.returncode == 0:
                if state:
                    state.lock_held = lock_name
                    state.write()
                log(f"dispatch: acquired {lock_name} lock → {name}")
                return name
        # Lock held — fall back to draining if queue > 0
        if queue_depth > 0 and draining:
            chosen = _choose_draining(config, draining)
            log(f"dispatch: lock held, draining fallback → {chosen}")
            return chosen
        # Queue empty and lock held — wait
        log("dispatch: lock held, queue empty → None")
        return None
    elif draining:
        chosen = _choose_draining(config, draining)
        if chosen is not None:
            log(f"dispatch: draining → {chosen}")
            return chosen
        # No labeled work available despite nonzero global queue (e.g. unlabeled issues
        # from before the typed-worker migration). Fall back to planner to create
        # properly-typed issues rather than stalling indefinitely.
        log("dispatch: _choose_draining returned None despite draining types existing, "
            "falling back to filling")
        if filling:
            for name, wt in filling.items():
                lock_name = wt["lock"]
                r = coordination(config, f"lock-{lock_name}")
                if r.returncode == 0:
                    if state:
                        state.lock_held = lock_name
                        state.write()
                    log(f"dispatch: draining-fallback acquired {lock_name} lock → {name}")
                    return name
        log("dispatch: no work available → None")
        return None
    elif filling:
        log("dispatch: no draining types, trying filling only")
        # Only filling types exist — try them
        for name, wt in filling.items():
            lock_name = wt["lock"]
            r = coordination(config, f"lock-{lock_name}")
            if r.returncode == 0:
                if state:
                    state.lock_held = lock_name
                    state.write()
                log(f"dispatch: filling-only acquired {lock_name} lock → {name}")
                return name
        log("dispatch: filling-only, all locks held → None")
        return None
    log("dispatch: no worker types matched → None")
    return None


_round_robin_idx = 0


def dispatch_round_robin(config: dict, queue_depth: int,
                          worker_types: dict,
                          state: AgentState | None = None) -> str | None:
    """Cycle through worker types, skipping locked ones."""
    global _round_robin_idx
    names = list(worker_types.keys())
    if not names:
        return None
    for _ in range(len(names)):
        name = names[_round_robin_idx % len(names)]
        _round_robin_idx += 1
        wt = worker_types[name]
        lock_name = wt.get("lock")
        if lock_name:
            r = coordination(config, f"lock-{lock_name}")
            if r.returncode != 0:
                continue
            if state:
                state.lock_held = lock_name
                state.write()
        return name
    return None


def dispatch_custom(config: dict, queue_depth: int,
                     worker_types: dict,
                     state: AgentState | None = None) -> str | None:
    """Run a custom dispatch script."""
    strategy = cfg_get(config, "dispatch", "strategy", default="")
    script = os.path.expanduser(strategy)
    env = dict(os.environ)
    env["POD_QUEUE_DEPTH"] = str(queue_depth)
    env["POD_AGENT_COUNT"] = str(len(read_all_agents()))
    env["POD_WORKER_TYPES"] = ",".join(worker_types.keys())
    try:
        r = subprocess.run(
            [script], capture_output=True, text=True, timeout=30, env=env,
            cwd=str(PROJECT_DIR),
        )
        if r.returncode != 0:
            return None
        name = r.stdout.strip()
        if name in worker_types:
            # If the chosen type has a lock, try to acquire it
            lock_name = worker_types[name].get("lock")
            if lock_name:
                lr = coordination(config, f"lock-{lock_name}")
                if lr.returncode != 0:
                    return None
                if state:
                    state.lock_held = lock_name
                    state.write()
            return name
        return None
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None


def dispatch(config: dict, state: AgentState | None = None) -> str | None:
    """Choose a worker type to run. Returns type name or None (wait).
    If state is provided, sets state.lock_held immediately upon lock acquisition."""
    strategy = cfg_get(config, "dispatch", "strategy", default="queue_balance")
    worker_types = cfg_get(config, "worker_types", default={})
    if not worker_types:
        say("No worker types configured")
        return None

    queue_depth = get_queue_depth(config)

    if strategy == "queue_balance":
        return dispatch_queue_balance(config, queue_depth, worker_types, state)
    elif strategy == "round_robin":
        return dispatch_round_robin(config, queue_depth, worker_types, state)
    elif os.path.exists(os.path.expanduser(strategy)):
        return dispatch_custom(config, queue_depth, worker_types, state)
    else:
        say(f"Unknown dispatch strategy: {strategy}, falling back to queue_balance")
        return dispatch_queue_balance(config, queue_depth, worker_types, state)


# ---------------------------------------------------------------------------
# Dead claim recovery
# ---------------------------------------------------------------------------

_cached_base_branch: str = ""
_cached_repo_name: str = ""


def _get_base_branch() -> str:
    """Auto-detect the default branch (e.g. 'main' or 'master'). Cached after first call."""
    global _cached_base_branch
    if _cached_base_branch:
        return _cached_base_branch
    try:
        r = subprocess.run(
            ["gh", "repo", "view", "--json", "defaultBranchRef", "-q",
             ".defaultBranchRef.name"],
            capture_output=True, text=True, timeout=15, cwd=str(PROJECT_DIR),
        )
        if r.returncode == 0 and r.stdout.strip():
            _cached_base_branch = r.stdout.strip()
            return _cached_base_branch
    except Exception:
        pass
    return "master"


def _get_repo() -> str:
    """Auto-detect GitHub repo (owner/name) from the current git remote. Cached after first call."""
    global _cached_repo_name
    if _cached_repo_name:
        return _cached_repo_name
    try:
        r = subprocess.run(
            ["gh", "repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner"],
            capture_output=True, text=True, timeout=15, cwd=str(PROJECT_DIR),
        )
        if r.returncode == 0 and r.stdout.strip():
            _cached_repo_name = r.stdout.strip()
            return _cached_repo_name
    except Exception:
        pass
    # Fallback: parse git remote
    try:
        r = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True, text=True, timeout=10, cwd=str(PROJECT_DIR),
        )
        url = r.stdout.strip()
        # Handle SSH (git@github.com:owner/repo.git) and HTTPS
        import re as _re
        m = _re.search(r"github\.com[:/](.+?)(?:\.git)?$", url)
        if m:
            return m.group(1)
    except Exception:
        pass
    return "unknown/unknown"


def _release_claim(issue_str: str, session_uuid: str, restart_count: int) -> bool:
    """Remove the 'claimed' label from an issue and leave an explanatory comment.
    Returns True on success, False on failure (caller may revert history deletion).
    Includes a GitHub-side CAS: only releases if the latest claim comment still
    belongs to session_uuid (prevents removing a fresh claim by a different agent)."""
    import re as _re
    repo = _get_repo()
    try:
        # GitHub-side CAS: verify latest claim comment is ours
        r_cas = subprocess.run(
            ["gh", "issue", "view", issue_str, "--repo", repo,
             "--json", "comments",
             "--jq", '[.comments[] | select(.body | startswith("Claimed by session"))] | sort_by(.createdAt) | last | .body'],
            capture_output=True, text=True, timeout=60, cwd=str(PROJECT_DIR),
        )
        if r_cas.returncode == 0 and r_cas.stdout.strip():
            m = _re.search(r'Claimed by session `([^`]+)`', r_cas.stdout.strip().strip('"'))
            if m and m.group(1) != session_uuid:
                log(f"Not releasing #{issue_str} — latest claim belongs to {m.group(1)[:8]}, not {session_uuid[:8]}")
                return False

        r1 = subprocess.run(
            ["gh", "issue", "edit", issue_str, "--repo", repo, "--remove-label", "claimed"],
            capture_output=True, timeout=30, cwd=str(PROJECT_DIR),
        )
        if r1.returncode != 0:
            log(f"Failed to remove claimed label on #{issue_str}: {r1.stderr.decode().strip()}")
            return False
        msg = (f"Claim released — worker session `{session_uuid}` died after "
               f"{restart_count} restart attempt(s). Available for reclaim.")
        r2 = subprocess.run(
            ["gh", "issue", "comment", issue_str, "--repo", repo, "--body", msg],
            capture_output=True, timeout=30, cwd=str(PROJECT_DIR),
        )
        if r2.returncode != 0:
            log(f"Failed to comment on #{issue_str}: {r2.stderr.decode().strip()}")
            return False
        log(f"Released claim on #{issue_str}")
        return True
    except Exception as e:
        log(f"Failed to release claim on #{issue_str}: {e}")
        return False


def sync_claims_from_github():
    """On pod startup, rebuild claim-history.json from GitHub for any claimed
    issues we have no local record of. This lets pod reattach to sessions
    that were running before a pod restart."""
    import re as _re
    repo = _get_repo()
    cwd = str(PROJECT_DIR)

    try:
        r = subprocess.run(
            ["gh", "issue", "list", "--label", "agent-plan", "--label", "claimed",
             "--state", "open", "--limit", "100", "--json", "number"],
            capture_output=True, text=True, timeout=30, cwd=cwd,
        )
        if r.returncode != 0:
            return
        issues = [iss["number"] for iss in json.loads(r.stdout)]
    except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
        return

    if not issues:
        return

    with _claim_history_filelock():
        history = load_claim_history()
        changed = False
        for issue_num in issues:
            key = str(issue_num)
            if key in history:
                continue  # Already tracked locally
            # Fetch comments to find the most recent claim comment
            try:
                r = subprocess.run(
                    ["gh", "issue", "view", str(issue_num), "--repo", repo,
                     "--json", "comments",
                     "--jq", '[.comments[] | select(.body | startswith("Claimed by session"))] | sort_by(.createdAt) | last | .body'],
                    capture_output=True, text=True, timeout=60, cwd=cwd,
                )
                if r.returncode != 0:
                    continue
                comment_body = r.stdout.strip().strip('"')
            except (subprocess.TimeoutExpired, OSError):
                continue
            # Parse: "Claimed by session `UUID` on branch `agent/SHORT_ID`"
            m = _re.search(r'Claimed by session `([^`]+)` on branch `agent/([^`]+)`', comment_body)
            if not m:
                continue
            session_uuid, short_id = m.group(1), m.group(2)
            history[key] = {"session_uuid": session_uuid, "short_id": short_id, "restart_count": 0}
            changed = True
            log(f"Recovered claim: issue #{issue_num} → session {session_uuid[:8]} (short: {short_id})")
        if changed:
            _save_claim_history(history)


def check_dead_claimed_issues(config: dict):
    """Detect locally-known sessions that claimed issues but are now dead.
    Restart them up to max_claim_restarts times, then release the claim.

    Collects actions under the file lock, then executes them outside to avoid
    fork-under-lock (spawn_agent forks) and subprocess-under-lock (_release_claim).
    """
    history = load_claim_history()
    if not history:
        return

    agents = read_all_agents()
    live_uuids = {a.uuid for a in agents if a.status not in ("dead", "stopped")}
    max_restarts = cfg_get(config, "monitor", "max_claim_restarts", default=1)

    to_restart: list[tuple[str, str, str, int]] = []   # (short_id, session_uuid, issue_str, new_count)
    to_release: list[tuple[str, str, int]] = []         # (issue_str, session_uuid, restart_count)

    with _claim_history_filelock():
        history = load_claim_history()  # Re-read under lock
        changed = False
        for issue_str, info in list(history.items()):
            session_uuid = info.get("session_uuid", "")
            if not session_uuid or session_uuid in live_uuids:
                continue  # Still running or malformed entry
            if info.get("released"):
                continue  # Already released, don't restart

            restart_count = info.get("restart_count", 0)
            short_id = info.get("short_id", "")

            if restart_count < max_restarts:
                to_restart.append((short_id, session_uuid, issue_str, restart_count + 1))
                info["restart_count"] = restart_count + 1
                changed = True
            else:
                to_release.append((issue_str, session_uuid, restart_count))
                # Mark as released rather than deleting, so record_claim
                # won't re-add it with restart_count=0 (which would cause
                # an infinite restart loop).
                info["released"] = True
                changed = True

        if changed:
            _save_claim_history(history)
    # Lock released before any fork or subprocess call

    # Deduplicate: if one session claimed N issues, only restart it once.
    seen_uuids: set[str] = set()
    deduped_restart = []
    for entry in to_restart:
        _, session_uuid, _, _ = entry
        if session_uuid not in seen_uuids:
            seen_uuids.add(session_uuid)
            deduped_restart.append(entry)

    # Respect target_agents: don't spawn more agents than the target.
    target = get_effective_target()
    if target is not None:
        current_count = len([a for a in agents if a.status not in ("dead", "stopped")])
        slots = max(0, target - current_count)
        if len(deduped_restart) > slots:
            log(f"Capping restarts at {slots} (target={target}, running={current_count})")
            deduped_restart = deduped_restart[:slots]

    for short_id, session_uuid, issue_str, new_count in deduped_restart:
        log(f"Dead session {session_uuid} claimed #{issue_str}, "
            f"restarting (attempt {new_count}/{max_restarts})")
        # Use a fresh agent_id — reusing the old short_id causes all restart
        # agents to share one state file, making old processes invisible.
        spawn_agent(config, resume_uuid=session_uuid)

    # Re-read live agents for the release check (state may have changed
    # during restart spawning above).
    fresh_agents = read_all_agents()
    live_claimed: dict[int, str] = {}  # issue → uuid of live agent working on it
    for a in fresh_agents:
        if a.status not in ("dead", "stopped", "killed") and a.claimed_issue > 0:
            live_claimed[a.claimed_issue] = a.uuid

    failed_releases: list[tuple[str, str, int]] = []
    for issue_str, session_uuid, restart_count in to_release:
        issue_int = int(issue_str)
        other = live_claimed.get(issue_int)
        if other and other != session_uuid:
            log(f"Not releasing #{issue_str} — agent {other[:8]} still has it")
            # Rebind history entry to the live owner so it stays tracked
            # (the dead session's tombstone was already written above).
            other_agent = next((a for a in fresh_agents if a.uuid == other), None)
            if other_agent:
                record_claim(issue_int, other_agent.uuid, other_agent.short_id)
            continue
        log(f"Max restarts reached for #{issue_str}, releasing claim")
        if not _release_claim(issue_str, session_uuid, restart_count):
            failed_releases.append((issue_str, session_uuid, restart_count))

    # Re-add any failed releases so we retry next housekeeping cycle
    if failed_releases:
        with _claim_history_filelock():
            history = load_claim_history()
            for issue_str, session_uuid, restart_count in failed_releases:
                existing = history.get(issue_str, {})
                if not existing or existing.get("released"):
                    history[issue_str] = {
                        "session_uuid": session_uuid,
                        "short_id": "",
                        "restart_count": restart_count,
                    }
            _save_claim_history(history)


def reconcile_untracked_github_claims():
    """Periodic safety net: find GitHub issues with 'claimed' label that are
    not tracked in local claim-history.json, and either backfill them (if the
    owning session is still alive) or release them (if dead and past the grace
    period).

    This covers the gap where a claim was recorded on GitHub (label + comment)
    but never made it into claim-history.json — e.g. because the JSONL monitor
    missed the `coordination claim` command, or the agent died before
    record_claim() fired.

    Fail-closed: any GitHub API error skips that issue rather than releasing it.
    """
    import re as _re

    repo = _get_repo()
    cwd = str(PROJECT_DIR)
    grace_seconds = 600  # 10 minutes — don't release very fresh claims

    # 1. Query GitHub for all currently-claimed issues
    try:
        r = subprocess.run(
            ["gh", "issue", "list", "--label", "agent-plan", "--label", "claimed",
             "--state", "open", "--limit", "100", "--json", "number"],
            capture_output=True, text=True, timeout=30, cwd=cwd,
        )
        if r.returncode != 0:
            return
        github_claimed = {iss["number"] for iss in json.loads(r.stdout)}
    except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
        return

    if not github_claimed:
        return

    # 2. Which of these are already tracked locally?
    history = load_claim_history()
    tracked = set()
    # Map issue → timestamp of the claim comment we already released.
    # If the latest claim comment on GitHub has a newer timestamp, it's
    # a different claim and we must not skip it.
    released_at: dict[int, str] = {}  # issue_num → released claim_comment_time
    for k, v in history.items():
        issue = int(k)
        if v.get("released"):
            released_at[issue] = v.get("released_comment_time", "")
        else:
            tracked.add(issue)
    untracked = github_claimed - tracked
    if not untracked:
        return

    # 3. Get live agent UUIDs for backfill check
    agents = read_all_agents()
    live_uuids = {a.uuid for a in agents if a.status not in ("dead", "stopped")}

    now_epoch = time.time()
    released_count = 0
    backfilled_count = 0

    for issue_num in sorted(untracked):
        # Fetch the latest "Claimed by session" comment for this issue
        try:
            r = subprocess.run(
                ["gh", "issue", "view", str(issue_num), "--repo", repo,
                 "--json", "comments",
                 "--jq", '[.comments[] | select(.body | startswith("Claimed by session"))] | sort_by(.createdAt) | last | {body, created_at: .createdAt}'],
                capture_output=True, text=True, timeout=60, cwd=cwd,
            )
            if r.returncode != 0:
                continue
            comment_data = json.loads(r.stdout.strip()) if r.stdout.strip() else None
            if not comment_data:
                continue
        except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
            continue

        comment_body = comment_data.get("body", "")
        comment_time = comment_data.get("created_at", "")

        # Parse: "Claimed by session `UUID` on branch `agent/SHORT_ID`"
        m = _re.search(r'Claimed by session `([^`]+)` on branch `agent/([^`]+)`', comment_body)
        if not m:
            continue
        owner_uuid, short_id = m.group(1), m.group(2)

        # Skip if the latest claim comment is the same one we already released.
        # Compare by timestamp: if a newer claim comment exists, it's a new
        # claim (possibly by the same UUID via resume) and must not be skipped.
        prev_released_time = released_at.get(issue_num, "")
        if prev_released_time and comment_time and comment_time <= prev_released_time:
            continue

        # If the owning session is still alive, backfill into claim history
        if owner_uuid in live_uuids:
            with _claim_history_filelock():
                h = load_claim_history()
                key = str(issue_num)
                if key not in h or h[key].get("released"):
                    h[key] = {"session_uuid": owner_uuid, "short_id": short_id, "restart_count": 0}
                    _save_claim_history(h)
                    backfilled_count += 1
                    log(f"Reconcile: backfilled claim #{issue_num} → session {owner_uuid[:8]}")
            continue

        # Owner is dead — check grace period using claim comment timestamp
        try:
            from datetime import datetime, timezone
            claim_dt = datetime.fromisoformat(comment_time.replace("Z", "+00:00"))
            claim_epoch = claim_dt.timestamp()
        except (ValueError, TypeError):
            continue  # Can't parse timestamp, skip

        age = now_epoch - claim_epoch
        if age < grace_seconds:
            continue  # Too fresh, might be a just-claimed issue

        # Re-fetch latest claim owner before releasing (compare-and-swap)
        try:
            r2 = subprocess.run(
                ["gh", "issue", "view", str(issue_num), "--repo", repo,
                 "--json", "comments",
                 "--jq", '[.comments[] | select(.body | startswith("Claimed by session"))] | sort_by(.createdAt) | last | .body'],
                capture_output=True, text=True, timeout=60, cwd=cwd,
            )
            if r2.returncode != 0:
                continue
            latest_body = r2.stdout.strip().strip('"')
        except (subprocess.TimeoutExpired, OSError):
            continue

        m2 = _re.search(r'Claimed by session `([^`]+)`', latest_body)
        if not m2 or m2.group(1) != owner_uuid:
            continue  # Owner changed since our first read — someone reclaimed it

        # Re-check liveness — agent state may have changed during API calls.
        # Check if ANY live agent has this issue claimed (not just the owner
        # from the GitHub comment — another agent may have picked it up).
        fresh_agents = read_all_agents()
        fresh_live = {a.uuid for a in fresh_agents if a.status not in ("dead", "stopped", "killed")}
        if owner_uuid in fresh_live:
            continue  # Owner came back to life (e.g. resumed session)
        if any(a.claimed_issue == issue_num and a.uuid in fresh_live for a in fresh_agents):
            continue  # Another live agent has this issue — don't release

        # Release the stale claim — first verify the label is still present
        # (prevents N parallel reconcilers from all posting release comments)
        try:
            r_label_check = subprocess.run(
                ["gh", "issue", "view", str(issue_num), "--repo", repo,
                 "--json", "labels", "--jq", '[.labels[].name] | any(. == "claimed")'],
                capture_output=True, text=True, timeout=30, cwd=cwd,
            )
            if r_label_check.returncode != 0 or r_label_check.stdout.strip() != "true":
                continue  # Label already removed by another reconciler

            r3 = subprocess.run(
                ["gh", "issue", "edit", str(issue_num), "--repo", repo, "--remove-label", "claimed"],
                capture_output=True, timeout=30, cwd=cwd,
            )
            if r3.returncode != 0:
                continue
            age_str = f"{int(age // 3600)}h{int((age % 3600) // 60)}m"
            msg = (f"Stale claim released by reconciler — session `{owner_uuid}` "
                   f"is no longer running (claimed {age_str} ago). Available for reclaim.")
            subprocess.run(
                ["gh", "issue", "comment", str(issue_num), "--repo", repo, "--body", msg],
                capture_output=True, timeout=30, cwd=cwd,
            )
            # Record in history so we don't re-process this exact claim.
            # Store the comment timestamp so we can distinguish this release
            # from a future re-claim (even by the same UUID via resume).
            with _claim_history_filelock():
                h = load_claim_history()
                key = str(issue_num)
                h[key] = {
                    "session_uuid": owner_uuid,
                    "short_id": short_id,
                    "restart_count": 0,
                    "released": True,
                    "released_comment_time": comment_time,
                }
                _save_claim_history(h)
            released_count += 1
            log(f"Reconcile: released stale claim #{issue_num} (owner {owner_uuid[:8]}, age {age_str})")
        except (subprocess.TimeoutExpired, OSError):
            continue

    if released_count or backfilled_count:
        log(f"Reconcile: {backfilled_count} backfilled, {released_count} released")


def check_dead_pr_claimed_prs(config: dict):
    """Release `repair-claimed` labels on PRs whose owning session is dead.

    Counterpart to check_dead_claimed_issues, but for the PR-claim namespace
    used by repair agents. Unlike issue claims, repair claims are never
    restarted — if the session died, we clear the claim and let a fresh
    repair agent pick the PR up next time.

    Also reconciles untracked `repair-claimed` labels (label present on GitHub
    but no local history entry) and stale-by-time claims where the PID cannot
    be resolved. Stale threshold defaults to 2× `repair.stuck_ci_minutes`.

    Fail-closed: any gh / parsing error on a specific PR skips it.
    """
    repo = _get_repo()
    cwd = str(PROJECT_DIR)
    stuck_minutes = cfg_get(config, "repair", "stuck_ci_minutes", default=120)
    stale_seconds = int(stuck_minutes) * 60 * 2

    # 1. Find all open PRs carrying repair-claimed.
    try:
        r = subprocess.run(
            ["gh", "pr", "list", "--repo", repo, "--state", "open",
             "--label", "repair-claimed", "--limit", "100",
             "--json", "number"],
            capture_output=True, text=True, timeout=30, cwd=cwd,
        )
        if r.returncode != 0:
            return
        labelled = {pr["number"] for pr in json.loads(r.stdout)}
    except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
        return

    history = load_pr_claim_history()
    agents = read_all_agents()
    live_uuids = {a.uuid for a in agents if a.status not in ("dead", "stopped", "killed")}
    # Also treat any PR actively tracked by a live agent's state.repair_pr as
    # owned, even if the local history hasn't caught up yet (e.g. within the
    # few-second window between label write and record_pr_claim).
    live_repair_prs = {a.repair_pr for a in agents
                        if a.repair_pr > 0 and a.uuid in live_uuids}

    now_epoch = time.time()
    grace_seconds = 300  # 5 min — don't yank labels out from under fresh claims
    to_release: list[tuple[int, str]] = []  # (pr_num, reason)

    # 2. Labelled PRs: check live-session liveness via history + agent state.
    for pr_num in labelled:
        if pr_num in live_repair_prs:
            continue  # A live agent reports this PR as its repair_pr
        entry = history.get(str(pr_num))
        if not entry:
            # Untracked label. Could be a leak (label added by a failed
            # claim run that died before record_pr_claim) or a very fresh
            # claim whose record hasn't been written yet. Fetch the latest
            # claim comment to distinguish, and only release if:
            #  - the comment is older than the grace window, AND
            #  - its owner session is not live.
            try:
                r = subprocess.run(
                    ["gh", "api", "--paginate", f"repos/{repo}/issues/{pr_num}/comments",
                     "--jq",
                     '[.[] | select(.body | startswith("Claimed PR repair by session"))] '
                     '| sort_by(.created_at) | last | {body, created_at}'],
                    capture_output=True, text=True, timeout=30, cwd=cwd,
                )
                if r.returncode != 0:
                    continue
                blob = r.stdout.strip()
                if not blob or blob == "null":
                    # No claim comment at all — label is orphaned from a
                    # failed claim attempt. Grace-release.
                    to_release.append((pr_num, "orphaned label: no claim comment"))
                    continue
                cdata = json.loads(blob)
            except (subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
                continue
            import re as _re
            m = _re.search(r'Claimed PR repair by session `([^`]+)`', cdata.get("body", ""))
            if not m:
                continue
            owner_uuid = m.group(1)
            try:
                from datetime import datetime
                created_at = cdata.get("created_at", "")
                claim_ts = datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()
            except (ValueError, TypeError):
                continue
            age = now_epoch - claim_ts
            if age < grace_seconds:
                continue  # Too fresh, might be a claim mid-write
            if owner_uuid in live_uuids:
                continue  # Owner is alive — leave their label alone
            to_release.append((pr_num, f"untracked: owner {owner_uuid[:8]} not live (age {int(age)}s)"))
            continue

        owner_uuid = entry.get("session_uuid", "")
        if owner_uuid in live_uuids:
            continue  # Still being worked on

        claimed_at = entry.get("claimed_at", 0)
        age = now_epoch - claimed_at if claimed_at else stale_seconds + 1
        if age < grace_seconds:
            continue
        to_release.append((pr_num, f"owner {owner_uuid[:8]} no longer live (age {int(age)}s)"))

    # 3. Issue the releases.
    released = 0
    for pr_num, reason in to_release:
        try:
            r = subprocess.run(
                ["gh", "pr", "edit", str(pr_num), "--repo", repo,
                 "--remove-label", "repair-claimed"],
                capture_output=True, timeout=30, cwd=cwd,
            )
            if r.returncode != 0:
                continue
            subprocess.run(
                ["gh", "pr", "comment", str(pr_num), "--repo", repo,
                 "--body", f"Repair claim released by reconciler: {reason}."],
                capture_output=True, timeout=30, cwd=cwd,
            )
            clear_pr_claim(pr_num)
            released += 1
            log(f"check_dead_pr_claimed_prs: released repair claim on PR #{pr_num} ({reason})")
        except (subprocess.TimeoutExpired, OSError):
            continue

    # 4. Clean history entries for PRs that no longer have the label
    #    (claim was cleared by claim-pr-repair itself, or the PR merged/closed).
    stale_history_keys = [
        k for k in history
        if int(k) not in labelled and (now_epoch - history[k].get("claimed_at", 0)) > 60
    ]
    if stale_history_keys:
        with _claim_history_filelock():
            h = load_pr_claim_history()
            for k in stale_history_keys:
                h.pop(k, None)
            _save_pr_claim_history(h)

    if released:
        log(f"check_dead_pr_claimed_prs: released {released} stale repair claim(s)")


# ---------------------------------------------------------------------------
# Agent Lifecycle
# ---------------------------------------------------------------------------

def setup_worktree(config: dict, short_id: str) -> tuple[str, str]:
    """Create a fresh git worktree. Returns (worktree_path, branch_name)."""
    base = PROJECT_DIR / cfg_get(config, "project", "worktree_base", default="worktrees")
    base.mkdir(parents=True, exist_ok=True)
    wt_dir = base / short_id
    branch = f"agent/{short_id}"

    # Fetch latest default branch
    base_branch = _get_base_branch()
    subprocess.run(
        ["git", "-C", str(PROJECT_DIR), "fetch", "origin", base_branch, "--quiet"],
        capture_output=True, timeout=60,
    )

    # Clean up any leftover worktree/branch from a crash
    if wt_dir.exists():
        # Unlock first — locked worktrees survive prune even when the dir is gone
        subprocess.run(
            ["git", "-C", str(PROJECT_DIR), "worktree", "unlock", str(wt_dir)],
            capture_output=True, timeout=10,
        )
        shutil.rmtree(str(wt_dir), ignore_errors=True)
        subprocess.run(
            ["git", "-C", str(PROJECT_DIR), "worktree", "prune"],
            capture_output=True, timeout=30,
        )
    subprocess.run(
        ["git", "branch", "-D", branch],
        capture_output=True, timeout=10, cwd=str(PROJECT_DIR),
    )

    # Create worktree
    r = subprocess.run(
        ["git", "-C", str(PROJECT_DIR), "worktree", "add", "-b", branch,
         str(wt_dir), f"origin/{base_branch}", "--quiet"],
        capture_output=True, timeout=60, check=True,
    )
    if not wt_dir.exists():
        raise subprocess.CalledProcessError(
            0, "git worktree add",
            output=r.stdout, stderr=(r.stderr + b" [dir not created]"),
        )

    return str(wt_dir), branch


def _pod_installed_files() -> list[str]:
    """Return list of relative paths that pod delivers into a worktree.

    Scans the bundled agent-config/claude for commands and skills files.
    These are automatically added to the protected-files list so agents
    cannot modify pod-delivered files in PRs (but can create new ones).
    """
    data_config = _data_dir() / "agent-config" / "claude"
    result: list[str] = []
    for subdir in ("commands", "skills"):
        src = data_config / subdir
        if src.is_dir():
            for item in src.rglob("*"):
                if item.is_file():
                    result.append(f".claude/{subdir}/{item.relative_to(src)}")
    result.append("AGENTS.md")
    return result


_POD_CODEX_AGENTS_SENTINEL = "<!-- pod-managed-codex-agents -->"


def _read_text(path: Path) -> str:
    return path.read_text() if path.is_file() else ""


def _codex_project_guidance(wt_dir: str) -> tuple[str, str]:
    """Return repo AGENTS.md and project .claude/CLAUDE.md text."""
    root = Path(wt_dir)
    repo_agents = _read_text(root / "AGENTS.md")
    if _POD_CODEX_AGENTS_SENTINEL in repo_agents:
        repo_agents = ""
    project_claude = _read_text(root / ".claude" / "CLAUDE.md")
    return repo_agents, project_claude


def _compose_pod_codex_agents(wt_dir: str) -> str:
    """Build the pod-managed AGENTS.md payload for Codex worktrees."""
    repo_agents, project_claude = _codex_project_guidance(wt_dir)
    pod_agents = (_data_dir() / "agent-config" / "codex" / "AGENTS.md").read_text()
    sections = [_POD_CODEX_AGENTS_SENTINEL]
    if project_claude.strip():
        sections.append("# Project .claude/CLAUDE.md\n\n" + project_claude.strip())
    sections.append("# Pod Codex Guidance\n\n" + pod_agents.strip())
    return "\n\n".join(sections) + "\n"


def install_agent_config(wt_dir: str, backend: str = "claude"):
    """Install bundled commands, skills, and agent instructions into worktree.

    For Claude: merges agent-config/claude into .claude/ (commands, skills,
    CLAUDE.md).
    For Codex: appends pod-managed guidance to the worktree's root AGENTS.md,
    creating it when needed. Skills are installed into CODEX_HOME by
    _setup_codex_home(). Commands are read at launch time by _worker_prompt()
    and delivered via stdin.
    """
    data_config = _data_dir() / "agent-config" / backend

    if backend == "codex":
        # Mirror Claude's .claude/CLAUDE.md behavior: append pod guidance to
        # the repo's AGENTS.md in-place, creating it when absent.
        dst_md = Path(wt_dir) / "AGENTS.md"
        existing = _read_text(dst_md)
        agent_text = _compose_pod_codex_agents(wt_dir)
        if _POD_CODEX_AGENTS_SENTINEL in existing:
            base_text = existing.split(_POD_CODEX_AGENTS_SENTINEL, 1)[0].rstrip()
            if base_text:
                dst_md.write_text(base_text + "\n\n" + agent_text)
            else:
                dst_md.write_text(agent_text)
        elif agent_text not in existing:
            if existing.strip():
                dst_md.write_text(existing.rstrip() + "\n\n" + agent_text)
            else:
                dst_md.write_text(agent_text)
        # Skills and commands are handled by _setup_codex_home and
        # _worker_prompt respectively — nothing else to install here.
    else:
        # Claude: install into .claude/
        wt_claude = Path(wt_dir) / ".claude"
        wt_claude.mkdir(parents=True, exist_ok=True)

        # Commands
        src_cmds = data_config / "commands"
        if src_cmds.is_dir():
            dst_cmds = wt_claude / "commands"
            shutil.copytree(str(src_cmds), str(dst_cmds), dirs_exist_ok=True)

        # Skills
        src_skills = data_config / "skills"
        if src_skills.is_dir():
            dst_skills = wt_claude / "skills"
            shutil.copytree(str(src_skills), str(dst_skills), dirs_exist_ok=True)

        # Append agent CLAUDE.md to project CLAUDE.md
        agent_md = data_config / "CLAUDE.md"
        if agent_md.is_file():
            dst_md = wt_claude / "CLAUDE.md"
            existing = dst_md.read_text() if dst_md.is_file() else ""
            agent_text = agent_md.read_text()
            if agent_text not in existing:
                with open(dst_md, "a") as f:
                    f.write("\n\n" + agent_text)


def _worker_prompt(config: dict, worker_type: str) -> str:
    """Return the prompt text for a given worker type.

    For Claude: returns the slash command name (e.g. "/work").
    For Codex: reads the command template file and returns its full text,
    since Codex has no slash command system.
    """
    backend = _backend(config)
    worker_types = cfg_get(config, "worker_types", default={})
    wt_cfg = worker_types.get(worker_type, {})
    prompt = wt_cfg.get("prompt", f"/{worker_type}")

    if backend == "codex" and prompt.startswith("/"):
        # Slash command — resolve to the command template file body
        cmd_name = prompt.lstrip("/")
        cmd_file = _data_dir() / "agent-config" / "codex" / "commands" / f"{cmd_name}.md"
        if cmd_file.is_file():
            prompt = cmd_file.read_text()
        # Fallback: use the command name as-is
        else:
            log(f"Warning: no Codex command template for '{cmd_name}', using raw prompt")
    return prompt


def copy_build_cache(wt_dir: str, config: dict):
    """rsync build cache directory into worktree for faster builds."""
    cache_dir = cfg_get(config, "project", "build_cache_dir", default=".lake")
    cache_src = PROJECT_DIR / cache_dir
    timeout = cfg_get(config, "project", "build_cache_timeout", default=600)
    if cache_src.is_dir():
        try:
            r = subprocess.run(
                ["rsync", "-a", "--quiet", f"{cache_src}/", f"{wt_dir}/{cache_dir}/"],
                capture_output=True, timeout=timeout,
            )
            if r.returncode != 0:
                log(f"Warning: rsync of {cache_dir} failed (exit {r.returncode}): "
                    f"{r.stderr.decode(errors='replace').strip()[:200]}")
                # Remove partial copy to avoid corrupt cache
                dest = Path(wt_dir) / cache_dir
                if dest.is_dir():
                    shutil.rmtree(str(dest), ignore_errors=True)
        except subprocess.TimeoutExpired:
            log(f"Warning: rsync of {cache_dir} timed out after {timeout}s "
                f"(source may be too large)")
            # Remove partial copy to avoid corrupt cache
            dest = Path(wt_dir) / cache_dir
            if dest.is_dir():
                shutil.rmtree(str(dest), ignore_errors=True)
        except OSError as e:
            log(f"Warning: rsync of {cache_dir} failed: {e}")


def cleanup_worktree(wt_dir: str, branch: str):
    """Remove worktree and delete branch.

    Uses shutil.rmtree instead of ``git worktree remove`` because the latter
    takes a git lock and can time out when many worktrees exist concurrently.
    After removing the directory we run ``git worktree prune`` to update git's
    metadata.  Errors are logged rather than silently suppressed.
    """
    if os.path.isdir(wt_dir):
        # Unlock first so prune can clean up metadata
        subprocess.run(
            ["git", "-C", str(PROJECT_DIR), "worktree", "unlock", wt_dir],
            capture_output=True, timeout=10,
        )
        shutil.rmtree(wt_dir, ignore_errors=True)
        if os.path.isdir(wt_dir):
            log(f"Warning: failed to fully remove worktree {wt_dir}")

    # Tell git to prune stale worktree metadata (fast — just scans admin dir)
    try:
        subprocess.run(
            ["git", "-C", str(PROJECT_DIR), "worktree", "prune"],
            capture_output=True, timeout=30,
        )
    except subprocess.TimeoutExpired:
        log("Warning: git worktree prune timed out")

    r = subprocess.run(
        ["git", "branch", "-D", branch],
        capture_output=True, text=True, timeout=10, cwd=str(PROJECT_DIR),
    )
    if r.returncode != 0 and "not found" not in r.stderr:
        log(f"Warning: git branch -D {branch} failed: {r.stderr.strip()}")


CLEANUP_LOCK_PATH = POD_DIR / "cleanup.lock"
CLEANUP_MIN_AGE_SECONDS = 120  # Don't delete worktrees younger than this


def _is_pod_owned_worktree(entry: Path) -> bool:
    """Check whether a worktree directory was created by pod.

    Verifies that a corresponding ``agent/<dirname>`` branch exists, which
    is the naming convention pod uses.  This prevents accidental deletion
    of non-pod directories that happen to live under worktree_base.
    """
    branch = f"agent/{entry.name}"
    r = subprocess.run(
        ["git", "rev-parse", "--verify", f"refs/heads/{branch}"],
        capture_output=True, timeout=10, cwd=str(PROJECT_DIR),
    )
    return r.returncode == 0


def cleanup_stale_worktrees(config: dict, *, verbose: bool = False,
                            min_age: float = CLEANUP_MIN_AGE_SECONDS,
                            force: bool = False) -> int:
    """Remove worktree directories that no running agent owns.

    Returns the number of worktrees actually removed.  JSONL session logs
    live in the isolated Claude config dir (.pod/claude-config/projects/)
    and are NOT touched by this function.

    Uses a cross-process file lock so only one cleanup runs at a time
    (prevents thundering-herd when many agents hit housekeeping together).
    Skips worktrees younger than *min_age* seconds to avoid racing with
    agents that are still setting up.  Only deletes directories that are
    confirmed pod-owned (have a matching ``agent/<name>`` branch).
    """
    import concurrent.futures

    base = PROJECT_DIR / cfg_get(config, "project", "worktree_base", default="worktrees")
    if not base.is_dir():
        return 0

    # --- Cross-process lock (non-blocking: skip if another process is cleaning) ---
    CLEANUP_LOCK_PATH.touch(exist_ok=True)
    lock_fd = open(CLEANUP_LOCK_PATH)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        # Another process holds the lock — skip this cycle
        lock_fd.close()
        if verbose:
            log("Skipping stale worktree cleanup (another process holds the lock)")
        return 0

    try:
        return _cleanup_stale_worktrees_locked(
            config, base, verbose=verbose, min_age=min_age, force=force,
        )
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


def _cleanup_stale_worktrees_locked(
    config: dict, base: Path, *, verbose: bool, min_age: float, force: bool,
) -> int:
    """Inner cleanup implementation — caller must hold CLEANUP_LOCK_PATH."""
    import concurrent.futures

    # Collect worktree dirs owned by live agents
    live_worktrees: set[str] = set()
    for agent in read_all_agents():
        if agent.worktree and agent.status != "dead":
            live_worktrees.add(agent.worktree)

    now = time.time()
    stale: list[Path] = []
    for entry in sorted(base.iterdir()):
        if not entry.is_dir():
            continue
        if str(entry) in live_worktrees:
            continue
        # Age threshold: skip recently-created worktrees unless force
        if not force:
            try:
                age = now - entry.stat().st_mtime
            except OSError:
                continue
            if age < min_age:
                if verbose:
                    log(f"Skipping young worktree {entry.name} (age {age:.0f}s < {min_age:.0f}s)")
                continue
        # Ownership check: only delete pod-owned worktrees
        if not _is_pod_owned_worktree(entry):
            if verbose:
                log(f"Skipping non-pod worktree {entry.name} (no agent/* branch)")
            continue
        stale.append(entry)

    if not stale:
        return 0

    if verbose:
        log(f"Cleaning {len(stale)} stale worktree(s)...")

    removed = 0
    failed: list[str] = []

    def _remove_one(entry: Path) -> bool:
        """Remove one worktree. Returns True on success."""
        # Only unlock if this is a pod-owned worktree (agent/* branch exists)
        subprocess.run(
            ["git", "-C", str(PROJECT_DIR), "worktree", "unlock",
             str(entry)],
            capture_output=True, timeout=10,
        )
        r = subprocess.run(
            ["rm", "-rf", str(entry)],
            capture_output=True, timeout=120,
        )
        return r.returncode == 0 and not entry.exists()

    # Parallel rm -rf — each is I/O-bound, so threads work well
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_remove_one, e): e for e in stale}
        for fut in concurrent.futures.as_completed(futures):
            entry = futures[fut]
            try:
                if fut.result():
                    removed += 1
                else:
                    failed.append(entry.name)
                    log(f"Failed to remove worktree {entry.name}")
            except Exception as exc:
                failed.append(entry.name)
                log(f"Exception removing worktree {entry.name}: {exc}")

    # Prune metadata, then delete branches (can't delete a branch
    # that git still thinks is checked out in a worktree)
    try:
        subprocess.run(
            ["git", "-C", str(PROJECT_DIR), "worktree", "prune"],
            capture_output=True, timeout=30,
        )
    except subprocess.TimeoutExpired:
        log("git worktree prune timed out")

    for entry in stale:
        if entry.name not in failed:
            subprocess.run(
                ["git", "branch", "-D", f"agent/{entry.name}"],
                capture_output=True, timeout=10, cwd=str(PROJECT_DIR),
            )

    if verbose:
        msg = f"Cleaned {removed} stale worktree(s)"
        if failed:
            msg += f" ({len(failed)} failed: {', '.join(failed)})"
        log(msg)

    return removed


def _log_credential_state(session_uuid: str, claude_config_dir: Path | None = None):
    """Log which credential Claude Code will actually use at agent launch time.

    Reports the resolved credential (the one Claude Code reads from the
    suffixed keychain entry, or the credentials.json fallback) and warns
    when the isolated config dir diverges from the default keychain entry
    — that divergence is the canonical sign of the "wrong account" bug.
    """
    parts = [f"session={session_uuid[:8]}"]

    resolved = resolve_claude_credential(claude_config_dir)
    parts.append(
        f"resolved=[{resolved['accountLabel']}] "
        f"token={resolved['tokenPrefix']}... via {resolved['source']}"
    )

    if claude_config_dir is not None:
        default_resolved = resolve_claude_credential(None)
        if default_resolved["accountLabel"] != resolved["accountLabel"]:
            parts.append(
                f"WARNING default keychain has [{default_resolved['accountLabel']}] "
                f"-- isolated config diverges"
            )

    log(f"Credential state at launch: {' | '.join(parts)}")


def launch_agent(config: dict, session_uuid: str, prompt: str,
                   wt_dir: str,
                   claude_config_dir: Path | None = None) -> subprocess.Popen:
    """Launch agent subprocess (Claude or Codex) in the worktree directory."""
    # Re-read model from disk so config.toml edits take effect without restart.
    backend = _reload_config_value("agent", "backend", default="claude")
    model = _reload_config_value("agent", backend, "model", default="opus")
    session_dir = PROJECT_DIR / cfg_get(config, "project", "session_dir", default="sessions")
    session_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = session_dir / f"{session_uuid}.stdout"

    env = dict(os.environ)
    env["POD_SESSION_ID"] = session_uuid
    # Inject bundled data dir into PATH so agents find `coordination`
    env["PATH"] = str(_data_dir()) + os.pathsep + env.get("PATH", "")

    stdin_pipe = None  # Only used for Codex (stdin prompt delivery)

    if backend == "codex":
        # --- Codex launch ---
        codex_args = ["codex", "exec", "--json",
                      "--dangerously-bypass-approvals-and-sandbox",
                      "--skip-git-repo-check",
                      "-m", model,
                      "-C", wt_dir,
                      "-"]  # Read prompt from stdin
        env["OPENAI_API_KEY"] = ""  # Force subscription auth
        env["POD_IS_RESUME"] = "0"

        # Set up isolated CODEX_HOME if configured
        codex_home = _setup_codex_home(config, wt_dir)
        if codex_home:
            env["CODEX_HOME"] = str(codex_home)
            log(f"Using strict pod-managed CODEX_HOME={codex_home}")

        # Write a sidecar manifest so the historical-cost scanner knows
        # which pricing table to use for rollouts of this session
        # (rollout JSONLs do not carry a stable model-name field).
        _write_codex_manifest(session_uuid, backend, model, wt_dir)

        stdin_pipe = subprocess.PIPE
        agent_args = codex_args
    else:
        # --- Claude launch (existing behavior) ---
        jsonl_dir = _claude_projects_dir(claude_config_dir) / wt_dir.replace("/", "-")
        local_jsonl = jsonl_dir / f"{session_uuid}.jsonl"

        claude_args = ["claude", "--model", model]
        if local_jsonl.exists():
            claude_args += ["--resume", session_uuid]
        else:
            claude_args += ["--session-id", session_uuid]
        if claude_config_dir is not None:
            claude_args += ["--dangerously-skip-permissions"]
        claude_args += ["-p", prompt]

        env["ANTHROPIC_API_KEY"] = ""  # Force subscription auth
        env["POD_IS_RESUME"] = "1" if local_jsonl.exists() else "0"
        if claude_config_dir is not None:
            env["CLAUDE_CONFIG_DIR"] = str(claude_config_dir)

        # Log credential state at launch for debugging account-swap issues
        _log_credential_state(session_uuid, claude_config_dir)

        agent_args = claude_args

    stdout_fd = os.open(str(stdout_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    proc = subprocess.Popen(
        agent_args,
        stdin=stdin_pipe,
        stdout=stdout_fd,
        stderr=subprocess.STDOUT,
        cwd=wt_dir,
        env=env,
        start_new_session=True,  # Create process group for clean killing
    )
    os.close(stdout_fd)  # Child inherited it; parent no longer needs it

    # For Codex, deliver prompt via stdin and close
    if backend == "codex" and proc.stdin is not None:
        try:
            proc.stdin.write(prompt.encode())
            proc.stdin.close()
        except OSError:
            pass  # Process may have exited immediately

    return proc


def _codex_rollouts_dir() -> Path:
    """Project-level directory where all Codex rollouts land, via CODEX_HOME symlink."""
    return PROJECT_DIR / ".pod" / "codex-sessions"


def _codex_manifest_dir() -> Path:
    """Directory for per-session sidecar manifests (backend, model, wt_dir, started_at)."""
    return _codex_rollouts_dir() / "manifests"


def _write_codex_manifest(session_uuid: str, backend: str, model: str, wt_dir: str) -> None:
    """Persist a tiny sidecar so the historical scanner can price rollouts.

    The rollout JSONL does not carry a stable model-name field; we write
    `<project>/.pod/codex-sessions/manifests/<uuid>.json` with the info
    known at launch time.
    """
    try:
        mdir = _codex_manifest_dir()
        mdir.mkdir(parents=True, exist_ok=True)
        mpath = mdir / f"{session_uuid}.json"
        tmp = mpath.with_suffix(".tmp")
        tmp.write_text(json.dumps({
            "session_id": session_uuid,
            "backend": backend,
            "model": model,
            "wt_dir": wt_dir,
            "started_at": time.time(),
        }) + "\n")
        tmp.replace(mpath)
    except OSError as e:
        log(f"Warning: failed to write Codex manifest for {session_uuid}: {e}")


def _setup_codex_home(config: dict, wt_dir: str) -> Path | None:
    """Create an isolated CODEX_HOME directory for a Codex agent.

    Sets up a strict pod-managed home: bundled skills, a minimal pod-owned
    config.toml, an auth symlink to ~/.codex/auth.json for login reuse, and
    a `sessions` symlink to `<project>/.pod/codex-sessions/` so Codex rollout
    JSONLs land in the project (scannable by historical cost, shared across
    worktrees) rather than being lost when the home is rebuilt.

    Refreshes only pod-owned paths in place rather than wiping the whole
    home. Wiping would race with any concurrent launch and would also blow
    away Codex-managed state (cache, logs, sqlite dbs) that has no business
    being pod-owned. Pod-owned paths: `auth.json`, `config.toml`, `skills/`,
    `sessions` (symlink). Everything else is Codex's to manage.

    Returns the CODEX_HOME path, or None if isolation is disabled.
    """
    if not _backend_cfg(config, "isolated_config", default=True):
        return None

    codex_home = Path(wt_dir) / ".pod-codex-home"
    codex_home.mkdir(parents=True, exist_ok=True)

    def _replace_symlink(link: Path, target: Path) -> None:
        """Atomically point `link` at `target` regardless of prior state."""
        tmp = link.with_name(link.name + ".tmp")
        try:
            if tmp.is_symlink() or tmp.exists():
                tmp.unlink()
        except OSError:
            pass
        tmp.symlink_to(target)
        os.replace(tmp, link)  # atomic on POSIX

    # Ensure the rollout dir lives in the project, not in the worktree.
    shared_sessions = PROJECT_DIR / ".pod" / "codex-sessions"
    shared_sessions.mkdir(parents=True, exist_ok=True)
    _replace_symlink(codex_home / "sessions", shared_sessions)

    # Symlink auth from user's global Codex config
    real_codex = Path.home() / ".codex"
    auth_target = real_codex / "auth.json"
    if auth_target.exists():
        _replace_symlink(codex_home / "auth.json", auth_target)

    # Write pod-owned minimal config for non-interactive execution.
    model = _reload_config_value("agent", "codex", "model", default="gpt-5.4")
    (codex_home / "config.toml").write_text(
        f'model = "{model}"\n'
        'approval_policy = "never"\n'
        'sandbox_mode = "danger-full-access"\n'
    )

    # Install bundled skills (refresh to match current package).
    data_skills = _data_dir() / "agent-config" / "codex" / "skills"
    if data_skills.is_dir():
        dst_skills = codex_home / "skills"
        if dst_skills.exists():
            shutil.rmtree(dst_skills)
        shutil.copytree(str(data_skills), str(dst_skills))

    return codex_home


def get_jsonl_path(wt_dir: str, session_uuid: str,
                   claude_config_dir: Path | None = None,
                   backend: str = "claude") -> str:
    """Compute JSONL file path for a session.

    For Claude: reads from ~/.claude/projects/{wt_dir}/{uuid}.jsonl
    For Codex: reads from sessions/{uuid}.stdout (the --json stream)
    """
    if backend == "codex":
        session_dir = PROJECT_DIR / "sessions"
        return str(session_dir / f"{session_uuid}.stdout")
    jsonl_dir = _claude_projects_dir(claude_config_dir) / wt_dir.replace("/", "-")
    return str(jsonl_dir / f"{session_uuid}.jsonl")


# ---------------------------------------------------------------------------
# Agent Process (forked background process)
# ---------------------------------------------------------------------------

# Globals for signal handlers
_agent_state: AgentState | None = None
_agent_proc: subprocess.Popen | None = None
_agent_config: dict = {}


def _sigterm_handler(signum, frame):
    """Handle SIGTERM: kill claude, unclaim, cleanup, exit."""
    global _agent_state, _agent_proc, _agent_config
    state = _agent_state
    config = _agent_config

    if state:
        log(f"Agent {state.short_id} received SIGTERM")
        state.status = "killed"
        state.write()

    # Kill claude subprocess
    if _agent_proc and _agent_proc.poll() is None:
        try:
            os.killpg(os.getpgid(_agent_proc.pid), signal.SIGTERM)
        except (ProcessLookupError, OSError):
            pass
        try:
            _agent_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(_agent_proc.pid), signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass

    if state and config:
        # Unclaim issue if claimed and no PR yet — but only if no other
        # live agent is also working on this issue (prevents unclaiming
        # when killing duplicate claimants).
        if state.claimed_issue > 0 and state.pr_number == 0:
            other_live = any(
                a.claimed_issue == state.claimed_issue
                and a.uuid != state.uuid
                and a.status not in ("dead", "stopped", "killed")
                for a in read_all_agents()
            )
            if other_live:
                log(f"Agent {state.short_id}: not unclaiming #{state.claimed_issue} — another agent has it")
                clear_claim(state.claimed_issue, session_uuid=state.uuid)
            else:
                try:
                    coordination(
                        config, "skip", str(state.claimed_issue),
                        f"Agent killed by operator (session {state.uuid})",
                        env_extra={"POD_SESSION_ID": state.uuid},
                    )
                    clear_claim(state.claimed_issue, session_uuid=state.uuid)
                    log(f"Unclaimed issue #{state.claimed_issue}")
                except Exception:
                    # Don't clear_claim — leave in history so housekeeping
                    # can retry the GitHub label removal later.
                    log(f"Failed to skip #{state.claimed_issue} on kill, keeping in history")

        # Release lock if held
        if state.lock_held:
            try:
                coordination(config, f"unlock-{state.lock_held}")
                log(f"Released {state.lock_held} lock")
            except Exception:
                pass

        # Cleanup worktree
        if state.worktree and state.branch:
            cleanup_worktree(state.worktree, state.branch)

        state.remove_file()

    os._exit(0)


def _sigusr1_handler(signum, frame):
    """Handle SIGUSR1: set finishing flag."""
    global _agent_state
    if _agent_state:
        _agent_state.finishing = True
        _agent_state.status = "finishing"
        _agent_state.write()
        log(f"Agent {_agent_state.short_id} marked as finishing")


def agent_process_main(config: dict, agent_id: str | None = None,
                        resume_uuid: str | None = None):
    """Entry point for a forked agent process. Runs the agent loop."""
    global _agent_state, _agent_proc, _agent_config
    _agent_config = config

    short_id = agent_id or uuid.uuid4().hex[:8]

    my_pid = os.getpid()
    _backend_name = _backend(config)
    state = AgentState(
        short_id=short_id,
        pid=my_pid,
        pid_start_time=_get_pid_start_time(my_pid),
        status="starting",
        resume_session_uuid=resume_uuid or "",
        backend=_backend_name,
        model=_backend_cfg(config, "model", default="") or "",
    )
    _agent_state = state
    state.write()

    # Install signal handlers
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGUSR1, _sigusr1_handler)

    poll_interval = cfg_get(config, "monitor", "poll_interval", default=2)
    quota_retry = _backend_cfg(config, "quota_retry_seconds", default=60)
    worker_types = cfg_get(config, "worker_types", default={})

    claude_config_dir = ensure_isolated_config(config)
    if claude_config_dir:
        log(f"Agent {short_id}: using isolated CLAUDE_CONFIG_DIR={claude_config_dir}")
    elif _backend(config) == "codex":
        log(f"Agent {short_id}: using codex backend (CODEX_HOME set per session)")

    log(f"Agent {short_id} started (PID {os.getpid()})")

    iteration = 0
    consecutive_wait = 0  # Consecutive dispatch-returned-None iterations (for backoff)
    last_housekeeping = 0.0  # Wall-clock time of last housekeeping run
    HOUSEKEEPING_INTERVAL = 600  # seconds between housekeeping runs (check-blocked, dead claims)
    while not state.finishing:
        iteration += 1
        state.loop_iteration = iteration

        # --- Quota check ---
        state.status = "waiting_quota"
        state.write()
        while not check_quota(config, force=state.force_quota,
                               claude_config_dir=claude_config_dir):
            if state.finishing:
                break
            # Re-read state file to pick up force_quota toggled by TUI
            try:
                sf = AGENTS_DIR / f"{short_id}.json"
                d = json.loads(sf.read_text())
                state.force_quota = d.get("force_quota", False)
            except (OSError, json.JSONDecodeError):
                pass
            if state.force_quota:
                log(f"Agent {short_id}: force_quota enabled, skipping wait")
                break
            log(f"Agent {short_id}: no quota, sleeping {quota_retry}s")
            time.sleep(quota_retry)
        if state.finishing:
            break

        # --- Housekeeping (time-based, every 10 minutes to conserve GH API calls) ---
        now_hk = time.time()
        if now_hk - last_housekeeping > HOUSEKEEPING_INTERVAL:
            last_housekeeping = now_hk
            try:
                coordination(config, "check-blocked")
            except Exception:
                pass
            try:
                check_dead_claimed_issues(config)
            except Exception:
                pass
            try:
                reconcile_untracked_github_claims()
            except Exception:
                pass
            try:
                check_dead_pr_claimed_prs(config)
            except Exception:
                pass
            try:
                cleanup_stale_worktrees(config, verbose=True)
            except Exception:
                pass

        # --- Dispatch (sets state.lock_held atomically if lock acquired) ---
        # If this agent was spawned to resume a specific session, skip dispatch.
        _resume_uuid = state.resume_session_uuid
        if _resume_uuid:
            state.resume_session_uuid = ""
            chosen_type = "work"
            prompt = "You were interrupted mid-task. Review your conversation history and continue where you left off."
            lock_name = ""
            wt_config = {}
            state.worker_type = chosen_type
            state.write()
            log(f"Agent {short_id}: resuming session {_resume_uuid}")
        else:
            _resume_uuid = None
            state.status = "dispatching"
            state.write()
            chosen_type = dispatch(config, state)
            if chosen_type is None:
                state.status = "waiting_dispatch"
                state.write()
                consecutive_wait += 1
                # If GH rate limit is exhausted, sleep until reset instead
                # of burning more calls every 60s
                rl_wait = _gh_rate_limit_wait()
                if rl_wait > 0:
                    wait = rl_wait + 30
                    log(f"Agent {short_id}: GH rate limit low, sleeping {wait}s until reset")
                    time.sleep(wait)
                    _clear_gh_cache()
                else:
                    # Linear backoff: 60s, 120s, 180s, ..., capped at 300s
                    wait = min(quota_retry * consecutive_wait, 300)
                    log(f"Agent {short_id}: dispatch returned None (waiting {wait}s, attempt {consecutive_wait})")
                    time.sleep(wait)
                continue

            consecutive_wait = 0  # Reset backoff on successful dispatch
            wt_config = worker_types.get(chosen_type, {})
            lock_name = wt_config.get("lock", "")

            state.worker_type = chosen_type
            state.write()

            log(f"Agent {short_id}: dispatched as {chosen_type}")

        # --- Session setup ---
        session_uuid = _resume_uuid if _resume_uuid else str(uuid.uuid4())
        state.uuid = session_uuid
        state.session_start = time.time()
        state.claimed_issue = 0
        state.pr_number = 0
        state.tokens_in = 0
        state.tokens_out = 0
        state.cache_read = 0
        state.cache_create = 0
        state.last_text = ""
        state.last_activity = 0.0

        # Use session UUID prefix for worktree/branch to avoid branch reuse
        # across sessions.  When the same agent process iterates, a persistent
        # short_id would reuse the same branch name, hitting an existing remote
        # PR from the *previous* issue.  A per-session prefix gives each
        # session its own branch (agent/<session-prefix>), preventing the
        # create-pr "PR already exists" shortcut from silently linking the
        # wrong issue.
        session_short = session_uuid[:8]

        # Pre-register worktree in state BEFORE creation so that
        # cleanup_stale_worktrees() won't delete it during setup.
        base = PROJECT_DIR / cfg_get(config, "project", "worktree_base", default="worktrees")
        wt_dir_expected = str(base / session_short)
        branch_expected = f"agent/{session_short}"
        state.worktree = wt_dir_expected
        state.branch = branch_expected
        state.write()

        try:
            wt_dir, branch = setup_worktree(config, session_short)
        except subprocess.CalledProcessError as e:
            log(f"Agent {short_id}: worktree setup failed: {e}")
            state.worktree = ""
            state.branch = ""
            if lock_name:
                coordination(config, f"unlock-{lock_name}")
                state.lock_held = ""
            state.status = "error"
            state.write()
            time.sleep(10)
            continue

        if not os.path.isdir(wt_dir):
            log(f"Agent {short_id}: worktree dir missing after setup_worktree returned: {wt_dir}")
            state.worktree = ""
            state.branch = ""
            if lock_name:
                coordination(config, f"unlock-{lock_name}")
                state.lock_held = ""
            state.status = "error"
            state.write()
            time.sleep(10)
            continue

        state.worktree = wt_dir
        state.branch = branch
        state.git_start = _git_rev(wt_dir)

        install_agent_config(wt_dir, backend=_backend(config))

        if not _resume_uuid:
            prompt = _worker_prompt(config, chosen_type)

        if wt_config.get("copy_build_cache", wt_config.get("copy_lake_cache", False)):
            copy_build_cache(wt_dir, config)

        # --- Start JSONL monitor ---
        jsonl_path = get_jsonl_path(wt_dir, session_uuid, claude_config_dir,
                                    backend=_backend(config))
        stop_monitor = threading.Event()
        monitor_thread = threading.Thread(
            target=jsonl_monitor,
            args=(jsonl_path, state, stop_monitor, _backend(config)),
            daemon=True,
        )
        monitor_thread.start()

        # --- Launch agent ---
        state.status = "running"
        state.write()
        log(f"Agent {short_id}: launching {_backend(config)} session {session_uuid} in {wt_dir}")

        try:
            _agent_proc = launch_agent(config, session_uuid, prompt, wt_dir,
                                        claude_config_dir)
        except (OSError, FileNotFoundError) as e:
            log(f"Agent {short_id}: failed to launch {_backend(config)}: {e}")
            stop_monitor.set()
            cleanup_worktree(wt_dir, branch)
            if lock_name:
                coordination(config, f"unlock-{lock_name}")
                state.lock_held = ""
            state.status = "error"
            state.write()
            time.sleep(10)
            continue

        # --- Monitor until claude exits ---
        _last_tracked_issue = 0
        _last_tracked_repair_pr = 0
        _stuck_first_detected = 0.0   # monotonic time of first stuck detection
        _stuck_kill_pending = False    # True after phase-1 detection, waiting for confirm
        _last_stuck_check = 0.0       # monotonic time of last health check

        while _agent_proc.poll() is None:
            state.write()
            # Track claim changes: write to history when claimed, clear when PR created
            if state.claimed_issue > 0 and state.pr_number == 0:
                if state.claimed_issue != _last_tracked_issue:
                    record_claim(state.claimed_issue, state.uuid, state.short_id)
                    _last_tracked_issue = state.claimed_issue
            elif state.pr_number > 0 and state.claimed_issue > 0:
                clear_claim(state.claimed_issue, session_uuid=state.uuid)
                _last_tracked_issue = 0
            # Track repair-PR claim lifecycle (parallel to issue-claim). A
            # repair agent never has a claimed_issue, so these live on a
            # separate branch keyed by state.repair_pr.
            if state.repair_pr > 0 and state.repair_pr != _last_tracked_repair_pr:
                record_pr_claim(state.repair_pr, state.uuid, state.short_id)
                _last_tracked_repair_pr = state.repair_pr
            elif state.repair_pr == 0 and _last_tracked_repair_pr > 0:
                clear_pr_claim(_last_tracked_repair_pr, session_uuid=state.uuid)
                _last_tracked_repair_pr = 0

            # --- Stuck-agent detection ---
            stuck_initial = _reload_config_value(
                "monitor", "stuck_initial_timeout", default=3600)
            stuck_confirm = _reload_config_value(
                "monitor", "stuck_confirm_timeout", default=1200)
            stuck_interval = _reload_config_value(
                "monitor", "stuck_check_interval", default=30)

            # Determine idle duration (wall-clock, since last_activity uses time.time)
            if state.last_activity > 0:
                idle = time.time() - state.last_activity
            elif state.session_start > 0:
                # Fallback: session never produced assistant output
                idle = time.time() - state.session_start
            else:
                idle = 0

            if idle >= stuck_initial and _agent_proc is not None:
                now_mono = time.monotonic()
                if now_mono - _last_stuck_check >= stuck_interval:
                    _last_stuck_check = now_mono
                    is_stuck, detail = _check_process_stuck(_agent_proc.pid)

                    if is_stuck and not _stuck_kill_pending:
                        # Phase 1: first stuck detection — log and start confirm timer
                        _stuck_first_detected = now_mono
                        _stuck_kill_pending = True
                        log(f"Agent {short_id}: stuck detected "
                            f"(idle {human_duration(idle)}, {detail}). "
                            f"Will re-check in {human_duration(stuck_confirm)}.")
                    elif is_stuck and _stuck_kill_pending and \
                            now_mono - _stuck_first_detected >= stuck_confirm:
                        # Phase 2: confirmed stuck — kill subprocess
                        log(f"Agent {short_id}: KILLING stuck subprocess "
                            f"(idle {human_duration(idle)}, confirmed after "
                            f"{human_duration(now_mono - _stuck_first_detected)}). "
                            f"Detail: {detail}")
                        _kill_stuck_subprocess(_agent_proc)
                        # Don't break — let poll() return non-None on next iteration
                    elif not is_stuck and _stuck_kill_pending:
                        # Activity detected — reset
                        log(f"Agent {short_id}: stuck detection reset — "
                            f"process showed activity: {detail}")
                        _stuck_kill_pending = False
                        _stuck_first_detected = 0.0
            elif _stuck_kill_pending:
                # New JSONL output arrived (idle dropped below threshold) — reset
                _stuck_kill_pending = False
                _stuck_first_detected = 0.0

            time.sleep(poll_interval)

        exit_code = _agent_proc.returncode
        _agent_proc = None

        # --- Session ended ---
        stop_monitor.set()
        monitor_thread.join(timeout=5)

        # (Final JSONL drain handled by the monitor thread's exit path.)

        elapsed = time.time() - state.session_start
        git_end = _git_rev(wt_dir)
        tok = token_summary(state, config)
        log(f"Agent {short_id}: session finished exit={exit_code} "
            f"duration={human_duration(elapsed)} {tok} "
            f"git:{state.git_start}..{git_end}")

        # --- Quota exhaustion detection from stdout ---
        # Must run BEFORE claim cleanup: coordination skip marks issues as
        # "replan", but quota exhaustion is temporary — the issue is fine,
        # only the quota is depleted.  Release the claim without replan.
        _is_quota_exhausted = False
        if exit_code != 0:
            try:
                _stdout_path = (PROJECT_DIR
                    / cfg_get(config, "project", "session_dir", default="sessions")
                    / f"{session_uuid}.stdout")
                with open(_stdout_path, "rb") as f:
                    f.seek(0, 2)  # end
                    tail_start = max(0, f.tell() - 4096)
                    f.seek(tail_start)
                    tail = f.read().decode(errors="replace").lower()
                if ("hit your limit" in tail
                        or "rate limit" in tail
                        or "quota exceeded" in tail):
                    _is_quota_exhausted = True
            except OSError:
                pass

        if _is_quota_exhausted:
            log(f"Agent {short_id}: session stdout indicates quota exhaustion, "
                f"will re-check quota before next dispatch")
            # Release claim without replan — issue is fine, just quota-gated
            if state.claimed_issue > 0 and state.pr_number == 0:
                clear_claim(state.claimed_issue, session_uuid=state.uuid)
                log(f"Agent {short_id}: released claim on #{state.claimed_issue} (quota exhaustion, not replan)")
            cleanup_worktree(wt_dir, branch)
            state.worktree = ""
            state.branch = ""
            if lock_name:
                try:
                    coordination(config, f"unlock-{lock_name}")
                except Exception:
                    pass
                state.lock_held = ""
            state.write()
            continue  # loops back to top-of-loop quota check

        # --- Clear uncompleted claims so check_dead_claimed_issues doesn't
        #     mistake our finished session for a dead one and spawn a new agent.
        #     Without this, every session that ends without a PR leaves a stale
        #     claim entry pointing to our old session UUID; the next housekeeping
        #     cycle from any agent sees that UUID as dead and forks a new agent,
        #     causing unbounded agent proliferation.
        if state.claimed_issue > 0 and state.pr_number == 0:
            skip_ok = False
            try:
                coordination(
                    config, "skip", str(state.claimed_issue),
                    f"Session ended without PR (session {state.uuid})",
                    env_extra={"POD_SESSION_ID": state.uuid},
                    cwd=wt_dir,
                )
                skip_ok = True
            except Exception:
                log(f"Agent {short_id}: coordination skip #{state.claimed_issue} failed")
            if skip_ok:
                clear_claim(state.claimed_issue, session_uuid=state.uuid)
                log(f"Agent {short_id}: cleared claim on #{state.claimed_issue} (session ended without PR)")
            else:
                # Leave in claim-history so check_dead_claimed_issues or
                # reconcile_untracked_github_claims can clean it up later.
                log(f"Agent {short_id}: keeping claim #{state.claimed_issue} in history (skip failed, will retry via housekeeping)")

        # --- Circuit breaker: sessions that exit too quickly are broken ---
        if elapsed < 15 and state.tokens_in == 0 and state.tokens_out == 0:
            rapid_failures = getattr(state, '_rapid_failures', 0) + 1
            state._rapid_failures = rapid_failures
            backoff = min(60 * rapid_failures, 300)
            log(f"Agent {short_id}: session exited in {elapsed:.0f}s with 0 tokens "
                f"(rapid failure #{rapid_failures}), backing off {backoff}s")
            cleanup_worktree(wt_dir, branch)
            state.worktree = ""
            state.branch = ""
            if lock_name:
                try:
                    coordination(config, f"unlock-{lock_name}")
                except Exception:
                    pass
                state.lock_held = ""
            state.status = "backoff"
            state.write()
            time.sleep(backoff)
            continue
        else:
            # Reset rapid failure counter on successful session
            state._rapid_failures = 0

        # --- Cleanup ---
        cleanup_worktree(wt_dir, branch)
        state.worktree = ""
        state.branch = ""

        if lock_name:
            try:
                coordination(config, f"unlock-{lock_name}")
            except Exception:
                pass
            state.lock_held = ""

        state.write()

    # --- Agent loop exited ---
    log(f"Agent {short_id} exiting (finishing={state.finishing})")
    state.status = "stopped"
    state.write()
    # Leave state file briefly so TUI can see "stopped", then remove
    time.sleep(2)
    state.remove_file()


def _git_rev(wt_dir: str) -> str:
    try:
        r = subprocess.run(
            ["git", "-C", wt_dir, "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=10,
        )
        return r.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def spawn_agent(config: dict, agent_id: str | None = None,
                resume_uuid: str | None = None) -> int:
    """Fork a new agent process. Returns PID of the intermediate child.

    Uses double-fork so the agent is orphaned (adopted by init) and never
    becomes a zombie — without touching SIGCHLD in the calling process.
    Corrupting SIGCHLD in an agent process that calls spawn_agent (via
    check_dead_claimed_issues) would break git's internal waitpid and cause
    silent failures in git worktree add.
    """
    pid = os.fork()
    if pid > 0:
        # Parent: wait for the short-lived intermediate child (exits immediately).
        # Retry on EINTR so a signal doesn't cause a spurious launch failure.
        while True:
            try:
                os.waitpid(pid, 0)
                break
            except ChildProcessError:
                break
            except InterruptedError:
                continue
        return pid
    # Intermediate child: fork the actual agent, then exit immediately so
    # the agent is adopted by init (no zombie, no SIGCHLD games needed).
    try:
        gc_pid = os.fork()
    except Exception:
        os._exit(1)
    if gc_pid > 0:
        os._exit(0)
    # Grandchild: the actual agent process, adopted by init upon exit.
    try:
        os.setsid()
        devnull_r = os.open(os.devnull, os.O_RDONLY)
        devnull_w = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull_r, 0)
        os.dup2(devnull_w, 1)
        os.dup2(devnull_w, 2)
        os.close(devnull_r)
        os.close(devnull_w)
        agent_process_main(config, agent_id, resume_uuid)
    except Exception as e:
        log(f"Agent process crashed: {e}")
    finally:
        os._exit(0)


# ---------------------------------------------------------------------------
# TUI
# ---------------------------------------------------------------------------

def run_tui(config: dict):
    """Run the interactive curses TUI."""
    curses.wrapper(_tui_main, config)


def _tui_main(stdscr, config: dict):
    # Rebuild claim history from GitHub before starting, so pod can reattach
    # to sessions that were running before a pod restart.
    try:
        sync_claims_from_github()
    except Exception:
        pass

    curses.curs_set(0)
    stdscr.timeout(1000)  # 1 second refresh
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_GREEN, -1)    # Running / merged
    curses.init_pair(2, curses.COLOR_YELLOW, -1)    # Finishing / blocked
    curses.init_pair(3, curses.COLOR_RED, -1)        # Dead/error / failing
    curses.init_pair(4, curses.COLOR_CYAN, -1)       # Header / closed
    curses.init_pair(5, curses.COLOR_WHITE, curses.COLOR_BLUE)  # Selected
    curses.init_pair(6, curses.COLOR_MAGENTA, -1)    # PR open / has-pr

    selected_section = "agents"  # "agents" or "items"
    selected_idx = 0
    message = ""
    message_time = 0.0
    input_mode = ""  # "", "kill_confirm"
    cached_queue: int | None = None
    queue_fetch_time = 0.0
    cached_items: list[GHItem] = []
    displayed_items: list[GHItem] = []  # subset shown on screen (active-first order)
    items_fetch_time = 0.0
    cached_blocked_deps: dict[int, list[int]] = {}
    blocked_deps_fetch_time = 0.0
    cached_lock_status: str | None = None  # "locked" or "unlocked"
    lock_fetch_time = 0.0
    cached_repair_candidates: int = 0
    repair_candidates_fetch_time = 0.0
    # Whether the repair worker type is configured (avoid recomputing each tick).
    repair_configured = "repair" in cfg_get(config, "worker_types", default={})
    # Check for pre-existing return-to-human signal synchronously at startup.
    # If it was already set before this TUI session, we honour it (target=0, banner)
    # but do NOT send SIGUSR1 — the human restarted pod intentionally.
    try:
        cached_return_to_human = get_return_to_human(config)
    except Exception:
        cached_return_to_human = False
    return_to_human_fetch_time = 0.0
    # _acted_on_return_to_human: True once this session has already sent SIGUSR1.
    # Set True at startup when signal was pre-existing so we skip the SIGUSR1 burst.
    _acted_on_return_to_human = cached_return_to_human
    if cached_return_to_human:
        write_target(0)
    CACHE_SECS = 60           # Refresh interval for primary GitHub API data (queue, items)
    CACHE_SECS_SLOW = 120     # Refresh interval for less-critical data (blocked deps, lock, return-to-human)

    # Background fetch infrastructure: all GH/coordination calls run in daemon
    # threads so the TUI never blocks on network I/O.
    _bg_data: dict = {"queue": None, "items": None, "blocked_deps": None, "lock_status": None,
                      "return_to_human": None, "repair_candidates": None}
    _bg_active: set = set()
    _bg_mutex = threading.Lock()

    def _bg_run(key, fn, *args):
        try:
            result = fn(*args)
            with _bg_mutex:
                _bg_data[key] = result
        except Exception:
            pass
        finally:
            with _bg_mutex:
                _bg_active.discard(key)

    def _bg_fetch(key, fn, *args):
        with _bg_mutex:
            if key in _bg_active:
                return
            _bg_active.add(key)
        threading.Thread(target=_bg_run, args=(key, fn) + args, daemon=True).start()

    def _get_lock_status_str():
        r = coordination(config, "lock-status")
        return r.stdout.strip()

    last_auto_spawn_time = 0.0  # Timestamp of last TUI-initiated auto-spawn
    auto_spawn_failures = 0     # Consecutive auto-spawns where agents died instantly
    auto_spawn_paused = False   # True when crash-loop detected

    show_costs = cfg_get(config, "monitor", "show_costs", default=False)

    # Compute all-time historical cost once at startup
    claude_config_dir = get_isolated_config_dir(config)
    historical_cost = compute_historical_cost(config, claude_config_dir) if show_costs else 0.0
    # Track session-accumulated costs (persists across agent deaths)
    session_agent_costs: dict[str, float] = {}  # agent short_id -> last known cost
    # Accumulated cost from previous iterations (when token counters reset)
    session_cost_offsets: dict[str, float] = {}  # agent short_id -> sum of prior iterations' costs
    # Baseline costs for agents already running when pod started (to avoid double-counting)
    baseline_costs: dict[str, float] = {}
    for a in read_all_agents():
        if a.status not in ("stopped", "dead"):
            baseline_costs[a.short_id] = a.cost(config)

    while True:
        agents = read_all_agents()
        # Accumulate costs from all agents (including dying ones) before cleanup
        for a in agents:
            current = a.cost(config)
            prev = session_agent_costs.get(a.short_id)
            if prev is not None and current < prev:
                # Token counters were reset (new loop iteration) — accumulate previous cost
                session_cost_offsets[a.short_id] = session_cost_offsets.get(a.short_id, 0.0) + prev
            session_agent_costs[a.short_id] = current
        # Clean up dead agent state files
        for a in agents:
            if a.status == "dead":
                a.remove_file()
        agents = [a for a in agents if a.status != "dead"]

        height, width = stdscr.getmaxyx()
        stdscr.erase()

        # --- Header ---
        # session_cost = delta from agents already running at startup + full cost of new agents
        session_cost = sum(
            session_cost_offsets.get(sid, 0.0) + cost - baseline_costs.get(sid, 0.0)
            for sid, cost in session_agent_costs.items()
        )
        session_runs = sum(1 for sid in session_agent_costs if sid not in baseline_costs)
        running = sum(1 for a in agents if a.status not in ("stopped", "dead"))

        # Kick off background refreshes (non-blocking) and read latest results.
        now = time.time()
        if cached_queue is None or now - queue_fetch_time > CACHE_SECS:
            _bg_fetch("queue", get_queue_depth, config)
            queue_fetch_time = now
        if now - items_fetch_time > CACHE_SECS:
            _bg_fetch("items", fetch_issues_and_prs)
            items_fetch_time = now
        if now - blocked_deps_fetch_time > CACHE_SECS_SLOW:
            _bg_fetch("blocked_deps", fetch_blocked_deps)
            blocked_deps_fetch_time = now
        # Return-to-human signal: planner labels sentinel issue when no work remains.
        if not _acted_on_return_to_human and now - return_to_human_fetch_time > CACHE_SECS:
            _bg_fetch("return_to_human", get_return_to_human, config)
            return_to_human_fetch_time = now
        # Repair candidates: count of PRs needing repair. Used by the
        # auto-spawn override to expand `desired` beyond `target` when
        # unhealthy PRs are present.
        if repair_configured and now - repair_candidates_fetch_time > CACHE_SECS_SLOW:
            _bg_fetch("repair_candidates", _list_pr_repair_count, config)
            repair_candidates_fetch_time = now
        # Lock status: check agent state first (cheap), fall back to background API
        if now - lock_fetch_time > CACHE_SECS_SLOW:
            lock_fetch_time = now
            agent_holds_lock = any(a.lock_held for a in agents
                                  if a.status not in ("stopped", "dead"))
            if agent_holds_lock:
                with _bg_mutex:
                    _bg_data["lock_status"] = "locked"
            else:
                _bg_fetch("lock_status", _get_lock_status_str)
        # Apply any completed background results to cached display values
        with _bg_mutex:
            if _bg_data["queue"] is not None:
                cached_queue = _bg_data["queue"]
            if _bg_data["items"] is not None:
                cached_items = _bg_data["items"]
            if _bg_data["blocked_deps"] is not None:
                cached_blocked_deps = _bg_data["blocked_deps"]
            if _bg_data["lock_status"] is not None:
                cached_lock_status = _bg_data["lock_status"]
            if _bg_data["return_to_human"] is not None:
                cached_return_to_human = _bg_data["return_to_human"]
            if _bg_data["repair_candidates"] is not None:
                cached_repair_candidates = _bg_data["repair_candidates"]

        # React to return-to-human signal: set target=0 and gracefully finish all agents.
        # Only act once per TUI session (idempotent).
        if cached_return_to_human and not _acted_on_return_to_human:
            _acted_on_return_to_human = True
            write_target(0)
            for a in agents:
                if a.pid > 0 and a.status not in ("stopped", "dead") and _pid_is_valid(a.pid, a.pid_start_time):
                    try:
                        os.kill(a.pid, signal.SIGUSR1)
                    except (ProcessLookupError, OSError):
                        pass
            message = "Return-to-human: planner found no remaining work. Target set to 0; agents finishing."
            message_time = now

        # Auto-spawn agents to maintain target (only if no agents are finishing).
        # Repair overflow: if there are PR repair candidates with spare
        # concurrency, expand `desired` beyond `target` so a repair agent can
        # spawn even when `planner_target` is 0. All spawns go through the
        # same crash-loop guard below.
        target = get_effective_target()
        if target is None:
            desired = 0
        else:
            desired = target
            if repair_configured:
                running_repair = _count_running_repair_agents(agents)
                cap = cfg_get(config, "repair", "concurrency_cap", default=2)
                repair_overflow = max(
                    0,
                    min(cached_repair_candidates - running_repair, cap - running_repair),
                )
                if repair_overflow > 0:
                    desired = max(desired, running + repair_overflow)
        if target is not None and running < desired and now - last_auto_spawn_time > 5.0:
            finishing_count = sum(1 for a in agents if a.status == "finishing")
            if finishing_count == 0:
                if auto_spawn_paused:
                    pass  # Crash-loop detected; suppress auto-spawn (user can press 'a')
                else:
                    # Detect crash-loop: if we spawned recently and all agents are already gone,
                    # they died instantly.
                    if last_auto_spawn_time > 0 and running == 0:
                        auto_spawn_failures += 1
                    else:
                        auto_spawn_failures = 0
                    if auto_spawn_failures >= 3:
                        log("Auto-spawn crash-loop detected (3 consecutive rapid deaths). "
                            "Pausing auto-spawn. Press 'a' to retry.")
                        auto_spawn_paused = True
                    else:
                        n_to_spawn = desired - running
                        for _ in range(n_to_spawn):
                            spawn_agent(config)
                        last_auto_spawn_time = now

        lock_indicator = ""
        if cached_lock_status == "locked":
            lock_indicator = " | LOCK"
        if FORCE_QUOTA_FILE.exists():
            lock_indicator += " | FORCE"
        effective = target  # already computed via get_effective_target() above
        user_t = read_target()
        if effective is not None and user_t is not None and effective < user_t:
            agent_str = f"{running}/{effective}({user_t})"
        else:
            agent_str = f"{running}/{effective}" if effective is not None else str(running)
        agent_word = "agent" if (effective or running) == 1 else "agents"
        if show_costs:
            all_time = historical_cost + session_cost
            session_info = f"${session_cost:.2f} this session, {session_runs} run{'s' if session_runs != 1 else ''}"
            cost_str = f" | ${all_time:.2f} total ({session_info})"
        else:
            cost_str = ""
        if cached_queue is not None:
            header = f" pod -- {agent_str} {agent_word} running | queue: {cached_queue}{lock_indicator}{cost_str}"
        else:
            header = f" pod -- {agent_str} {agent_word} running{lock_indicator}{cost_str}"

        _addstr(stdscr, 0, 0, header[:width], curses.color_pair(4) | curses.A_BOLD)
        _addstr(stdscr, 1, 0, "─" * min(width, 80), curses.color_pair(4))

        # --- Agent table header ---
        col_fmt = "  {:>2} {:8} {:16} {:>6} {:>15} {}"
        hdr = " " + col_fmt.format("#", "ID", "Type", "Time", "Tokens", "Activity")
        _addstr(stdscr, 2, 0, hdr[:width], curses.A_DIM)

        # --- Clamp agent selection (items clamped after panel renders) ---
        if selected_section == "agents":
            if not agents:
                selected_idx = 0
            else:
                selected_idx = min(selected_idx, len(agents) - 1)

        is_agent_selected = selected_section == "agents"

        # --- Agent rows ---
        agents_rendered = 0
        for i, agent in enumerate(agents):
            row = 3 + i
            if row >= height - 3:
                break
            agents_rendered = i + 1

            # Mode label
            mode = agent.worker_type or "???"
            if agent.claimed_issue > 0:
                if agent.pr_number > 0:
                    mode = f"{mode} #{agent.claimed_issue}->PR"
                else:
                    mode = f"{mode} #{agent.claimed_issue}"
            if agent.finishing:
                mode += " (fin)"
            if agent.force_quota:
                mode += " !"

            # Elapsed
            elapsed = ""
            if agent.session_start > 0:
                elapsed = human_duration(time.time() - agent.session_start)

            # Tokens
            tok = token_summary(agent, config, show_costs)

            # Activity text: for non-running states show status, not stale last_text
            if agent.status in ("running", "finishing"):
                activity = agent.last_text or agent.status
            else:
                activity = agent.status
            # Thinking detection: if JSONL is stale but process is alive
            if (agent.last_activity > 0 and
                    time.time() - agent.last_activity > 10 and
                    agent.status == "running"):
                stale = int(time.time() - agent.last_activity)
                activity = f"thinking {human_duration(stale)}"

            # Sanitize: collapse newlines/tabs to spaces
            activity = " ".join(activity.split())

            # Truncate activity to fit (1 extra for marker char)
            prefix = col_fmt.format(i + 1, agent.short_id, mode[:16], elapsed, tok, "")
            max_act = width - len(prefix) - 1  # -1 for marker
            if max_act > 10 and len(activity) > max_act:
                activity = activity[:max_act - 3] + "..."

            marker = ">" if is_agent_selected and i == selected_idx else " "
            line = f"{marker}" + col_fmt.format(i + 1, agent.short_id, mode[:16], elapsed, tok, activity)

            attr = curses.A_NORMAL
            if is_agent_selected and i == selected_idx:
                attr = curses.color_pair(5) | curses.A_BOLD
            elif agent.status == "finishing" or agent.finishing:
                attr = curses.color_pair(2)
            elif agent.status in ("dead", "error", "killed"):
                attr = curses.color_pair(3)
            elif agent.status == "running":
                attr = curses.color_pair(1)

            _addstr(stdscr, row, 0, line[:width], attr)

        # --- Issues/PRs panel ---
        # Row budget: 3 top (header+sep+colhdr) + rendered agents + 3 bottom (sep+help+msg)
        agents_end = 3 + agents_rendered
        footer_fixed = 4 if cached_return_to_human else 3  # separator + help + message [+ banner]
        avail_for_items = height - agents_end - footer_fixed
        # Need: 1 blank + 1 separator + 1 header + at least 1 item row = 4
        items_shown = 0
        items_start_row = 0
        displayed_items = []  # reset each frame
        if cached_items and avail_for_items >= 4:
            max_item_rows = avail_for_items - 3  # blank + separator + header
            # Open items are always shown. Closed/merged fill remaining slots
            # (newest first), and are the first to go when space is tight.
            active = [it for it in cached_items if it.state not in ("closed", "merged")]
            inactive = [it for it in cached_items if it.state in ("closed", "merged")]
            slots_for_inactive = max(0, max_item_rows - len(active))
            # inactive is already sorted newest-first; take from the front
            selected = active + inactive[:slots_for_inactive]
            # Restore original sort order across the combined set
            selected_nums = {id(it) for it in selected}
            items_to_show = [it for it in cached_items if id(it) in selected_nums]
            displayed_items = items_to_show
            items_shown = len(items_to_show)

            items_start_row = agents_end + 1  # skip 1 blank line
            _addstr(stdscr, items_start_row, 0, "─" * min(width, 80), curses.color_pair(4))
            item_fmt = "  {:<6} {:<10} {:<8}  {}"
            item_hdr = " " + item_fmt.format("#", "State", "When", "Title")
            _addstr(stdscr, items_start_row + 1, 0, item_hdr[:width], curses.A_DIM)

            for j, item in enumerate(items_to_show):
                irow = items_start_row + 2 + j
                if irow >= height - footer_fixed:
                    items_shown = j
                    break

                # Unified state: combines kind, labels, CI, and state
                if item.kind == "issue":
                    if item.state == "closed":
                        state_str, state_color = "closed", curses.color_pair(4)
                    elif "claimed" in item.labels:
                        state_str, state_color = "claimed", curses.color_pair(1)
                    elif "has-pr" in item.labels:
                        state_str, state_color = "has-pr", curses.color_pair(6)
                    elif "blocked" in item.labels:
                        state_str, state_color = "blocked", curses.color_pair(2)
                    elif "replan" in item.labels:
                        state_str, state_color = "replan", curses.color_pair(2)
                    else:
                        state_str, state_color = "open", curses.A_NORMAL
                else:  # pr
                    if item.state == "merged":
                        state_str, state_color = "merged", curses.color_pair(4)
                    elif item.state == "closed":
                        state_str, state_color = "closed", curses.color_pair(4)
                    elif item.ci_status == "fail":
                        state_str, state_color = "failing", curses.color_pair(3)
                    else:
                        state_str, state_color = "open", curses.color_pair(6)

                age = timeago(item.timestamp)
                kind_prefix = "PR" if item.kind == "pr" else "I"
                state_display = f"{kind_prefix} {state_str}"

                title = item.title
                if "blocked" in item.labels and item.number in cached_blocked_deps:
                    deps = cached_blocked_deps[item.number]
                    title = f"[Blocked on {', '.join(f'#{d}' for d in deps)}] {title}"
                # Truncate title to fit
                prefix_len = 31
                max_title = width - prefix_len - 1
                if max_title > 10 and len(title) > max_title:
                    title = title[:max_title - 3] + "..."

                is_item_sel = not is_agent_selected and j == selected_idx
                marker = ">" if is_item_sel else " "
                num_str = f"#{item.number}"
                line = marker + item_fmt.format(num_str, state_display, age, title)

                attr = state_color
                if is_item_sel:
                    attr = curses.color_pair(5) | curses.A_BOLD

                _addstr(stdscr, irow, 0, line[:width], attr)

        # --- Clamp items selection (now that items_shown is known) ---
        if selected_section == "items":
            if items_shown == 0:
                selected_section = "agents"
                selected_idx = max(0, agents_rendered - 1)
                is_agent_selected = True
            else:
                selected_idx = min(selected_idx, items_shown - 1)

        # --- Footer separator ---
        footer_row = max(agents_end, height - footer_fixed)
        if items_shown > 0:
            footer_row = max(items_start_row + 2 + items_shown, height - footer_fixed)
        if footer_row < height - 1:
            _addstr(stdscr, footer_row, 0, "─" * min(width, 80), curses.color_pair(4))

        # --- Footer ---
        footer_row2 = footer_row + 1
        if input_mode == "kill_confirm":
            if agents and is_agent_selected and 0 <= selected_idx < len(agents):
                footer_text = f" Kill agent {agents[selected_idx].short_id}? (y/n)"
            else:
                footer_text = " No agent selected"
                input_mode = ""
        else:
            footer_text = " [a]dd  [f]inish  [k]ill  [o]pen  [!]force  [F]orce-all  [L]ock  [q]uit  [Q]uit all  ↑↓/1-9"
            if cached_return_to_human:
                footer_text = " [r]esume work  " + footer_text.lstrip()

        if footer_row2 < height:
            _addstr(stdscr, footer_row2, 0, footer_text[:width], curses.A_DIM)

        # --- Message line ---
        msg_active = bool(message and time.time() - message_time < 3)
        msg_row = footer_row2 + 1
        if msg_active and msg_row < height:
            _addstr(stdscr, msg_row, 0, f" {message}"[:width], curses.A_BOLD)

        # --- Return-to-human banner (persistent) ---
        if cached_return_to_human:
            banner_row = msg_row + 1 if msg_active else msg_row
            if banner_row < height:
                banner = " *** return-to-human: planner found no remaining work — target=0, agents finishing ***"
                _addstr(stdscr, banner_row, 0, banner[:width], curses.color_pair(2) | curses.A_BOLD)

        stdscr.refresh()

        # --- Input ---
        try:
            ch = stdscr.getch()
        except curses.error:
            continue

        if ch == -1:
            continue

        if input_mode == "kill_confirm":
            if ch in (ord("y"), ord("Y")):
                if agents and is_agent_selected and 0 <= selected_idx < len(agents):
                    agent = agents[selected_idx]
                    _kill_agent(config, agent)
                    message = f"Killed agent {agent.short_id}"
                    message_time = time.time()
                    cur_target = read_target()
                    if cur_target is not None and cur_target > 0:
                        write_target(cur_target - 1)
            else:
                message = "Kill cancelled"
                message_time = time.time()
            input_mode = ""
            continue

        # Normal mode
        if ch == ord("q"):
            break
        elif ch == ord("Q"):
            # Quit all: finish all agents, then wait
            write_target(0)  # Don't auto-respawn after quitting all
            for a in agents:
                if a.pid > 0 and a.status not in ("stopped", "dead") and _pid_is_valid(a.pid, a.pid_start_time):
                    try:
                        os.kill(a.pid, signal.SIGUSR1)
                    except (ProcessLookupError, OSError):
                        pass
            message = "Sent finish signal to all agents. Waiting..."
            message_time = time.time()
            stdscr.refresh()
            # Wait for all to stop (with timeout display)
            deadline = time.time() + 600  # 10 min max
            while time.time() < deadline:
                live = [a for a in read_all_agents() if a.status not in ("stopped", "dead")]
                if not live:
                    break
                time.sleep(2)
            break
        elif ch == ord("a") or ch == ord("A"):
            spawn_agent(config)
            cur_target = read_target()
            write_target((cur_target or running) + 1)
            # Clear planner advisories so user intent takes immediate effect
            PLANNER_TARGET_FILE.unlink(missing_ok=True)
            PLANNER_MIN_QUEUE_FILE.unlink(missing_ok=True)
            last_auto_spawn_time = time.time()
            auto_spawn_failures = 0
            auto_spawn_paused = False
            message = "Launched 1 agent"
            message_time = time.time()
        elif ch == ord("f"):
            if is_agent_selected and agents and 0 <= selected_idx < len(agents):
                agent = agents[selected_idx]
                if agent.pid > 0 and _pid_is_valid(agent.pid, agent.pid_start_time):
                    try:
                        os.kill(agent.pid, signal.SIGUSR1)
                        message = f"Finish signal sent to {agent.short_id}"
                        cur_target = read_target()
                        if cur_target is not None and cur_target > 0:
                            write_target(cur_target - 1)
                    except (ProcessLookupError, OSError):
                        message = f"Agent {agent.short_id} not running"
                else:
                    message = f"Agent {agent.short_id} not running"
                message_time = time.time()
        elif ch == ord("k") or ch == ord("K"):
            if is_agent_selected and agents and 0 <= selected_idx < len(agents):
                input_mode = "kill_confirm"
        elif ch == ord("o") or ch == ord("O"):
            if is_agent_selected:
                # Open agent's claimed issue
                if agents and 0 <= selected_idx < len(agents):
                    agent = agents[selected_idx]
                    issue = agent.claimed_issue
                    if issue > 0:
                        try:
                            subprocess.Popen(
                                ["gh", "issue", "view", str(issue), "--web"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                cwd=str(PROJECT_DIR),
                            )
                            message = f"Opening issue #{issue}"
                        except OSError as e:
                            message = f"Failed to open issue: {e}"
                    else:
                        message = f"Agent {agent.short_id} has no claimed issue"
                    message_time = time.time()
            else:
                # Open selected issue/PR
                if displayed_items and 0 <= selected_idx < len(displayed_items):
                    item = displayed_items[selected_idx]
                    gh_cmd = "issue" if item.kind == "issue" else "pr"
                    try:
                        subprocess.Popen(
                            ["gh", gh_cmd, "view", str(item.number), "--web"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            cwd=str(PROJECT_DIR),
                        )
                        message = f"Opening {item.kind} #{item.number}"
                    except OSError as e:
                        message = f"Failed to open: {e}"
                    message_time = time.time()
        elif ch == ord("!"):
            if is_agent_selected and agents and 0 <= selected_idx < len(agents):
                agent = agents[selected_idx]
                # Toggle force_quota in the agent's state file
                try:
                    sf = AGENTS_DIR / f"{agent.short_id}.json"
                    d = json.loads(sf.read_text())
                    new_val = not d.get("force_quota", False)
                    d["force_quota"] = new_val
                    tmp = sf.with_suffix(".tmp")
                    tmp.write_text(json.dumps(d, indent=2) + "\n")
                    tmp.rename(sf)
                    label = "ON" if new_val else "OFF"
                    message = f"Force quota {label} for {agent.short_id}"
                except (OSError, json.JSONDecodeError) as e:
                    message = f"Failed to toggle force: {e}"
                message_time = time.time()
        elif ch == ord("F"):
            # Toggle global force-quota override
            if FORCE_QUOTA_FILE.exists():
                FORCE_QUOTA_FILE.unlink()
                message = "Global force-quota OFF"
            else:
                FORCE_QUOTA_FILE.write_text("")
                message = "Global force-quota ON (all agents skip quota checks)"
            message_time = time.time()
        elif ch == ord("r") or ch == ord("R"):
            # Clear return-to-human signal and resume normal operation
            if cached_return_to_human:
                try:
                    clear_return_to_human(config)
                    cached_return_to_human = False
                    _acted_on_return_to_human = False
                    message = "Return-to-human signal cleared. Press [a] to add an agent."
                except Exception as e:
                    message = f"Failed to clear signal: {e}"
                message_time = time.time()
        elif ch == ord("l") or ch == ord("L"):
            # Toggle planner lock
            try:
                if cached_lock_status == "locked":
                    coordination(config, "force-unlock-planner")
                    cached_lock_status = "unlocked"
                    lock_fetch_time = time.time()
                    message = "Planner lock released"
                else:
                    r = coordination(config, "lock-planner")
                    if r.returncode == 0:
                        cached_lock_status = "locked"
                        lock_fetch_time = time.time()
                        message = "Planner lock acquired"
                    else:
                        message = "Failed to acquire planner lock"
            except Exception as e:
                message = f"Lock toggle failed: {e}"
            message_time = time.time()
        elif ch == curses.KEY_UP:
            if selected_section == "agents":
                if selected_idx > 0:
                    selected_idx -= 1
                # At top of agents — stay (nowhere to go)
            else:
                if selected_idx > 0:
                    selected_idx -= 1
                elif agents:
                    # Jump to last agent
                    selected_section = "agents"
                    selected_idx = len(agents) - 1
        elif ch == curses.KEY_DOWN:
            if selected_section == "agents":
                if agents and selected_idx < len(agents) - 1:
                    selected_idx += 1
                elif items_shown > 0:
                    # Jump to first item
                    selected_section = "items"
                    selected_idx = 0
            else:
                if selected_idx < items_shown - 1:
                    selected_idx += 1
        elif 49 <= ch <= 57:  # 1-9
            idx = ch - 49
            if idx < len(agents):
                selected_section = "agents"
                selected_idx = idx


def _addstr(win, y: int, x: int, s: str, attr: int = 0):
    """Safe addstr that doesn't crash on edge writes."""
    try:
        win.addstr(y, x, s, attr)
    except curses.error:
        pass


def _kill_agent(config: dict, agent: AgentState):
    """Send SIGTERM to an agent process."""
    if agent.pid > 0 and _pid_is_valid(agent.pid, agent.pid_start_time):
        try:
            os.kill(agent.pid, signal.SIGTERM)
        except (ProcessLookupError, OSError):
            # Process already dead — clean up manually
            if agent.claimed_issue > 0 and agent.pr_number == 0:
                try:
                    coordination(
                        config, "skip", str(agent.claimed_issue),
                        f"Agent killed by operator (session {agent.uuid})",
                        env_extra={"POD_SESSION_ID": agent.uuid},
                    )
                except Exception:
                    pass
            if agent.lock_held:
                try:
                    coordination(config, f"unlock-{agent.lock_held}")
                except Exception:
                    pass
            if agent.worktree and agent.branch:
                cleanup_worktree(agent.worktree, agent.branch)
            agent.remove_file()


# ---------------------------------------------------------------------------
# CLI Commands
# ---------------------------------------------------------------------------

def cmd_list(config: dict, args):
    """Show running agents."""
    agents = read_all_agents()

    if not agents:
        print("No agents running.")
        return

    alive = [a for a in agents if a.status != "dead"]
    dead = [a for a in agents if a.status == "dead"]

    # Clean up dead state files
    for a in dead:
        a.remove_file()

    if not alive:
        print("No agents running.")
        return

    fmt = "{:>2}  {:8}  {:16}  {:>6}  {:>15}  {}"
    print(fmt.format("#", "ID", "Type", "Time", "Tokens", "Status"))
    print("─" * 74)
    for i, a in enumerate(alive):
        mode = a.worker_type or "???"
        if a.claimed_issue > 0:
            if a.pr_number > 0:
                mode = f"{mode} #{a.claimed_issue}->PR"
            else:
                mode = f"{mode} #{a.claimed_issue}"
        if a.finishing:
            mode += " (fin)"

        elapsed = human_duration(time.time() - a.session_start) if a.session_start > 0 else ""
        tok = token_summary(a, config)
        if a.status in ("running", "finishing"):
            activity = a.last_text or a.status
        else:
            activity = a.status

        print(fmt.format(i + 1, a.short_id, mode[:16], elapsed, tok, activity[:40]))


def cmd_add(config: dict, args):
    """Launch new agents and update the target count."""
    n = args.count if args.count else 1
    for _ in range(n):
        pid = spawn_agent(config)
        say(f"Launched agent (PID {pid})")
    alive = len([a for a in read_all_agents() if a.status not in ("stopped", "dead")])
    cur_target = read_target()
    new_target = max(alive, (cur_target or 0) + n)
    write_target(new_target)
    print(f"Launched {n} agent{'s' if n != 1 else ''}. Target: {new_target}.")


def cmd_finish(config: dict, args):
    """Signal agent(s) to finish after current work."""
    agents = read_all_agents()
    alive = [a for a in agents if a.status not in ("stopped", "dead")]

    if args.target == "all":
        targets = alive
    else:
        targets = [a for a in alive if a.short_id.startswith(args.target)]
        if not targets:
            print(f"No running agent matching '{args.target}'")
            return

    signaled = 0
    for a in targets:
        if not _pid_is_valid(a.pid, a.pid_start_time):
            print(f"Agent {a.short_id} not running (PID {a.pid})")
            continue
        try:
            os.kill(a.pid, signal.SIGUSR1)
            print(f"Finish signal sent to {a.short_id} (PID {a.pid})")
            signaled += 1
        except (ProcessLookupError, OSError):
            print(f"Agent {a.short_id} not running (PID {a.pid})")

    # Decrement target so auto-spawn doesn't immediately refill the pool.
    cur_target = read_target()
    if cur_target is not None and signaled > 0:
        write_target(max(0, cur_target - signaled))


def cmd_kill(config: dict, args):
    """Kill agent(s) immediately."""
    agents = read_all_agents()
    alive = [a for a in agents if a.status not in ("stopped", "dead")]

    if args.target == "all":
        targets = alive
    else:
        targets = [a for a in alive if a.short_id.startswith(args.target)]
        if not targets:
            print(f"No running agent matching '{args.target}'")
            return

    killed = 0
    for a in targets:
        _kill_agent(config, a)
        print(f"Killed {a.short_id} (PID {a.pid})")
        killed += 1

    # Decrement target so auto-spawn doesn't immediately refill the pool.
    cur_target = read_target()
    if cur_target is not None and killed > 0:
        write_target(max(0, cur_target - killed))


def cmd_status(config: dict, args):
    """Show aggregate status."""
    agents = read_all_agents()
    alive = [a for a in agents if a.status not in ("stopped", "dead")]
    show_costs = cfg_get(config, "monitor", "show_costs", default=False)

    total_in = sum(a.tokens_in + a.cache_read + a.cache_create for a in alive)
    total_out = sum(a.tokens_out for a in alive)

    try:
        queue = get_queue_depth(config)
        print(f"Queue depth:    {queue}")
    except Exception:
        print("Queue depth:    (unavailable)")

    print(f"Running agents: {len(alive)}")

    types = {}
    for a in alive:
        t = a.worker_type or "unknown"
        types[t] = types.get(t, 0) + 1
    if types:
        parts = [f"{v} {k}" for k, v in types.items()]
        print(f"  Breakdown:    {', '.join(parts)}")

    print(f"Total tokens:   {fmt_tokens(total_in)} in / {fmt_tokens(total_out)} out")
    if show_costs:
        session_cost = sum(a.cost(config) for a in alive)
        claude_config_dir = get_isolated_config_dir(config)
        historical_cost = compute_historical_cost(config, claude_config_dir)
        print(f"Running cost:   ${session_cost:.2f}")
        print(f"All-time cost:  ${historical_cost:.2f}")


def cmd_log(config: dict, args):
    """Tail agent's session stdout."""
    agents = read_all_agents()
    if args.target:
        matches = [a for a in agents if a.short_id.startswith(args.target)]
    else:
        # Default to most recent agent
        matches = sorted(agents, key=lambda a: a.session_start, reverse=True)[:1]

    if not matches:
        print("No matching agent found.")
        return

    agent = matches[0]
    session_dir = PROJECT_DIR / cfg_get(config, "project", "session_dir", default="sessions")
    stdout_path = session_dir / f"{agent.uuid}.stdout"
    if not stdout_path.exists():
        print(f"No log file for agent {agent.short_id}")
        return

    # Print last 50 lines
    lines = stdout_path.read_text().splitlines()
    for line in lines[-50:]:
        print(line)


def cmd_config(config: dict, args):
    """Print current configuration."""
    if args.edit:
        editor = os.environ.get("EDITOR", "vi")
        os.execlp(editor, editor, str(CONFIG_PATH))
    else:
        print(CONFIG_PATH.read_text(), end="")


def cmd_cleanup(config: dict, args):
    """Remove stale worktrees that no running agent owns."""
    base = PROJECT_DIR / cfg_get(config, "project", "worktree_base", default="worktrees")
    if not base.is_dir():
        print("No worktrees directory found.")
        return

    # Show current state
    all_dirs = [d for d in base.iterdir() if d.is_dir()]
    live_worktrees: set[str] = set()
    for agent in read_all_agents():
        if agent.worktree and agent.status != "dead":
            live_worktrees.add(agent.worktree)

    now = time.time()
    min_age = 0 if args.force else CLEANUP_MIN_AGE_SECONDS
    stale = []
    skipped_young = 0
    skipped_non_pod = 0
    for d in sorted(all_dirs):
        if str(d) in live_worktrees:
            continue
        if not args.force:
            try:
                age = now - d.stat().st_mtime
            except OSError:
                continue
            if age < min_age:
                skipped_young += 1
                continue
        if not _is_pod_owned_worktree(d):
            skipped_non_pod += 1
            continue
        stale.append(d)

    if not stale:
        parts = [f"All {len(all_dirs)} worktrees are owned by running agents"]
        if skipped_young:
            parts.append(f"{skipped_young} skipped (too young, use --force)")
        if skipped_non_pod:
            parts.append(f"{skipped_non_pod} skipped (not pod-owned)")
        print(". ".join(parts) + ".")
        return

    print(f"Found {len(stale)} stale worktrees (of {len(all_dirs)} total).")
    if skipped_young:
        print(f"  {skipped_young} skipped (younger than {CLEANUP_MIN_AGE_SECONDS}s, use --force)")
    if skipped_non_pod:
        print(f"  {skipped_non_pod} skipped (not pod-owned)")

    if args.dry_run:
        for d in stale:
            try:
                age = now - d.stat().st_mtime
            except OSError:
                age = 0
            print(f"  would remove: {d.name}  (age {age:.0f}s)")
        print(f"Dry run: {len(stale)} worktrees would be removed.")
        return

    removed = cleanup_stale_worktrees(config, verbose=True, force=args.force)
    print(f"Removed {removed} stale worktrees.")


# ---------------------------------------------------------------------------
# Init / Update / Coordination subcommands
# ---------------------------------------------------------------------------


def _populate_agent_config():
    """Copy bundled agent-config/claude into .pod/claude-config/.

    Only copies credentials/settings files — commands/ and skills/ are
    installed directly into each worktree's .claude/ by install_agent_config,
    not into the isolated config dir.
    """
    src = _data_dir() / "agent-config" / "claude"
    dst = ISOLATED_CONFIG_DIR
    dst.mkdir(parents=True, exist_ok=True)
    EXCLUDE = {"commands", "skills", "CLAUDE.md"}
    for item in src.iterdir():
        if item.name in EXCLUDE:
            continue
        d = dst / item.name
        if item.is_dir():
            if d.exists():
                shutil.rmtree(d)
            shutil.copytree(str(item), str(d))
        else:
            shutil.copy2(str(item), str(d))


REQUIRED_LABELS = {
    "agent-plan": "1D76DB",
    "claimed": "FBCA04",
    "blocked": "B60205",
    "has-pr": "5319E7",
    "replan": "D93F0B",
    "coordination": "0E8A16",
    "critical-path": "E11D48",
    "repair-claimed": "7057FF",   # PR is claimed by a repair agent
    "unsalvageable": "5C5C5C",    # PR closed by a repair agent as unsalvageable
}


def _ensure_github_labels():
    """Create any missing GitHub labels required by pod."""
    try:
        r = subprocess.run(
            ["gh", "label", "list", "--limit", "100", "--json", "name", "--jq", ".[].name"],
            capture_output=True, text=True, timeout=15,
            cwd=str(PROJECT_DIR),
        )
        if r.returncode != 0:
            print("  warning: could not list labels (gh CLI issue)")
            return
        existing = set(r.stdout.strip().splitlines())
        created = []
        for label, color in REQUIRED_LABELS.items():
            if label not in existing:
                cr = subprocess.run(
                    ["gh", "label", "create", label, "--color", color,
                     "--description", "pod coordination"],
                    capture_output=True, text=True, timeout=15,
                    cwd=str(PROJECT_DIR),
                )
                if cr.returncode == 0:
                    created.append(label)
        if created:
            print(f"  created GitHub labels: {', '.join(created)}")
        else:
            print("  GitHub labels already exist")
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("  warning: could not ensure GitHub labels (gh CLI not available)")


def _ensure_repo_merge_settings():
    """Enable auto-merge and squash merge on the GitHub repo (best effort)."""
    try:
        r = subprocess.run(
            ["gh", "repo", "edit", "--enable-auto-merge", "--enable-squash-merge"],
            capture_output=True, text=True, timeout=15,
            cwd=str(PROJECT_DIR),
        )
        if r.returncode == 0:
            print("  enabled auto-merge and squash merge on GitHub repo")
        else:
            print("  warning: could not enable auto-merge/squash-merge "
                  "(may need admin access — enable manually in repo Settings → General)")
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("  warning: could not configure repo merge settings (gh CLI not available)")


def _ensure_branch_protection(init_config):
    """Configure branch protection on the default branch (best effort).

    Required for `gh pr merge --auto` to actually queue PRs — without a
    required status check on the default branch, --auto silently no-ops
    and green PRs sit unmerged.
    """
    required = init_config.get("merge", {}).get("required_checks", []) or []
    if not required:
        print("  branch protection: skipped (empty [merge] required_checks)")
        print("    → Auto-merge on PRs will NOT work until you configure this.")
        print("    → Edit .pod/config.toml, set e.g.:")
        print("        [merge]")
        print("        required_checks = [\"build-and-test\"]")
        print("      using the `name:` of your CI job(s) in .github/workflows/,")
        print("      then re-run `pod init`.")
        return

    try:
        r = subprocess.run(
            ["gh", "repo", "view", "--json", "defaultBranchRef",
             "--jq", ".defaultBranchRef.name"],
            capture_output=True, text=True, timeout=15,
            cwd=str(PROJECT_DIR),
        )
        if r.returncode != 0 or not r.stdout.strip():
            print("  warning: could not detect default branch (gh CLI issue)")
            return
        default_branch = r.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("  warning: could not detect default branch (gh CLI not available)")
        return

    payload = {
        "required_status_checks": {
            "strict": False,
            "contexts": list(required),
        },
        "enforce_admins": False,
        "required_pull_request_reviews": None,
        "restrictions": None,
        "allow_force_pushes": False,
        "allow_deletions": False,
    }

    try:
        r = subprocess.run(
            ["gh", "api",
             f"repos/{{owner}}/{{repo}}/branches/{default_branch}/protection",
             "-X", "PUT", "--input", "-"],
            input=json.dumps(payload),
            capture_output=True, text=True, timeout=15,
            cwd=str(PROJECT_DIR),
        )
        if r.returncode == 0:
            print(f"  configured branch protection on `{default_branch}` "
                  f"(required: {', '.join(required)})")
        else:
            print(f"  warning: could not set branch protection on `{default_branch}` "
                  f"(may need admin access)")
            print(f"    → Settings → Branches → add rule for `{default_branch}`")
            print(f"      with required status checks: {', '.join(required)}")
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("  warning: could not configure branch protection (gh CLI not available)")


def cmd_init(args):
    """Bootstrap .pod/ in the current git repo."""
    # Verify we're in a git repo
    r = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                       capture_output=True, text=True, timeout=5)
    if r.returncode != 0:
        print("Not in a git repository.", file=sys.stderr)
        sys.exit(1)
    git_root = Path(r.stdout.strip())
    pod_dir = git_root / ".pod"
    config_path = pod_dir / "config.toml"

    pod_dir.mkdir(parents=True, exist_ok=True)
    (pod_dir / "agents").mkdir(exist_ok=True)

    # .gitignore for .pod/
    gitignore = pod_dir / ".gitignore"
    gitignore.write_text(
        "agents/\n"
        "pod.log\n"
        "claim-history.*\n"
        "claude-config/\n"
        "codex-home/\n"
        "codex-sessions/\n"
        "force-quota\n"
    )

    # Ensure .claude/.pod-checksums is gitignored in the project root
    proj_gitignore = git_root / ".gitignore"
    checksums_pattern = ".claude/.pod-checksums"
    if proj_gitignore.exists():
        existing = proj_gitignore.read_text()
        if checksums_pattern not in existing:
            proj_gitignore.write_text(existing.rstrip("\n") + f"\n{checksums_pattern}\n")
            print(f"  added {checksums_pattern} to .gitignore")
    else:
        proj_gitignore.write_text(f"{checksums_pattern}\n")
        print(f"  created .gitignore with {checksums_pattern}")

    # config.toml
    if not config_path.exists() or getattr(args, "force", False):
        config_path.write_text(DEFAULT_CONFIG)
        print(f"  wrote {config_path.relative_to(git_root)}")
    else:
        print(f"  {config_path.relative_to(git_root)} already exists (use --force to overwrite)")

    # Backend-specific config setup
    # Read backend from the just-written config
    with open(config_path, "rb") as f:
        init_config = tomllib.load(f)
    init_config = _migrate_legacy_config(init_config)
    init_backend = _backend(init_config)

    warning = _instruction_file_warning(git_root, init_backend)
    if warning:
        print(f"  {warning}")

    if init_backend == "claude":
        global ISOLATED_CONFIG_DIR
        ISOLATED_CONFIG_DIR = pod_dir / "claude-config"
        _populate_agent_config()
        print(f"  populated {ISOLATED_CONFIG_DIR.relative_to(git_root)}/")
    else:
        print(f"  {init_backend} backend — agent config installed per-session via CODEX_HOME")

    # Ensure required GitHub labels exist
    _ensure_github_labels()

    # GitHub repo merge settings (required for coordination create-pr auto-merge to work)
    _ensure_repo_merge_settings()

    # Branch protection (also required for --auto on `gh pr merge` to take effect)
    _ensure_branch_protection(init_config)

    print("pod init complete.")


def cmd_update(args):
    """Re-populate agent config from installed package."""
    if not POD_DIR.is_dir():
        print("No .pod/ directory found. Run 'pod init' first.", file=sys.stderr)
        sys.exit(1)
    config = ensure_config()
    backend = _backend(config)
    if backend == "claude":
        _populate_agent_config()
        print("Updated .pod/claude-config/ from installed package.")
    else:
        print(f"{backend} backend — agent config is installed per-session, nothing to update.")


def cmd_coordination(args):
    """Pass-through to bundled coordination script."""
    script = str(_data_dir() / "coordination")
    env = dict(os.environ)
    # Pass protected-files from config if available
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "rb") as f:
            config = tomllib.load(f)
        pf = cfg_get(config, "project", "protected_files", default=["PLAN.md"])
        if isinstance(pf, list):
            pf = list(pf)
        else:
            pf = pf.split(":")
        for rel in _pod_installed_files():
            if rel not in pf:
                pf.append(rel)
        env["POD_PROTECTED_FILES"] = ":".join(pf)
    result = subprocess.run(
        [script] + args.coordination_args,
        cwd=str(PROJECT_DIR), env=env,
    )
    sys.exit(result.returncode)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        prog="pod",
        description="pod — multi-agent manager",
    )
    sub = parser.add_subparsers(dest="command")

    # Subcommands that don't require ensure_config()
    p_init = sub.add_parser("init", help="Bootstrap .pod/ in current git repo")
    p_init.add_argument("--force", action="store_true",
                         help="Overwrite existing config.toml")

    sub.add_parser("update", help="Re-populate .pod/claude-config/ from package")

    p_coord = sub.add_parser("coordination",
                              help="Run bundled coordination script")
    p_coord.add_argument("coordination_args", nargs=argparse.REMAINDER,
                          help="Arguments passed to coordination")

    # Subcommands that require an existing .pod/config.toml
    sub.add_parser("list", help="Show running agents")

    p_add = sub.add_parser("add", help="Launch new agents")
    p_add.add_argument("count", type=int, nargs="?", default=1,
                        help="Number of agents to launch (default: 1)")

    p_target = sub.add_parser("target", help="Set target agent count")
    p_target.add_argument("count", type=int, help="Target number of agents")

    p_finish = sub.add_parser("finish", help="Signal agent to finish current work")
    p_finish.add_argument("target", help="Agent ID prefix or 'all'")

    p_kill = sub.add_parser("kill", help="Kill agent immediately")
    p_kill.add_argument("target", help="Agent ID prefix or 'all'")

    sub.add_parser("status", help="Show aggregate status")
    p_cleanup = sub.add_parser("cleanup", help="Remove stale worktrees not owned by any agent")
    p_cleanup.add_argument("--dry-run", "-n", action="store_true",
                            help="Show what would be removed without deleting")
    p_cleanup.add_argument("--force", "-f", action="store_true",
                            help="Skip age threshold (remove all stale worktrees)")

    p_log = sub.add_parser("log", help="Tail agent session output")
    p_log.add_argument("target", nargs="?", default=None,
                        help="Agent ID prefix (default: most recent)")

    p_config = sub.add_parser("config", help="Show configuration")
    p_config.add_argument("--edit", action="store_true",
                           help="Open config in $EDITOR")

    args = parser.parse_args()

    # Handle subcommands that don't require ensure_config()
    if args.command == "init":
        cmd_init(args)
        return
    elif args.command == "update":
        cmd_update(args)
        return
    elif args.command == "coordination":
        cmd_coordination(args)
        return

    config = ensure_config()

    if args.command is None:
        # No subcommand → TUI
        run_tui(config)
    elif args.command == "list":
        cmd_list(config, args)
    elif args.command == "add":
        cmd_add(config, args)
    elif args.command == "target":
        write_target(args.count)
        # Clear planner advisories so user target takes immediate effect
        PLANNER_TARGET_FILE.unlink(missing_ok=True)
        PLANNER_MIN_QUEUE_FILE.unlink(missing_ok=True)
        print(f"Target set to {args.count} agents (planner advisories cleared).")
    elif args.command == "finish":
        cmd_finish(config, args)
    elif args.command == "kill":
        cmd_kill(config, args)
    elif args.command == "status":
        cmd_status(config, args)
    elif args.command == "cleanup":
        cmd_cleanup(config, args)
    elif args.command == "log":
        cmd_log(config, args)
    elif args.command == "config":
        cmd_config(config, args)


if __name__ == "__main__":
    main()
