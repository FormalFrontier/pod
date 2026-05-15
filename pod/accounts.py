"""Per-agent Claude account leasing, credential mirroring, and candidate
enumeration.

Pod runs multiple agents concurrently on one host. Historically every
agent shared one ``CLAUDE_CONFIG_DIR`` and therefore one active Claude
account: when that account exhausted its weekly Opus quota, every agent
hit ``waiting_quota`` even though sibling accounts had headroom. This
module is the per-agent layer that lets each agent pick — independently
— the best available ``(backend, account, model)`` triple from the
union of all ``~/.claude/credentials*.json`` accounts and Codex.

Three guarantees:

1. **Atomic lease + revalidate.** Account selection happens under a
   host-wide meta-lock (``~/.claude/pod-account-leases/.lock``). The
   chosen account is leased first, then its quota is re-probed; if the
   quota dropped between probe and lease, the lease is released and
   selection continues. Two agents cannot commit to the same account.

2. **Refresh-safe credential mirroring.** Each agent has its own
   isolated keychain entry (keyed by sha256 of its per-agent config
   dir). On lease acquire we mirror the canonical
   ``~/.claude/credentials<N>.json`` *only if* the isolated entry is
   not already fresher. On release we harvest a fresher isolated token
   back to canonical. All credential I/O is serialised by a
   per-account file lock (``~/.claude/.<label>.credentials.lock``) so
   pod, ``swap-account``, and the periodic launchd job interleave
   safely.

3. **Resume pinning.** When an agent resumes a JSONL session the
   account it was originally on is the only valid candidate; mismatch
   would silently fork the conversation or refresh under the wrong
   account.

Nothing in this module imports ``pod.cli`` — cli imports from here.
Logging is injected via ``set_logger`` (no-op by default) so the unit
tests can run without spinning up cli's filesystem layout.
"""

from __future__ import annotations

import contextlib
import dataclasses
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import unicodedata
from pathlib import Path
from typing import Callable, Iterable, Iterator


# --- Logging injection ------------------------------------------------------


def _default_log(msg: str) -> None:  # pragma: no cover - replaced by cli
    pass


_log: Callable[[str], None] = _default_log


def set_logger(fn: Callable[[str], None]) -> None:
    """Install a process-wide logger. cli.py calls this at import time."""
    global _log
    _log = fn


# --- Paths ------------------------------------------------------------------

CLAUDE_DIR = Path.home() / ".claude"
LEASE_DIR = CLAUDE_DIR / "pod-account-leases"
LEASE_META_LOCK = LEASE_DIR / ".lock"


def _credentials_path(account_num: int) -> Path:
    return CLAUDE_DIR / f"credentials{account_num}.json"


def _credential_lock_path(label: str) -> Path:
    """Per-account credential file lock.

    Lives alongside the canonical credential files. Held during read,
    write, keychain mirror, and harvest. Shared with ``swap-account``'s
    harvest path so concurrent invocations interleave safely.
    """
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in label)
    return CLAUDE_DIR / f".{safe}.credentials.lock"


def _lease_path(label: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in label)
    return LEASE_DIR / f"{safe}.lock"


# --- File locks -------------------------------------------------------------


@contextlib.contextmanager
def _flock(path: Path, exclusive: bool = True,
           blocking: bool = True) -> Iterator[bool]:
    """Yield True iff the lock was acquired.

    When ``blocking=False`` and contention is detected, yields False
    without holding the lock — callers should treat that as "skip".
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    fd = open(path)
    op = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    if not blocking:
        op |= fcntl.LOCK_NB
    acquired = False
    try:
        try:
            fcntl.flock(fd, op)
            acquired = True
        except (BlockingIOError, OSError):
            yield False
            return
        yield True
    finally:
        if acquired:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
        fd.close()


@contextlib.contextmanager
def credential_lock(label: str) -> Iterator[None]:
    """Hold the per-account credential lock for the duration of the block.

    Use around any read/write of ``~/.claude/credentials<N>.json`` or
    the corresponding macOS keychain entry. Blocking.
    """
    with _flock(_credential_lock_path(label), exclusive=True, blocking=True):
        yield


# --- Keychain service name --------------------------------------------------


def claude_keychain_service(claude_config_dir: Path | None) -> str:
    """Return the macOS keychain service name Claude Code uses.

    Default ~/.claude dir → ``"Claude Code-credentials"``. A non-default
    ``CLAUDE_CONFIG_DIR`` → ``"Claude Code-credentials-<sha256-hex8>"``
    of the NFC-normalised path. Mirrors @anthropic-ai/claude-code.
    """
    if claude_config_dir is None:
        return "Claude Code-credentials"
    normalized = unicodedata.normalize("NFC", str(claude_config_dir))
    suffix = hashlib.sha256(normalized.encode()).hexdigest()[:8]
    return f"Claude Code-credentials-{suffix}"


# --- Credential helpers -----------------------------------------------------


def _read_credential_blob(path: Path) -> str | None:
    try:
        return path.read_text().strip() or None
    except OSError:
        return None


def _parse_token_meta(blob: str) -> dict:
    """Extract {expiresAt, accessTokenPrefix, accountLabel} from a JSON blob.

    All fields are best-effort; missing/unparseable returns ``{}``.
    """
    try:
        d = json.loads(blob)
    except (TypeError, ValueError):
        return {}
    oauth = d.get("claudeAiOauth") or {}
    out = {}
    if "accountLabel" in d:
        out["accountLabel"] = d["accountLabel"]
    if "accessToken" in oauth:
        out["accessTokenPrefix"] = oauth["accessToken"][:20]
    for key in ("expiresAt", "expires_at"):
        if key in oauth:
            out["expiresAt"] = oauth[key]
            break
    return out


def _keychain_read(service: str) -> str | None:
    """Return the raw credential blob from the macOS keychain, or None."""
    if sys.platform != "darwin":
        return None
    try:
        r = subprocess.run(
            ["security", "find-generic-password",
             "-s", service, "-a", os.getenv("USER", ""), "-w"],
            capture_output=True, text=True, timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None
    if r.returncode != 0:
        return None
    out = (r.stdout or "").strip()
    return out or None


def _keychain_write(service: str, blob: str) -> bool:
    """Write the credential blob to the macOS keychain. True on success."""
    if sys.platform != "darwin":
        return False
    user = os.getenv("USER", "")
    hex_data = blob.encode().hex()
    try:
        subprocess.run(
            ["security", "add-generic-password", "-U",
             "-a", user, "-s", service, "-X", hex_data],
            capture_output=True, timeout=5, check=True,
        )
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
            FileNotFoundError, OSError) as e:
        _log(f"accounts: keychain write to {service} failed: {e}")
        return False


def _keychain_delete(service: str) -> None:
    """Best-effort delete of a keychain entry. Doesn't raise.

    Caller must verify deletion via ``_keychain_read`` if it matters —
    ``security delete-generic-password`` returns non-zero both when the
    entry is missing (already gone) and when deletion was refused, and
    we don't try to disambiguate.
    """
    if sys.platform != "darwin":
        return
    try:
        subprocess.run(
            ["security", "delete-generic-password",
             "-s", service, "-a", os.getenv("USER", "")],
            capture_output=True, timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        pass


def resolve_claude_credential(claude_config_dir: Path | None) -> dict:
    """Resolve the credential Claude Code will actually use at launch time.

    Mirrors Claude Code's lookup order: keychain entry keyed by config
    dir first, then ``.credentials.json`` fallback. Returns
    ``{accountLabel, tokenPrefix, source}``; never raises.
    """
    service = claude_keychain_service(claude_config_dir)
    blob = _keychain_read(service)
    if blob:
        meta = _parse_token_meta(blob)
        return {
            "accountLabel": meta.get("accountLabel", "?"),
            "tokenPrefix": meta.get("accessTokenPrefix", ""),
            "source": f"keychain[{service}]",
        }
    if claude_config_dir is not None:
        creds_path = claude_config_dir / ".credentials.json"
    else:
        creds_path = CLAUDE_DIR / ".credentials.json"
    blob = _read_credential_blob(creds_path)
    if blob:
        meta = _parse_token_meta(blob)
        return {
            "accountLabel": meta.get("accountLabel", "?"),
            "tokenPrefix": meta.get("accessTokenPrefix", ""),
            "source": f"file[{creds_path}]",
        }
    return {"accountLabel": "?", "tokenPrefix": "", "source": "none"}


# --- Account enumeration ----------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Account:
    label: str
    number: int
    path: Path  # ~/.claude/credentials<N>.json


def list_claude_accounts() -> list[Account]:
    """Return all (label, number, path) accounts from ~/.claude/credentialsN.json.

    Sorted by account number. Files that fail to parse or lack an
    ``accountLabel`` are skipped (logged once).
    """
    out: list[Account] = []
    try:
        entries = sorted(CLAUDE_DIR.glob("credentials*.json"))
    except OSError:
        return out
    for path in entries:
        stem = path.stem  # "credentials3"
        num_str = stem[len("credentials"):]
        try:
            num = int(num_str)
        except ValueError:
            continue
        blob = _read_credential_blob(path)
        if not blob:
            continue
        meta = _parse_token_meta(blob)
        label = meta.get("accountLabel")
        if not label or label == "?":
            continue
        out.append(Account(label=label, number=num, path=path))
    return out


# --- Lease layer ------------------------------------------------------------


@dataclasses.dataclass
class Lease:
    label: str
    short_id: str
    pid: int
    project_dir: str
    acquired_at: float

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


class LeaseError(Exception):
    pass


class CredentialMirrorError(Exception):
    """Raised when ``mirror_canonical_to_isolated`` cannot guarantee the
    isolated keychain entry has been updated or removed.

    Claude Code reads the per-config-dir keychain entry before falling
    back to ``.credentials.json``. If we can't overwrite a stale entry
    *and* can't delete it, the agent would silently launch under the
    wrong account. The caller (``acquire_backend``) treats this as a
    hard failure: release the lease, try the next candidate.
    """


def _read_lease(path: Path) -> Lease | None:
    try:
        d = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    try:
        return Lease(
            label=d["label"],
            short_id=d["short_id"],
            pid=int(d.get("pid", 0)),
            project_dir=d.get("project_dir", ""),
            acquired_at=float(d.get("acquired_at", 0.0)),
        )
    except (KeyError, TypeError, ValueError):
        return None


@contextlib.contextmanager
def lease_critical_section() -> Iterator[None]:
    """Acquire the host-wide lease meta-lock.

    Hold across ``try_acquire_lease`` / ``release_lease`` calls so two
    agents can't both create-or-skip the same lease atomically. Blocks.
    """
    LEASE_DIR.mkdir(parents=True, exist_ok=True)
    with _flock(LEASE_META_LOCK, exclusive=True, blocking=True):
        yield


def try_acquire_lease(label: str, short_id: str,
                       project_dir: str = "",
                       pid: int | None = None) -> bool:
    """Attempt to acquire ``label``'s lease for ``short_id``.

    Must be called inside ``lease_critical_section()``. Returns True on
    success, False if another agent holds the lease.
    """
    LEASE_DIR.mkdir(parents=True, exist_ok=True)
    path = _lease_path(label)
    existing = _read_lease(path)
    if existing is not None and existing.short_id != short_id:
        return False
    lease = Lease(
        label=label,
        short_id=short_id,
        pid=pid if pid is not None else os.getpid(),
        project_dir=project_dir,
        acquired_at=time.time(),
    )
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(lease.to_dict(), indent=2) + "\n")
    tmp.rename(path)
    return True


def release_lease(label: str, short_id: str) -> bool:
    """Release ``label``'s lease if held by ``short_id``.

    Safe to call outside the meta-lock. Returns True if a lease was
    removed, False if no lease existed or the owner did not match.
    """
    path = _lease_path(label)
    existing = _read_lease(path)
    if existing is None:
        return False
    if existing.short_id != short_id:
        _log(f"accounts: refusing to release {label} lease held by "
             f"{existing.short_id} (caller={short_id})")
        return False
    try:
        path.unlink()
        return True
    except OSError:
        return False


def list_leases() -> list[Lease]:
    """Return all current leases (label, owner, pid, acquired_at)."""
    out: list[Lease] = []
    if not LEASE_DIR.exists():
        return out
    for path in LEASE_DIR.iterdir():
        if path.name == LEASE_META_LOCK.name or path.suffix == ".tmp":
            continue
        lease = _read_lease(path)
        if lease is not None:
            out.append(lease)
    return out


def evict_orphan_leases(live_short_ids: Iterable[str]) -> list[str]:
    """Remove leases whose owner short_id is not in ``live_short_ids``.

    Returns the labels of evicted leases. Must be called inside
    ``lease_critical_section()``.
    """
    live = set(live_short_ids)
    evicted: list[str] = []
    for lease in list_leases():
        if lease.short_id in live:
            continue
        try:
            _lease_path(lease.label).unlink()
            evicted.append(lease.label)
            _log(f"accounts: evicted orphan lease for {lease.label} "
                 f"(was owned by {lease.short_id})")
        except OSError:
            pass
    return evicted


# --- Credential mirror / harvest --------------------------------------------


def _expires_at(blob: str | None) -> float:
    """Return token expiresAt as a unix timestamp, or 0 if unknown.

    Accepts either an ISO-8601 string or a numeric epoch (ms or s).
    """
    if not blob:
        return 0.0
    meta = _parse_token_meta(blob)
    val = meta.get("expiresAt")
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        # Assume ms if it's "too big" to be seconds-since-epoch in 2026
        return float(val) / 1000.0 if val > 1e11 else float(val)
    if isinstance(val, str):
        try:
            # fromisoformat in 3.11+ handles trailing Z via timespec parser?
            from datetime import datetime
            return datetime.fromisoformat(val.replace("Z", "+00:00")).timestamp()
        except (ValueError, ImportError):
            return 0.0
    return 0.0


def mirror_canonical_to_isolated(label: str, account_num: int,
                                   claude_config_dir: Path) -> bool:
    """Mirror ``~/.claude/credentials<N>.json`` into the agent's keychain
    entry and ``<config_dir>/.credentials.json``.

    Skips the write if the isolated entry's token already looks fresher
    (newer ``expiresAt``). Returns True iff we wrote.

    Held under the per-account credential lock for the duration. Safe
    to call concurrently with ``harvest_isolated_to_canonical`` for
    other agents on the same account because the lock is exclusive
    per-account.

    Raises ``CredentialMirrorError`` if the keychain write fails *and*
    a stale entry persists that we can't delete: Claude Code reads the
    keychain before the file fallback, so a stale entry would silently
    launch the agent under the wrong account.
    """
    with credential_lock(label):
        canonical = _read_credential_blob(_credentials_path(account_num))
        if not canonical:
            return False
        service = claude_keychain_service(claude_config_dir)
        isolated = _keychain_read(service)
        if isolated and _expires_at(isolated) >= _expires_at(canonical):
            # Isolated already has a token at least as fresh; don't clobber.
            return False
        wrote_kc = _keychain_write(service, canonical)
        # Always write the file fallback so non-Darwin platforms work
        # and so a session that bypasses the keychain still loads the
        # right account.
        claude_config_dir.mkdir(parents=True, exist_ok=True)
        cred_file = claude_config_dir / ".credentials.json"
        tmp = cred_file.with_suffix(".tmp")
        tmp.write_text(canonical + ("\n" if not canonical.endswith("\n") else ""))
        try:
            os.chmod(tmp, 0o600)
        except OSError:
            pass
        tmp.rename(cred_file)
        if wrote_kc:
            _log(f"accounts: mirrored canonical {label} → keychain {service}")
            return True
        # Keychain write failed. The file fallback we just wrote is only
        # consulted if the keychain entry is absent; force-clear any
        # stale entry and verify it's gone before reporting success.
        _keychain_delete(service)
        if _keychain_read(service) is not None:
            raise CredentialMirrorError(
                f"keychain write to {service} failed for account {label} "
                f"and stale entry persists; Claude Code would load a "
                f"different credential than {cred_file}")
        _log(f"accounts: keychain write to {service} failed; cleared "
             f"stale entry — Claude Code will use {cred_file.name} fallback")
        return True


def harvest_isolated_to_canonical(label: str, account_num: int,
                                    claude_config_dir: Path) -> bool:
    """If the isolated keychain entry's token is fresher than the
    canonical ``credentials<N>.json``, write it back atomically.

    Returns True iff canonical was updated. Held under the per-account
    credential lock for the duration.
    """
    with credential_lock(label):
        service = claude_keychain_service(claude_config_dir)
        isolated = _keychain_read(service)
        if not isolated:
            return False
        canonical_path = _credentials_path(account_num)
        canonical = _read_credential_blob(canonical_path)
        if canonical and _expires_at(canonical) >= _expires_at(isolated):
            return False
        # Validate JSON before overwriting.
        try:
            json.loads(isolated)
        except ValueError:
            _log(f"accounts: refusing to harvest non-JSON blob for {label}")
            return False
        tmp = canonical_path.with_suffix(".json.tmp")
        tmp.write_text(isolated + ("\n" if not isolated.endswith("\n") else ""))
        try:
            os.chmod(tmp, 0o600)
        except OSError:
            pass
        tmp.rename(canonical_path)
        _log(f"accounts: harvested refreshed {label} token → {canonical_path.name}")
        return True


# --- Quota probing ----------------------------------------------------------


def probe_account(quota_cmd: str, label: str,
                   force: bool = False) -> str | None:
    """Run ``claude-available-model --account <label>`` and return its
    stdout (e.g. ``"opus"``, ``"sonnet"``) or None if no quota / error.

    ``force=True`` adds ``--force`` to bypass the helper's own cache.
    Network/timeout errors yield None (treated as no quota).
    """
    if not quota_cmd:
        return None
    cmd = os.path.expanduser(quota_cmd)
    args = [cmd]
    if force:
        args.append("--force")
    if label:
        args += ["--account", label]
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=30)
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        _log(f"accounts: probe({label}) failed: {e}")
        return None
    if r.returncode != 0:
        return None
    out = (r.stdout or "").strip()
    return out or None


def probe_codex(quota_cmd: str) -> bool:
    """Return True iff Codex's quota helper says quota is available.

    Codex has no per-account dimension on the helper side. Exit 0 with
    non-empty stdout → available; exit 1 → exhausted; exit 2 →
    cache-stale (treated as unavailable; the launchd refresher will
    settle it within a minute).
    """
    if not quota_cmd:
        return True
    cmd = os.path.expanduser(quota_cmd)
    try:
        r = subprocess.run([cmd], capture_output=True, text=True, timeout=30)
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False
    return r.returncode == 0 and bool((r.stdout or "").strip())


# --- Candidate enumeration --------------------------------------------------


# Local copy of the tier ordering. Imported by cli.py too via
# `from pod.accounts import MODEL_TIER`. Kept here to avoid pod.cli ↔
# pod.accounts circular imports.
MODEL_TIER: dict[str, dict[str, int]] = {
    "claude": {"sonnet": 1, "opus": 2},
    "codex": {"gpt-5.4": 1, "gpt-5.5": 2},
}


def model_tier(backend: str, model: str) -> int:
    return MODEL_TIER.get(backend, {}).get(model, 0)


@dataclasses.dataclass(frozen=True)
class Candidate:
    backend: str       # "claude" or "codex"
    label: str         # account label (Claude only; "" for Codex)
    model: str         # selected model — propagated to launch_agent
    account_num: int = 0  # for Claude credential mirroring


def _claude_candidates_for(label: str, available: str | None,
                             accepted_models: list[str]) -> Iterator[Candidate]:
    """Yield the highest-tier Candidate this account can satisfy.

    Iterates ``accepted_models`` in the configured order and yields the
    first one whose tier the available model meets. (Only one Candidate
    per account — we don't yield both opus and sonnet for the same
    account; the agent would only ever launch one of them.)
    """
    if not available:
        return
    avail_tier = model_tier("claude", available)
    for want in accepted_models:
        if avail_tier >= model_tier("claude", want):
            yield Candidate(backend="claude", label=label, model=want)
            return


def enumerate_candidates(
    *,
    claude_accounts: list[Account],
    available_by_label: dict[str, str | None],
    accepted_models: list[str],
    codex_available: bool,
    codex_model: str,
    prefer: str,
    pin_label: str | None = None,
) -> list[Candidate]:
    """Build the ordered candidate list for one selection pass.

    Args:
      claude_accounts: result of ``list_claude_accounts()``
      available_by_label: ``label -> "opus"|"sonnet"|None`` (probe results)
      accepted_models: ordered model tiers the agent will accept,
        e.g. ``["opus", "sonnet"]``. First-satisfiable wins per account.
      codex_available: result of ``probe_codex(...)``
      codex_model: model name to launch codex with
      prefer: ``"claude"`` or ``"codex"`` — backend ordering hint
      pin_label: if set, restrict Claude candidates to this account
        (resume-pinning). Codex is excluded when pinned.

    Returns the ordered list; an empty list means "wait".
    """
    claude_cands: list[Candidate] = []
    accounts_by_label = {a.label: a for a in claude_accounts}
    iter_labels: Iterable[str]
    if pin_label is not None:
        iter_labels = [pin_label] if pin_label in accounts_by_label else []
    else:
        iter_labels = [a.label for a in claude_accounts]
    for label in iter_labels:
        avail = available_by_label.get(label)
        for cand in _claude_candidates_for(label, avail, accepted_models):
            account = accounts_by_label[label]
            claude_cands.append(dataclasses.replace(
                cand, account_num=account.number))
    codex_cands: list[Candidate] = []
    if pin_label is None and codex_available:
        codex_cands.append(Candidate(
            backend="codex", label="", model=codex_model))
    if prefer == "codex":
        return codex_cands + claude_cands
    return claude_cands + codex_cands


# --- Per-agent CLAUDE_CONFIG_DIR --------------------------------------------


def agent_claude_config_dir(pod_dir: Path, short_id: str) -> Path:
    """Return the per-agent CLAUDE_CONFIG_DIR for ``short_id``.

    Always under ``<pod_dir>/claude-config/<short_id>/``. Materialised
    on demand by ``ensure_agent_claude_config_dir``.
    """
    return pod_dir / "claude-config" / short_id


def ensure_agent_claude_config_dir(pod_dir: Path, short_id: str) -> Path:
    """Create the agent's isolated CLAUDE_CONFIG_DIR with minimal contents.

    Idempotent. Creates ``projects/`` for JSONL transcripts and writes
    a minimal ``settings.json``. The ``.credentials.json`` file is
    written by ``mirror_canonical_to_isolated`` once an account is
    leased — *not* here, since we don't know yet which account this
    agent will use.
    """
    config_dir = agent_claude_config_dir(pod_dir, short_id)
    config_dir.mkdir(parents=True, exist_ok=True)
    settings_path = config_dir / "settings.json"
    if not settings_path.exists():
        settings_path.write_text('{"skipDangerousModePermissionPrompt": true}\n')
    (config_dir / "projects").mkdir(exist_ok=True)
    return config_dir


def cleanup_agent_claude_config_dir(pod_dir: Path, short_id: str) -> None:
    """Remove the per-agent CLAUDE_CONFIG_DIR (e.g. on agent exit).

    Tolerates missing dirs. The macOS keychain entry keyed by sha256
    of the path is *not* cleaned up here — keychain entries cost ~nil
    and removing them would lose any refreshed token that hasn't been
    harvested back. Stale keychain entries from past short_ids are
    harmless because new agents get a new short_id and therefore a
    new keychain key.
    """
    config_dir = agent_claude_config_dir(pod_dir, short_id)
    try:
        shutil.rmtree(config_dir, ignore_errors=True)
    except OSError:
        pass
