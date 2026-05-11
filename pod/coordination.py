"""Python port of the legacy `pod/data/coordination` bash script.

Routes every GitHub interaction through `pod.github.get_client()` so
`pod gh-stats` sees the agent hot path. The bash file is reduced to a
2-line shim that exec's `pod _coordination "$@"`; this module is the
real implementation.

Stdout/stderr contracts are preserved exactly so existing `cli.py`
parsers and external callers (agents shelling out to `coordination
queue-depth` etc.) keep working unchanged.
"""

from __future__ import annotations

import datetime
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from pod import github as gh


# ---------------------------------------------------------------------------
# Module constants (must match the bash exactly)
# ---------------------------------------------------------------------------

PLANNER_LOCK_TTL = 1200  # 20 minutes
REPO_CACHE_TTL = 3600    # 1 hour
RACE_SLEEP_SHORT = 2     # claim / claim-pr-repair race-detect window
RACE_SLEEP_LONG = 3      # lock-planner race-detect window

# Same labels/colors as the bash _ensure_labels (line 150).
REQUIRED_LABELS = {
    "agent-plan": "1D76DB",
    "claimed": "FBCA04",
    "blocked": "B60205",
    "has-pr": "5319E7",
    "replan": "D93F0B",
    "coordination": "0E8A16",
    "human-oversight": "CC317C",
    "return-to-human": "E4A400",
    "critical-path": "FF6600",
    "repair-claimed": "7057FF",
    "unsalvageable": "5C5C5C",
}


# ---------------------------------------------------------------------------
# Tiny helpers
# ---------------------------------------------------------------------------

def die(msg: str) -> "NoReturn":  # type: ignore[name-defined]
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def _now_epoch() -> int:
    return int(time.time())


def _iso_to_epoch(s: str) -> int | None:
    if not s:
        return None
    try:
        return int(datetime.datetime.fromisoformat(
            s.replace("Z", "+00:00")).timestamp())
    except (ValueError, TypeError):
        return None


def _atomic_write(path: Path, content: str) -> None:
    """Write `content` to `path` atomically via mktemp + rename (matches the
    bash `mktemp /tmp/pod-cache.XXXXXX && mv -f` pattern)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="pod-cache.", dir="/tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(content)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _git(*args: str, cwd: str | None = None,
         input: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], capture_output=True, text=True,
                          timeout=30, cwd=cwd, input=input)


def _pod(*args: str, input: str | None = None,
         timeout: int = 60) -> subprocess.CompletedProcess:
    """Re-enter `pod` as a subprocess for internal helpers (`_filter-
    trusted-issues`, `_check-provenance`). The bash original did the
    same — both versions reuse pod's existing CLI plumbing rather than
    re-implementing provenance logic.

    Uses the `pod` executable from PATH (the shim that exec'd us into
    `_coordination` ran via PATH, so it's available). Falls back to
    `python -c 'from pod.cli import main; main()'` if `pod` isn't
    found (e.g. tests, or if PATH was scrubbed)."""
    import shutil
    pod_bin = shutil.which("pod")
    if pod_bin:
        argv = [pod_bin, *args]
    else:
        argv = [sys.executable, "-c",
                "from pod.cli import main; main()", *args]
    return subprocess.run(argv, capture_output=True, text=True,
                          timeout=timeout, input=input)


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------

@dataclass
class CoordinationContext:
    """Equivalent of the bash module-level state. Built lazily by `main`.

    The bash script computes all of this on every invocation; we do the
    same so the behavior matches (cache TTLs etc. are file-based, shared
    across invocations).
    """
    repo: str
    base_branch: str
    session_id: str
    branch: str
    protected_files: list[str]
    stuck_ci_minutes: int
    include_untrusted: bool
    repo_slug: str  # repo with `/` → `-`
    git_toplevel: str
    cache_key: str  # for /tmp/ cache filenames (git_toplevel with / → -)
    _planner_lock_issue: int | None = field(default=None, init=False)

    @property
    def planner_lock_comment_file(self) -> Path:
        return Path(f"/tmp/pod-planner-lock-{self.repo_slug}-{self.session_id}.id")

    @property
    def planner_lock_issue_cache(self) -> Path:
        return Path(f"/tmp/pod-planner-lock-issue-{self.repo_slug}")

    @property
    def labels_ensured_flag(self) -> Path:
        return Path(f"/tmp/pod-labels-ensured-{self.repo_slug}")


def _build_context() -> CoordinationContext:
    """Mirror lines 47–93 of the bash: detect repo, base_branch via TTL'd
    /tmp caches, with `gh repo view` as the cache-miss fallback."""
    git_top = _git("rev-parse", "--show-toplevel").stdout.strip() or os.getcwd()
    cache_key = git_top.replace("/", "-")

    repo_cache = Path(f"/tmp/pod-repo-cache{cache_key}")
    base_cache = Path(f"/tmp/pod-base-branch-cache{cache_key}")

    cached_repo = ""
    cached_base = ""
    cache_stale = False
    if repo_cache.is_file() and base_cache.is_file():
        try:
            cached_repo = repo_cache.read_text().strip()
            cached_base = base_cache.read_text().strip()
            age = _now_epoch() - int(repo_cache.stat().st_mtime)
            if age > REPO_CACHE_TTL:
                cache_stale = True
        except OSError:
            cache_stale = True

    repo_re = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")
    if (not cache_stale and repo_re.match(cached_repo or "")
            and cached_base):
        repo, base_branch = cached_repo, cached_base
    else:
        client = gh.get_client()
        try:
            r = _pod_view_repo_name_with_owner()
            if not r:
                die("cannot detect GitHub repo (is gh authenticated?)")
            repo = r
        except Exception as e:
            die(f"cannot detect GitHub repo (is gh authenticated?): {e}")
        # Resolve default branch via the layer (ETag-cached).
        resp = client.get(f"/repos/{repo}", timeout=15)
        base_branch = "master"
        if resp.ok():
            db = ((resp.body() or {}).get("default_branch") or "").strip()
            if db:
                base_branch = db
        try:
            _atomic_write(repo_cache, repo + "\n")
            _atomic_write(base_cache, base_branch + "\n")
        except OSError:
            pass

    repo_slug = repo.replace("/", "-")
    session_id = os.environ.get("POD_SESSION_ID", "unknown")
    branch = _git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip() or "detached"
    pf_raw = os.environ.get("POD_PROTECTED_FILES", "PLAN.md")
    protected_files = [p for p in pf_raw.split(":") if p]
    stuck_ci_minutes = int(os.environ.get("POD_STUCK_CI_MINUTES", "120"))
    include_untrusted = os.environ.get("POD_INCLUDE_UNTRUSTED", "0") == "1"

    return CoordinationContext(
        repo=repo,
        base_branch=base_branch,
        session_id=session_id,
        branch=branch,
        protected_files=protected_files,
        stuck_ci_minutes=stuck_ci_minutes,
        include_untrusted=include_untrusted,
        repo_slug=repo_slug,
        git_toplevel=git_top,
        cache_key=cache_key,
    )


def _pod_view_repo_name_with_owner() -> str | None:
    """Detect the repo slug via the local git remote (no API call).

    Mirrors cli.py:_get_repo's logic so the two cache layers agree.
    """
    r = _git("remote", "get-url", "origin")
    url = r.stdout.strip()
    m = re.search(r"github\.com[:/](.+?)(?:\.git)?$", url)
    if m:
        return m.group(1)
    # Fallback: ask gh through the layer.
    client = gh.get_client()
    res = client.gh_cli("repo", "view", "--json", "nameWithOwner",
                        "-q", ".nameWithOwner", timeout=15)
    if res.returncode == 0:
        return res.stdout.strip() or None
    return None


# ---------------------------------------------------------------------------
# Common helpers (bash analogues)
# ---------------------------------------------------------------------------

def _ensure_labels(ctx: CoordinationContext) -> None:
    """Create any missing pod labels on the repo. Once-per-session flag
    file in /tmp prevents repeated work."""
    if ctx.labels_ensured_flag.exists():
        return
    client = gh.get_client()
    # Use porcelain via the layer so behavior matches bash (gh label
    # supports description with colons in body that the REST API would
    # require URL-encoding; porcelain is also tolerant).
    r = client.gh_cli("label", "list", "--repo", ctx.repo, "--limit", "100",
                       "--json", "name", "--jq", ".[].name", timeout=15)
    existing = set()
    if r.returncode == 0:
        existing = {ln for ln in r.stdout.splitlines() if ln}
    for label, color in REQUIRED_LABELS.items():
        if label not in existing:
            client.gh_cli("label", "create", label, "--repo", ctx.repo,
                           "--color", color, "--description",
                           "pod coordination", timeout=15)
    try:
        ctx.labels_ensured_flag.touch()
    except OSError:
        pass


def _ensure_auth() -> None:
    """Bash equivalent: `gh auth status --hostname github.com`."""
    r = subprocess.run(["gh", "auth", "status", "--hostname", "github.com"],
                       capture_output=True, timeout=10)
    if r.returncode != 0:
        die("gh CLI not authenticated — run 'gh auth login' first")


def _resolve_planner_lock_issue(ctx: CoordinationContext) -> int:
    """Locate or create the planner-lock sentinel issue. Result is cached
    on `ctx` after the first lookup."""
    if ctx._planner_lock_issue is not None:
        return ctx._planner_lock_issue

    client = gh.get_client()

    # Check the /tmp cache. Validate that the cached issue is still OPEN.
    cache = ctx.planner_lock_issue_cache
    if cache.is_file():
        try:
            cached = cache.read_text().strip()
        except OSError:
            cached = ""
        if cached.isdigit():
            r = client.get(
                f"/repos/{ctx.repo}/issues/{cached}", timeout=15,
                cache="none")
            if r.ok() and (r.body() or {}).get("state") == "open":
                ctx._planner_lock_issue = int(cached)
                return ctx._planner_lock_issue
            # Stale cache — remove
            try:
                cache.unlink()
            except OSError:
                pass

    # Search for the sentinel by title via porcelain (cheaper than
    # multi-label JSON enumeration).
    r = client.gh_cli(
        "issue", "list", "--repo", ctx.repo, "--state", "open",
        "--label", "coordination",
        "--json", "number,title",
        "--jq", '.[] | select(.title == "pod: planner lock sentinel") | .number',
        timeout=30,
    )
    issue_num = ""
    if r.returncode == 0:
        first_line = next((ln for ln in r.stdout.splitlines() if ln.strip()),
                          "")
        issue_num = first_line.strip()

    if not issue_num:
        # Create the sentinel issue.
        create = client.gh_cli(
            "issue", "create", "--repo", ctx.repo,
            "--title", "pod: planner lock sentinel",
            "--body", ("This issue is used by pod for advisory planner "
                       "locking. Do not close or modify."),
            "--label", "coordination",
            timeout=30,
        )
        if create.returncode != 0:
            die("Failed to create planner lock sentinel issue")
        url = create.stdout.strip()
        # gh issue create prints the URL; extract trailing number.
        trail = url.rstrip("/").rsplit("/", 1)[-1] if url else ""
        if not trail.isdigit():
            die("Failed to create planner lock sentinel issue")
        issue_num = trail
        # Try to pin it; ignore failure.
        client.gh_cli("issue", "pin", issue_num, "--repo", ctx.repo,
                       timeout=15)

    try:
        _atomic_write(cache, issue_num + "\n")
    except OSError:
        pass
    ctx._planner_lock_issue = int(issue_num)
    return ctx._planner_lock_issue


# ---------------------------------------------------------------------------
# Provenance integration (delegated to existing pod subcommands)
# ---------------------------------------------------------------------------

def _filter_trusted_issues(args: list[str]) -> str:
    """Invoke `pod _filter-trusted-issues …`, return stdout JSON.

    Lets the bash-side semantics (silent filtering + --include-untrusted
    annotation) live in cli.py:cmd_filter_trusted_issues unchanged.
    """
    r = _pod("_filter-trusted-issues", *args, timeout=120)
    if r.returncode != 0:
        # Surface gh error verbatim like the bash did.
        sys.stderr.write(r.stderr)
        sys.exit(r.returncode or 1)
    return r.stdout


def _check_provenance(issue_num: int | str) -> tuple[bool, str]:
    """Invoke `pod _check-provenance N --fresh`. Returns (ok, stderr)."""
    r = _pod("_check-provenance", str(issue_num), "--fresh", timeout=60)
    return (r.returncode == 0, r.stderr.strip() or r.stdout.strip())


# ---------------------------------------------------------------------------
# Issue list helpers
# ---------------------------------------------------------------------------

_UNCLAIMED_EXCLUDE = {"claimed", "blocked", "has-pr", "replan"}
_REPLAN_EXCLUDE = {"claimed", "blocked", "has-pr"}


def _filtered_issues(items: list[dict], exclude: set[str]) -> list[dict]:
    """Apply the bash jq filter inline: drop items whose labels intersect
    `exclude`; sort critical-path first, then by createdAt."""
    out = []
    for it in items:
        names = {l.get("name") for l in (it.get("labels") or [])}
        if names & exclude:
            continue
        out.append(it)
    out.sort(key=lambda it: (
        0 if any(l.get("name") == "critical-path"
                 for l in (it.get("labels") or [])) else 1,
        it.get("createdAt", "") or it.get("created_at", "") or "",
    ))
    return out


def _unclaimed_filter(items: list[dict]) -> list[dict]:
    """Apply the bash jq filter inline: exclude {claimed, blocked, has-pr,
    replan}; sort critical-path first, then by createdAt."""
    return _filtered_issues(items, _UNCLAIMED_EXCLUDE)


def _unclaimed_issues(ctx: CoordinationContext,
                      extra_label: str | None = None) -> list[dict]:
    """Return the unclaimed issue list. Goes through
    `pod _filter-trusted-issues` so untrusted authors are filtered (or
    annotated, if POD_INCLUDE_UNTRUSTED=1)."""
    include = ["--include-untrusted"] if ctx.include_untrusted else []
    if extra_label:
        args = ["--label", "agent-plan", "--label", extra_label,
                "--state", "open", "--limit", "50",
                "--json", "number,title,labels,createdAt", *include]
        out = _filter_trusted_issues(args)
        try:
            items = _safe_json(out, default=[])
        except ValueError:
            items = []
        return _unclaimed_filter(items)

    # No extra label: union of agent-plan + human-oversight, dedup by number.
    items_a = _safe_json(_filter_trusted_issues([
        "--label", "agent-plan", "--state", "open", "--limit", "50",
        "--json", "number,title,labels,createdAt", *include,
    ]), default=[])
    items_h = _safe_json(_filter_trusted_issues([
        "--label", "human-oversight", "--state", "open", "--limit", "50",
        "--json", "number,title,labels,createdAt", *include,
    ]), default=[])
    seen: dict[int, dict] = {}
    for x in (items_a or []) + (items_h or []):
        n = x.get("number")
        if n is not None and n not in seen:
            seen[n] = x
    return _unclaimed_filter(list(seen.values()))


def _replan_issues(ctx: CoordinationContext) -> list[dict]:
    """Return `agent-plan + replan` issues filtered through the trusted-
    author gate. `human-oversight` is intentionally excluded — those
    issues have an owner-closes-only policy and must not be auto-replanned.
    """
    include = ["--include-untrusted"] if ctx.include_untrusted else []
    args = ["--label", "agent-plan", "--label", "replan",
            "--state", "open", "--limit", "500",
            "--json", "number,title,labels,createdAt", *include]
    out = _filter_trusted_issues(args)
    try:
        items = _safe_json(out, default=[])
    except ValueError:
        items = []
    return _filtered_issues(items, _REPLAN_EXCLUDE)


def _safe_json(s: str, default=None):
    import json as _json
    s = (s or "").strip()
    if not s:
        return default
    try:
        return _json.loads(s)
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# Subcommand: orient
# ---------------------------------------------------------------------------

def cmd_orient(ctx: CoordinationContext, argv: list[str]) -> int:
    client = gh.get_client()

    print("=== HUMAN OVERSIGHT DIRECTIVES (highest priority) ===")
    r = client.gh_cli(
        "issue", "list", "--repo", ctx.repo, "--label", "human-oversight",
        "--state", "open", "--limit", "50",
        "--json", "number,title,labels,createdAt",
        "--jq", '.[] | select(.labels | all(.name != "has-pr")) '
                '| "#\\(.number) \\(.title) (\\(.createdAt[:16]))"',
        timeout=30,
    )
    if r.returncode == 0:
        sys.stdout.write(r.stdout)

    print()
    print("=== Unclaimed work items ===")
    for it in _unclaimed_issues(ctx):
        ts = (it.get("createdAt") or "")[:16]
        print(f"#{it['number']} {it.get('title','')} ({ts})")

    print()
    print("=== Blocked issues ===")
    # We need the list of currently-open issue numbers so we can omit
    # closed deps from the "[Blocked on …]" annotation.
    rop = client.gh_cli(
        "issue", "list", "--repo", ctx.repo, "--state", "open",
        "--limit", "100", "--json", "number", "--jq", "[.[].number]",
        timeout=30,
    )
    open_nums: set[int] = set()
    if rop.returncode == 0:
        try:
            open_nums = set(_safe_json(rop.stdout, default=[]) or [])
        except Exception:
            open_nums = set()
    include = ["--include-untrusted"] if ctx.include_untrusted else []
    raw = _filter_trusted_issues([
        "--label", "agent-plan", "--label", "blocked",
        "--state", "open", "--limit", "50",
        "--json", "number,title,body", *include,
    ])
    blocked_items = _safe_json(raw, default=[]) or []
    dep_re = re.compile(r"depends-on: #(\d+)")
    for it in blocked_items:
        deps = [int(d) for d in dep_re.findall(it.get("body", "") or "")]
        open_deps = [d for d in deps if d in open_nums]
        dep_str = ", ".join(f"#{d}" for d in open_deps)
        print(f"#{it['number']} [Blocked on {dep_str}] {it.get('title','')}")

    print()
    print("=== Claimed work items ===")
    r = client.gh_cli(
        "issue", "list", "--repo", ctx.repo,
        "--label", "agent-plan", "--label", "claimed",
        "--state", "open", "--limit", "20",
        "--json", "number,title,createdAt",
        "--template",
        '{{range .}}#{{.number}} {{.title}} ({{timeago .createdAt}}){{"\\n"}}{{end}}',
        timeout=30,
    )
    if r.returncode == 0:
        sys.stdout.write(r.stdout)

    print()
    print("=== Issues with open PRs ===")
    r = client.gh_cli(
        "issue", "list", "--repo", ctx.repo,
        "--label", "agent-plan", "--label", "has-pr",
        "--state", "open", "--limit", "20",
        "--json", "number,title,createdAt",
        "--template",
        '{{range .}}#{{.number}} {{.title}} ({{timeago .createdAt}}){{"\\n"}}{{end}}',
        timeout=30,
    )
    if r.returncode == 0:
        sys.stdout.write(r.stdout)

    print()
    print("=== Open pull requests ===")
    r = client.gh_cli(
        "pr", "list", "--repo", ctx.repo, "--state", "open", "--limit", "20",
        "--json", "number,title,headRefName,statusCheckRollup,labels,mergeable",
        "--jq",
        '.[] | "#\\(.number) [\\(.headRefName)] \\(.title)'
        '\\(.labels | map(" (\\(.name))") | join(""))'
        '\\(if ((.labels | any(.name == "merge-conflict")) or .mergeable == "CONFLICTING") '
        'then " \\u001b[31m[CONFLICTS]\\u001b[0m" else "" end)'
        '\\(if (.statusCheckRollup | any(.conclusion == "FAILURE")) '
        'then " ✗CI" else "" end)"',
        timeout=30,
    )
    if r.returncode == 0:
        sys.stdout.write(r.stdout)

    print()
    print("=== PRs needing attention (failing CI, merge-conflict, or dirty) ===")
    r = client.gh_cli(
        "pr", "list", "--repo", ctx.repo, "--state", "open", "--limit", "20",
        "--json", "number,title,labels,mergeable,statusCheckRollup",
        "--jq",
        '[.[] | select('
        '(.labels | any(.name == "merge-conflict")) or '
        '(.mergeable == "CONFLICTING") or '
        '(.statusCheckRollup | any(.conclusion == "FAILURE")))] | .[] | '
        '"#\\(.number) \\(.title)'
        '\\(if ((.labels | any(.name == "merge-conflict")) or .mergeable == "CONFLICTING") '
        'then " \\u001b[31m[CONFLICTS]\\u001b[0m" else "" end)'
        '\\(if (.statusCheckRollup | any(.conclusion == "FAILURE")) '
        'then " [CI FAILED]" else "" end)"',
        timeout=30,
    )
    if r.returncode == 0:
        sys.stdout.write(r.stdout)

    return 0


# ---------------------------------------------------------------------------
# Subcommand: plan
# ---------------------------------------------------------------------------

def cmd_plan(ctx: CoordinationContext, argv: list[str]) -> int:
    extra_label = ""
    critical_path = False
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--label":
            if i + 1 >= len(argv):
                die("--label requires a value")
            extra_label = argv[i + 1]
            i += 2
        elif a == "--critical-path":
            critical_path = True
            i += 1
        else:
            break
    if i >= len(argv):
        die('usage: coordination plan [--label L] [--critical-path] "title"')
    title = argv[i]

    client = gh.get_client()

    # Overlap warning: compare new title against existing agent-plan titles.
    r = client.gh_cli(
        "issue", "list", "--repo", ctx.repo, "--label", "agent-plan",
        "--state", "open", "--limit", "50", "--json", "number,title",
        timeout=30,
    )
    existing = []
    if r.returncode == 0:
        existing = [x.get("title", "") for x in
                    (_safe_json(r.stdout, default=[]) or [])]
    if existing:
        keywords = [w for w in re.findall(r"[A-Za-z]+", title.lower())
                    if len(w) > 4]
        keywords = sorted(set(keywords))
        warned = False
        for et in existing:
            etl = et.lower()
            match = sum(1 for kw in keywords if kw in etl)
            if keywords and match >= 2:
                if not warned:
                    print("warning: potentially overlapping open issues found:")
                    warned = True
                print(f"  - {et}")
        if warned:
            print("Proceeding anyway...")

    body = sys.stdin.read()

    create_labels = ["agent-plan"]
    if extra_label:
        create_labels.append(extra_label)
    if critical_path:
        create_labels.append("critical-path")

    deps = re.findall(r"depends-on: #(\d+)", body)
    if deps:
        has_open_dep = False
        for d in deps:
            rs = client.gh_cli(
                "issue", "view", d, "--repo", ctx.repo,
                "--json", "state", "--jq", ".state", timeout=15,
            )
            if rs.returncode == 0 and rs.stdout.strip() != "CLOSED":
                has_open_dep = True
                break
        if has_open_dep:
            create_labels.append("blocked")

    cr = client.gh_cli(
        "issue", "create", "--repo", ctx.repo,
        "--title", title, "--label", ",".join(create_labels),
        "--body", body, timeout=30,
    )
    if cr.returncode != 0:
        sys.stderr.write(cr.stderr)
        return cr.returncode or 1
    url = cr.stdout.strip()
    issue_num = url.rstrip("/").rsplit("/", 1)[-1] if url else ""

    if "blocked" in create_labels:
        print(f"Created issue #{issue_num}: {title} (blocked — depends on open issue(s))")
    else:
        print(f"Created issue #{issue_num}: {title}")

    if issue_num:
        client.gh_cli(
            "issue", "comment", issue_num, "--repo", ctx.repo,
            "--body", f"Session: `{ctx.session_id}` Branch: `{ctx.branch}`",
            timeout=30,
        )
    return 0


# ---------------------------------------------------------------------------
# Subcommand: create-pr
# ---------------------------------------------------------------------------

def cmd_create_pr(ctx: CoordinationContext, argv: list[str]) -> int:
    if not argv:
        die('usage: coordination create-pr N [--partial] ["custom title"]')
    issue_num = argv[0]
    partial = False
    custom_title = ""
    for a in argv[1:]:
        if a == "--partial":
            partial = True
        else:
            custom_title = a

    if ctx.branch in ("master", "main", "detached"):
        die(f"cannot create PR from branch '{ctx.branch}' — use an agent/* branch")

    client = gh.get_client()

    # Title resolution
    if custom_title:
        pr_title = custom_title
    else:
        r = client.gh_cli(
            "issue", "view", issue_num, "--repo", ctx.repo,
            "--json", "title", "--jq", ".title", timeout=15,
        )
        pr_title = r.stdout.strip() if r.returncode == 0 else f"#{issue_num}"

    issue_ref = (f"Partial progress on #{issue_num}"
                 if partial else f"Closes #{issue_num}")

    # Protected-files guard
    mb = _git("merge-base", f"origin/{ctx.base_branch}", "HEAD")
    merge_base = mb.stdout.strip() if mb.returncode == 0 else ""
    if merge_base and ctx.protected_files:
        d = _git("diff", "--name-only", merge_base, "HEAD", "--",
                 *ctx.protected_files)
        if d.returncode == 0:
            touched = [ln for ln in d.stdout.splitlines() if ln.strip()]
            if touched:
                die(f"This PR touches protected files ({', '.join(touched)}). "
                    "These files are off-limits to agents. Aborting PR creation.")

    # Push branch.
    p = _git("push", "--force-with-lease", "-u", "origin", ctx.branch)
    if p.returncode != 0:
        # Mirror bash: surface git output and continue (will fail at PR create).
        sys.stderr.write(p.stderr)

    # Existing-PR path
    r = client.gh_cli(
        "pr", "list", "--repo", ctx.repo, "--head", ctx.branch,
        "--json", "number", "--jq", ".[0].number // empty", timeout=15,
    )
    existing_pr = r.stdout.strip() if r.returncode == 0 else ""

    if existing_pr:
        print(f"PR #{existing_pr} already exists for branch {ctx.branch}. "
              "Enabling auto-merge.")
        if not partial:
            br = client.gh_cli(
                "pr", "view", existing_pr, "--repo", ctx.repo,
                "--json", "body", "--jq", ".body", timeout=15,
            )
            body = br.stdout if br.returncode == 0 else ""
            if not re.search(rf"(closes|fixes|resolves) #{issue_num}\b",
                              body, re.IGNORECASE):
                die(
                    f"PR #{existing_pr} body does not reference 'Closes #{issue_num}'.\n"
                    f"Refusing to set 'has-pr' on issue #{issue_num} — the label would "
                    f"become orphan after merge.\n"
                    f"Either edit the PR body to add 'Closes #{issue_num}', or rerun "
                    f"'coordination create-pr {issue_num} --partial'."
                )
        m = client.gh_cli(
            "pr", "merge", existing_pr, "--repo", ctx.repo,
            "--auto", "--squash", "--delete-branch", timeout=30,
        )
        if m.returncode != 0:
            print("warning: auto-merge not available "
                  "(branch protection may not be set up)")
        if partial:
            client.gh_cli("issue", "edit", issue_num, "--repo", ctx.repo,
                           "--add-label", "replan", timeout=15)
            client.gh_cli("issue", "edit", issue_num, "--repo", ctx.repo,
                           "--remove-label", "claimed", timeout=15)
            print(f"Issue #{issue_num} marked replan (partial completion)")
        else:
            client.gh_cli("issue", "edit", issue_num, "--repo", ctx.repo,
                           "--add-label", "has-pr", timeout=15)
            client.gh_cli("issue", "edit", issue_num, "--repo", ctx.repo,
                           "--remove-label", "claimed", timeout=15)
        return 0

    # Create new PR
    log_r = _git("log", f"origin/{ctx.base_branch}..HEAD", "--oneline")
    log_text = log_r.stdout if log_r.returncode == 0 else ""
    body = (
        f"{issue_ref}\n\n"
        f"Session: `{ctx.session_id}`\n\n"
        f"{log_text}\n"
        "🤖 Prepared with Claude Code"
    )
    cr = client.gh_cli(
        "pr", "create", "--repo", ctx.repo, "--head", ctx.branch,
        "--title", pr_title, "--body", body, timeout=60,
    )
    if cr.returncode != 0:
        sys.stderr.write(cr.stderr)
        return cr.returncode or 1
    sys.stdout.write(cr.stdout)

    # Look up the new PR number.
    r = client.gh_cli(
        "pr", "list", "--repo", ctx.repo, "--head", ctx.branch,
        "--json", "number", "--jq", ".[0].number", timeout=15,
    )
    pr_num = r.stdout.strip() if r.returncode == 0 else ""
    if pr_num:
        m = client.gh_cli(
            "pr", "merge", pr_num, "--repo", ctx.repo,
            "--auto", "--squash", "--delete-branch", timeout=30,
        )
        if m.returncode != 0:
            print("warning: auto-merge not available "
                  "(branch protection may not be set up)")

    if partial:
        client.gh_cli("issue", "edit", issue_num, "--repo", ctx.repo,
                       "--add-label", "replan", timeout=15)
        client.gh_cli("issue", "edit", issue_num, "--repo", ctx.repo,
                       "--remove-label", "claimed", timeout=15)
        print(f"Issue #{issue_num} marked replan (partial completion)")
    else:
        client.gh_cli("issue", "edit", issue_num, "--repo", ctx.repo,
                       "--add-label", "has-pr", timeout=15)
        client.gh_cli("issue", "edit", issue_num, "--repo", ctx.repo,
                       "--remove-label", "claimed", timeout=15)
    return 0


# ---------------------------------------------------------------------------
# Subcommand: claim-fix
# ---------------------------------------------------------------------------

def cmd_claim_fix(ctx: CoordinationContext, argv: list[str]) -> int:
    if not argv:
        die("usage: coordination claim-fix N")
    pr_num = argv[0]
    client = gh.get_client()
    cutoff = _now_epoch() - 1800  # 30 min
    recent = 0
    for page in client.paginate(f"/repos/{ctx.repo}/issues/{pr_num}/comments",
                                cache="none"):
        if not page.ok():
            break
        for c in (page.body() or []):
            if not isinstance(c, dict):
                continue
            if "Session " in (c.get("body") or "") and " attempting fix" in (c.get("body") or ""):
                ts = _iso_to_epoch(c.get("created_at", "") or "")
                if ts and ts > cutoff:
                    recent += 1
    if recent > 0:
        print("Another session claimed this PR in the last 30 minutes. Skipping.")
        return 1
    client.gh_cli(
        "issue", "comment", pr_num, "--repo", ctx.repo,
        "--body",
        f"Session `{ctx.session_id}` attempting fix on branch `{ctx.branch}`",
        timeout=30,
    )
    return 0


# ---------------------------------------------------------------------------
# Subcommand: close-pr
# ---------------------------------------------------------------------------

def cmd_close_pr(ctx: CoordinationContext, argv: list[str]) -> int:
    if len(argv) < 2:
        die('usage: coordination close-pr N "reason"')
    pr_num, reason = argv[0], argv[1]
    client = gh.get_client()
    client.gh_cli(
        "pr", "close", pr_num, "--repo", ctx.repo,
        "--comment", f"{reason} (Session: `{ctx.session_id}`)",
        timeout=30,
    )
    # Remove has-pr from any linked issue
    br = client.gh_cli(
        "pr", "view", pr_num, "--repo", ctx.repo, "--json", "body",
        "--jq", ".body", timeout=15,
    )
    if br.returncode == 0:
        m = re.search(r"Closes #(\d+)", br.stdout)
        if m:
            client.gh_cli("issue", "edit", m.group(1), "--repo", ctx.repo,
                           "--remove-label", "has-pr", timeout=15)
    return 0


# ---------------------------------------------------------------------------
# Subcommand: list-pr-repair
# ---------------------------------------------------------------------------

def _rollup_failure(checks: list[dict]) -> bool:
    for c in checks or []:
        if not isinstance(c, dict):
            continue
        if c.get("conclusion") == "FAILURE":
            return True
        if c.get("state") in ("FAILURE", "ERROR"):
            return True
    return False


def _rollup_pending(checks: list[dict]) -> bool:
    for c in checks or []:
        if not isinstance(c, dict):
            continue
        if c.get("status") == "IN_PROGRESS":
            return True
        if c.get("state") in ("PENDING", "EXPECTED"):
            return True
    return False


def cmd_list_pr_repair(ctx: CoordinationContext, argv: list[str]) -> int:
    client = gh.get_client()
    stuck_seconds = ctx.stuck_ci_minutes * 60
    now = _now_epoch()

    r = client.gh_cli(
        "pr", "list", "--repo", ctx.repo, "--state", "open", "--limit", "500",
        "--json", "number,title,mergeable,statusCheckRollup,labels",
        timeout=60,
    )
    if r.returncode != 0:
        return 0
    prs = _safe_json(r.stdout, default=[]) or []

    def claimed(pr):
        return any(l.get("name") == "repair-claimed"
                   for l in (pr.get("labels") or []))

    # Conflicts
    confl = sorted(
        (p for p in prs
         if p.get("mergeable") == "CONFLICTING" and not claimed(p)),
        key=lambda p: p.get("number", 0),
    )
    for p in confl:
        print(f"#{p['number']} [conflict] {p.get('title','')}")

    # Failed CI
    failed = sorted(
        (p for p in prs
         if p.get("mergeable") != "CONFLICTING"
         and _rollup_failure(p.get("statusCheckRollup") or [])
         and not claimed(p)),
        key=lambda p: p.get("number", 0),
    )
    for p in failed:
        print(f"#{p['number']} [failed] {p.get('title','')}")

    # Stuck CI candidates
    suspects = [
        p for p in prs
        if p.get("mergeable") != "CONFLICTING"
        and not _rollup_failure(p.get("statusCheckRollup") or [])
        and _rollup_pending(p.get("statusCheckRollup") or [])
        and not claimed(p)
    ]
    for p in suspects:
        pr_num = p["number"]
        # GraphQL: per-check startedAt / per-status createdAt.
        q = """
        query($owner:String!,$name:String!,$pr:Int!){
          repository(owner:$owner,name:$name){
            pullRequest(number:$pr){
              commits(last:1){nodes{commit{
                checkSuites(first:20){nodes{
                  checkRuns(first:50){nodes{status conclusion startedAt}}
                }}
                status{ contexts{ state createdAt } }
              }}}
            }
          }
        }
        """
        owner, name = ctx.repo.split("/", 1)
        resp = client.graphql(q, variables={
            "owner": owner, "name": name, "pr": pr_num,
        })
        if not resp.ok():
            continue
        try:
            commit = (((resp.body() or {}).get("data") or {})
                      .get("repository", {}) or {}).get("pullRequest", {}) or {}
            commit = ((commit.get("commits") or {}).get("nodes") or [{}])[0]
            commit = (commit or {}).get("commit") or {}
        except (AttributeError, IndexError, TypeError):
            commit = {}
        starts: list[str] = []
        for suite in ((commit.get("checkSuites") or {}).get("nodes") or []):
            for run in ((suite or {}).get("checkRuns") or {}).get("nodes") or []:
                if (run.get("status") == "IN_PROGRESS"
                        and run.get("startedAt")):
                    starts.append(run["startedAt"])
        for ctx_status in ((commit.get("status") or {}).get("contexts") or []):
            if (ctx_status.get("state") in ("PENDING", "EXPECTED")
                    and ctx_status.get("createdAt")):
                starts.append(ctx_status["createdAt"])
        is_stuck = False
        for s in starts:
            e = _iso_to_epoch(s)
            if e is not None and (now - e) > stuck_seconds:
                is_stuck = True
                break
        if is_stuck:
            print(f"#{pr_num} [stuck] {p.get('title','')}")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: list-replan
# ---------------------------------------------------------------------------

def cmd_list_replan(ctx: CoordinationContext, argv: list[str]) -> int:
    """List `agent-plan + replan` issues ready for /replan triage.

    Goes through the same trusted-author gate as `list-unclaimed`.
    Excludes `claimed`, `blocked`, `has-pr`. `human-oversight` is
    excluded by construction (we only ask for `agent-plan`).
    """
    for it in _replan_issues(ctx):
        print(f"#{it.get('number')} {it.get('title','')}")
    return 0


class _RaceCheckFailed(Exception):
    """Raised when the race-detection re-read can't complete (e.g. the
    comments endpoint returns non-OK). Callers MUST fail closed: the
    bash original ran under `set -euo pipefail` so a failed re-read
    aborted the whole `claim`. Swallowing the failure here would let
    two concurrent claimants both succeed."""


# ---------------------------------------------------------------------------
# Subcommand: claim-pr-repair
# ---------------------------------------------------------------------------

def _count_recent_repair_claims(client, repo: str, pr_num: str,
                                 since_epoch: int) -> int:
    """Count `Claimed PR repair by session` comments with
    `created_at > since_epoch`. Raises `_RaceCheckFailed` on any
    non-OK comments page so callers can fail closed."""
    count = 0
    for page in client.paginate(f"/repos/{repo}/issues/{pr_num}/comments",
                                cache="none"):
        if not page.ok():
            raise _RaceCheckFailed(
                f"comments fetch for PR #{pr_num} returned HTTP {page.status}")
        for c in (page.body() or []):
            if not isinstance(c, dict):
                continue
            if "Claimed PR repair by session" not in (c.get("body") or ""):
                continue
            ts = _iso_to_epoch(c.get("created_at", "") or "")
            if ts and ts > since_epoch:
                count += 1
    return count


def _recent_repair_claim_winner(client, repo: str, pr_num: str,
                                since_epoch: int) -> str | None:
    """Return the lowest-sorted (lexicographic) `session` UUID among
    repair-claim comments newer than `since_epoch`. Raises
    `_RaceCheckFailed` on non-OK pages."""
    bodies: list[str] = []
    for page in client.paginate(f"/repos/{repo}/issues/{pr_num}/comments",
                                cache="none"):
        if not page.ok():
            raise _RaceCheckFailed(
                f"comments fetch for PR #{pr_num} returned HTTP {page.status}")
        for c in (page.body() or []):
            if not isinstance(c, dict):
                continue
            body = c.get("body") or ""
            if "Claimed PR repair by session" not in body:
                continue
            ts = _iso_to_epoch(c.get("created_at", "") or "")
            if ts and ts > since_epoch:
                bodies.append(body)
    if not bodies:
        return None
    bodies.sort()
    m = re.search(r"session `([^`]+)`", bodies[0])
    return m.group(1) if m else None


def cmd_claim_pr_repair(ctx: CoordinationContext, argv: list[str]) -> int:
    if not argv:
        die("usage: coordination claim-pr-repair N")
    pr_num = argv[0]
    client = gh.get_client()

    r = client.gh_cli(
        "pr", "view", pr_num, "--repo", ctx.repo,
        "--json", "labels", "--jq", "[.labels[].name] | join(\",\")",
        timeout=15,
    )
    labels = r.stdout if r.returncode == 0 else ""
    if re.search(r"\brepair-claimed\b", labels):
        print(f"CLAIM FAILED: PR #{pr_num} is already repair-claimed.")
        return 1

    # Freeze the race-detect cutoff BEFORE posting so the layer's
    # rate-limit back-pressure (which can sleep up to 60s) can't push
    # our concurrent attempts past the window.
    # No long-form cooldown: the `repair-claimed` label + race-detect
    # window (RACE_SLEEP_SHORT, ~2 s) are sufficient to serialise
    # concurrent claims. An older 30-minute comment-history cooldown
    # was removed because crashed-session re-attempts were dominating
    # legitimate parallel use, causing every fresh agent to walk the
    # candidate list hitting `CLAIM FAILED: ... within the last 30
    # minutes` on every PR. Dead-session cleanup is the housekeeping
    # sweep's job; truly-unsalvageable PRs should be closed with
    # `coordination close-pr-unsalvageable` so they exit the candidate
    # list entirely.
    race_since = _now_epoch()

    client.gh_cli("pr", "edit", pr_num, "--repo", ctx.repo,
                   "--add-label", "repair-claimed", timeout=15)
    client.gh_cli(
        "issue", "comment", pr_num, "--repo", ctx.repo,
        "--body",
        f"Claimed PR repair by session `{ctx.session_id}` on branch `{ctx.branch}`",
        timeout=30,
    )

    time.sleep(RACE_SLEEP_SHORT)
    try:
        race_count = _count_recent_repair_claims(client, ctx.repo, pr_num,
                                                  race_since)
    except _RaceCheckFailed as e:
        print(f"CLAIM FAILED: race verification for PR #{pr_num} failed: {e}")
        # Don't strip the label — another session may have legitimately
        # claimed in the same window. The reconciler will clean up if
        # nobody actually completes the repair.
        return 1
    if race_count > 1:
        try:
            winner = _recent_repair_claim_winner(client, ctx.repo, pr_num,
                                                  race_since)
        except _RaceCheckFailed as e:
            print(f"CLAIM FAILED: race verification for PR #{pr_num} failed: {e}")
            return 1
        if winner and winner != ctx.session_id:
            print(f"CLAIM FAILED: Race lost on PR #{pr_num} — another "
                  "session won.")
            # Don't remove the label — the winner needs it.
            return 1
        print(f"Race detected on PR #{pr_num} — this session won.")
    print(f"Claimed PR #{pr_num} for repair")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: mark-pr-salvaged
# ---------------------------------------------------------------------------

def cmd_mark_pr_salvaged(ctx: CoordinationContext, argv: list[str]) -> int:
    if not argv:
        die("usage: coordination mark-pr-salvaged N")
    pr_num = argv[0]
    client = gh.get_client()
    client.gh_cli("pr", "edit", pr_num, "--repo", ctx.repo,
                   "--remove-label", "repair-claimed", timeout=15)
    client.gh_cli(
        "issue", "comment", pr_num, "--repo", ctx.repo,
        "--body",
        f"Repair complete (session `{ctx.session_id}`). "
        "Verification passed; pushed.",
        timeout=30,
    )
    return 0


# ---------------------------------------------------------------------------
# Subcommand: close-pr-unsalvageable
# ---------------------------------------------------------------------------

def cmd_close_pr_unsalvageable(ctx: CoordinationContext,
                                argv: list[str]) -> int:
    if len(argv) < 2:
        die('usage: coordination close-pr-unsalvageable N "reason"')
    pr_num, reason = argv[0], argv[1]
    client = gh.get_client()

    r = client.gh_cli(
        "pr", "view", pr_num, "--repo", ctx.repo,
        "--json", "closingIssuesReferences",
        "--jq",
        "[.closingIssuesReferences[] | select(.number != null) | .number] | .[]",
        timeout=15,
    )
    linked = []
    if r.returncode == 0:
        linked = [int(ln) for ln in r.stdout.splitlines()
                   if ln.strip().isdigit()]

    client.gh_cli(
        "pr", "close", pr_num, "--repo", ctx.repo,
        "--comment",
        f"Closing as unsalvageable: {reason} (Session: `{ctx.session_id}`)",
        timeout=30,
    )
    client.gh_cli("pr", "edit", pr_num, "--repo", ctx.repo,
                   "--add-label", "unsalvageable", timeout=15)
    client.gh_cli("pr", "edit", pr_num, "--repo", ctx.repo,
                   "--remove-label", "repair-claimed", timeout=15)

    if linked:
        updated = []
        for inum in linked:
            client.gh_cli("issue", "edit", str(inum), "--repo", ctx.repo,
                           "--add-label", "replan", timeout=15)
            client.gh_cli("issue", "edit", str(inum), "--repo", ctx.repo,
                           "--remove-label", "has-pr", timeout=15)
            client.gh_cli(
                "issue", "comment", str(inum), "--repo", ctx.repo,
                "--body",
                f"PR #{pr_num} closed as unsalvageable: {reason}. "
                "Marked replan so a new plan can be produced.",
                timeout=30,
            )
            updated.append(f"#{inum}")
        print(f"PR #{pr_num} closed; linked issue(s) marked replan: "
              + " ".join(updated))
    else:
        br = client.gh_cli(
            "pr", "view", pr_num, "--repo", ctx.repo,
            "--json", "body", "--jq", ".body", timeout=15,
        )
        body = br.stdout if br.returncode == 0 else ""
        fallback = sorted(set(int(n) for n in re.findall(
            r"(?:closes|closed|close|fixes|fixed|fix|resolves|resolved|resolve)\s+#(\d+)",
            body, re.IGNORECASE)))
        if fallback:
            for inum in fallback:
                client.gh_cli("issue", "edit", str(inum), "--repo", ctx.repo,
                               "--add-label", "replan", timeout=15)
                client.gh_cli("issue", "edit", str(inum), "--repo", ctx.repo,
                               "--remove-label", "has-pr", timeout=15)
                client.gh_cli(
                    "issue", "comment", str(inum), "--repo", ctx.repo,
                    "--body",
                    f"PR #{pr_num} closed as unsalvageable: {reason}. "
                    "Marked replan so a new plan can be produced.",
                    timeout=30,
                )
            print(f"PR #{pr_num} closed; fallback-resolved issue(s) marked "
                  f"replan: {' '.join(str(n) for n in fallback)}")
        else:
            print(f"PR #{pr_num} closed; no linked issue found "
                  "(closingIssuesReferences empty, body grep empty).")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: list-unclaimed
# ---------------------------------------------------------------------------

def cmd_list_unclaimed(ctx: CoordinationContext, argv: list[str]) -> int:
    extra_label = ""
    i = 0
    while i < len(argv):
        if argv[i] == "--label":
            if i + 1 >= len(argv):
                die("--label requires a value")
            extra_label = argv[i + 1]
            i += 2
        else:
            break
    for it in _unclaimed_issues(ctx, extra_label or None):
        ts = (it.get("createdAt") or "")[:16]
        print(f"#{it['number']} {it.get('title','')} ({ts})")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: queue-depth
# ---------------------------------------------------------------------------

def cmd_queue_depth(ctx: CoordinationContext, argv: list[str]) -> int:
    extra_label = ""
    i = 0
    while i < len(argv):
        if argv[i] == "--label":
            if i + 1 >= len(argv):
                die("--label requires a value")
            extra_label = argv[i + 1]
            i += 2
        else:
            extra_label = argv[i]
            i += 1
    items = _unclaimed_issues(ctx, extra_label or None)
    print(len(items))
    return 0


# ---------------------------------------------------------------------------
# Subcommand: critical-path-depth
# ---------------------------------------------------------------------------

def cmd_critical_path_depth(ctx: CoordinationContext,
                              argv: list[str]) -> int:
    extra_label = argv[0] if argv else ""
    client = gh.get_client()
    args = ["issue", "list", "--repo", ctx.repo, "--label", "critical-path"]
    if extra_label:
        args += ["--label", extra_label]
    args += ["--state", "open", "--limit", "50",
             "--json", "number,title,labels,createdAt",
             "--jq",
             '[.[] | select(.labels | all(.name != "claimed") '
             'and all(.name != "blocked") '
             'and all(.name != "has-pr") '
             'and all(.name != "replan"))] | length']
    r = client.gh_cli(*args, timeout=30)
    if r.returncode != 0:
        print(0)
        return r.returncode
    sys.stdout.write(r.stdout)
    if not r.stdout.endswith("\n"):
        print()
    return 0


# ---------------------------------------------------------------------------
# Subcommand: claim
# ---------------------------------------------------------------------------

_CLAIM_RACE_SECONDS = 60


def _count_recent_claims(client, repo: str, issue_num: str,
                          since_epoch: int) -> int:
    """Count `Claimed by session` comments with `created_at > since_epoch`.
    Raises `_RaceCheckFailed` on any non-OK page so the caller can fail
    closed (bash's `set -euo pipefail` did this implicitly)."""
    count = 0
    for page in client.paginate(f"/repos/{repo}/issues/{issue_num}/comments",
                                cache="none"):
        if not page.ok():
            raise _RaceCheckFailed(
                f"comments fetch for #{issue_num} returned HTTP {page.status}")
        for c in (page.body() or []):
            if not isinstance(c, dict):
                continue
            if "Claimed by session" not in (c.get("body") or ""):
                continue
            ts = _iso_to_epoch(c.get("created_at", "") or "")
            if ts and ts > since_epoch:
                count += 1
    return count


def _recent_claim_winner(client, repo: str, issue_num: str,
                          since_epoch: int) -> str | None:
    """Return the lowest-sorted (lexicographic) `session` UUID among
    claim comments newer than `since_epoch`. Raises `_RaceCheckFailed`
    on non-OK pages."""
    bodies: list[str] = []
    for page in client.paginate(f"/repos/{repo}/issues/{issue_num}/comments",
                                cache="none"):
        if not page.ok():
            raise _RaceCheckFailed(
                f"comments fetch for #{issue_num} returned HTTP {page.status}")
        for c in (page.body() or []):
            if not isinstance(c, dict):
                continue
            body = c.get("body") or ""
            if "Claimed by session" not in body:
                continue
            ts = _iso_to_epoch(c.get("created_at", "") or "")
            if ts and ts > since_epoch:
                bodies.append(body)
    if not bodies:
        return None
    bodies.sort()
    m = re.search(r"session `([^`]+)`", bodies[0])
    return m.group(1) if m else None


def cmd_claim(ctx: CoordinationContext, argv: list[str]) -> int:
    if not argv:
        die("usage: coordination claim N")
    issue_num = argv[0]
    client = gh.get_client()

    ok, prov_err = _check_provenance(issue_num)
    if not ok:
        print(f"CLAIM FAILED: {prov_err}")
        print("You MUST NOT work on this issue. "
              "Pick a different issue immediately.")
        return 1

    r = client.gh_cli(
        "issue", "view", issue_num, "--repo", ctx.repo,
        "--json", "labels", "--jq", "[.labels[].name] | join(\",\")",
        timeout=15,
    )
    labels = r.stdout if r.returncode == 0 else ""
    for label, msg in [
        ("claimed", f"Issue #{issue_num} is already claimed by another agent."),
        ("replan",  f"Issue #{issue_num} needs replan."),
        ("blocked", f"Issue #{issue_num} is blocked by dependencies."),
        ("has-pr",  f"Issue #{issue_num} already has an open PR."),
    ]:
        if re.search(rf"\b{label}\b", labels):
            print(f"CLAIM FAILED: {msg} You MUST NOT work on this issue. "
                  "Pick a different issue immediately.")
            return 1

    is_resume = os.environ.get("POD_IS_RESUME", "0") == "1"
    note = " (resumed conversation)" if is_resume else ""
    # Freeze the race-detect cutoff BEFORE posting so the layer's
    # rate-limit back-pressure (which can sleep up to 60s on quota
    # exhaustion) can't push concurrent claim comments out of the
    # window. The bash original computed `now - 60` inside its jq
    # filter, so the cutoff drifted with each re-read; that was a
    # latent bug — we fix it here.
    race_since = _now_epoch() - _CLAIM_RACE_SECONDS

    client.gh_cli("issue", "edit", issue_num, "--repo", ctx.repo,
                   "--add-label", "claimed", timeout=15)
    client.gh_cli(
        "issue", "comment", issue_num, "--repo", ctx.repo,
        "--body",
        f"Claimed by session `{ctx.session_id}` on branch `{ctx.branch}`{note}",
        timeout=30,
    )

    time.sleep(RACE_SLEEP_SHORT)
    try:
        recent = _count_recent_claims(client, ctx.repo, issue_num, race_since)
    except _RaceCheckFailed as e:
        print(f"CLAIM FAILED: race verification for #{issue_num} failed: {e}")
        # Don't strip the label — another session may have claimed in
        # the same window. The reconciler / release-stale-claims sweep
        # will clean up if nobody actually completes the work.
        return 1
    if recent > 1:
        try:
            winner = _recent_claim_winner(client, ctx.repo, issue_num,
                                           race_since)
        except _RaceCheckFailed as e:
            print(f"CLAIM FAILED: race verification for #{issue_num} failed: {e}")
            return 1
        if winner and winner != ctx.session_id:
            print(f"CLAIM FAILED: Race detected on #{issue_num} — another "
                  "agent won. You MUST NOT work on this issue. "
                  "Pick a different issue immediately.")
            return 1
        print(f"Race detected on #{issue_num} — this session won.")
    print(f"Claimed issue #{issue_num}")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: skip
# ---------------------------------------------------------------------------

def cmd_skip(ctx: CoordinationContext, argv: list[str]) -> int:
    if len(argv) < 2:
        die('usage: coordination skip N "reason"')
    issue_num, reason = argv[0], argv[1]
    client = gh.get_client()
    client.gh_cli(
        "issue", "edit", issue_num, "--repo", ctx.repo,
        "--remove-label", "claimed", "--add-label", "replan", timeout=15,
    )
    client.gh_cli(
        "issue", "comment", issue_num, "--repo", ctx.repo,
        "--body",
        f"Skipped by session `{ctx.session_id}` (needs replan): {reason}",
        timeout=30,
    )
    print(f"Skipped issue #{issue_num} (marked replan): {reason}")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: add-dep
# ---------------------------------------------------------------------------

def cmd_add_dep(ctx: CoordinationContext, argv: list[str]) -> int:
    if len(argv) < 2:
        die("usage: coordination add-dep <issue> <dep>")
    issue_num, dep_num = argv[0], argv[1]
    client = gh.get_client()
    r = client.gh_cli(
        "issue", "view", issue_num, "--repo", ctx.repo,
        "--json", "body", "--jq", ".body", timeout=15,
    )
    body = r.stdout if r.returncode == 0 else ""
    body = body.rstrip("\n")
    if re.search(rf"depends-on: #{dep_num}(?:[^0-9]|$)", body):
        print(f"Issue #{issue_num} already has depends-on: #{dep_num} in its body")
    else:
        new_body = f"{body}\ndepends-on: #{dep_num}"
        client.gh_cli(
            "issue", "edit", issue_num, "--repo", ctx.repo,
            "--body", new_body, timeout=30,
        )
        print(f"Added depends-on: #{dep_num} to body of issue #{issue_num}")
    sr = client.gh_cli(
        "issue", "view", dep_num, "--repo", ctx.repo,
        "--json", "state", "--jq", ".state", timeout=15,
    )
    state = sr.stdout.strip() if sr.returncode == 0 else "UNKNOWN"
    if state == "OPEN":
        client.gh_cli("issue", "edit", issue_num, "--repo", ctx.repo,
                       "--add-label", "blocked", timeout=15)
        print(f"Added blocked label (dependency #{dep_num} is open)")
    else:
        print(f"Dependency #{dep_num} is already closed; "
              "blocked label not added")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: check-blocked
# ---------------------------------------------------------------------------

def cmd_check_blocked(ctx: CoordinationContext, argv: list[str]) -> int:
    client = gh.get_client()
    r = client.gh_cli(
        "issue", "list", "--repo", ctx.repo, "--label", "blocked",
        "--state", "open", "--limit", "50", "--json", "number,body",
        timeout=30,
    )
    issues = _safe_json(r.stdout, default=[]) if r.returncode == 0 else []
    for it in (issues or []):
        num = it.get("number")
        body = it.get("body", "") or ""
        deps = [int(d) for d in re.findall(r"depends-on: #(\d+)", body)]
        if not deps:
            client.gh_cli("issue", "edit", str(num), "--repo", ctx.repo,
                           "--remove-label", "blocked", timeout=15)
            client.gh_cli(
                "issue", "comment", str(num), "--repo", ctx.repo,
                "--body",
                "Orphan `blocked` removed: issue body has no `depends-on: #N` "
                "lines, so the label could not be auto-cleared. Use "
                "`coordination add-dep` to record dependencies properly.",
                timeout=30,
            )
            print(f"Cleared orphan blocked on #{num} "
                  "(no depends-on lines in body)")
            continue
        all_closed = True
        for d in deps:
            sr = client.gh_cli(
                "issue", "view", str(d), "--repo", ctx.repo,
                "--json", "state", "--jq", ".state", timeout=15,
            )
            # PRs share the issue number space; `gh issue view` on a merged
            # PR returns "MERGED", not "CLOSED". Both count as terminal.
            state = sr.stdout.strip() if sr.returncode == 0 else "UNKNOWN"
            if state not in ("CLOSED", "MERGED"):
                all_closed = False
                break
        if all_closed:
            client.gh_cli("issue", "edit", str(num), "--repo", ctx.repo,
                           "--remove-label", "blocked", timeout=15)
            print(f"Unblocked issue #{num} (all dependencies resolved)")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: check-has-pr
# ---------------------------------------------------------------------------

def cmd_check_has_pr(ctx: CoordinationContext, argv: list[str]) -> int:
    client = gh.get_client()
    r = client.gh_cli(
        "issue", "list", "--repo", ctx.repo, "--label", "has-pr",
        "--state", "open", "--limit", "50", "--json", "number,title",
        timeout=30,
    )
    issues = _safe_json(r.stdout, default=[]) if r.returncode == 0 else []
    for it in (issues or []):
        num = it.get("number")
        title = it.get("title", "") or ""
        pr = client.gh_cli(
            "issue", "view", str(num), "--repo", ctx.repo,
            "--json", "closedByPullRequestsReferences",
            "--jq", ".closedByPullRequestsReferences[].number",
            timeout=15,
        )
        pr_nums = []
        if pr.returncode == 0:
            pr_nums = [ln.strip() for ln in pr.stdout.splitlines()
                       if ln.strip()]
        has_open_pr = False
        pr_states: list[str] = []
        for p in pr_nums:
            sr = client.gh_cli(
                "pr", "view", p, "--repo", ctx.repo,
                "--json", "state", "--jq", ".state", timeout=15,
            )
            state = sr.stdout.strip() if sr.returncode == 0 else "UNKNOWN"
            pr_states.append(f"#{p} ({state.lower()})")
            if state == "OPEN":
                has_open_pr = True
        if has_open_pr:
            continue
        prior_msg = ""
        if pr_states:
            prior_msg = f" Last linked PRs: {', '.join(pr_states)}."
        client.gh_cli("issue", "edit", str(num), "--repo", ctx.repo,
                       "--remove-label", "has-pr", timeout=15)
        client.gh_cli(
            "issue", "comment", str(num), "--repo", ctx.repo,
            "--body",
            f"Orphan `has-pr` removed: no open PR currently references this "
            f"issue with `Closes #{num}`.{prior_msg} If decomposition is in "
            "flight, use `coordination add-dep` to record sub-issue "
            "dependencies; the directive will become `blocked` until they close.",
            timeout=30,
        )
        print(f"Cleared orphan has-pr on #{num} ({title})")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: release-stale-claims
# ---------------------------------------------------------------------------

def cmd_release_stale_claims(ctx: CoordinationContext,
                               argv: list[str]) -> int:
    stale_secs = int(argv[0]) if argv else 14400
    client = gh.get_client()
    now = _now_epoch()
    r = client.gh_cli(
        "issue", "list", "--repo", ctx.repo, "--label", "claimed",
        "--state", "open", "--limit", "50", "--json", "number,title",
        timeout=30,
    )
    issues = _safe_json(r.stdout, default=[]) if r.returncode == 0 else []
    for it in (issues or []):
        num = it.get("number")
        title = it.get("title", "") or ""
        cr = client.gh_cli(
            "issue", "view", str(num), "--repo", ctx.repo,
            "--json", "comments",
            "--jq",
            '[.comments[] | select(.body | startswith("Claimed by session"))] '
            '| last | .createdAt // ""',
            timeout=30,
        )
        claim_ts = cr.stdout.strip().strip('"') if cr.returncode == 0 else ""
        if not claim_ts:
            continue
        ts = _iso_to_epoch(claim_ts) or 0
        age = now - ts
        if age > stale_secs:
            age_str = f"{age // 3600}h{(age % 3600) // 60}m"
            client.gh_cli("issue", "edit", str(num), "--repo", ctx.repo,
                           "--remove-label", "claimed", timeout=15)
            client.gh_cli(
                "issue", "comment", str(num), "--repo", ctx.repo,
                "--body",
                f"Stale claim released — worker session appears to have died "
                f"(claimed {age_str} ago). Available for reclaim.",
                timeout=30,
            )
            print(f"Released stale claim on #{num} ({title}, age {age_str})")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: release-orphan-claims
# ---------------------------------------------------------------------------

def _live_session_uuids(ctx: CoordinationContext) -> set[str] | None:
    """Read live agent session UUIDs from `<repo>/.pod/agents/*.json`.

    Returns the set, or `None` if the agents directory is unreadable / not
    found — callers treat `None` as "can't make a liveness decision" and
    fall back to age-based heuristics.
    """
    import json as _json
    pod_dir = Path(ctx.git_toplevel) / ".pod" / "agents"
    if not pod_dir.is_dir():
        return None
    live: set[str] = set()
    try:
        for p in pod_dir.glob("*.json"):
            if p.name.endswith(".tmp"):
                continue
            try:
                d = _json.loads(p.read_text())
            except (OSError, _json.JSONDecodeError):
                continue
            if d.get("status") in ("dead", "stopped", "killed"):
                continue
            uid = d.get("uuid")
            if isinstance(uid, str) and uid:
                live.add(uid)
    except OSError:
        return None
    return live


def cmd_release_orphan_claims(ctx: CoordinationContext,
                                argv: list[str]) -> int:
    """Liveness-based orphan-claim sweep: release any `claimed` issue whose
    owning session UUID is not present in `.pod/agents/*.json`.

    Complements `release-stale-claims` (age-based) and the housekeeping
    `reconcile_untracked_github_claims` loop (10-min cadence, 60s grace).
    Runs on demand, no grace period, no GitHub-time math — local agent
    state is authoritative for whether a session is still running.

    Exits non-zero if the local agents directory cannot be read, so the
    caller doesn't mistake an "infrastructure problem" for "no orphans".
    Otherwise prints one line per released claim and returns 0.
    """
    live = _live_session_uuids(ctx)
    if live is None:
        sys.stderr.write(
            "release-orphan-claims: cannot read .pod/agents/ — "
            "refusing to release without liveness data\n")
        return 1

    client = gh.get_client()
    r = client.gh_cli(
        "issue", "list", "--repo", ctx.repo, "--label", "claimed",
        "--state", "open", "--limit", "100", "--json", "number,title",
        timeout=30,
    )
    if r.returncode != 0:
        sys.stderr.write(f"release-orphan-claims: gh issue list failed: "
                         f"{r.stderr.strip()}\n")
        return 1
    issues = _safe_json(r.stdout, default=[]) or []

    claim_re = re.compile(r'Claimed by session `([^`]+)`')
    released = 0
    for it in issues:
        num = it.get("number")
        title = it.get("title", "") or ""
        if not isinstance(num, int):
            continue
        cr = client.gh_cli(
            "issue", "view", str(num), "--repo", ctx.repo,
            "--json", "comments",
            "--jq",
            '[.comments[] | select(.body | startswith("Claimed by session"))] '
            '| sort_by(.createdAt) | last | .body // ""',
            timeout=30,
        )
        body = cr.stdout.strip().strip('"') if cr.returncode == 0 else ""
        if not body:
            # No parseable claim comment — leave for age-based sweep.
            continue
        m = claim_re.search(body)
        if not m:
            continue
        owner_uuid = m.group(1)
        if owner_uuid in live:
            continue  # owner alive — keep

        # Owner not in live set → release.
        e1 = client.gh_cli("issue", "edit", str(num), "--repo", ctx.repo,
                            "--remove-label", "claimed", timeout=15)
        if e1.returncode != 0:
            sys.stderr.write(f"#{num}: label remove failed: "
                             f"{e1.stderr.strip()}\n")
            continue
        client.gh_cli(
            "issue", "comment", str(num), "--repo", ctx.repo,
            "--body",
            f"Orphan claim released — owning session `{owner_uuid}` is "
            f"no longer in the local agent table. Available for reclaim.",
            timeout=30,
        )
        print(f"Released orphan claim on #{num} ({title}, owner {owner_uuid[:8]})")
        released += 1
    if released == 0:
        print("No orphan claims found.")
    return 0


# ---------------------------------------------------------------------------
# Lock family
# ---------------------------------------------------------------------------

def _active_lock_comment_ids(client, ctx: CoordinationContext) -> list[int]:
    """Return the database IDs of `planner-lock-attempt:` comments that
    are still "active" — created within `PLANNER_LOCK_TTL` seconds.

    Note on TTL clock: the cutoff combines a server-side comment
    timestamp (`created_at`) with a local-clock "now". This matches the
    bash original exactly; both versions share the same modest clock-
    skew sensitivity. A holder whose clock is N seconds ahead of GitHub
    sees the lock expire N seconds early; one behind, N seconds late.
    PLANNER_LOCK_TTL=1200s is generous enough that this is acceptable.
    """
    issue = _resolve_planner_lock_issue(ctx)
    cutoff = _now_epoch() - PLANNER_LOCK_TTL
    ids: list[int] = []
    for page in client.paginate(
            f"/repos/{ctx.repo}/issues/{issue}/comments", cache="none"):
        if not page.ok():
            break
        for c in (page.body() or []):
            if not isinstance(c, dict):
                continue
            body = c.get("body") or ""
            if not body.startswith("planner-lock-attempt:"):
                continue
            ts = _iso_to_epoch(c.get("created_at", "") or "")
            if ts and ts > cutoff:
                cid = c.get("id")
                if isinstance(cid, int):
                    ids.append(cid)
                elif isinstance(cid, str) and cid.isdigit():
                    ids.append(int(cid))
    return ids


def cmd_lock_planner(ctx: CoordinationContext, argv: list[str]) -> int:
    client = gh.get_client()
    issue = _resolve_planner_lock_issue(ctx)

    post = client.post(
        f"/repos/{ctx.repo}/issues/{issue}/comments",
        json={"body": f"planner-lock-attempt: {ctx.session_id}"},
        timeout=30,
    )
    if not post.ok():
        die(f"failed to post lock-attempt comment: HTTP {post.status}")
    my_id = (post.body() or {}).get("id")
    if not isinstance(my_id, int):
        try:
            my_id = int(my_id)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            die(f"unparseable comment id from POST: {my_id!r}")

    time.sleep(RACE_SLEEP_LONG)

    ids = _active_lock_comment_ids(client, ctx)
    earliest = min(ids) if ids else None
    if earliest is not None and my_id == earliest:
        try:
            _atomic_write(ctx.planner_lock_comment_file, str(my_id) + "\n")
        except OSError:
            pass
        print(f"Planner lock acquired (comment #{my_id})")
        return 0
    # Lost the race; delete our comment.
    client.delete(f"/repos/{ctx.repo}/issues/comments/{my_id}", timeout=15)
    label = f"#{earliest}" if earliest is not None else "#unknown"
    print(f"Another planner is active (lock held by comment {label}). Skipping.")
    return 1


def cmd_unlock_planner(ctx: CoordinationContext, argv: list[str]) -> int:
    my_id = ""
    if ctx.planner_lock_comment_file.is_file():
        try:
            my_id = ctx.planner_lock_comment_file.read_text().strip()
        except OSError:
            my_id = ""
        try:
            ctx.planner_lock_comment_file.unlink()
        except OSError:
            pass
    if my_id:
        client = gh.get_client()
        client.delete(f"/repos/{ctx.repo}/issues/comments/{my_id}",
                       timeout=15)
        print(f"Planner lock released (deleted comment #{my_id})")
    else:
        print("No lock comment ID found; nothing to release")
    return 0


def cmd_lock_status(ctx: CoordinationContext, argv: list[str]) -> int:
    client = gh.get_client()
    ids = _active_lock_comment_ids(client, ctx)
    if ids:
        print("locked")
        return 0
    print("unlocked")
    return 1


def cmd_force_unlock_planner(ctx: CoordinationContext,
                               argv: list[str]) -> int:
    client = gh.get_client()
    ids = _active_lock_comment_ids(client, ctx)
    for cid in ids:
        client.delete(f"/repos/{ctx.repo}/issues/comments/{cid}", timeout=15)
    print(f"Deleted {len(ids)} lock comment(s)")
    return 0


# ---------------------------------------------------------------------------
# return-to-human family
# ---------------------------------------------------------------------------

def cmd_return_to_human(ctx: CoordinationContext, argv: list[str]) -> int:
    issue = _resolve_planner_lock_issue(ctx)
    client = gh.get_client()
    client.gh_cli("issue", "edit", str(issue), "--repo", ctx.repo,
                   "--add-label", "return-to-human", timeout=15)
    print(f"Return-to-human signal sent (label added to sentinel issue #{issue})")
    return 0


def cmd_check_return_to_human(ctx: CoordinationContext,
                                 argv: list[str]) -> int:
    issue = _resolve_planner_lock_issue(ctx)
    client = gh.get_client()
    r = client.gh_cli(
        "issue", "view", str(issue), "--repo", ctx.repo,
        "--json", "labels", "--jq", "[.labels[].name] | join(\",\")",
        timeout=15,
    )
    if r.returncode == 0 and re.search(r"\breturn-to-human\b", r.stdout):
        print("true")
        return 0
    print("false")
    return 1


def cmd_clear_return_to_human(ctx: CoordinationContext,
                                 argv: list[str]) -> int:
    issue = _resolve_planner_lock_issue(ctx)
    client = gh.get_client()
    client.gh_cli("issue", "edit", str(issue), "--repo", ctx.repo,
                   "--remove-label", "return-to-human", timeout=15)
    print(f"Return-to-human signal cleared "
          f"(label removed from sentinel issue #{issue})")
    return 0


# ---------------------------------------------------------------------------
# set-target / set-min-queue
# ---------------------------------------------------------------------------

def _main_repo_root() -> Path:
    """Equivalent of `cd $(git rev-parse --git-common-dir)/.. && pwd -P`.
    Returns the main repo root (not a worktree)."""
    r = _git("rev-parse", "--git-common-dir")
    gcd = r.stdout.strip() if r.returncode == 0 else ".git"
    return Path(gcd).resolve().parent


def cmd_set_target(ctx: CoordinationContext, argv: list[str]) -> int:
    if not argv:
        die("usage: coordination set-target N")
    n = argv[0]
    pod_dir = _main_repo_root() / ".pod"
    if not pod_dir.is_dir():
        print(f"error: .pod/ not found at {pod_dir}", file=sys.stderr)
        return 1
    (pod_dir / "planner-target").write_text(
        "# Planner advisory: cap effective target to this many agents.\n"
        "# Effective target is min(target, planner-target).\n"
        f"{n}\n"
    )
    print(f"Planner target set to {n}")
    return 0


def cmd_set_min_queue(ctx: CoordinationContext, argv: list[str]) -> int:
    if not argv:
        die("usage: coordination set-min-queue N")
    n = argv[0]
    pod_dir = _main_repo_root() / ".pod"
    if not pod_dir.is_dir():
        print(f"error: .pod/ not found at {pod_dir}", file=sys.stderr)
        return 1
    (pod_dir / "planner-min-queue").write_text(
        "# Planner advisory: lower queue_balance min_queue threshold.\n"
        "# Effective min_queue is min(config min_queue, planner-min-queue).\n"
        "# Set to 0 to explicitly disable queue-low backfill "
        "(no planner re-fire).\n"
        f"{n}\n"
    )
    print(f"Planner min_queue set to {n}")
    return 0


# ---------------------------------------------------------------------------
# read-issue
# ---------------------------------------------------------------------------

def cmd_read_issue(ctx: CoordinationContext, argv: list[str]) -> int:
    if not argv:
        die("usage: coordination read-issue N [--json FIELDS] [--jq EXPR]")
    issue_num = argv[0]
    json_fields = "body"
    jq_expr = ".body"
    i = 1
    while i < len(argv):
        if argv[i] == "--json":
            if i + 1 >= len(argv):
                die("--json requires a value")
            json_fields = argv[i + 1]
            i += 2
        elif argv[i] == "--jq":
            if i + 1 >= len(argv):
                die("--jq requires a value")
            jq_expr = argv[i + 1]
            i += 2
        else:
            die(f"unknown arg to read-issue: {argv[i]}")
    ok, err = _check_provenance(issue_num)
    if not ok:
        print(f"READ FAILED: {err}", file=sys.stderr)
        return 1
    client = gh.get_client()
    r = client.gh_cli(
        "issue", "view", issue_num, "--repo", ctx.repo,
        "--json", json_fields, "--jq", jq_expr, timeout=30,
    )
    sys.stdout.write(r.stdout)
    return r.returncode


# ---------------------------------------------------------------------------
# nothing-to-plan (deprecated no-op)
# ---------------------------------------------------------------------------

def cmd_nothing_to_plan(ctx: CoordinationContext, argv: list[str]) -> int:
    print("nothing-to-plan: deprecated — use 'coordination set-target N' "
          "or 'coordination set-min-queue N' instead")
    return 0


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_COMMANDS = {
    "orient": cmd_orient,
    "plan": cmd_plan,
    "create-pr": cmd_create_pr,
    "claim-fix": cmd_claim_fix,
    "close-pr": cmd_close_pr,
    "list-pr-repair": cmd_list_pr_repair,
    "list-replan": cmd_list_replan,
    "claim-pr-repair": cmd_claim_pr_repair,
    "mark-pr-salvaged": cmd_mark_pr_salvaged,
    "close-pr-unsalvageable": cmd_close_pr_unsalvageable,
    "list-unclaimed": cmd_list_unclaimed,
    "queue-depth": cmd_queue_depth,
    "critical-path-depth": cmd_critical_path_depth,
    "claim": cmd_claim,
    "skip": cmd_skip,
    "add-dep": cmd_add_dep,
    "check-blocked": cmd_check_blocked,
    "check-has-pr": cmd_check_has_pr,
    "release-stale-claims": cmd_release_stale_claims,
    "release-orphan-claims": cmd_release_orphan_claims,
    "lock-planner": cmd_lock_planner,
    "unlock-planner": cmd_unlock_planner,
    "lock-status": cmd_lock_status,
    "force-unlock-planner": cmd_force_unlock_planner,
    "return-to-human": cmd_return_to_human,
    "check-return-to-human": cmd_check_return_to_human,
    "clear-return-to-human": cmd_clear_return_to_human,
    "nothing-to-plan": cmd_nothing_to_plan,
    "set-target": cmd_set_target,
    "set-min-queue": cmd_set_min_queue,
    "read-issue": cmd_read_issue,
}


_USAGE = (
    "Usage: coordination "
    "{orient|plan [--label L] [--critical-path]|create-pr|claim-fix|"
    "close-pr|list-pr-repair|list-replan|claim-pr-repair|mark-pr-salvaged|"
    "close-pr-unsalvageable|list-unclaimed [--label L]|queue-depth [L]|"
    "critical-path-depth|claim|skip|add-dep|check-blocked|check-has-pr|"
    "release-stale-claims|release-orphan-claims|lock-planner|unlock-planner|lock-status|"
    "force-unlock-planner|return-to-human|check-return-to-human|"
    "clear-return-to-human|nothing-to-plan|set-target|set-min-queue|"
    "read-issue}"
)


def main(argv: list[str]) -> int:
    if not argv:
        die(f"unknown command: \n{_USAGE}")
    cmd = argv[0]
    rest = list(argv[1:])
    handler = _COMMANDS.get(cmd)
    if handler is None:
        die(f"unknown command: {cmd}\n{_USAGE}")

    _ensure_auth()
    ctx = _build_context()
    _ensure_labels(ctx)
    return handler(ctx, rest)
