"""Clone/fetch each registered tool's repo and run its native setup command.

Usage:
    python bootstrap.py            # clone+setup everything that's configured
    python bootstrap.py --update   # fetch/reset existing clones, then re-setup
    python bootstrap.py --check    # report which tools are out of sync with remote, exit non-zero if any
    python bootstrap.py --product DSWX_S1 --tool accountability
"""

from __future__ import annotations

import argparse
import configparser
import getpass
import json
import netrc
import os
import shlex
import shutil
import subprocess
import sys
from collections import deque
from pathlib import Path

import yaml
from rich import box
from rich.console import Console
from rich.table import Table

ROOT = Path(__file__).resolve().parent
REGISTRY_PATH = ROOT / "registry.yaml"
REPOS_DIR = ROOT / "repos"

# Several tools (e.g. every opera-sds-pcm cmr_audit script) point at the same,
# large upstream repo on the same branch. Cloning it fresh over the network
# once per tool wastes bandwidth and disk. Instead, each unique repo URL is
# cloned over the network exactly once into this cache, and every tool's own
# working tree (still at `repos/<product>/<tool>/`, still pointed at the real
# remote) is fast-cloned locally from the cache.
REPO_CACHE_DIR = REPOS_DIR / "_cache"

# Where a tool's venv pip lives, relative to its own directory. Tools that
# lay their venv out differently can set `pip:` in the registry.
DEFAULT_PIP = "venv/bin/pip"

# The real (non-test-fixture) MGRS tile collection DB is bundled in
# opera-sds-ops, not opera-sds-pcm — tools that need it (registry:
# `requires_mgrs_db: true`) get a local copy instead of falling back to
# opera-sds-pcm's S3 download path, which needs AWS credentials we don't have.
MGRS_DB_FILENAME = "MGRS_tile_collection_v0.3.sqlite"
MGRS_DB_SOURCE_REPO = "https://github.com/nasa/opera-sds-ops.git"
MGRS_DB_SOURCE_RELPATH = f"accountability_tools/dswx_s1/{MGRS_DB_FILENAME}"

# cmr_audit_disp_s1_static.py's --frame-to-burst-db has no default and no
# documented canonical download source anywhere in opera-sds-pcm or
# opera-sds-ops -- operators are expected to already have their own copy
# from the DISP-S1 processing team. opera-sds-ops does bundle a tiny (5
# frame) *sample* fixture, though, meant for its own testing; better to
# hand that to an operator with no file of their own (with results
# clearly caveated) than leave a required field nobody can ever fill in.
# See `requires_frame_to_burst_sample: true` / `ensure_frame_to_burst_sample`.
FRAME_TO_BURST_SAMPLE_FILENAME = "opera-s1-disp-frame-to-burst-sample.json"
FRAME_TO_BURST_SAMPLE_SOURCE_RELPATH = f"opera-audit/data/{FRAME_TO_BURST_SAMPLE_FILENAME}"

# The EDL host tools authenticate against via ~/.netrc (registry:
# `requires_edl: true`). See `ensure_edl_credentials`.
EDL_HOST = "urs.earthdata.nasa.gov"

# Dedicated profile name for the s3://opera-ancillaries AWS credentials some
# tools need (registry: `requires_aws_ancillaries: true`), written to
# ~/.aws/credentials under this name rather than [default] so operators who
# already have their own AWS setup for other work aren't affected — cli.py
# only passes AWS_PROFILE=this to the tool's own subprocess, not the
# operator's shell. See `ensure_aws_credentials`.
AWS_ANCILLARIES_PROFILE = "opera-ancillaries"

# Dropped into a tool's own directory once its repo is cloned, recording
# whether setup (run_setup/apply_overrides/run_verify) actually finished
# clean. tool_dir existing on its own only means "cloned"; a tool whose
# setup died partway (e.g. a missing credential) still has a tool_dir, so
# cli.py needs this to tell the two apart. See `write_setup_status` /
# `read_setup_status`.
SETUP_STATUS_FILENAME = ".launchpad_setup_status.json"

console = Console()


def load_registry() -> dict:
    with open(REGISTRY_PATH) as fp:
        return yaml.safe_load(fp)


def tool_clone_dir(product: str, tool_name: str) -> Path:
    """Every tool gets its own clone, at `repos/<product>/<tool>/`.

    Keying on the tool rather than the repo is what lets several tools from
    one repo sit on different branches at the same time. A git checkout
    swaps the whole working tree, so tools sharing a clone can only ever be
    on one branch between them — whichever was bootstrapped last — no
    matter how their `path` subdirectories differ. Separate clones mean a
    tool pinned to a feature branch can't drag everyone else onto it.

    Mirrors the `output/<product>/<tool>/` layout, and product+tool is the
    registry's own key, so the paths can't collide.
    """
    return REPOS_DIR / product / tool_name


def is_configured(tool_cfg: dict | None) -> bool:
    """A tool entry is usable only once it names the repo it lives in."""
    return bool(tool_cfg) and bool(tool_cfg.get("repo"))


def iter_configured_tools(registry: dict):
    """Yield (product, tool_name, tool_cfg) for every tool with a real repo configured."""
    for product, product_cfg in (registry.get("products") or {}).items():
        for tool_name, tool_cfg in (product_cfg.get("tools") or {}).items():
            if is_configured(tool_cfg):
                yield product, tool_name, tool_cfg


def find_legacy_clones() -> list[Path]:
    """Clones left over from when `repos/` held one clone per *repo*.

    Those sat at `repos/<repo-name>/`; tool clones now live one level
    deeper at `repos/<product>/<tool>/`. Nothing reads the old ones any
    more, and they are big, so point them out rather than leaving the
    operator to wonder.
    """
    if not REPOS_DIR.exists():
        return []
    return sorted(
        child
        for child in REPOS_DIR.iterdir()
        if child.is_dir() and child != REPO_CACHE_DIR and (child / ".git").exists()
    )


RUN_ERROR_TAIL_LINES = 8


def run(cmd: str, cwd: Path) -> None:
    """Runs cmd via shell, streaming its output live (setup/pip install
    commands can run for minutes and an operator watching wants to see
    progress) while also keeping the last few lines around. On failure,
    that tail goes into the RuntimeError so a final summary can show more
    than just "Command failed (1): <cmd>" for something that failed deep
    inside a long pip install.
    """
    console.print(f"  [dim]$ {cmd}[/dim]")
    # OPERA_LAUNCHPAD_ROOT lets setup/override commands (which run with cwd
    # set to the tool's own directory, at varying depth) reference shared
    # helpers like scripts/ensure_gdal.sh by absolute path.
    env = {**os.environ, "OPERA_LAUNCHPAD_ROOT": str(ROOT)}
    tail: deque[str] = deque(maxlen=RUN_ERROR_TAIL_LINES)
    process = subprocess.Popen(
        cmd, cwd=cwd, shell=True, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    )
    assert process.stdout is not None
    for line in process.stdout:
        sys.stdout.write(line)
        tail.append(line.rstrip())
    process.wait()
    if process.returncode != 0:
        detail = "\n".join(tail)
        raise RuntimeError(f"Command failed ({process.returncode}): {cmd}" + (f"\n{detail}" if detail else ""))


def repo_slug(repo_url: str) -> str:
    name = repo_url.rstrip("/").rsplit("/", 1)[-1]
    return name[:-4] if name.endswith(".git") else name


def ensure_repo_cache(repo_url: str, update: bool) -> Path:
    """One network clone per unique repo URL, shared by every tool that names it.

    Cloned with full history/branches (no --single-branch) since different
    tools sharing a repo may pin different branches. Individual tool clones
    are then fast local clones off this cache (see `clone_or_update_repo`).
    """
    cache_dest = REPO_CACHE_DIR / repo_slug(repo_url)
    q_url, q_dest = shlex.quote(repo_url), shlex.quote(str(cache_dest))
    if cache_dest.exists():
        if update:
            console.print(f"[cyan]Updating cache[/cyan] {cache_dest.relative_to(ROOT)}")
            run("git fetch --all --prune", cwd=cache_dest)
    else:
        console.print(f"[green]Caching[/green] {repo_url} -> {cache_dest.relative_to(ROOT)}")
        cache_dest.parent.mkdir(parents=True, exist_ok=True)
        run(f"git clone {q_url} {q_dest}", cwd=cache_dest.parent)
    return cache_dest


def ensure_mgrs_db(tool_dir: Path) -> None:
    """Drop a local copy of the real MGRS tile collection DB into a tool's dir.

    Several opera-sds-pcm tools import `rtc.mgrs_bursts_collection_db_client`,
    which looks for this file at `~/Downloads/MGRS_tile_collection_v0.3.sqlite`
    (or `$MGRS_TILE_COLLECTION_DB_FILEPATH`) and, failing that, downloads it
    from an internal S3 bucket — which needs AWS credentials operators may not
    have configured. opera-sds-ops already bundles the real DB (not just the
    small test fixture opera-sds-pcm ships), so cache that repo (cheap: it's
    already cached if any `duplicates`/`accountability` tool has been
    bootstrapped) and copy it in locally instead. `cli.py` points
    `MGRS_TILE_COLLECTION_DB_FILEPATH` at this copy when it runs the tool.
    """
    cache_dir = ensure_repo_cache(MGRS_DB_SOURCE_REPO, update=False)
    source = cache_dir / MGRS_DB_SOURCE_RELPATH
    if not source.exists():
        console.print(f"  [yellow]warning:[/yellow] {source.relative_to(ROOT)} not found, skipping MGRS DB copy")
        return
    dest = tool_dir / MGRS_DB_FILENAME
    shutil.copy2(source, dest)
    console.print(f"  [dim]copied {MGRS_DB_FILENAME} -> {dest.relative_to(ROOT)}[/dim]")


def ensure_frame_to_burst_sample(tool_dir: Path) -> None:
    """Drop opera-sds-ops's tiny (5 frame) sample frame-to-burst fixture into
    a tool's dir, purely so `--frame-to-burst-db` has *something* usable as
    a registry default instead of leaving operators stuck on a required
    field with no file of their own and nowhere to get one. cli.py's help
    text for that param spells out clearly that this is only a sample.
    """
    cache_dir = ensure_repo_cache(MGRS_DB_SOURCE_REPO, update=False)
    source = cache_dir / FRAME_TO_BURST_SAMPLE_SOURCE_RELPATH
    if not source.exists():
        console.print(
            f"  [yellow]warning:[/yellow] {source.relative_to(ROOT)} not found, "
            "skipping frame-to-burst sample copy"
        )
        return
    dest = tool_dir / FRAME_TO_BURST_SAMPLE_FILENAME
    shutil.copy2(source, dest)
    console.print(f"  [dim]copied {FRAME_TO_BURST_SAMPLE_FILENAME} -> {dest.relative_to(ROOT)}[/dim]")


def _read_netrc_hosts() -> dict:
    """All entries currently in ~/.netrc, keyed by host: {host: (login, account, password)}."""
    netrc_path = Path.home() / ".netrc"
    if not netrc_path.exists():
        return {}
    try:
        return dict(netrc.netrc(str(netrc_path)).hosts or {})
    except (netrc.NetrcParseError, OSError):
        return {}


def edl_netrc_login() -> str | None:
    """The username in ~/.netrc's existing EDL_HOST entry, or None if there isn't one."""
    auth = _read_netrc_hosts().get(EDL_HOST)
    return auth[0] if auth else None


def _replace_netrc_entry(host: str, username: str, password: str) -> None:
    """Overwrite (or add) one host's entry in ~/.netrc, leaving every other
    host's entry untouched. Python's `netrc` module has no writer, so this
    rebuilds the file from its own parsed representation — which loses any
    comments/formatting, but every entry's data is preserved.
    """
    entries = _read_netrc_hosts()
    entries[host] = (username, None, password)
    lines = []
    for machine, (login, account, pw) in entries.items():
        lines.append(f"machine {machine}")
        lines.append(f"  login {login}")
        if account:
            lines.append(f"  account {account}")
        lines.append(f"  password {pw}")
    netrc_path = Path.home() / ".netrc"
    netrc_path.write_text("\n".join(lines) + "\n")
    netrc_path.chmod(0o600)


def prompt_and_save_edl_credentials() -> None:
    """Show whatever EDL credentials ~/.netrc already has (if any), let the
    operator keep or replace them, and save to ~/.netrc — the one place
    every tool that needs EDL auth (via Python's stdlib `netrc` module)
    actually looks; it doesn't honor a `NETRC` env var, so there's no
    project-local alternative to writing there.
    """
    console.print("\n[bold]Some selected tools authenticate to NASA Earthdata (EDL) via ~/.netrc.[/bold]")

    existing_login = edl_netrc_login()
    if existing_login:
        console.print(f"[dim]~/.netrc already has an entry for {EDL_HOST} (login: {existing_login}).[/dim]")
        try:
            answer = input("Replace it with new credentials? [y/N]: ").strip().lower()
        except EOFError:
            answer = ""
        if answer not in ("y", "yes"):
            console.print("[dim]Keeping existing ~/.netrc credentials.[/dim]\n")
            return
    else:
        console.print(f"[dim]No existing ~/.netrc entry for {EDL_HOST}.[/dim]")

    console.print("[dim]Leave the username blank to skip (tools needing EDL will run with degraded/partial results).[/dim]")
    try:
        username = input("EDL username: ").strip()
    except EOFError:
        username = ""
    if not username:
        console.print("[yellow]Skipped EDL setup.[/yellow]\n")
        return
    password = getpass.getpass("EDL password: ")
    if not password:
        console.print("[yellow]Skipped EDL setup (no password entered).[/yellow]\n")
        return

    _replace_netrc_entry(EDL_HOST, username, password)
    console.print(f"[green]Saved EDL credentials to {Path.home() / '.netrc'}[/green]\n")


def ensure_edl_credentials(targets: list[tuple[str, str, dict]]) -> None:
    if not any(tool_cfg.get("requires_edl") for _, _, tool_cfg in targets):
        return
    try:
        prompt_and_save_edl_credentials()
    except (EOFError, KeyboardInterrupt):
        console.print("\n[yellow]Skipped EDL setup (non-interactive or cancelled).[/yellow]")


def prompt_and_save_aws_credentials() -> None:
    """Show whether ~/.aws/credentials already has an AWS_ANCILLARIES_PROFILE
    entry, let the operator keep or replace it, and save there — the
    standard place boto3's default credential chain looks for a named
    profile (via AWS_PROFILE, which cli.py sets for the tool's subprocess
    only). Written under its own profile rather than [default] so this
    never touches credentials an operator already has configured for
    other, unrelated AWS work.
    """
    console.print(
        "\n[bold]Some selected tools fetch ancillary data (e.g. the DIST-S1 burst lookup\n"
        "table) from a private s3://opera-ancillaries/... bucket.[/bold]\n"
        "[dim]Without credentials for it, they either crash outright or silently\n"
        "degrade to an empty result after ~40s of doomed retries.[/dim]"
    )

    aws_creds_path = Path.home() / ".aws" / "credentials"
    parser = configparser.ConfigParser()
    if aws_creds_path.exists():
        parser.read(aws_creds_path)
    if parser.has_section(AWS_ANCILLARIES_PROFILE):
        console.print(f"[dim]~/.aws/credentials already has a '{AWS_ANCILLARIES_PROFILE}' profile.[/dim]")
        try:
            answer = input("Replace it with new credentials? [y/N]: ").strip().lower()
        except EOFError:
            answer = ""
        if answer not in ("y", "yes"):
            console.print("[dim]Keeping existing AWS credentials.[/dim]\n")
            return
    else:
        console.print(f"[dim]No existing '{AWS_ANCILLARIES_PROFILE}' profile in ~/.aws/credentials.[/dim]")

    console.print("[dim]Leave the access key blank to skip (those tools will crash/degrade instead).[/dim]")
    try:
        access_key_id = input("AWS access key ID: ").strip()
    except EOFError:
        access_key_id = ""
    if not access_key_id:
        console.print("[yellow]Skipped AWS credentials setup.[/yellow]\n")
        return
    secret_access_key = getpass.getpass("AWS secret access key: ")
    if not secret_access_key:
        console.print("[yellow]Skipped AWS credentials setup (no secret key entered).[/yellow]\n")
        return
    session_token = getpass.getpass("AWS session token (optional, for temporary credentials, Enter to skip): ")

    parser[AWS_ANCILLARIES_PROFILE] = {
        "aws_access_key_id": access_key_id,
        "aws_secret_access_key": secret_access_key,
        **({"aws_session_token": session_token} if session_token else {}),
    }
    aws_creds_path.parent.mkdir(parents=True, exist_ok=True)
    with open(aws_creds_path, "w") as fp:
        parser.write(fp)
    aws_creds_path.chmod(0o600)
    console.print(f"[green]Saved AWS credentials to {aws_creds_path} under '{AWS_ANCILLARIES_PROFILE}'.[/green]\n")


def ensure_aws_credentials(targets: list[tuple[str, str, dict]]) -> None:
    if not any(tool_cfg.get("requires_aws_ancillaries") for _, _, tool_cfg in targets):
        return
    try:
        prompt_and_save_aws_credentials()
    except (EOFError, KeyboardInterrupt):
        console.print("\n[yellow]Skipped AWS credentials setup (non-interactive or cancelled).[/yellow]")


def ensure_git_oauth_token(targets: list[tuple[str, str, dict]]) -> None:
    """Ask for a JPL GitHub Enterprise personal access token once, up front,
    for tools that need it (registry: `requires_git_oauth_token: true`) to
    clone private repos like pcm_commons (see scripts/ensure_pcm_commons.sh).

    Only kept in this process's environment for the current run (`run()`
    copies os.environ into every setup/override subprocess) — never written
    to disk, since unlike EDL there's no on-disk convention (~/.netrc) for
    it to also benefit tools invoked outside opera-launchpad.
    """
    if not any(tool_cfg.get("requires_git_oauth_token") for _, _, tool_cfg in targets):
        return
    if os.environ.get("GIT_OAUTH_TOKEN"):
        console.print("[dim]GIT_OAUTH_TOKEN already set in the environment — reusing it.[/dim]\n")
        return

    console.print(
        "\n[bold]Some selected tools need a JPL GitHub Enterprise personal access token[/bold]\n"
        "[dim]to clone private repos (e.g. pcm_commons, github.jpl.nasa.gov/IEMS-SDS/pcm_commons).\n"
        "Generate one at https://github.jpl.nasa.gov/settings/tokens (read access to that repo).\n"
        "Leave blank to skip (those tools' setup will fail with a clear error instead).[/dim]"
    )
    try:
        token = getpass.getpass("GIT_OAUTH_TOKEN (input hidden): ").strip()
    except (EOFError, KeyboardInterrupt):
        token = ""
    if not token:
        console.print("[yellow]Skipped — GIT_OAUTH_TOKEN not set for this run.[/yellow]\n")
        return
    os.environ["GIT_OAUTH_TOKEN"] = token
    console.print("[green]GIT_OAUTH_TOKEN set for this run.[/green]\n")


def clone_or_update_repo(repo_url: str, branch: str, dest: Path, update: bool) -> Path:
    # Everything below goes through a shell, and the destination is derived
    # from wherever the operator checked opera-launchpad out — a directory
    # with a space in it would otherwise split into two arguments.
    q_branch, q_dest = shlex.quote(branch), shlex.quote(str(dest))
    if dest.exists():
        if update:
            console.print(f"[cyan]Updating[/cyan] {dest.relative_to(ROOT)} ({branch})")
            run(f"git fetch origin {q_branch}", cwd=dest)
            run(f"git checkout {q_branch}", cwd=dest)
            run(f"git reset --hard origin/{q_branch}", cwd=dest)
        else:
            console.print(f"[dim]Already cloned[/dim] {dest.relative_to(ROOT)} ({branch})")
        return dest

    cache_dir = ensure_repo_cache(repo_url, update)
    q_cache = shlex.quote(str(cache_dir))
    console.print(
        f"[green]Cloning[/green] {repo_url} ({branch}) -> {dest.relative_to(ROOT)} [dim](from local cache)[/dim]"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    # --single-branch is safe now that a clone is never shared: this
    # directory only ever holds `branch`, so it is never checked out
    # from under another tool. Cloning from the local cache instead of the
    # network is fast (same-filesystem hardlinks); the remote is then
    # repointed at the real repo so future fetches/updates and
    # `check_repo_sync` behave exactly as if it had been cloned from there
    # directly.
    run(f"git clone --branch {q_branch} --single-branch {q_cache} {q_dest}", cwd=dest.parent)
    run(f"git remote set-url origin {shlex.quote(repo_url)}", cwd=dest)
    return dest


def check_repo_sync(repo_dir: Path, branch: str) -> dict:
    """Fetch from origin and report whether the local clone is behind/ahead.

    Returns a dict: {"cloned": bool, "up_to_date": bool, "behind": int, "ahead": int, "error": str | None}
    """
    if not repo_dir.exists():
        return {"cloned": False, "up_to_date": False, "behind": 0, "ahead": 0, "error": None}

    try:
        subprocess.run(
            ["git", "fetch", "--quiet", "origin", branch],
            cwd=repo_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        counts = subprocess.run(
            ["git", "rev-list", "--left-right", "--count", f"HEAD...origin/{branch}"],
            cwd=repo_dir,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        ahead_str, behind_str = counts.split()
        ahead, behind = int(ahead_str), int(behind_str)
        return {
            "cloned": True,
            "up_to_date": behind == 0,
            "behind": behind,
            "ahead": ahead,
            "error": None,
        }
    except subprocess.CalledProcessError as exc:
        return {
            "cloned": True,
            "up_to_date": False,
            "behind": 0,
            "ahead": 0,
            "error": (exc.stderr or str(exc)).strip(),
        }


def run_setup(setup_cmd: str, tool_dir: Path) -> None:
    if not setup_cmd:
        return
    console.print(f"[yellow]Setting up[/yellow] {tool_dir.relative_to(ROOT)}")
    run(setup_cmd, cwd=tool_dir)


def apply_overrides(tool_cfg: dict, tool_dir: Path) -> None:
    """Apply local environment fixes on top of the tool's own setup.

    Escape hatch for when a tool's upstream install isn't quite right and we
    don't want to fork it: missing packages in `requirements.txt`, a version
    that needs pinning locally, or an extra command that has to run.
    Registry shape:

        overrides:
          reason: "why this is needed (shown at install time)"
          pip_install: ["pandas>=2.0", "urllib3<2"]
          post_setup: ["venv/bin/python -m spacy download en"]
    """
    overrides = tool_cfg.get("overrides") or {}
    if not overrides:
        return

    console.print(f"[magenta]Applying overrides[/magenta] {tool_dir.relative_to(ROOT)}")
    if reason := overrides.get("reason"):
        console.print(f"  [dim]reason: {reason}[/dim]")

    if packages := overrides.get("pip_install"):
        pip = tool_cfg.get("pip", DEFAULT_PIP)
        specs = " ".join(shlex.quote(str(p)) for p in packages)
        run(f"{pip} install {specs}", cwd=tool_dir)

    for cmd in overrides.get("post_setup") or []:
        run(cmd, cwd=tool_dir)


def run_verify(tool_cfg: dict, tool_dir: Path) -> bool:
    """Optional smoke test proving the tool's env is actually usable.

    This is what catches "upstream forgot a dependency" right at install
    time instead of halfway through an operator's run.
    """
    verify_cmd = tool_cfg.get("verify")
    if not verify_cmd:
        return True

    console.print(f"  [dim]$ {verify_cmd}[/dim]")
    result = subprocess.run(verify_cmd, cwd=tool_dir, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        console.print("  [green]verify ok[/green]")
        return True

    detail = (result.stderr or result.stdout or "").strip().splitlines()
    console.print(f"  [bold red]verify failed[/bold red] (exit {result.returncode})")
    for line in detail[-5:]:
        console.print(f"    [red]{line}[/red]")
    console.print(
        "  [yellow]hint:[/yellow] if a dependency is missing upstream, add it under this tool's\n"
        "        [bold]overrides.pip_install[/bold] in registry.yaml and rerun setup."
    )
    return False


def check_all_out_of_sync(registry: dict, targets: list[tuple[str, str, dict]] | None = None) -> list[dict]:
    """Check each configured tool's repo against its remote branch.

    Checks every configured tool unless `targets` narrows it to a subset
    (as produced by `select_targets`).

    Returns a list of dicts (one per out-of-date/uncloned tool):
    {"product": str, "tool": str, "status": "missing"|"behind"|"error", "behind": int, "error": str | None}
    """
    results = []

    # No caching by repo any more: each tool owns its clone, so each one
    # has to be checked against its own branch.
    for product, tool_name, tool_cfg in targets if targets is not None else iter_configured_tools(registry):
        branch = tool_cfg.get("branch", "main")
        sync = check_repo_sync(tool_clone_dir(product, tool_name), branch)

        if not sync["cloned"]:
            results.append({"product": product, "tool": tool_name, "status": "missing", "behind": 0, "error": None})
        elif sync["error"]:
            results.append(
                {"product": product, "tool": tool_name, "status": "error", "behind": 0, "error": sync["error"]}
            )
        elif not sync["up_to_date"]:
            results.append(
                {"product": product, "tool": tool_name, "status": "behind", "behind": sync["behind"], "error": None}
            )

    return results


def select_targets(registry: dict, product: str | None, tool: str | None) -> list[tuple[str, str, dict]]:
    targets = list(iter_configured_tools(registry))
    if product:
        targets = [t for t in targets if t[0] == product]
    if tool:
        targets = [t for t in targets if t[1] == tool]
    return targets


def write_setup_status(tool_dir: Path, ok: bool, detail: str) -> None:
    """Best-effort; a missing/stale status file just means cli.py falls
    back to assuming setup finished fine (matching pre-existing behavior).
    """
    try:
        (tool_dir / SETUP_STATUS_FILENAME).write_text(json.dumps({"ok": ok, "detail": detail}))
    except OSError:
        pass


def read_setup_status(tool_dir: Path) -> dict | None:
    path = tool_dir / SETUP_STATUS_FILENAME
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def bootstrap_tool(product: str, tool_name: str, tool_cfg: dict, update: bool) -> None:
    console.rule(f"[bold]{product} / {tool_name}[/bold]")
    repo_url = tool_cfg["repo"]
    branch = tool_cfg.get("branch", "main")
    path = tool_cfg.get("path", "")

    dest = tool_clone_dir(product, tool_name)
    repo_root = clone_or_update_repo(repo_url, branch, dest, update)
    tool_dir = repo_root / path if path else repo_root

    try:
        setup_cmd = tool_cfg.get("setup")
        run_setup(setup_cmd, tool_dir)
        apply_overrides(tool_cfg, tool_dir)
        if tool_cfg.get("requires_mgrs_db"):
            ensure_mgrs_db(tool_dir)
        if tool_cfg.get("requires_frame_to_burst_sample"):
            ensure_frame_to_burst_sample(tool_dir)
        # run_verify is intentionally non-fatal (returns bool, doesn't
        # raise) -- setup/apply_overrides having succeeded is still worth
        # recording even if the smoke test itself failed.
        verify_ok = run_verify(tool_cfg, tool_dir)
    except RuntimeError as exc:
        write_setup_status(tool_dir, ok=False, detail=str(exc))
        raise
    else:
        write_setup_status(
            tool_dir, ok=verify_ok, detail="" if verify_ok else "verify failed after setup — see verify: above"
        )


def setup_summary_table(results: list[tuple[str, str, bool, str]]) -> Table:
    """Final per-tool setup outcome table: one row each, ✓/✗ + the
    RuntimeError detail (now including a tail of the command's own output,
    see `run`) for anything that failed.
    """
    succeeded = sum(1 for _, _, ok, _ in results if ok)
    table = Table(
        title=f"setup summary \u00b7 {succeeded}/{len(results)} succeeded",
        title_justify="left",
        box=box.SIMPLE_HEAVY,
        header_style="bold",
        padding=(0, 2),
        show_lines=True,
    )
    table.add_column(" ", justify="center", no_wrap=True)
    table.add_column("product", style="bold", no_wrap=True)
    table.add_column("tool", no_wrap=True)
    table.add_column("detail", overflow="fold")

    for product, tool_name, ok, detail in results:
        mark = "[green]\u2713[/green]" if ok else "[red]\u2717[/red]"
        table.add_row(mark, product, tool_name, "" if ok else f"[red]{detail}[/red]")
    return table


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap opera-launchpad tool repos + envs.")
    parser.add_argument("--update", action="store_true", help="Fetch/reset existing clones and rerun setup.")
    parser.add_argument("--product", help="Only bootstrap this product.")
    parser.add_argument("--tool", help="Only bootstrap this tool (requires --product).")
    parser.add_argument(
        "--check", action="store_true", help="Only check repos against remote, don't clone/setup anything."
    )
    args = parser.parse_args()

    registry = load_registry()
    targets = select_targets(registry, args.product, args.tool)
    if not targets:
        console.print("[red]No configured tools matched.[/red]")
        return 1

    if args.check:
        out_of_sync = check_all_out_of_sync(registry, targets)
        if not out_of_sync:
            console.print("[bold green]Everything up to date.[/bold green]")
            return 0
        for item in out_of_sync:
            if item["status"] == "missing":
                console.print(f"[red]{item['product']}/{item['tool']}[/red]: not cloned yet")
            elif item["status"] == "error":
                console.print(f"[red]{item['product']}/{item['tool']}[/red]: check failed ({item['error']})")
            else:
                console.print(
                    f"[yellow]{item['product']}/{item['tool']}[/yellow]: {item['behind']} commit(s) behind remote"
                )
        console.print("\nRun [bold]./setup.sh --update[/bold] to sync.")
        return 1

    ensure_edl_credentials(targets)
    ensure_git_oauth_token(targets)
    ensure_aws_credentials(targets)

    # One tool's setup failing (e.g. a private-repo credential missing, a
    # transient network blip) shouldn't block every other tool from getting
    # set up — keep going and report every target's outcome at the end.
    results: list[tuple[str, str, bool, str]] = []
    for product, tool_name, tool_cfg in targets:
        try:
            bootstrap_tool(product, tool_name, tool_cfg, args.update)
            results.append((product, tool_name, True, ""))
        except RuntimeError as exc:
            console.print(f"[bold red]FAILED[/bold red] {product}/{tool_name}: {exc}\n")
            results.append((product, tool_name, False, str(exc)))

    console.print()
    console.print(setup_summary_table(results))
    report_legacy_clones()
    return 1 if any(not ok for _, _, ok, _ in results) else 0


def report_legacy_clones() -> None:
    legacy = find_legacy_clones()
    if not legacy:
        return
    console.print(
        "\n[yellow]Note:[/yellow] these are clones from the old one-per-repo layout "
        "and are no longer used by anything:"
    )
    for path in legacy:
        console.print(f"  {path.relative_to(ROOT)}")
    console.print("[dim]Delete them when you're satisfied the new clones work.[/dim]")


if __name__ == "__main__":
    sys.exit(main())
