"""Clone/fetch each registered tool's repo and run its native setup command.

Usage:
    python bootstrap.py            # clone+setup everything that's configured
    python bootstrap.py --update   # fetch/reset existing clones, then re-setup
    python bootstrap.py --check    # report which tools are out of sync with remote, exit non-zero if any
    python bootstrap.py --product DSWX_S1 --tool accountability
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

import yaml
from rich.console import Console

ROOT = Path(__file__).resolve().parent
REGISTRY_PATH = ROOT / "registry.yaml"
REPOS_DIR = ROOT / "repos"

# Where a tool's venv pip lives, relative to its own directory. Tools that
# lay their venv out differently can set `pip:` in the registry.
DEFAULT_PIP = "venv/bin/pip"

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
    return sorted(child for child in REPOS_DIR.iterdir() if child.is_dir() and (child / ".git").exists())


def run(cmd: str, cwd: Path) -> None:
    console.print(f"  [dim]$ {cmd}[/dim]")
    result = subprocess.run(cmd, cwd=cwd, shell=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {cmd}")


def clone_or_update_repo(repo_url: str, branch: str, dest: Path, update: bool) -> Path:
    # Everything below goes through a shell, and the destination is derived
    # from wherever the operator checked opera-launchpad out — a directory
    # with a space in it would otherwise split into two arguments.
    q_branch, q_url, q_dest = shlex.quote(branch), shlex.quote(repo_url), shlex.quote(str(dest))
    if dest.exists():
        if update:
            console.print(f"[cyan]Updating[/cyan] {dest.relative_to(ROOT)} ({branch})")
            run(f"git fetch origin {q_branch}", cwd=dest)
            run(f"git checkout {q_branch}", cwd=dest)
            run(f"git reset --hard origin/{q_branch}", cwd=dest)
        else:
            console.print(f"[dim]Already cloned[/dim] {dest.relative_to(ROOT)} ({branch})")
    else:
        console.print(f"[green]Cloning[/green] {repo_url} ({branch}) -> {dest.relative_to(ROOT)}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        # --single-branch is safe now that a clone is never shared: this
        # directory only ever holds `branch`, so it is never checked out
        # from under another tool.
        run(f"git clone --branch {q_branch} --single-branch {q_url} {q_dest}", cwd=dest.parent)
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


def bootstrap_tool(product: str, tool_name: str, tool_cfg: dict, update: bool) -> None:
    console.rule(f"[bold]{product} / {tool_name}[/bold]")
    repo_url = tool_cfg["repo"]
    branch = tool_cfg.get("branch", "main")
    path = tool_cfg.get("path", "")

    dest = tool_clone_dir(product, tool_name)
    repo_root = clone_or_update_repo(repo_url, branch, dest, update)
    tool_dir = repo_root / path if path else repo_root

    setup_cmd = tool_cfg.get("setup")
    run_setup(setup_cmd, tool_dir)
    apply_overrides(tool_cfg, tool_dir)
    run_verify(tool_cfg, tool_dir)


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

    for product, tool_name, tool_cfg in targets:
        try:
            bootstrap_tool(product, tool_name, tool_cfg, args.update)
        except RuntimeError as exc:
            console.print(f"[bold red]FAILED[/bold red] {product}/{tool_name}: {exc}")
            return 1

    console.print("\n[bold green]Bootstrap complete.[/bold green]")
    report_legacy_clones()
    return 0


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
