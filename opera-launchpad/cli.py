"""opera-launchpad interactive CLI.

Select a product -> a tool (accountability/duplicates/...) -> fill in
params -> run the real, unmodified tool in its cloned repo + venv.

Usage:
    python cli.py
"""

from __future__ import annotations

import shlex
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

import questionary
from rich import box
from rich.align import Align
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from bootstrap import (
    ROOT,
    check_all_out_of_sync,
    is_configured,
    load_registry,
    tool_clone_dir,
)

console = Console()

QMARK = "❯"
ACCENT = "#00d7ff"
ACCENT_ALT = "#7f8bff"
MUTED = "#5f6672"
OK = "#3ddc84"
WARN = "#ffb454"
BAD = "#ff5f6b"

# Vertical gradient applied to the banner, top row -> bottom row.
GRADIENT = ["#00e5ff", "#00d7ff", "#28bdff", "#5ba4ff", "#7f8bff", "#9a7bff"]

STYLE = questionary.Style(
    [
        ("qmark", f"fg:{ACCENT} bold"),
        ("question", "bold"),
        ("answer", f"fg:{ACCENT} bold"),
        ("pointer", f"fg:{ACCENT} bold"),
        ("highlighted", f"fg:{ACCENT} bold"),
        ("selected", f"fg:{ACCENT}"),
        ("separator", f"fg:{MUTED}"),
        ("disabled", f"fg:{MUTED} italic"),
        ("instruction", "fg:#767676 italic"),
        ("text", ""),
    ]
)


def _human_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def tool_dir_for(product: str, tool_name: str, tool_cfg: dict) -> Path:
    """The directory the tool's command actually runs in.

    Its clone is keyed on product+tool, not on the repo, so each tool stays
    on its own branch; `path` then points at the subdirectory within it.
    """
    repo_root = tool_clone_dir(product, tool_name)
    path = tool_cfg.get("path", "")
    return repo_root / path if path else repo_root


OUTPUT_DIR = ROOT / "output"
RUN_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"


def run_output_dir(product: str, tool_name: str) -> Path:
    """Return output/<product>/<tool>/<timestamp>/ for this run.

    Deliberately does not create the directory: `collect_outputs` makes it
    on the first file it actually copies. Runs the operator backs out of at
    the confirm prompt — or that write nothing — leave no empty directory
    behind to sift through later.
    """
    timestamp = datetime.now().strftime(RUN_TIMESTAMP_FORMAT)
    return OUTPUT_DIR / product / tool_name / timestamp


DATETIME_ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Accepted lenient input formats, in order of preference, mapped to how
# missing components should be filled in before reformatting to
# DATETIME_ISO_FORMAT.
_LENIENT_DATETIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d",
]


def normalize_datetime_iso(raw: str) -> str | None:
    """Best-effort parse of a user-entered date/time into DATETIME_ISO_FORMAT.

    Accepts missing time components (defaults to 00:00:00) and a missing
    trailing 'Z' or 'T' separator. Returns None if nothing could be parsed.
    """
    raw = raw.strip()
    for fmt in _LENIENT_DATETIME_FORMATS:
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.strftime(DATETIME_ISO_FORMAT)
        except ValueError:
            continue
    return None


def prompt_param(param: dict) -> str | None:
    """Prompt the user for a single param, returns the raw string value or None if skipped.

    Defaults are always the tool's own native defaults (unmodified) — the
    registry's `is_output` flag on a param just marks where that tool
    natively writes its output, so it can be copied into the run's output
    directory afterward. See `resolve_native_outputs`.

    Prompts use `unsafe_ask()` so a ctrl-c propagates as KeyboardInterrupt
    and unwinds the whole run. Plain `ask()` swallows it and returns None,
    which is indistinguishable from "left blank" — an operator bailing out
    partway through would just get marched through every remaining prompt.
    """
    name = param["name"]
    ptype = param.get("type", "str")
    default = param.get("default")
    required = param.get("required", False)
    help_text = param.get("help", "")

    label = f"{name}"
    if help_text:
        label += f"  ({help_text})"
    if default is not None:
        label += f"  [default: {default}]"
    if required:
        label += "  *required"

    if ptype == "bool":
        return "true" if questionary.confirm(label, default=bool(default), style=STYLE).unsafe_ask() else None

    if ptype == "choice":
        choices = param.get("choices", [])
        return questionary.select(label, choices=choices, default=default, style=STYLE).unsafe_ask()

    if ptype == "datetime_iso":
        while True:
            raw = questionary.text(label, default=default or "", style=STYLE).unsafe_ask()
            if not raw:
                if required:
                    console.print("[red]This field is required.[/red]")
                    continue
                return None
            normalized = normalize_datetime_iso(raw)
            if normalized is not None:
                if normalized != raw:
                    console.print(f"[dim]Interpreted as {normalized}[/dim]")
                return normalized
            console.print("[red]Could not parse date/time. Expected format: YYYY-MM-DDTHH:MM:SSZ[/red]")

    # path / str / int fall through to plain text prompt
    raw = questionary.text(label, default=str(default) if default is not None else "", style=STYLE).unsafe_ask()
    if not raw:
        if required:
            console.print("[red]This field is required, please try again.[/red]")
            return prompt_param(param)
        return None
    return raw


def build_flags(params: list[dict]) -> tuple[list[str], dict[str, str]]:
    """Prompt for each param, returning (cli flags, {param_name: value})
    for every param that produced a value (skips bool flags in the values map
    unless True, and skips params the user left blank).
    """
    flags: list[str] = []
    values: dict[str, str] = {}
    for param in params:
        value = prompt_param(param)
        if value is None:
            continue
        values[param["name"]] = value
        flags.append(param["flag"])
        # bool params map to store_true flags, which take no value.
        if param.get("type") != "bool":
            flags.append(value)
    return flags, values


def params_summary_panel(params: list[dict], values: dict[str, str]) -> Panel | None:
    """Recap of everything the user just entered, before the command runs."""
    if not values:
        return None
    grid = Table.grid(padding=(0, 3))
    grid.add_column(justify="right", style=MUTED, no_wrap=True)
    grid.add_column(style=f"bold {ACCENT}")
    for param in params:
        name = param["name"]
        if name in values:
            grid.add_row(name.replace("_", " "), values[name])
    return Panel(
        grid,
        title=f"[bold {MUTED}]parameters[/bold {MUTED}]",
        border_style=MUTED,
        box=box.ROUNDED,
        padding=(1, 2),
    )


def command_panel(command: list[str], cwd: Path) -> Panel:
    """Render the command with flags highlighted so it's easy to scan."""
    text = Text()
    for i, part in enumerate(command):
        quoted = shlex.quote(part)
        if i == 0:
            style = f"bold {ACCENT_ALT}"
        elif part.startswith("-"):
            style = f"bold {ACCENT}"
        else:
            style = "white"
        text.append(quoted, style=style)
        text.append(" ")
    try:
        shown_cwd = cwd.relative_to(ROOT)
    except ValueError:
        shown_cwd = cwd
    return Panel(
        text,
        title="[bold]command[/bold]",
        subtitle=f"[{MUTED}]in {shown_cwd}[/{MUTED}]",
        subtitle_align="right",
        border_style=ACCENT,
        box=box.ROUNDED,
        padding=(1, 2),
    )


def confirm_and_run(command: list[str], cwd: Path) -> bool:
    console.print(command_panel(command, cwd))
    if not questionary.confirm("Run this command?", default=True, style=STYLE, qmark=QMARK).unsafe_ask():
        console.print(f"[{WARN}]\u25cb Skipped.[/{WARN}]\n")
        return False

    console.print()
    console.rule(f"[{MUTED}]live output[/{MUTED}]", style=MUTED)
    started = datetime.now()
    result = subprocess.run(command, cwd=cwd)
    elapsed = datetime.now() - started
    console.rule(style=MUTED)

    duration = f"{elapsed.total_seconds():.1f}s"
    if result.returncode != 0:
        console.print(
            f"[bold {BAD}]\u2717 exited with code {result.returncode}[/bold {BAD}] "
            f"[{MUTED}]after {duration}[/{MUTED}]\n"
        )
        return False
    console.print(f"[bold {OK}]\u2713 completed[/bold {OK}] [{MUTED}]in {duration}[/{MUTED}]\n")
    return True


def not_installed_panel(product: str, tool_name: str, tool_dir: Path) -> Panel:
    body = Text()
    body.append(f"{tool_dir}\n", style="bold")
    body.append("does not exist yet.\n\n", style=MUTED)
    body.append("Install it with\n", style=MUTED)
    body.append(f"  ./setup.sh --product {product} --tool {tool_name}", style=f"bold {ACCENT}")
    return Panel(
        body,
        title=f"[bold {BAD}]not installed[/bold {BAD}]",
        border_style=BAD,
        box=box.ROUNDED,
        padding=(1, 2),
    )


def output_summary_panel(out_dir: Path) -> Panel:
    files = sorted((p for p in out_dir.iterdir() if p.is_file()), key=lambda p: p.name) if out_dir.exists() else []

    body = Text()
    try:
        shown = out_dir.relative_to(ROOT)
    except ValueError:
        shown = out_dir
    body.append(f"{shown}\n", style=f"bold {ACCENT}")

    if files:
        body.append("\n")
        total = 0
        for i, path in enumerate(files):
            size = path.stat().st_size
            total += size
            connector = "\u2514\u2500" if i == len(files) - 1 else "\u251c\u2500"
            body.append(f" {connector} ", style=MUTED)
            body.append(path.name, style="white")
            body.append(f"  {_human_size(size)}\n", style=MUTED)
        body.append(f"\n {len(files)} file(s) \u00b7 {_human_size(total)}", style=f"italic {MUTED}")
    else:
        body.append("\n(nothing was written here)", style=f"italic {MUTED}")

    return Panel(
        body,
        title=f"[bold {OK}]output[/bold {OK}]",
        border_style=OK,
        box=box.ROUNDED,
        padding=(1, 2),
    )


def section_header(product: str, tool_name: str) -> None:
    """Breadcrumb rule marking the start of a run."""
    console.print()
    console.rule(
        f"[bold {ACCENT}]{product}[/bold {ACCENT}] [{MUTED}]\u203a[/{MUTED}] [bold]{tool_name}[/bold]",
        style=ACCENT,
    )
    console.print()


def resolve_native_outputs(tool_cfg: dict, values: dict[str, str], tool_dir: Path) -> list[Path]:
    """Where the tool natively writes its output, as declared in the registry.

    Two registry mechanisms, both resolved relative to `tool_dir` (the tool's
    own working directory, where a relative path from its CLI would land):

    - `produces: [...]` on the tool — fixed native filenames (same field
      pipeline steps use).
    - `is_output: true` on a param — the output location is whatever value
      that param ended up with, so a user-overridden path is still found.
    """

    def _resolve(value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else tool_dir / path

    resolved: list[Path] = [_resolve(name) for name in tool_cfg.get("produces", [])]

    for param in tool_cfg.get("params", []):
        if not param.get("is_output"):
            continue
        value = values.get(param["name"], param.get("default"))
        if value:
            resolved.append(_resolve(value))

    seen: set[Path] = set()
    unique: list[Path] = []
    for path in resolved:
        if path not in seen:
            seen.add(path)
            unique.append(path)
    return unique


def clean_expected_outputs(paths: list[Path]) -> None:
    """Remove any files at the tool's expected output paths.

    Called both before a run starts and after it finishes (once everything
    has been copied out). Tools reuse the same native filenames every time,
    so without this a leftover from an earlier run could be mistaken for
    this run's result — clearing the path before the run means a tool that
    errors out or writes nothing simply leaves it missing, not stale;
    clearing it again after the run means nothing lingers in the tool's own
    directory to confuse the *next* run either.
    """
    for path in paths:
        if path.exists():
            path.unlink()


def collect_outputs(paths: list[Path], out_dir: Path, since: datetime) -> None:
    """Copy freshly written outputs into the run's output directory.

    Files whose mtime predates the run are left behind as a second line of
    defense against reporting a stale, leftover result as this run's own
    (the primary defense is `clean_expected_outputs`, called before the run).
    """
    cutoff = since.timestamp()
    for path in paths:
        if not path.exists():
            console.print(f"  [{BAD}]\u2717[/{BAD}] expected output not found: [bold]{path.name}[/bold]")
        elif path.stat().st_mtime < cutoff:
            console.print(
                f"  [{WARN}]\u25cb[/{WARN}] skipped [bold]{path.name}[/bold] "
                f"[{MUTED}](stale \u2014 left over from an earlier run, not written by this one)[/{MUTED}]"
            )
        else:
            out_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, out_dir / path.name)
            console.print(f"  [{OK}]\u2713[/{OK}] collected [bold]{path.name}[/bold]")


def run_single_tool(product: str, tool_name: str, tool_cfg: dict) -> None:
    tool_dir = tool_dir_for(product, tool_name, tool_cfg)
    if not tool_dir.exists():
        console.print(not_installed_panel(product, tool_name, tool_dir))
        return

    entrypoint = tool_cfg["entrypoint"]
    fixed_args = tool_cfg.get("fixed_args", [])
    params = tool_cfg.get("params", [])

    out_dir = run_output_dir(product, tool_name)

    section_header(product, tool_name)
    flags, values = build_flags(params)

    if summary := params_summary_panel(params, values):
        console.print(summary)

    command = shlex.split(entrypoint) + fixed_args + flags
    expected_outputs = resolve_native_outputs(tool_cfg, values, tool_dir)
    clean_expected_outputs(expected_outputs)

    started_at = datetime.now()
    ok = confirm_and_run(command, cwd=tool_dir)

    if ok:
        collect_outputs(expected_outputs, out_dir, started_at)
        clean_expected_outputs(expected_outputs)
        console.print()

    console.print(output_summary_panel(out_dir))


_STEP_STATUS_MARKS = {
    "done": (f"[{OK}]\u2713[/{OK}]", OK),
    "failed": (f"[{BAD}]\u2717[/{BAD}]", BAD),
    "aborted": (f"[{WARN}]\u25cb[/{WARN}]", WARN),
}


def pipeline_table(steps: list[dict], statuses: dict[int, str]) -> Table:
    """Plan/progress table. `statuses` maps 1-based step number -> state."""
    done = sum(1 for s in statuses.values() if s == "done")
    table = Table(
        title=f"[bold]pipeline[/bold] [{MUTED}]\u00b7 {done}/{len(steps)} complete[/{MUTED}]",
        title_justify="left",
        box=box.SIMPLE_HEAVY,
        header_style=f"bold {MUTED}",
        border_style=MUTED,
        padding=(0, 2),
    )
    table.add_column(" ", justify="center", no_wrap=True)
    table.add_column("#", justify="right", style=MUTED, no_wrap=True)
    table.add_column("step", style="bold")
    table.add_column("produces", style=MUTED)

    for i, step in enumerate(steps, 1):
        state = statuses.get(i)
        mark, name_color = _STEP_STATUS_MARKS.get(state, (f"[{MUTED}]\u00b7[/{MUTED}]", None))
        name = f"[{name_color}]{step['name']}[/{name_color}]" if name_color else step["name"]
        table.add_row(mark, str(i), name, ", ".join(step.get("produces", [])))
    return table


def run_pipeline_tool(product: str, tool_name: str, tool_cfg: dict) -> None:
    tool_dir = tool_dir_for(product, tool_name, tool_cfg)
    if not tool_dir.exists():
        console.print(not_installed_panel(product, tool_name, tool_dir))
        return

    out_dir = run_output_dir(product, tool_name)

    steps = tool_cfg.get("steps", [])
    section_header(product, tool_name)

    if notes := tool_cfg.get("notes"):
        console.print(
            Panel(
                Text(" ".join(notes.split()), style="white"),
                title=f"[bold {WARN}]heads up[/bold {WARN}]",
                border_style=WARN,
                box=box.ROUNDED,
                padding=(1, 2),
            )
        )
        console.print()

    console.print(pipeline_table(steps, statuses={}))
    console.print()

    all_produces = [tool_dir / name for step in steps for name in step.get("produces", [])]
    clean_expected_outputs(all_produces)

    statuses: dict[int, str] = {}

    for i, step in enumerate(steps, 1):
        dots = "".join("\u25cf" if n < i else "\u25cb" for n in range(1, len(steps) + 1))
        console.rule(
            f"[{MUTED}]{dots}[/{MUTED}]  [bold]step {i}/{len(steps)}[/bold] [{MUTED}]\u00b7[/{MUTED}] "
            f"[bold {ACCENT}]{step['name']}[/bold {ACCENT}]",
            style=MUTED,
            align="left",
        )
        console.print()

        missing = [f for f in step.get("requires", []) if not (tool_dir / f).exists()]
        if missing:
            console.print(
                f"  [bold {WARN}]\u26a0 missing input(s) from a previous step:[/bold {WARN}] {', '.join(missing)}"
            )
            if not questionary.confirm("Continue anyway?", default=False, style=STYLE, qmark=QMARK).unsafe_ask():
                statuses[i] = "aborted"
                console.print(f"[{WARN}]\u25cb pipeline aborted.[/{WARN}]\n")
                console.print(pipeline_table(steps, statuses))
                console.print(output_summary_panel(out_dir))
                return

        flags, values = build_flags(step.get("params", []))
        if summary := params_summary_panel(step.get("params", []), values):
            console.print(summary)

        command = shlex.split(step["entrypoint"]) + flags
        started_at = datetime.now()
        ok = confirm_and_run(command, cwd=tool_dir)
        if not ok:
            statuses[i] = "failed"
            if not questionary.confirm(
                "Step failed or was skipped. Continue to next step anyway?", default=False, style=STYLE, qmark=QMARK
            ).unsafe_ask():
                console.print(f"[{WARN}]\u25cb pipeline stopped.[/{WARN}]\n")
                console.print(pipeline_table(steps, statuses))
                console.print(output_summary_panel(out_dir))
                return
        else:
            statuses[i] = "done"

        collect_outputs([tool_dir / name for name in step.get("produces", [])], out_dir, started_at)
        console.print()

    clean_expected_outputs(all_produces)

    console.print(pipeline_table(steps, statuses))
    console.print()
    console.print(output_summary_panel(out_dir))


def select_product(registry: dict) -> str | None:
    products = registry.get("products") or {}

    ready = {n: c for n, c in products.items() if any(is_configured(t) for t in (c.get("tools") or {}).values())}
    pending = {n: c for n, c in products.items() if n not in ready}
    width = max((len(n) for n in products), default=0)

    def _choice(name: str, cfg: dict, configured: bool) -> questionary.Choice:
        label = cfg.get("display_name") or name
        title = f"{name.ljust(width)}   {label}" if label != name else name
        return questionary.Choice(title=title, value=name, disabled=None if configured else "not configured yet")

    choices: list[Any] = []
    if ready:
        choices.append(questionary.Separator("  ready"))
        choices += [_choice(n, c, True) for n, c in ready.items()]
    if pending:
        choices.append(questionary.Separator(""))
        choices.append(questionary.Separator("  not configured yet"))
        choices += [_choice(n, c, False) for n, c in pending.items()]

    console.print()
    return questionary.select(
        "Select a product",
        choices=choices,
        qmark=QMARK,
        style=STYLE,
        instruction="(\u2191/\u2193 to move, enter to select, ctrl-c to quit)",
    ).ask()


_TOOL_BLURBS = {
    "duplicates": "find duplicate granules in CMR",
    "accountability": "track expected vs. delivered products",
    "audit": "trace processing steps across the SDS",
    "validity": "verify product integrity",
}


def select_tool(product: str, product_cfg: dict) -> tuple[str, dict] | None:
    tools = product_cfg.get("tools") or {}
    width = max((len(n) for n in tools), default=0)

    choices = []
    for name, cfg in tools.items():
        # A half-filled entry (no `repo`) is as unrunnable as an empty one,
        # and picking it would blow up later in tool_dir_for.
        configured = is_configured(cfg)
        blurb = _TOOL_BLURBS.get(name, "")
        kind = cfg.get("kind", "single") if configured else ""
        suffix = f" ({len(cfg.get('steps', []))} steps)" if kind == "pipeline" else ""
        title = f"{name.ljust(width)}   {blurb}{suffix}" if blurb else name
        choices.append(
            questionary.Choice(title=title, value=name, disabled=None if configured else "not configured yet")
        )

    console.print()
    selected = questionary.select(
        f"Select an operation for {product}",
        choices=choices,
        qmark=QMARK,
        style=STYLE,
        instruction="(\u2191/\u2193 to move, enter to select, ctrl-c to quit)",
    ).ask()
    if selected is None:
        return None
    return selected, tools[selected]


def warn_if_out_of_sync(registry: dict) -> None:
    with console.status(f"[{MUTED}]checking tool repos against remote\u2026[/{MUTED}]", spinner="dots"):
        out_of_sync = check_all_out_of_sync(registry)

    if not out_of_sync:
        console.print(f"  [{OK}]\u2713[/{OK}] [{MUTED}]all tool repos up to date[/{MUTED}]")
        return

    table = Table(
        title=f"[bold {WARN}]\u26a0 tools out of sync[/bold {WARN}]",
        title_justify="left",
        box=box.SIMPLE_HEAVY,
        border_style=WARN,
        header_style=f"bold {MUTED}",
        padding=(0, 2),
    )
    table.add_column("product", style="bold")
    table.add_column("tool")
    table.add_column("status")

    for item in out_of_sync:
        if item["status"] == "missing":
            status = f"[{BAD}]not cloned yet[/{BAD}]"
        elif item["status"] == "error":
            status = f"[{BAD}]check failed: {item['error']}[/{BAD}]"
        else:
            status = f"[{WARN}]{item['behind']} commit(s) behind[/{WARN}]"
        table.add_row(item["product"], item["tool"], status)

    console.print(table)

    fix = Text()
    fix.append("./setup.sh --update", style=f"bold {ACCENT}")
    fix.append("  sync everything\n", style=MUTED)
    fix.append("./setup.sh --product <PRODUCT> --tool <TOOL> --update", style=f"bold {ACCENT}")
    fix.append("  just one", style=MUTED)
    console.print(
        Panel(
            fix,
            title=f"[bold {WARN}]action needed[/bold {WARN}]",
            border_style=WARN,
            box=box.ROUNDED,
            padding=(1, 2),
        )
    )
    console.print()


# "OPERA" in block letters (ANSI Shadow style figlet font).
_OPERA_ART = [
    " ██████╗ " "██████╗ " "███████╗" "██████╗ " " █████╗ ",
    "██╔═══██╗" "██╔══██╗" "██╔════╝" "██╔══██╗" "██╔══██╗",
    "██║   ██║" "██████╔╝" "█████╗  " "██████╔╝" "███████║",
    "██║   ██║" "██╔═══╝ " "██╔══╝  " "██╔══██╗" "██╔══██║",
    "╚██████╔╝" "██║     " "███████╗" "██║  ██║" "██║  ██║",
    " ╚═════╝ " "╚═╝     " "╚══════╝" "╚═╝  ╚═╝" "╚═╝  ╚═╝",
]

# "LAUNCHPAD" in block letters (ANSI Shadow style figlet font).
_LAUNCHPAD_ART = [
    "██╗     " " █████╗ " "██╗   ██╗" "███╗   ██╗" " ██████╗" "██╗  ██╗" "██████╗ " " █████╗ " "██████╗ ",
    "██║     " "██╔══██╗" "██║   ██║" "████╗  ██║" "██╔════╝" "██║  ██║" "██╔══██╗" "██╔══██╗" "██╔══██╗",
    "██║     " "███████║" "██║   ██║" "██╔██╗ ██║" "██║     " "███████║" "██████╔╝" "███████║" "██║  ██║",
    "██║     " "██╔══██║" "██║   ██║" "██║╚██╗██║" "██║     " "██╔══██║" "██╔═══╝ " "██╔══██║" "██║  ██║",
    "███████╗" "██║  ██║" "╚██████╔╝" "██║ ╚████║" "╚██████╗" "██║  ██║" "██║     " "██║  ██║" "██████╔╝",
    "╚══════╝" "╚═╝  ╚═╝" " ╚═════╝ " "╚═╝  ╚═══╝" " ╚═════╝" "╚═╝  ╚═╝" "╚═╝     " "╚═╝  ╚═╝" "╚═════╝ ",
]


_TAGLINE = "run the real tool, wherever it lives"

# Progressively narrower renditions of the wordmark: (art blocks, tagline).
_BANNER_TIERS = [
    ([_OPERA_ART, _LAUNCHPAD_ART], _TAGLINE),
    ([_OPERA_ART], f"launchpad \u00b7 {_TAGLINE}"),
]


def _banner_panel(blocks: list[list[str]], tagline: str, padding: int) -> Panel:
    body = Text(justify="center")
    for block in blocks:
        for i, line in enumerate(block):
            body.append(line + "\n", style=f"bold {GRADIENT[i % len(GRADIENT)]}")
        body.append("\n")
    body.append(tagline, style=f"italic {MUTED}")
    return Panel(body, border_style=ACCENT_ALT, box=box.HEAVY, padding=(1, padding))


def print_banner() -> None:
    """Draw the wordmark, shrinking it to whatever the terminal can hold.

    The LAUNCHPAD block letters are 75 columns wide before the panel's
    borders and padding, so the full banner needs 85 — more than the
    80-column default an operator gets over plain SSH. Left alone, rich
    wraps it into unreadable confetti, so fall back to the narrower OPERA
    mark and finally to plain text.
    """
    console.print()
    for blocks, tagline in _BANNER_TIERS:
        content = max([len(line) for block in blocks for line in block] + [len(tagline)])
        for padding in (4, 2, 1):
            if content + 2 * padding + 2 <= console.width:
                console.print(Align.center(_banner_panel(blocks, tagline, padding)))
                console.print()
                return
    console.print(Align.center(Text(f"OPERA launchpad \u00b7 {_TAGLINE}", style=f"bold {ACCENT}")))
    console.print()


def main() -> None:
    print_banner()

    registry = load_registry()

    warn_if_out_of_sync(registry)

    product = select_product(registry)
    if product is None:
        console.print(f"\n[{MUTED}]nothing selected \u2014 bye.[/{MUTED}]\n")
        return

    product_cfg = registry["products"][product]
    result = select_tool(product, product_cfg)
    if result is None:
        console.print(f"\n[{MUTED}]nothing selected \u2014 bye.[/{MUTED}]\n")
        return
    tool_name, tool_cfg = result

    kind = tool_cfg.get("kind", "single")
    if kind == "pipeline":
        run_pipeline_tool(product, tool_name, tool_cfg)
    else:
        run_single_tool(product, tool_name, tool_cfg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print(f"\n[{WARN}]\u25cb interrupted.[/{WARN}]\n")
