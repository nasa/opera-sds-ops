# opera-launchpad

A lightweight launcher for OPERA operator tools. Instead of asking every
developer to migrate their tool into a shared framework, `opera-launchpad`
clones each tool's **own** repo, installs it with its **own** native setup
steps, and runs it **unmodified**. Developers keep working in whatever repo
they already use; this just orchestrates discovery + invocation.

## How it works

1. `registry.yaml` is the single source of truth: for each product (e.g.
   `DSWX_S1`) and operation (`duplicates`, `accountability`, ...), it records:
   - the git repo + branch the tool lives in
   - the subdirectory of that repo
   - the native setup command (e.g. `pip install -r requirements.txt`)
   - the command to run it, and what parameters it takes
2. `bootstrap.py` clones (or updates) each tool into its own directory under
   `./repos/<product>/<tool>/`, and runs that tool's setup command in place.
3. `cli.py` is the interactive launcher: pick a product, pick an operation,
   fill in the prompted parameters, and it runs the real tool's command in
   its own directory/venv. On startup it checks every configured tool's
   clone against its remote branch and warns you (with instructions) if
   anything is out of date or not yet cloned.

Nothing about the underlying tool's code changes. Repos are cloned
workspace-locally under `./repos/` (gitignored) — no shared venv, each tool
gets its own per the registry's `setup` command.

### One clone per tool

Each tool is cloned separately, to `repos/<product>/<tool>/`, even when
several tools come from the same repo. That's deliberate: `git checkout`
swaps the *entire* working tree, so tools sharing one clone can only ever
be on a single branch between them — whichever was bootstrapped last —
however different their `path` subdirectories are. Sharing a clone made
two tools on different branches either silently run the wrong branch's
code, or flip-flop the clone back and forth on every `--update`.

So a registry can point three tools at one repo on three different
branches and each stays put. The cost is a full clone per tool.

## Quick start

```bash
# One-time (or after registry changes): creates .venv, installs
# opera-launchpad's own deps, clones + installs every configured tool
./setup.sh

# Every day: launch the interactive menu
./start.sh
```

`setup.sh` is a thin convenience wrapper around:

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/python bootstrap.py
```

`start.sh` just runs `.venv/bin/python cli.py` (after checking `.venv`
exists). It's the one you'll run most often.

### Setup for just one product/tool

```bash
./setup.sh --product DSWX_S1 --tool accountability
```

## Updating after a developer pushes changes

`start.sh` / `cli.py` automatically warn you at startup if a tool's clone is
behind its remote branch, e.g.:

```
┏━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Product ┃ Tool           ┃ Status                     ┃
┡━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ DSWX_S1 │ duplicates     │ 31 commit(s) behind remote │
└─────────┴────────────────┴────────────────────────────┘
```

To sync:

```bash
./setup.sh --update
```

This fetches + hard-resets every cloned repo to its configured branch and
reruns setup. No changes are needed to `opera-launchpad` itself when a tool's
internals change — only `registry.yaml` needs updating if the CLI surface
(flags/params) changes.

You can also check sync status without cloning/installing anything:

```bash
.venv/bin/python bootstrap.py --check
```

## Output

Every run that writes something gets its own timestamped directory under
`output/` (workspace-local, gitignored):

```
output/<product>/<tool>/<YYYYMMDD_HHMMSS>/
```

The directory is created on the first file actually collected, so runs you
back out of at the confirm prompt don't leave empty ones behind.

e.g. `output/DSWX_S1/duplicates/20260827_182637/duplicate_report.json`.

Tools always run unmodified, in their own repo directory, using their own
native defaults — `opera-launchpad` never rewrites a tool's CLI flags. Instead,
the registry records where each tool's output *natively* lands, and
`opera-launchpad` copies it into the run's output directory afterward:

- For `single` tools, the tool's `produces` list names the native relative
  filenames it writes in its own directory; those are copied out after the
  run. A param marked `is_output: true` also works, and additionally tracks a
  path the user overrode at the prompt. Use one or the other for a given
  file, not both: if `produces` names `report.json` *and* an `is_output`
  param defaults to it, overriding that param at the prompt leaves the
  untouched default listed as a missing output.
- For `pipeline` tools, each step's `produces` list names the native relative
  filenames that step writes in the tool's directory; those are copied into
  the run's output directory after the step completes (originals stay in
  place too, since later steps still need to read them).

At the end of a run, `cli.py` prints an **Output** panel showing the run's
directory and the files found in it.

## Registry schema

Two tool "kinds" are supported:

- **`single`** — one command, run once with collected params. Example:
  `DSWX_S1.duplicates` (wraps `duplicate_check.py`).
- **`pipeline`** — an ordered list of steps run in sequence, where later
  steps typically consume files produced by earlier ones. Example:
  `DSWX_S1.accountability` (5-step `survey.py` → `accountability.py` →
  `missing_rtcs_to_tile_sets.py` → `add_cycle_indices.py` →
  `check_burst_coverage.py` pipeline).

See `registry.yaml` for the full schema and the fully-specified `DSWX_S1`
entry. Other products are stubbed with empty `tools: {}` entries — the CLI
will show them as "not configured" until filled in.

### Environment overrides

Tools are installed with their own `setup` command, which normally just means
their `requirements.txt`. When that isn't enough — upstream forgot a package,
or a version has to be pinned locally — patch it in the registry instead of
forking the tool:

```yaml
        setup: "python3 -m venv venv && venv/bin/pip install -r requirements.txt"
        verify: "venv/bin/python duplicate_check.py --help"
        overrides:
          reason: "requirements.txt omits pandas; urllib3 2.x breaks boto3"
          pip_install: ["pandas>=2.0", "urllib3<2"]
          post_setup: ["venv/bin/python -m nltk.downloader punkt"]
```

- **`verify`** — optional smoke test run right after setup, so a missing
  dependency surfaces at install time instead of halfway through an
  operator's run. On failure it prints the error and points at `overrides`.
  It's a warning, not a hard stop.
- **`overrides.reason`** — printed during setup, so the next person knows why
  the patch exists.
- **`overrides.pip_install`** — installed through the tool's own venv pip
  *after* `setup`, so these specs win over whatever `requirements.txt` pulled
  in. Use it to add missing packages or force a different version.
- **`overrides.post_setup`** — arbitrary extra commands, run in order.
- **`pip`** — only needed if a tool's venv isn't at the default `venv/bin/pip`.

Overrides are meant to be temporary. When upstream fixes its `requirements.txt`,
delete the block.

## Status

`DSWX_S1` (`duplicates` + `accountability`) is fully wired up as the
reference implementation. All other products are placeholders in
`registry.yaml` pending migration.
