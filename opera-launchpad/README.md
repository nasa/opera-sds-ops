# opera-launchpad

A lightweight launcher for OPERA operator tools. Instead of asking every
developer to migrate their tool into a shared framework, `opera-launchpad`
clones each tool's **own** repo, installs it with its **own** native setup
steps, and runs it **unmodified**. Developers keep working in whatever repo
they already use; this just orchestrates discovery + invocation.

## How it works

1. `registry.yaml` is the single source of truth: for each product (e.g.
   `DSWX_S1`) and operation (`duplicates`, `accountability`, `audit`, ...), it
   records:
   - the git repo + branch the tool lives in
   - the subdirectory of that repo
   - the native setup command (e.g. `pip install -r requirements.txt`)
   - the command to run it, and what parameters it takes
   - any extra credentials/ancillary files/environment it needs (see
     [Credentials](#credentials) and [Registry schema](#registry-schema))
2. `bootstrap.py` clones (or updates) each tool into its own directory under
   `./repos/<product>/<tool>/`, runs that tool's setup command in place, and
   prompts once for any credentials the selected tools need.
3. `cli.py` is the interactive launcher: pick a product, pick an operation,
   fill in the prompted parameters, and it runs the real tool's command in
   its own directory/venv. On startup it checks every configured tool's
   clone against its remote branch, and separately flags every tool that may
   not fully work here (see [Tools needing attention](#tools-needing-attention)).

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
branches and each stays put.

That would normally mean a full network clone per tool, even when several
tools share a repo — expensive for something like `opera-sds-pcm` (~650MB),
cloned by several tools in this registry. Instead, each unique repo URL is
cloned over the network once into `repos/_cache/<repo-name>/`, and every
tool's own working tree is a fast, local clone off that cache, checked out
to its own branch and pointed back at the real remote — so it fetches,
updates, and reports sync status exactly as if it had been cloned from
there directly. `./setup.sh --update` refreshes the cache once and then
each tool's clone as usual.

## Quick start

```bash
# One-time (or after registry changes): creates .venv, installs
# opera-launchpad's own deps, clones + installs every configured tool.
# Prompts once for any credentials the configured tools need (see below).
./setup.sh

# Every day: launch the interactive menu
./start.sh
```

`setup.sh` is a thin convenience wrapper around:

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/python bootstrap.py "$@"   # forwards all of setup.sh's own args
```

`start.sh` just runs `.venv/bin/python cli.py` (after checking `.venv`
exists). It's the one you'll run most often; it takes no arguments — every
choice (product, operation, parameters) is made interactively.

### `setup.sh` / `bootstrap.py` command line options

`setup.sh` forwards every argument straight to `bootstrap.py`, so all of
these work with either:

| Flag | Effect |
| --- | --- |
| *(none)* | Clone (if missing) + run native setup for every tool configured in `registry.yaml`. |
| `--update` | Also `git fetch` + hard-reset every **already-cloned** repo to its configured branch before rerunning setup. Without this, an existing clone is left as-is and only its setup/overrides are (re-)run. |
| `--product PRODUCT` | Only bootstrap this product's tools, e.g. `--product DSWX_S1`. |
| `--tool TOOL` | Only bootstrap this one tool (requires `--product`), e.g. `--product DSWX_S1 --tool audit`. |
| `--check` | Don't clone or install anything — just report which configured tools are out of sync with their remote branch (missing clone, behind, or a check error). Exits non-zero if anything is out of sync; this is what `./start.sh` runs automatically on every launch. |

Examples:

```bash
./setup.sh                                          # everything
./setup.sh --update                                 # everything, resyncing existing clones
./setup.sh --product DSWX_S1 --tool accountability  # just one tool
./setup.sh --product RTC_S1 --tool audit --update   # just one tool, resyncing it first
.venv/bin/python bootstrap.py --check                # sync check only, no install
```

One tool's setup failing (e.g. a missing credential) never blocks the rest —
`bootstrap.py` keeps going through every remaining target and prints a
summary table of what succeeded/failed at the end, with the tail of each
failure's own output. It exits `1` if anything failed, `0` otherwise.

## Credentials

Some tools need credentials or tokens beyond what `pip install` can set up.
`bootstrap.py` prompts for each of these **once**, up front, only if a
selected tool's registry entry actually needs it — never unconditionally.
Skipping a prompt (leaving it blank) is always fine: the affected tool just
runs in a degraded/crashing state later, exactly as if you'd never set up
`opera-launchpad` credential support at all.

| Credential | Registry flag | Stored at | Used for |
| --- | --- | --- | --- |
| NASA Earthdata Login (EDL) | `requires_edl: true` | `~/.netrc` (`urs.earthdata.nasa.gov`) | Tools that fetch an EDL Bearer token for CMR/DAAC queries (e.g. burst-ID derivation from SLC annotations). Only that one host's entry is touched; every other `~/.netrc` entry is preserved. |
| AWS (ancillary S3 data) | `requires_aws_ancillaries: true` | `~/.aws/credentials`, under a dedicated `[opera-ancillaries]` profile (**not** `[default]`) | Tools that fetch ancillary data from a private `s3://opera-ancillaries/...` bucket (e.g. the DIST-S1 burst lookup table). Written under its own profile so this never touches AWS credentials you already have configured for other work; `cli.py` only sets `AWS_PROFILE=opera-ancillaries` for that tool's own subprocess, not your shell. |
| JPL GitHub Enterprise token | `requires_git_oauth_token: true` | Kept in memory for the current `bootstrap.py` run only (`GIT_OAUTH_TOKEN` env var) — **not** written to disk | Cloning a private JPL GHE repo a tool's setup needs (e.g. `pcm_commons`). If you already have `GIT_OAUTH_TOKEN` exported in your shell, that's reused and you won't be prompted. |

All three prompts happen right after the "tools out of sync" check and
before any cloning/setup starts, so you only get asked once even if several
selected tools need the same kind of credential.

To add/replace a credential later without re-running everything:

```bash
./setup.sh --product <PRODUCT> --tool <TOOL>   # re-prompts for just what that tool needs
```

### Which tools need which credential

| Credential | Tools | Without it |
| --- | --- | --- |
| EDL username/password | `RTC_S1.audit`, `CSLC_S1.audit`, `DIST_S1.validity` | Runs, but degraded/incomplete results (EDL-gated queries silently skip/fail per-item). |
| AWS (S3 ancillary bucket) | `DIST_S1.input_selection`, `DIST_S1.audit` | `input_selection` silently returns an empty/wrong result; `audit` hard-crashes. |
| `GIT_OAUTH_TOKEN` (JPL GHE PAT) | `DISP_S1.audit` | Setup fails outright for this one tool with a clear error (others unaffected). No unauthenticated path works for this repo — plain clone, zip download, and the GHE API were all tested and all require it. |

Tools that also need live network access to the OPERA SDS's internal
Elasticsearch/GRQ cluster (won't fully work from a personal laptop even with
every credential above set): `DIST_S1.input_selection`, `DIST_S1.audit`,
`DISP_S1.audit`.

## Updating after a developer pushes changes

`start.sh` / `cli.py` automatically warn you at startup if a tool's clone is
behind its remote branch, e.g.:

```
⚠ tools out of sync

   product   tool          status
 ─────────────────────────────────────────
   DSWX_S1   duplicates    31 commit(s) behind
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

## Tools needing attention

Right after the sync check, `start.sh` also prints a table of every
configured tool that may not fully work here, and why — so you find out
*before* filling in parameters and running it, not partway through:

```
⚠ tools needing attention

   product   tool     why
 ──────────────────────────────────────────────────────────────────────
   DISP_S1   audit    Setup failed: Command failed (1): .../ensure_pcm_commons.sh
                       ERROR: GIT_OAUTH_TOKEN is not set.

                       Needs live network access to the OPERA SDS's internal
                       Elasticsearch/GRQ cluster ... only works from an SDS
                       cluster node/VM, not a personal laptop.
```

A tool can show up here for any combination of:

- **Setup never finished cleanly.** `bootstrap.py` drops a small
  `.launchpad_setup_status.json` marker in a tool's own directory recording
  whether its last setup run actually succeeded (a cloned repo on its own
  only means "cloned", not "fully set up" — something like a missing
  credential partway through `./setup.sh` still leaves a tool directory
  behind). Fix the underlying cause and rerun `./setup.sh --product ... --tool ...`.
- **`requires_vm: true`** — needs cluster/VM-only resources (internal ES/GRQ
  access, etc.) that no amount of local credential setup fixes; only really
  usable from an actual SDS node. The specific reason is in the tool's
  `vm_note`.
- **Missing EDL or AWS credentials** — the tool is configured to use them
  (`requires_edl`/`requires_aws_ancillaries`) but `./setup.sh` was never run
  with them provided; see [Credentials](#credentials).

The same reasons appear as a `⚠ needs attention` suffix next to the tool
once you've navigated into its product, and again as a panel right before
you're asked to confirm running it.

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
`opera-launchpad` copies it into the run's output directory afterward, via
whichever of these fit the tool:

- **`produces: [...]`** on a `single` tool, or on a `pipeline` step — fixed
  native filenames the tool/step always writes in its own directory. Copied
  out after the run (or, for pipeline steps, after that step completes;
  originals stay in place too, since later steps still need to read them).
- **`is_output: true`** on a param — the output location is whatever value
  that param ended up with (its registry `default`, or whatever the operator
  typed at the prompt), so an overridden path is still found and collected.
- **`produces_glob: [...]`** on a tool — a glob pattern (relative to the
  tool's own directory) to search for *after* the run, for tools that name
  their own output file dynamically (e.g. with a timestamp baked in) when no
  explicit path is given, so there's no fixed name to know ahead of time.
  Only files newer than the run's start are collected, so a same-shaped
  leftover from an earlier run is never mistaken for this run's own.

Use only one mechanism for a given file: if `produces` names `report.json`
*and* an `is_output` param also defaults to it, overriding that param at the
prompt leaves the untouched default listed as a missing output.

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

See `registry.yaml` for the full schema and its many fully-specified
entries — it's heavily commented with the specific reasons behind each
tool's overrides/flags, which are usually more informative than this README
for any one tool's quirks.

### Core fields (every tool)

```yaml
      duplicates:
        kind: single                    # or `pipeline` (see `steps:` below)
        repo: "https://github.com/..."
        branch: "main"
        path: "duplicates"               # subdirectory of the repo the tool lives in
        setup: "python3 -m venv venv && venv/bin/pip install -r requirements.txt"
        verify: "venv/bin/python duplicate_check.py --help"
        entrypoint: "venv/bin/python duplicate_check.py"
        fixed_args: ["DSWX_S1"]          # always-present positional/flag args
        params: [ ... ]                  # see below
```

### Param fields

```yaml
          - name: output               # key into the collected values dict
            flag: "-o"                 # CLI flag passed with the value
            type: path                 # str | path | datetime_iso | int | bool |
                                        # bool_optional | choice | str_list
            required: false            # re-prompts until a value is given if true
            default: "duplicate_report.json"
            choices: ["a", "b"]        # required for type: choice
            is_output: true            # see Output, above
            help: "shown under the prompt label"
```

- **`type: bool_optional`** is for `argparse.BooleanOptionalAction`-style
  flags, where "false" has to be spelled out as `--no-foo`, not just an
  omitted flag.
- **`type: str_list`** is for `nargs='+'`-style flags that need each value as
  its own argv token, not one space-separated string.

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
  It's a warning, not a hard stop — but is recorded in the tool's setup
  status (see [Tools needing attention](#tools-needing-attention)).
- **`overrides.reason`** — printed during setup, so the next person knows why
  the patch exists.
- **`overrides.pip_install`** — installed through the tool's own venv pip
  *after* `setup`, so these specs win over whatever `requirements.txt` pulled
  in. Use it to add missing packages or force a different version.
- **`overrides.post_setup`** — arbitrary extra commands, run in order.
- **`pip`** — only needed if a tool's venv isn't at the default `venv/bin/pip`.

Overrides are meant to be temporary. When upstream fixes its `requirements.txt`,
delete the block.

### Credential/ancillary-data flags

See [Credentials](#credentials) for `requires_edl`, `requires_aws_ancillaries`,
and `requires_git_oauth_token` in detail. A few more, narrower flags exist
for specific ancillary files some tools need and can't otherwise get without
AWS credentials most operators don't have:

- **`requires_mgrs_db: true`** — the tool imports opera-sds-pcm's
  `rtc.mgrs_bursts_collection_db_client`, which falls back to an S3 download
  needing AWS credentials if it can't find its DB locally. `opera-sds-ops`
  bundles the real DB (not just opera-sds-pcm's small test fixture), so
  `bootstrap.py` copies that in locally instead, and `cli.py` points
  `MGRS_TILE_COLLECTION_DB_FILEPATH` at it when running the tool.
- **`requires_frame_to_burst_sample: true`** — narrower still: one tool's
  `--frame-to-burst-db` has no default and no documented download source
  anywhere, so `bootstrap.py` instead copies in `opera-sds-ops`'s own tiny
  (5-frame) *sample* fixture as a registry default, so the field isn't a
  dead end for anyone without their own copy from the DISP-S1 processing
  team. Results against it only ever cover those 5 frames — the param's
  `help` text says so explicitly.

### VM/cluster-only flags

- **`requires_vm: true`** — this tool needs cluster/VM-only resources
  (internal Elasticsearch/GRQ access, live network paths only available on
  an SDS node, etc.) that no amount of local setup fixes. Surfaced in the
  ["tools needing attention"](#tools-needing-attention) table and as a
  warning panel before running.
- **`vm_note: >`** — the specific reason, shown verbatim in both of those
  places. Write it assuming the reader has never seen the tool before.

## Status

Most products in `registry.yaml` are now fully wired up, spanning
`duplicates`/`accountability`/`audit`/`validity`/`input_selection`-style
tools across `DSWX_S1`, `DSWX_HLS`, `RTC_S1`, `CSLC_S1`, `DIST_S1`, `DISP_S1`,
`TROPO`, `DIST_ALERT_HLS`, `CSLC_S1_STATIC`, `RTC_S1_STATIC`, and
`DISP_S1_STATIC`. Check `./start.sh`'s startup output (or
`.venv/bin/python bootstrap.py --check`) for the current, authoritative
state of any given tool — this README won't always be perfectly in sync
with fast-moving registry changes.
