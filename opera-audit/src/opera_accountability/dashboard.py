"""Streamlit dashboard for visualizing OPERA accountability and duplicates.

OPERA / JPL-branded, built on ``streamlit-shadcn-ui`` for modern components
(cards, metric cards, tabs, badges). Supports a user-facing light/dark theme
toggle persisted in :mod:`streamlit.session_state`.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
import streamlit_shadcn_ui as sui


# ---------------------------------------------------------------------------
# OPERA / JPL brand palette
# ---------------------------------------------------------------------------

JPL_BLUE = "#0059A0"
JPL_BLUE_DEEP = "#003A73"
NASA_RED = "#FC3D21"
OPERA_ACCENT = "#00B4D8"

# shadcn component color mode — we mirror the app theme into each call.
_CARD_KEY_COUNTER = "_card_key_counter"


def _next_key(prefix: str) -> str:
    """Generate a stable-in-one-pass unique key for shadcn components."""
    st.session_state[_CARD_KEY_COUNTER] = st.session_state.get(_CARD_KEY_COUNTER, 0) + 1
    return f"{prefix}_{st.session_state[_CARD_KEY_COUNTER]}"


# ---------------------------------------------------------------------------
# Report loading
# ---------------------------------------------------------------------------


def load_reports(data_dir: Path) -> dict:
    """Load the latest JSON reports from the data directory.

    Supports two accountability layouts:

    * Flat (DSWX_HLS):
        ``reports/accountability/<PRODUCT>/<YYYY-MM-DD>.json`` — a single JSON
        file with a ``results`` wrapper written by :mod:`reports.save_reports`.
    * Nested (DSWX_S1):
        ``reports/accountability/<PRODUCT>/<YYYY-MM-DD>/summary.json`` — the
        pipeline-level summary plus sibling artifact JSONs written by
        :mod:`strategies.dswx_s1.pipeline`.
    """
    reports_dir = data_dir / "reports"
    reports = {"duplicates": {}, "accountability": {}, "burst_coverage": {}}

    if not reports_dir.exists():
        return reports

    # Duplicates — always flat.
    dup_dir = reports_dir / "duplicates"
    if dup_dir.exists():
        for product_dir in dup_dir.iterdir():
            if product_dir.is_dir():
                json_files = sorted(product_dir.glob("*.json"), reverse=True)
                if json_files:
                    with open(json_files[0]) as f:
                        reports["duplicates"][product_dir.name] = json.load(f)

    # Accountability — flat OR nested-by-date.
    acc_dir = reports_dir / "accountability"
    if acc_dir.exists():
        for product_dir in acc_dir.iterdir():
            if not product_dir.is_dir():
                continue
            product = product_dir.name

            flat_jsons = sorted(product_dir.glob("*.json"), reverse=True)
            if flat_jsons:
                with open(flat_jsons[0]) as f:
                    reports["accountability"][product] = json.load(f)
                continue

            date_dirs = sorted(
                (d for d in product_dir.iterdir() if d.is_dir()), reverse=True
            )
            if not date_dirs:
                continue
            latest = date_dirs[0]
            summary_path = latest / "summary.json"
            if not summary_path.exists():
                continue

            with open(summary_path) as f:
                summary = json.load(f)
            summary["_report_dir"] = str(latest)
            reports["accountability"][product] = summary

    # Burst coverage — flat JSON per run.
    bc_dir = reports_dir / "burst_coverage"
    if bc_dir.exists():
        json_files = sorted(bc_dir.glob("*.json"), reverse=True)
        for jf in json_files:
            try:
                with open(jf) as f:
                    data = json.load(f)
                # Use the filename stem as the report key (e.g. "2025-06-30_12-00")
                reports["burst_coverage"][jf.stem] = data
            except (json.JSONDecodeError, OSError):
                continue

    return reports


def _unwrap_accountability_results(report: dict) -> dict:
    """Return the canonical results dict regardless of report shape."""
    if "results" in report and isinstance(report["results"], dict):
        return report["results"]
    return report


def _is_dswx_s1_report(report: dict) -> bool:
    """Heuristic: DSWX_S1 summary.json carries pipeline-specific keys."""
    return "tile_set_count" in report and "rtc_surveyed" in report


def _is_dist_s1_report(report: dict) -> bool:
    return "dist_surveyed" in report and report.get("metadata", {}).get("strategy") == "dist_s1"


def _extract_generated_at(report: dict) -> str | None:
    """Pull the ``generated_at`` ISO timestamp from whichever report schema.

    - DSWX_HLS / duplicates: ``report['report_metadata']['generated_at']``
    - DSWX_S1 pipeline:      ``report['metadata']['generated_at']``
    """
    if "report_metadata" in report and isinstance(report["report_metadata"], dict):
        return report["report_metadata"].get("generated_at")
    if "metadata" in report and isinstance(report["metadata"], dict):
        return report["metadata"].get("generated_at")
    return None


def _format_age(generated_at: str | None) -> str:
    """Format an ISO timestamp as a wall-clock label for the "Generated" column.

    Returns the local-time timestamp as ``YYYY-MM-DD HH:MM TZ`` (operators asked
    for absolute timestamps instead of the original relative labels like
    ``"Today 17:05"`` / ``"3d ago"`` — easier to correlate with log lines and
    cron schedules). Returns ``"unknown"`` if the input is falsy or cannot be
    parsed.
    """
    if not generated_at:
        return "unknown"
    try:
        # Handle both "...Z" and naive ISO strings.
        ts = generated_at.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is not None:
            dt = dt.astimezone()
        else:
            # Naive timestamp - assume it's already in local time
            dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
    except (ValueError, TypeError):
        return "unknown"

    return dt.strftime("%Y-%m-%d %H:%M %Z")


# Status thresholds — single source of truth used by the helpers AND rendered
# by the Overview legend, so the UI and the rules can never drift.
DUPLICATE_THRESHOLDS = {
    "healthy": 1.0,   # rate < 1%  → healthy
    "warning": 5.0,   # 1% ≤ rate < 5% → warning; ≥ 5% → critical
}
ACCOUNTABILITY_THRESHOLDS = {
    "healthy": 98.0,  # rate ≥ 98% → healthy
    "warning": 90.0,  # 90% ≤ rate < 98% → warning; < 90% → critical
}

# Material Symbols (Rounded) icon names + human-readable labels for each
# severity. Text labels are used when we can't render a font icon (e.g.
# inside a shadcn table cell); HTML output uses the glyph inline.
STATUS_HEALTHY = "check_circle"
STATUS_WARNING = "warning"
STATUS_CRITICAL = "cancel"

STATUS_LABELS = {
    STATUS_HEALTHY: "Healthy",
    STATUS_WARNING: "Warning",
    STATUS_CRITICAL: "Critical",
}

_PILL_CLASS = {
    STATUS_HEALTHY: "opera-pill-healthy",
    STATUS_WARNING: "opera-pill-warning",
    STATUS_CRITICAL: "opera-pill-critical",
}


def status_pill_html(icon: str, rate_pct: float, label: str | None = None) -> str:
    """Render a colored status pill with a Material icon + rate or label.

    When ``label`` is provided it is rendered verbatim (escaped) in place of
    the numeric rate. This is the mechanism used by the legend to show
    "Healthy" / "Warning" / "Critical" words instead of percentages.
    """
    from html import escape
    body = escape(label) if label is not None else f"{rate_pct:.2f}%"
    return (
        f'<span class="opera-pill {_PILL_CLASS[icon]}">'
        f'<span class="material-symbols-rounded">{icon}</span>'
        f'{body}'
        f'</span>'
    )


def freshness_chip_html(label: str) -> str:
    """Render the 'Today 17:05' / 'Yesterday' chip used in Overview tables."""
    from html import escape
    return f'<span class="opera-freshness">{escape(label)}</span>'


def _status_for_duplicate_rate(rate_pct: float) -> str:
    """HTML status pill for duplicate rates (lower is better)."""
    if rate_pct < DUPLICATE_THRESHOLDS["healthy"]:
        icon = STATUS_HEALTHY
    elif rate_pct < DUPLICATE_THRESHOLDS["warning"]:
        icon = STATUS_WARNING
    else:
        icon = STATUS_CRITICAL
    return status_pill_html(icon, rate_pct)


def _status_for_accountability_rate(rate_pct: float) -> str:
    """HTML status pill for accountability rates (higher is better)."""
    if rate_pct >= ACCOUNTABILITY_THRESHOLDS["healthy"]:
        icon = STATUS_HEALTHY
    elif rate_pct >= ACCOUNTABILITY_THRESHOLDS["warning"]:
        icon = STATUS_WARNING
    else:
        icon = STATUS_CRITICAL
    return status_pill_html(icon, rate_pct)


def _render_html_table(columns: list[str], rows: list[list[str]]) -> None:
    """Render a custom HTML table that preserves rich (HTML) cell content.

    ``rows`` are lists of **pre-escaped** HTML strings — status pills and
    freshness chips emit their own markup via ``status_pill_html`` /
    ``freshness_chip_html``. Text columns should go through :func:`html.escape`.
    """
    head = "".join(f"<th>{c}</th>" for c in columns)
    body = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    st.markdown(
        f'<table class="opera-table"><thead><tr>{head}</tr></thead>'
        f'<tbody>{body}</tbody></table>',
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Theming
# ---------------------------------------------------------------------------


def _inject_theme_css() -> None:
    """Inject the OPERA/JPL-branded light theme CSS.

    Overrides Streamlit's CSS custom properties and native widgets so that
    shadcn-ui components, native widgets and Altair charts share a cohesive
    JPL-blue palette.
    """
    css_vars = f"""
        --opera-bg: #FFFFFF;
        --opera-bg-elev: #F5F7FA;
        --opera-surface: #FFFFFF;
        --opera-text: #0B1F3A;
        --opera-text-muted: #5A6A85;
        --opera-border: #E2E8F0;
        --opera-primary: {JPL_BLUE};
        --opera-primary-deep: {JPL_BLUE_DEEP};
        --opera-accent: {NASA_RED};
    """

    # NB: the Material Symbols font is loaded via @import INSIDE the <style>
    # tag. Using a top-level <link rel="stylesheet"> gets stripped by
    # Streamlit's HTML sanitizer and leaves the <style> orphaned, causing the
    # raw CSS to render as page text.
    css = f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded:opsz,wght,FILL,GRAD@24,500,1,0');

            :root {{
                {css_vars}
            }}

            /* Material Symbols base style (used in status pills & legend). */
            .material-symbols-rounded {{
                font-family: 'Material Symbols Rounded';
                font-weight: 500;
                font-style: normal;
                font-size: 18px;
                line-height: 1;
                letter-spacing: normal;
                text-transform: none;
                display: inline-block;
                white-space: nowrap;
                word-wrap: normal;
                direction: ltr;
                -webkit-font-feature-settings: 'liga';
                -webkit-font-smoothing: antialiased;
                font-variation-settings: 'FILL' 1, 'wght' 500, 'GRAD' 0, 'opsz' 24;
                vertical-align: -4px;
            }}

            /* Status pill (used in custom HTML tables). */
            .opera-pill {{
                display: inline-flex;
                align-items: center;
                gap: 6px;
                padding: 3px 10px 3px 8px;
                border-radius: 999px;
                font-size: 12px;
                font-weight: 600;
                border: 1px solid transparent;
            }}
            .opera-pill .material-symbols-rounded {{ font-size: 16px; vertical-align: -3px; }}
            .opera-pill.opera-pill-healthy {{
                color: #1A7F4B; background: #E8F6EE; border-color: #BFE4CD;
            }}
            .opera-pill.opera-pill-warning {{
                color: #8A5A00; background: #FFF4E0; border-color: #F7D9A1;
            }}
            .opera-pill.opera-pill-critical {{
                color: #B42318; background: #FEE4E2; border-color: #F8BFB9;
            }}

            /* Freshness chip — distinct from status pill. */
            .opera-freshness {{
                display: inline-block;
                padding: 2px 10px;
                border-radius: 999px;
                background: var(--opera-bg-elev);
                color: var(--opera-primary-deep);
                border: 1px solid var(--opera-border);
                font-size: 12px;
                font-weight: 500;
                font-variant-numeric: tabular-nums;
            }}

            /* Custom HTML tables (replace sui.table where we want rich cells). */
            .opera-table {{
                width: 100%;
                border-collapse: separate;
                border-spacing: 0;
                background: var(--opera-surface);
                border: 1px solid var(--opera-border);
                border-radius: 12px;
                overflow: hidden;
                font-size: 13px;
                color: var(--opera-text);
            }}
            .opera-table th {{
                background: var(--opera-bg-elev);
                color: var(--opera-text-muted);
                text-transform: uppercase;
                letter-spacing: 0.06em;
                font-size: 11px;
                font-weight: 700;
                text-align: left;
                padding: 10px 14px;
                border-bottom: 1px solid var(--opera-border);
            }}
            .opera-table td {{
                padding: 10px 14px;
                border-bottom: 1px solid var(--opera-border);
                font-variant-numeric: tabular-nums;
            }}
            .opera-table tr:last-child td {{ border-bottom: none; }}
            .opera-table tr:hover td {{ background: rgba(0, 89, 160, 0.03); }}
            .opera-table td.opera-num {{ text-align: right; }}
            .opera-table td.opera-center {{ text-align: center; }}
            .opera-table td strong {{ color: var(--opera-text); font-weight: 600; }}

            /* Hide default Streamlit chrome we don't need. */
            [data-testid="stToolbar"] {{ display: none !important; }}
            [data-testid="stSidebar"], section[data-testid="stSidebar"] {{ display: none !important; }}
            header[data-testid="stHeader"] {{ background: transparent !important; height: 0 !important; }}
            footer {{ visibility: hidden; }}

            /* Backdrop + typography. */
            .stApp {{
                background: var(--opera-bg) !important;
                color: var(--opera-text) !important;
            }}
            body, .stApp, [data-testid="stAppViewContainer"] {{
                background: var(--opera-bg) !important;
                color: var(--opera-text) !important;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Inter", Roboto, sans-serif !important;
            }}
            .stApp h1, .stApp h2, .stApp h3 {{
                color: var(--opera-text) !important;
                letter-spacing: -0.01em;
            }}

            /* OPERA branded header strip. */
            .opera-header {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                padding: 18px 24px;
                margin: 0 0 16px 0;
                border-radius: 14px;
                background: linear-gradient(90deg, var(--opera-primary-deep) 0%, var(--opera-primary) 100%);
                color: white;
                box-shadow: 0 4px 20px rgba(0, 89, 160, 0.18);
            }}
            .opera-header .opera-title {{
                font-size: 22px;
                font-weight: 700;
                letter-spacing: -0.02em;
            }}
            .opera-header .opera-subtitle {{
                font-size: 13px;
                opacity: 0.85;
                margin-top: 2px;
                font-weight: 400;
            }}
            .opera-header .opera-badge {{
                background: rgba(255, 255, 255, 0.18);
                padding: 4px 10px;
                border-radius: 999px;
                font-size: 11px;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.06em;
                margin-left: 8px;
            }}

            /* Section labels — subtle uppercase cap headers. */
            .opera-section {{
                color: var(--opera-text-muted);
                font-size: 11px;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                margin: 24px 0 12px 0;
            }}

            /* Metadata strip under the header. */
            .opera-metastrip {{
                display: flex;
                flex-wrap: wrap;
                align-items: center;
                gap: 18px;
                color: var(--opera-text-muted);
                font-size: 13px;
                margin: -4px 0 18px 4px;
                position: relative;   /* anchor for the legend's absolute panel */
            }}
            .opera-metastrip span strong {{ color: var(--opera-text); font-weight: 600; }}

            /* Polish the native expander + selectbox + dataframe to match theme. */
            .streamlit-expanderHeader, [data-testid="stExpander"] details summary {{
                background: var(--opera-bg-elev) !important;
                border: 1px solid var(--opera-border) !important;
                border-radius: 10px !important;
                color: var(--opera-text) !important;
            }}
            [data-testid="stDataFrame"], [data-testid="stTable"] {{
                background: var(--opera-surface) !important;
                border-radius: 10px;
                border: 1px solid var(--opera-border);
            }}
            .stSelectbox > div > div {{
                background: var(--opera-surface) !important;
                border: 1px solid var(--opera-border) !important;
                color: var(--opera-text) !important;
            }}

            /* Buttons — neutral light surface with dark label, JPL blue
               icon + focus ring. Covers native buttons, download buttons,
               and popover triggers. */
            .stButton > button,
            .stDownloadButton > button,
            [data-testid="stPopover"] > div > button,
            [data-testid="stPopoverButton"] {{
                background: var(--opera-bg-elev) !important;
                color: var(--opera-text) !important;
                border: 1px solid var(--opera-border) !important;
                border-radius: 10px !important;
                font-weight: 600 !important;
                padding: 8px 14px !important;
                transition: transform 0.05s ease, background 0.15s ease, border-color 0.15s ease, box-shadow 0.15s ease !important;
                box-shadow: 0 1px 2px rgba(11, 31, 58, 0.04);
            }}
            .stButton > button:hover,
            .stDownloadButton > button:hover,
            [data-testid="stPopover"] > div > button:hover,
            [data-testid="stPopoverButton"]:hover {{
                background: #FFFFFF !important;
                border-color: var(--opera-primary) !important;
                color: var(--opera-primary-deep) !important;
                transform: translateY(-1px);
                box-shadow: 0 3px 10px rgba(11, 31, 58, 0.08);
            }}
            .stButton > button:active,
            .stDownloadButton > button:active {{ transform: translateY(0); }}
            .stButton > button:focus-visible,
            .stDownloadButton > button:focus-visible,
            [data-testid="stPopover"] > div > button:focus-visible {{
                outline: 2px solid var(--opera-primary) !important;
                outline-offset: 2px !important;
            }}

            /* Icons inside buttons use the JPL accent so they read as
               interactive affordances against the light button background. */
            .stButton > button svg,
            .stDownloadButton > button svg,
            [data-testid="stPopover"] > div > button svg,
            [data-testid="stPopoverButton"] svg {{
                fill: var(--opera-primary) !important;
                color: var(--opera-primary) !important;
            }}
            .stButton > button:hover svg,
            .stDownloadButton > button:hover svg,
            [data-testid="stPopover"] > div > button:hover svg,
            [data-testid="stPopoverButton"]:hover svg {{
                color: var(--opera-primary-deep) !important;
            }}

            /* Popover panel — clean card look. */
            [data-baseweb="popover"] div[role="dialog"],
            [data-testid="stPopoverBody"] {{
                background: var(--opera-surface) !important;
                border: 1px solid var(--opera-border) !important;
                border-radius: 12px !important;
                box-shadow: 0 12px 32px rgba(11, 31, 58, 0.12) !important;
                padding: 12px !important;
            }}

            /* Light-on-dark fixes for native text. */
            .stMarkdown, .stText, p, li, span {{ color: var(--opera-text) !important; }}
            code {{
                background: var(--opera-bg-elev) !important;
                color: var(--opera-primary) !important;
                padding: 2px 6px;
                border-radius: 6px;
            }}

            /* Pull shadcn cards into our palette when possible. */
            div[data-testid="stIFrame"] {{ background: transparent !important; }}

            /* Main container spacing. */
            .block-container {{
                padding-top: 1.2rem !important;
                padding-bottom: 3rem !important;
                max-width: 1400px;
            }}

            /* Background for plotly / altair chart containers. */
            [data-testid="stAltairChart"], [data-testid="stArrowVegaLiteChart"] {{
                background: var(--opera-surface);
                border: 1px solid var(--opera-border);
                border-radius: 12px;
                padding: 12px;
            }}

            /* Legend popover body. */
            .opera-legend-pop {{
                display: flex;
                flex-direction: column;
                gap: 6px;
                min-width: 340px;
            }}
            .opera-legend-title {{
                color: var(--opera-text-muted);
                font-size: 11px;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                margin-bottom: 4px;
            }}
            .opera-legend-row {{
                display: flex;
                align-items: center;
                gap: 10px;
                font-size: 13px;
                color: var(--opera-text);
                line-height: 1.5;
            }}
            .opera-legend-row .opera-pill,
            .opera-legend-row .opera-freshness {{
                flex: 0 0 auto;
                min-width: 96px;
                justify-content: center;
                text-align: center;
            }}

            /* Meta strip — let the Material icons sit inline with the text. */
            .opera-metastrip .material-symbols-rounded {{
                font-size: 16px;
                color: var(--opera-primary);
                vertical-align: -3px;
                margin-right: 4px;
            }}

            /* Inline "More info" legend — native <details> disclosure.
               Styled as a text link that lives flush-right in the meta strip
               and pops a floating panel on click. */
            .opera-legend-details {{
                position: relative;
                margin-left: auto;    /* push to the right edge of the flex row */
                font-size: 13px;
            }}
            .opera-legend-details summary {{
                list-style: none;
                display: inline-flex;
                align-items: center;
                gap: 4px;
                cursor: pointer;
                color: var(--opera-primary);
                font-weight: 500;
                text-decoration: none;
                user-select: none;
                padding: 0;
            }}
            .opera-legend-details summary::-webkit-details-marker {{ display: none; }}
            .opera-legend-details summary::marker {{ content: ''; }}
            .opera-legend-details summary:hover {{ color: var(--opera-primary-deep); }}
            .opera-legend-details summary .material-symbols-rounded {{
                font-size: 16px;
                color: var(--opera-primary);
                margin-right: 0;
            }}

            /* Floating panel anchored to the disclosure. */
            .opera-legend-panel {{
                position: absolute;
                top: calc(100% + 8px);
                right: 0;
                z-index: 1000;
                min-width: 420px;
                background: var(--opera-surface);
                border: 1px solid var(--opera-border);
                border-radius: 12px;
                box-shadow: 0 16px 40px rgba(11, 31, 58, 0.14);
                padding: 14px 16px;
            }}
        </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def _altair_theme() -> dict:
    """Altair theme matching the OPERA light palette."""
    bg = "#FFFFFF"
    fg = "#0B1F3A"
    grid = "#E2E8F0"
    return {
        "config": {
            "background": bg,
            "title": {"color": fg, "fontSize": 14, "fontWeight": 600},
            "axis": {
                "labelColor": fg,
                "titleColor": fg,
                "gridColor": grid,
                "domainColor": grid,
                "tickColor": grid,
            },
            "legend": {"labelColor": fg, "titleColor": fg},
            "view": {"stroke": "transparent"},
            "range": {"category": [JPL_BLUE, OPERA_ACCENT, NASA_RED, "#8B5CF6", "#10B981"]},
        }
    }


# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------


def _render_header(data_dir: Path) -> None:
    """Render the OPERA-branded header + meta strip (with inline legend disclosure)."""
    st.markdown(
        # NOTE: No leading indentation! Streamlit's markdown renderer treats
        # 4-space-indented lines as a <pre> code block even with
        # unsafe_allow_html=True, which dumps raw HTML to the page.
        (
            '<div class="opera-header">'
            '<div>'
            '<div class="opera-title">OPERA Accountability Dashboard '
            '<span class="opera-badge">JPL · SDS</span>'
            '</div>'
            '<div class="opera-subtitle">'
            'Duplicate detection, accountability &amp; burst coverage for OPERA products'
            '</div>'
            '</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

    # Meta strip — all inline including the legend disclosure. Using a native
    # HTML5 <details> here instead of st.popover because Streamlit's popover
    # renders as a separate block-level widget and can't be placed inline with
    # the surrounding text nodes.
    st.markdown(_build_meta_strip(data_dir), unsafe_allow_html=True)


def _build_meta_strip(data_dir: Path) -> str:
    """Produce the meta strip HTML + inline legend <details> disclosure."""
    loaded = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    dup_healthy = DUPLICATE_THRESHOLDS["healthy"]
    dup_warning = DUPLICATE_THRESHOLDS["warning"]
    acc_healthy = ACCOUNTABILITY_THRESHOLDS["healthy"]
    acc_warning = ACCOUNTABILITY_THRESHOLDS["warning"]

    healthy_pill = status_pill_html(STATUS_HEALTHY, 0.0, label="Healthy")
    warning_pill = status_pill_html(STATUS_WARNING, 0.0, label="Warning")
    critical_pill = status_pill_html(STATUS_CRITICAL, 0.0, label="Critical")

    legend_body = (
        '<div class="opera-legend-pop">'
        '<div class="opera-legend-title">Status thresholds</div>'
        f'<div class="opera-legend-row">{healthy_pill}'
        f'<span>duplicates &lt; {dup_healthy:g}% · accountability ≥ {acc_healthy:g}%</span></div>'
        f'<div class="opera-legend-row">{warning_pill}'
        f'<span>duplicates {dup_healthy:g}–{dup_warning:g}% · accountability {acc_warning:g}–{acc_healthy:g}%</span></div>'
        f'<div class="opera-legend-row">{critical_pill}'
        f'<span>duplicates ≥ {dup_warning:g}% · accountability &lt; {acc_warning:g}%</span></div>'
        '<div class="opera-legend-title" style="margin-top:14px;">Generated column</div>'
        '<div class="opera-legend-row"><span class="opera-freshness">YYYY-MM-DD HH:MM TZ</span>'
        '<span>wall-clock time the report was generated</span></div>'
        '</div>'
    )

    from html import escape as _e
    return (
        '<div class="opera-metastrip">'
        '<span><span class="material-symbols-rounded">folder_open</span>'
        f'<strong>Data directory:</strong> <code>{_e(str(data_dir))}</code></span>'
        '<span><span class="material-symbols-rounded">schedule</span>'
        f'<strong>Loaded:</strong> {_e(loaded)}</span>'
        '<details class="opera-legend-details">'
        '<summary>'
        '<span class="material-symbols-rounded">info</span>'
        '<span class="opera-legend-link-label">More info</span>'
        '</summary>'
        f'<div class="opera-legend-panel">{legend_body}</div>'
        '</details>'
        '</div>'
    )


def _format_meta_timestamp(value: str | None) -> str | None:
    """Format an ISO timestamp for the meta strip (UTC), or return ``None``."""
    if not value:
        return None
    try:
        ts = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        # Keep in UTC
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        else:
            # Naive timestamp - assume it's UTC
            dt = dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return value
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def _format_meta_timestamp_local(value: str | None) -> str | None:
    """Format an ISO timestamp for the meta strip (local time), or return ``None``."""
    if not value:
        return None
    try:
        ts = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is not None:
            dt = dt.astimezone()
        else:
            # Naive timestamp - assume it's already in local time
            dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
    except (ValueError, TypeError):
        return value
    return dt.strftime("%Y-%m-%d %H:%M %Z")


def _render_report_meta_strip(report: dict) -> None:
    """Render a meta strip (Start / End / Venue / Generated) for any report.

    Handles both schemas:
    - duplicates / DSWX_HLS: ``report['report_metadata']``
    - DSWX_S1 pipeline:      ``report['metadata']``
    """
    meta = {}
    if isinstance(report.get("report_metadata"), dict):
        meta = report["report_metadata"]
    elif isinstance(report.get("metadata"), dict):
        meta = report["metadata"]

    start = _format_meta_timestamp(meta.get("start_date"))
    end = _format_meta_timestamp(meta.get("end_date"))
    generated = _format_meta_timestamp_local(meta.get("generated_at"))
    venue = meta.get("venue")

    from html import escape as _e
    parts = []
    if start:
        parts.append(f"<strong>Start:</strong> {_e(start)}")
    if end:
        parts.append(f"<strong>End:</strong> {_e(end)}")
    if venue:
        parts.append(f"<strong>Venue:</strong> {_e(venue)}")
    if generated:
        parts.append(f"<strong>Generated:</strong> {_e(generated)}")
    if not parts:
        return

    st.markdown(
        '<div class="opera-metastrip"><span>'
        + "</span><span>".join(parts)
        + "</span></div>",
        unsafe_allow_html=True,
    )


def _section_label(text: str) -> None:
    st.markdown(f'<div class="opera-section">{text}</div>', unsafe_allow_html=True)


def _download_header(title: str, count: int, file_base: str, items: list[str],
                     include_json: bool = False) -> None:
    """Render a section heading with a right-aligned "Export" popover dropdown.

    The popover reveals one or more format-specific download buttons (TXT /
    JSON) with Material icons. Uses Streamlit's native ``st.popover`` so the
    state handling is delegated to the framework.
    """
    label_col, action_col = st.columns([7, 3])
    with label_col:
        _section_label(f"{title} ({count:,})")
    with action_col:
        # Streamlit already wraps the popover in its own block, so we rely on
        # the column layout (not a manual flex div, which would be closed by
        # Streamlit's own wrappers before the popover renders).
        with st.popover(
            f"Export · {count:,}",
            icon=":material/download:",
            use_container_width=True,
        ):
            st.caption("Choose a format — the full list is exported, not just the preview.")
            st.download_button(
                label="Plain text",
                data="\n".join(items) + "\n",
                file_name=f"{file_base}.txt",
                mime="text/plain",
                icon=":material/description:",
                use_container_width=True,
                key=_next_key("dl"),
            )
            if include_json:
                st.download_button(
                    label="JSON",
                    data=json.dumps(items, indent=2),
                    file_name=f"{file_base}.json",
                    mime="application/json",
                    icon=":material/data_object:",
                    use_container_width=True,
                    key=_next_key("dl"),
                )


# ---------------------------------------------------------------------------
# Page renderers
# ---------------------------------------------------------------------------


def _render_overview(reports: dict) -> None:
    _section_label("Overview")

    if not reports["duplicates"] and not reports["accountability"] and not reports["burst_coverage"]:
        sui.alert(
            title="No reports yet",
            description=(
                "Run `opera-audit duplicates <PRODUCT> --save` or "
                "`opera-audit accountability <PRODUCT> --save` or "
                "`opera-audit burst-coverage --save ...` to generate reports."
            ),
            key=_next_key("alert_empty"),
        )
        return

    # Topline metrics — shadcn metric cards.
    #
    # End-conflict reports (DISP_S1 ``--check-end-conflicts``) share the
    # ``reports/duplicates/`` tree but expose ``conflicting_products`` instead
    # of ``duplicates``. Counting them as duplicates keeps the topline
    # roll-up meaningful and — critically — avoids a ``KeyError`` that used
    # to take down the entire Overview tab whenever an end-conflict report
    # was loaded alongside ordinary duplicate reports.
    total_granules = sum(
        r["results"].get("total", 0) for r in reports["duplicates"].values()
    )
    total_duplicates = sum(
        r["results"].get("duplicates", r["results"].get("conflicting_products", 0))
        for r in reports["duplicates"].values()
    )
    total_accountability_products = len(reports["accountability"])

    total_bc_reports = len(reports["burst_coverage"])

    cols = st.columns(5)
    with cols[0]:
        sui.metric_card(
            title="Duplicate reports",
            content=f"{len(reports['duplicates'])}",
            description="product(s) with duplicate analysis",
            key=_next_key("m"),
        )
    with cols[1]:
        sui.metric_card(
            title="Total granules analyzed",
            content=f"{total_granules:,}",
            description="across all duplicate reports",
            key=_next_key("m"),
        )
    with cols[2]:
        dup_rate = (total_duplicates / total_granules * 100) if total_granules else 0.0
        sui.metric_card(
            title="Total duplicates",
            content=f"{total_duplicates:,}",
            description=f"{dup_rate:.2f}% of granules",
            key=_next_key("m"),
        )
    with cols[3]:
        sui.metric_card(
            title="Accountability reports",
            content=f"{total_accountability_products}",
            description="product(s) with accountability analysis",
            key=_next_key("m"),
        )
    with cols[4]:
        sui.metric_card(
            title="Burst coverage reports",
            content=f"{total_bc_reports}",
            description="CSLC/RTC burst audits",
            key=_next_key("m"),
        )

    # (Legend now lives in the header popover — see _render_legend_popover.)

    # Duplicate rate bar chart across products.
    if reports["duplicates"]:
        _section_label("Duplicate rate by product")
        chart_rows = []
        for product, report in reports["duplicates"].items():
            # All duplicate reports written by ``save_reports`` are wrapped
            # under ``results``; reading from the top-level ``report`` dict
            # silently produced 0% rates for every product.
            results = report.get("results", report)
            total = results.get("total", 0)
            # End-conflict reports (DISP_S1 ``--check-end-conflicts``) expose
            # ``conflicting_products`` instead of ``duplicates``; surface
            # whichever metric the report actually contains.
            duplicates = results.get(
                "duplicates", results.get("conflicting_products", 0)
            )
            rate = (duplicates / total * 100) if total else 0.0
            chart_rows.append({
                "Product": product,
                "Rate (%)": round(rate, 2),
                "Duplicates": duplicates,
            })
        chart_df = pd.DataFrame(chart_rows).sort_values("Rate (%)", ascending=False)
        chart = (
            alt.Chart(chart_df)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("Product:N", sort="-y", title=None),
                y=alt.Y("Rate (%):Q"),
                color=alt.value(JPL_BLUE),
                tooltip=["Product", "Rate (%)", "Duplicates"],
            )
            .configure(**_altair_theme()["config"])
            .properties(height=220)
        )
        st.altair_chart(chart, use_container_width=True)

    # Per-product duplicate summary with freshness + status columns.
    if reports["duplicates"]:
        _section_label("Products — duplicate summary")
        from html import escape as _e
        rows = []
        for product, report in reports["duplicates"].items():
            # Check if this is an end-conflict report
            if product == "DISP_S1" and "conflict_groups" in report.get("results", {}):
                results = report["results"]
                total = results.get("total", 0)
                conflict_groups = results.get("conflict_groups", 0)
                conflicting_products = results.get("conflicting_products", 0)
                rate = (conflicting_products / total * 100) if total else 0.0
                status = _status_for_duplicate_rate(rate)
                rows.append([
                    f"<strong>{_e(product)}</strong>",
                    f"{total:,}",
                    f"{conflict_groups:,}",
                    f"{conflicting_products:,}",
                    status,
                    freshness_chip_html(_format_age(_extract_generated_at(report))),
                ])
            else:
                results = report["results"]
                rate = (results["duplicates"] / results["total"] * 100) if results["total"] else 0.0
                rows.append([
                    f"<strong>{_e(product)}</strong>",
                    f"{results['total']:,}",
                    f"{results['unique']:,}",
                    f"{results['duplicates']:,}",
                    _status_for_duplicate_rate(rate),
                    freshness_chip_html(_format_age(_extract_generated_at(report))),
                ])
        _render_html_table(
            ["Product", "Granules", "Unique/Conflicts", "Duplicates/Conflicting", "Status", "Generated"],
            rows,
        )

    # Per-product accountability summary (with freshness + status).
    if reports["accountability"]:
        _section_label("Products — accountability summary")
        from html import escape as _e
        acc_rows = []
        for product, report in reports["accountability"].items():
            if _is_dswx_s1_report(report) or _is_dist_s1_report(report):
                filtered = report.get("filtered_rtc_count", 0)
                missing = report.get("missing_count", 0)
                expected = report.get("expected", filtered)
                actual = report.get("actual", report.get("used_rtc_count", 0))
                # actual / expected is bounded to [0, 100] (used/filtered
                # can overshoot when DSWx references pre-window RTCs).
                rate = (actual / expected * 100) if expected else 0.0
                expected_label = f"{expected:,}"
            else:
                r = _unwrap_accountability_results(report)
                expected = r.get("expected") or 0
                actual = r.get("actual") or 0
                missing = r.get("missing_count") or 0
                rate = (actual / expected * 100) if expected else 0.0
                expected_label = f"{expected:,}"
            acc_rows.append([
                f"<strong>{_e(product)}</strong>",
                expected_label,
                f"{missing:,}",
                f"{rate:.2f}%",
                _status_for_accountability_rate(rate),
                freshness_chip_html(_format_age(_extract_generated_at(report))),
            ])
        _render_html_table(
            ["Product", "Expected", "Missing", "Rate", "Status", "Generated"],
            acc_rows,
        )

    # Burst coverage summary.
    if reports["burst_coverage"]:
        _section_label("Burst coverage summary")
        from html import escape as _ebc
        bc_rows = []
        for report_key, report in reports["burst_coverage"].items():
            meta = report.get("metadata", {})
            for pt, stats in report.get("products", {}).items():
                cov = stats.get("coverage_percent", 0)
                bc_rows.append([
                    f"<strong>{_ebc(pt)}</strong>",
                    f"{stats.get('expected_count', 0):,}",
                    f"{stats.get('found_count', 0):,}",
                    f"{stats.get('missing_count', 0):,}",
                    _status_for_accountability_rate(cov),
                    freshness_chip_html(_ebc(report_key)),
                ])
        _render_html_table(
            ["Product", "Expected", "Found", "Missing", "Status", "Report"],
            bc_rows,
        )


def _render_duplicates(reports: dict) -> None:
    _section_label("Duplicate analysis")

    if not reports["duplicates"]:
        sui.alert(
            title="No duplicate reports",
            description="Run `opera-audit duplicates [PRODUCT] --save` to generate reports.",
            key=_next_key("alert"),
        )
        return

    selected = st.selectbox(
        "Product",
        list(reports["duplicates"].keys()),
        key="dup_product_selectbox",
    )
    if not selected:
        return

    report = reports["duplicates"][selected]
    _render_report_meta_strip(report)
    results = report["results"]

    # Check if this is an end-conflict report
    is_end_conflict = "conflict_groups" in results

    if is_end_conflict:
        cols = st.columns(4)
        with cols[0]:
            sui.metric_card(title="Total granules", content=f"{results['total']:,}",
                            description="in selected window", key=_next_key("m"))
        with cols[1]:
            sui.metric_card(title="Conflict groups", content=f"{results['conflict_groups']:,}",
                            description="same frame+end date", key=_next_key("m"))
        with cols[2]:
            sui.metric_card(title="Conflicting products", content=f"{results['conflicting_products']:,}",
                            description="different begin dates", key=_next_key("m"))
        with cols[3]:
            rate = (results["conflicting_products"] / results["total"] * 100) if results["total"] else 0.0
            sui.metric_card(title="Conflict rate", content=f"{rate:.2f}%",
                            description="conflicts ÷ total", key=_next_key("m"))
    else:
        cols = st.columns(4)
        with cols[0]:
            sui.metric_card(title="Total granules", content=f"{results['total']:,}",
                            description="in selected window", key=_next_key("m"))
        with cols[1]:
            sui.metric_card(title="Unique granules", content=f"{results['unique']:,}",
                            description="after dedup", key=_next_key("m"))
        with cols[2]:
            sui.metric_card(title="Duplicates", content=f"{results['duplicates']:,}",
                            description="older copies superseded", key=_next_key("m"))
        with cols[3]:
            rate = (results["duplicates"] / results["total"] * 100) if results["total"] else 0.0
            sui.metric_card(title="Duplicate rate", content=f"{rate:.2f}%",
                            description="duplicates ÷ total", key=_next_key("m"))

    # Altair bar chart — one bar per date, two layers (total vs duplicates).
    by_date = results.get("by_date") or {}
    if by_date:
        _section_label("Duplicates by date")
        df = pd.DataFrame([
            {
                "Date": d, 
                "Total": by_date[d].get("total", by_date[d].get("n_granules", 0)), 
                "Duplicates": by_date[d].get("n_duplicates", 0)
            }
            for d in sorted(by_date.keys())
        ])
        melted = df.melt("Date", var_name="Series", value_name="Count")
        chart = (
            alt.Chart(melted)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("Date:N", title=None),
                y=alt.Y("Count:Q", title="Granules"),
                color=alt.Color("Series:N", legend=alt.Legend(orient="top")),
                xOffset="Series:N",
                tooltip=["Date", "Series", "Count"],
            )
            .configure(**_altair_theme()["config"])
            .properties(height=260)
        )
        st.altair_chart(chart, use_container_width=True)

    dup_list = results.get("duplicate_list") or []
    
    # Show conflict details if end-conflict report
    if is_end_conflict and "conflicts" in results:
        _section_label("End conflicts by frame")
        conflicts = results["conflicts"]
        if conflicts:
            conflict_data = []
            for key, conf in conflicts.items():
                conflict_data.append({
                    "Frame": conf["frame_id"],
                    "End Date": conf["end_dt"],
                    "Begin Dates": ", ".join(conf["begin_dts"]),
                    "Count": conf["count"]
                })
            df_conflicts = pd.DataFrame(conflict_data)
            st.dataframe(df_conflicts, use_container_width=True)
        else:
            st.info("No end conflicts found")
    
    # Show duplicate list for regular reports
    if not is_end_conflict and dup_list:
        today = datetime.now().strftime("%Y-%m-%d")
        _download_header(
            title="Duplicate granule IDs",
            count=len(dup_list),
            file_base=f"{selected}_duplicates_{today}",
            items=dup_list,
            include_json=True,
        )
        with st.expander(f"Preview first {min(100, len(dup_list))} of {len(dup_list):,}"):
            for granule_id in dup_list[:100]:
                st.code(granule_id, language=None)
            if len(dup_list) > 100:
                sui.badges(
                    badge_list=[(f"+{len(dup_list) - 100:,} more", "outline")],
                    key=_next_key("badge"),
                )


def _render_accountability(reports: dict) -> None:
    _section_label("Accountability analysis")

    if not reports["accountability"]:
        sui.alert(
            title="No accountability reports",
            description="Run `opera-audit accountability [PRODUCT] --save` to generate reports.",
            key=_next_key("alert"),
        )
        return

    selected = st.selectbox(
        "Product",
        list(reports["accountability"].keys()),
        key="acc_product_selectbox",
    )
    if not selected:
        return

    report = reports["accountability"][selected]
    results = _unwrap_accountability_results(report)
    strategy = results.get("strategy", "unknown")
    
    if _is_dswx_s1_report(report):
        _render_dswx_s1_panel(selected, report)
    elif _is_dist_s1_report(report):
        _render_dist_s1_panel(selected, report)
    elif strategy in ["date_count", "db_based", "forward_map", "delegated_validator"]:
        _render_generic_strategy_panel(selected, report, strategy)
    else:
        _render_dswx_hls_panel(selected, report)


def _render_dswx_hls_panel(product: str, report: dict) -> None:
    _render_report_meta_strip(report)
    results = _unwrap_accountability_results(report)
    cols = st.columns(4)
    with cols[0]:
        sui.metric_card(title="Expected HLS granules",
                        content=f"{results['expected']:,}",
                        description="after L9 cutoff filter",
                        key=_next_key("m"))
    with cols[1]:
        sui.metric_card(title="Matched DSWx outputs",
                        content=f"{results['actual']:,}",
                        description="HLS inputs with a DSWx",
                        key=_next_key("m"))
    with cols[2]:
        sui.metric_card(title="Missing outputs",
                        content=f"{results['missing_count']:,}",
                        description="HLS without DSWx",
                        key=_next_key("m"))
    with cols[3]:
        rate = (results["actual"] / results["expected"] * 100) if results["expected"] else 0.0
        sui.metric_card(title="Accountability rate",
                        content=f"{rate:.2f}%",
                        description="matched ÷ expected",
                        key=_next_key("m"))

    missing = results.get("missing") or []
    if missing:
        today = datetime.now().strftime("%Y-%m-%d")
        _download_header(
            title=f"Missing {product} outputs",
            count=len(missing),
            file_base=f"{product}_missing_{today}",
            items=missing,
            include_json=True,
        )
        with st.expander(f"Preview first {min(100, len(missing))} of {len(missing):,}"):
            for granule_id in missing[:100]:
                st.code(granule_id, language=None)


def _render_generic_strategy_panel(product: str, report: dict, strategy: str) -> None:
    """Render panel for Chris's accountability strategies (forward_map, date_count, delegated_validator, db_based)."""
    _render_report_meta_strip(report)
    results = _unwrap_accountability_results(report)
    
    _section_label(f"Accountability summary (strategy: {strategy})")
    
    # Handle different result structures based on strategy
    if strategy == "date_count":
        cols = st.columns(3)
        with cols[0]:
            sui.metric_card(title="Expected Per Day", content=f"{results.get('expected_per_day', 0)}",
                            description="granules per day", key=_next_key("m"))
        with cols[1]:
            sui.metric_card(title="Missing Dates", content=f"{results.get('missing_dates', 0):,}",
                            description="days below threshold", key=_next_key("m"))
        with cols[2]:
            sui.metric_card(title="Total Dates", content=f"{results.get('total_dates', 0):,}",
                            description="date range coverage", key=_next_key("m"))
        
        # Show date counts if available
        if "date_counts" in results:
            _section_label("Granule counts by date")
            date_data = [{"Date": d, "Count": c} for d, c in results["date_counts"].items()]
            df_dates = pd.DataFrame(date_data).sort_values("Date")
            st.dataframe(df_dates, use_container_width=True)
            
            # Highlight missing dates
            missing_dates = {d: c for d, c in results["date_counts"].items() 
                           if c < results.get("expected_per_day", 1)}
            if missing_dates:
                _section_label(f"Dates with missing granules ({len(missing_dates)})")
                st.json(missing_dates)
    
    elif strategy == "db_based":
        cols = st.columns(4)
        with cols[0]:
            sui.metric_card(title="Expected", content=f"{results.get('expected', 0):,}",
                            description="items in database", key=_next_key("m"))
        with cols[1]:
            sui.metric_card(title="Actual", content=f"{results.get('actual', 0):,}",
                            description="items in CMR", key=_next_key("m"))
        with cols[2]:
            sui.metric_card(title="Missing", content=f"{results.get('missing_count', 0):,}",
                            description="items not found", key=_next_key("m"))
        with cols[3]:
            sui.metric_card(title="Coverage", content=f"{results.get('coverage_pct', 0):.1f}%",
                            description="actual ÷ expected", key=_next_key("m"))
    
    else:  # forward_map, delegated_validator, or generic
        cols = st.columns(3)
        with cols[0]:
            sui.metric_card(title="Expected", content=f"{results.get('expected', 0):,}",
                            description="expected products", key=_next_key("m"))
        with cols[1]:
            sui.metric_card(title="Actual", content=f"{results.get('actual', 0):,}",
                            description="found products", key=_next_key("m"))
        with cols[2]:
            sui.metric_card(title="Missing", content=f"{results.get('missing_count', 0):,}",
                            description="missing products", key=_next_key("m"))
        
        if strategy == "delegated_validator":
            # ``st.info`` is not a context manager and requires the message
            # as a positional arg; using ``with st.info():`` raised
            # AttributeError every time a delegated_validator panel rendered.
            st.info(
                f"Validation delegated to external validator: "
                f"{results.get('delegated', 'N/A')}"
            )
    
    # Show missing items if available
    if "missing" in results and results["missing"]:
        _section_label(f"Missing items ({len(results['missing'])})")
        st.json(results["missing"][:100])  # Show first 100


def _render_dswx_s1_panel(product: str, report: dict) -> None:
    _render_report_meta_strip(report)

    _section_label("Accountability")
    filtered = report.get("filtered_rtc_count", 0)
    used = report.get("used_rtc_count", 0)
    missing = report.get("missing_count", 0)
    expected = report.get("expected", filtered)
    actual = report.get("actual", used)

    cols = st.columns(4)
    with cols[0]:
        sui.metric_card(title="RTCs (after sensor filter)",
                        content=f"{filtered:,}",
                        description="within S1A/B/C windows",
                        key=_next_key("m"))
    with cols[1]:
        sui.metric_card(title="RTCs used in DSWx-S1",
                        content=f"{used:,}",
                        description="appear as inputs",
                        key=_next_key("m"))
    with cols[2]:
        sui.metric_card(title="Missing RTCs",
                        content=f"{missing:,}",
                        description="filtered − used",
                        key=_next_key("m"))
    with cols[3]:
        # actual / expected stays within [0, 100]; used / filtered can exceed
        # 100% when DSWx references RTCs outside the surveyed window.
        rate = (actual / expected * 100) if expected else 0.0
        sui.metric_card(title="Accountability rate",
                        content=f"{rate:.2f}%",
                        description="matched ÷ expected",
                        key=_next_key("m"))

    _section_label("Pipeline breakdown")
    cols = st.columns(4)
    with cols[0]:
        sui.metric_card(title="RTC-S1 surveyed",
                        content=f"{report.get('rtc_surveyed', 0):,}",
                        description="raw CMR → deduped",
                        key=_next_key("m"))
    with cols[1]:
        sui.metric_card(title="DSWx-S1 surveyed",
                        content=f"{report.get('dswx_surveyed', 0):,}",
                        description="raw CMR → deduped",
                        key=_next_key("m"))
    with cols[2]:
        sui.metric_card(title="MGRS tile sets affected",
                        content=f"{report.get('tile_set_count', 0):,}",
                        description="land tile sets (water dropped)",
                        key=_next_key("m"))
    with cols[3]:
        sui.metric_card(title="Cycle / sensor buckets",
                        content=f"{report.get('cycle_bucket_count', 0):,}",
                        description="unique (tile-set, 12-day cycle, sensor) groups",
                        key=_next_key("m"))

    # Missing RTC list.
    missing_list = report.get("missing") or []
    if missing_list:
        today = datetime.now().strftime("%Y-%m-%d")
        _download_header(
            title="Missing RTC products",
            count=len(missing_list),
            file_base=f"{product}_missing_rtcs_{today}",
            items=missing_list,
            include_json=True,
        )
        with st.expander(f"Preview first {min(100, len(missing_list))} of {len(missing_list):,}"):
            for granule_id in missing_list[:100]:
                st.code(granule_id, language=None)

    # Cycle / tile-set drill-down.
    report_dir = report.get("_report_dir")
    if not report_dir:
        return

    cycles_path = Path(report_dir) / "missing_mgrs_set_cycle_indices.json"
    if cycles_path.exists():
        _section_label("Tile-set / cycle / sensor buckets")
        try:
            with open(cycles_path) as f:
                cycle_map = json.load(f)
        except (OSError, json.JSONDecodeError) as err:
            sui.alert(title="Could not read cycle buckets", description=str(err),
                      key=_next_key("alert"))
            cycle_map = {}

        if cycle_map:
            rows = [{"Bucket": key, "RTC count": len(rtcs)}
                    for key, rtcs in list(cycle_map.items())[:200]]
            sui.table(data=pd.DataFrame(rows), key=_next_key("tbl"))
            if len(cycle_map) > 200:
                sui.badges(
                    badge_list=[(f"+{len(cycle_map) - 200:,} more buckets", "outline")],
                    key=_next_key("badge"),
                )

    # Artifact manifest.
    _section_label("Raw artifacts")
    artifact_rows = []
    for name in sorted(Path(report_dir).glob("*.json")):
        size_kb = name.stat().st_size / 1024
        artifact_rows.append({
            "File": name.name,
            "Size (KB)": f"{size_kb:,.1f}",
        })
    if artifact_rows:
        sui.table(data=pd.DataFrame(artifact_rows), key=_next_key("tbl"))
        st.caption(f"Report directory: `{report_dir}`")


def _render_dist_s1_panel(product: str, report: dict) -> None:
    _render_report_meta_strip(report)

    _section_label("Accountability")
    expected = report.get("expected", 0)
    actual = report.get("actual", 0)
    missing = report.get("missing_count", 0)
    used = report.get("used_rtc_count", 0)

    cols = st.columns(4)
    with cols[0]:
        sui.metric_card(title="RTC-S1 surveyed",
                        content=f"{report.get('rtc_surveyed', 0):,}",
                        description="deduped RTC granules",
                        key=_next_key("m"))
    with cols[1]:
        sui.metric_card(title="RTCs used in DIST-S1",
                        content=f"{used:,}",
                        description="from ISO XML PostRtcOperaIds",
                        key=_next_key("m"))
    with cols[2]:
        sui.metric_card(title="Missing RTCs",
                        content=f"{missing:,}",
                        description="surveyed − ISO XML inputs",
                        key=_next_key("m"))
    with cols[3]:
        rate = (actual / expected * 100) if expected else 0.0
        sui.metric_card(title="Accountability rate",
                        content=f"{rate:.2f}%",
                        description="matched ÷ expected",
                        key=_next_key("m"))

    _section_label("DIST-S1 audit details")
    cols = st.columns(4)
    with cols[0]:
        sui.metric_card(title="DIST-S1 surveyed",
                        content=f"{report.get('dist_surveyed', 0):,}",
                        description="ISO XML parsed",
                        key=_next_key("m"))
    with cols[1]:
        sui.metric_card(title="Existing tile/time keys",
                        content=f"{report.get('existing_tile_time_count', 0):,}",
                        description="parsed from DIST IDs",
                        key=_next_key("m"))
    with cols[2]:
        sui.metric_card(title="Burst DB mode",
                        content="On" if report.get("burst_db_enabled") else "CMR-only",
                        description="optional RTC → tile mapping",
                        key=_next_key("m"))
    with cols[3]:
        sui.metric_card(title="Missing product times",
                        content=f"{report.get('missing_dist_product_count', 0):,}",
                        description="after existing product filter",
                        key=_next_key("m"))

    missing_rtcs = report.get("missing") or []
    if missing_rtcs:
        today = datetime.now().strftime("%Y-%m-%d")
        _download_header(
            title="Missing RTC products",
            count=len(missing_rtcs),
            file_base=f"{product}_missing_rtcs_{today}",
            items=missing_rtcs,
            include_json=True,
        )
        with st.expander(f"Preview first {min(100, len(missing_rtcs))} of {len(missing_rtcs):,}"):
            for granule_id in missing_rtcs[:100]:
                st.code(granule_id, language=None)

    missing_dist = report.get("missing_dist_products") or []
    if missing_dist:
        today = datetime.now().strftime("%Y-%m-%d")
        _download_header(
            title="Potential missing DIST-S1 product times",
            count=len(missing_dist),
            file_base=f"{product}_missing_product_times_{today}",
            items=missing_dist,
            include_json=True,
        )
        with st.expander(f"Preview first {min(100, len(missing_dist))} of {len(missing_dist):,}"):
            for value in missing_dist[:100]:
                st.code(value, language=None)

    report_dir = report.get("_report_dir")
    if not report_dir:
        return

    rows_path = Path(report_dir) / "missing_dist_product_rows.json"
    if rows_path.exists():
        _section_label("Potential missing products by tile/time group")
        try:
            with open(rows_path) as f:
                rows = json.load(f)
        except (OSError, json.JSONDecodeError) as err:
            sui.alert(title="Could not read DIST-S1 rows", description=str(err),
                      key=_next_key("alert"))
            rows = []
        if rows:
            table_rows = [
                {
                    "Tile/acq group": row.get("mgrs_tile_id_acq_group"),
                    "RTC count": len(row.get("rtc_granules") or []),
                    "Product times": len(row.get("product_id_time") or []),
                }
                for row in rows[:200]
            ]
            sui.table(data=pd.DataFrame(table_rows), key=_next_key("tbl"))

    _section_label("Raw artifacts")
    artifact_rows = []
    for name in sorted(Path(report_dir).glob("*.json")):
        size_kb = name.stat().st_size / 1024
        artifact_rows.append({
            "File": name.name,
            "Size (KB)": f"{size_kb:,.1f}",
        })
    if artifact_rows:
        sui.table(data=pd.DataFrame(artifact_rows), key=_next_key("tbl"))
        st.caption(f"Report directory: `{report_dir}`")


def _render_burst_coverage(reports: dict) -> None:
    _section_label("Burst coverage analysis")

    if not reports["burst_coverage"]:
        sui.alert(
            title="No burst coverage reports",
            description=(
                "Run `opera-audit burst-coverage --save --output-dir ./output ...` "
                "to generate reports."
            ),
            key=_next_key("alert"),
        )
        return

    # Let the user pick a report by timestamp key.
    report_keys = list(reports["burst_coverage"].keys())
    selected = st.selectbox(
        "Report",
        report_keys,
        key="bc_report_selectbox",
    )
    if not selected:
        return

    report = reports["burst_coverage"][selected]
    meta = report.get("metadata", {})
    products = report.get("products", {})

    # Meta strip
    from html import escape as _e
    meta_parts = []
    if meta.get("start_datetime"):
        meta_parts.append(f"<strong>Start:</strong> {_e(str(meta['start_datetime']))}")
    if meta.get("end_datetime"):
        meta_parts.append(f"<strong>End:</strong> {_e(str(meta['end_datetime']))}")
    if meta.get("geojson"):
        meta_parts.append(f"<strong>GeoJSON:</strong> <code>{_e(str(meta['geojson']))}</code>")
    if meta.get("polarizations"):
        meta_parts.append(f"<strong>Polarizations:</strong> {_e(', '.join(meta['polarizations']))}")
    if meta_parts:
        st.markdown(
            '<div class="opera-metastrip"><span>'
            + "</span><span>".join(meta_parts)
            + "</span></div>",
            unsafe_allow_html=True,
        )

    # Topline metrics
    cols = st.columns(4)
    with cols[0]:
        sui.metric_card(
            title="SLC granules",
            content=f"{meta.get('slc_count', 0):,}",
            description="Sentinel-1 SLCs in AOI",
            key=_next_key("m"),
        )
    with cols[1]:
        sui.metric_card(
            title="Raw bursts",
            content=f"{meta.get('total_bursts_raw', 0):,}",
            description="before deduplication",
            key=_next_key("m"),
        )
    with cols[2]:
        sui.metric_card(
            title="Unique bursts",
            content=f"{meta.get('unique_bursts', 0):,}",
            description="after deduplication",
            key=_next_key("m"),
        )
    with cols[3]:
        # Average coverage across product types
        coverages = [p.get("coverage_percent", 0) for p in products.values()]
        avg_cov = sum(coverages) / len(coverages) if coverages else 0
        sui.metric_card(
            title="Avg coverage",
            content=f"{avg_cov:.1f}%",
            description=f"across {len(products)} product type(s)",
            key=_next_key("m"),
        )

    # Per-product coverage cards
    if products:
        _section_label("Coverage by product type")
        prod_cols = st.columns(len(products))
        for idx, (pt, stats) in enumerate(products.items()):
            with prod_cols[idx]:
                cov = stats.get("coverage_percent", 0)
                sui.metric_card(
                    title=pt,
                    content=f"{cov:.1f}%",
                    description=(
                        f"{stats.get('found_count', 0):,} found · "
                        f"{stats.get('missing_count', 0):,} missing · "
                        f"{stats.get('expected_count', 0):,} expected"
                    ),
                    key=_next_key("m"),
                )

        # Coverage bar chart
        chart_rows = []
        for pt, stats in products.items():
            chart_rows.append({
                "Product": pt,
                "Coverage (%)": stats.get("coverage_percent", 0),
                "Found": stats.get("found_count", 0),
                "Missing": stats.get("missing_count", 0),
                "Expected": stats.get("expected_count", 0),
            })
        if chart_rows:
            chart_df = pd.DataFrame(chart_rows)
            chart = (
                alt.Chart(chart_df)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("Product:N", title=None),
                    y=alt.Y("Coverage (%):Q", scale=alt.Scale(domain=[0, 100])),
                    color=alt.value(OPERA_ACCENT),
                    tooltip=["Product", "Coverage (%)", "Found", "Missing", "Expected"],
                )
                .configure(**_altair_theme()["config"])
                .properties(height=220)
            )
            st.altair_chart(chart, use_container_width=True)

        # Found vs missing stacked bar chart
        stacked_rows = []
        for pt, stats in products.items():
            stacked_rows.append({"Product": pt, "Status": "Found", "Count": stats.get("found_count", 0)})
            stacked_rows.append({"Product": pt, "Status": "Missing", "Count": stats.get("missing_count", 0)})
        if stacked_rows:
            _section_label("Found vs missing")
            stacked_df = pd.DataFrame(stacked_rows)
            stacked_chart = (
                alt.Chart(stacked_df)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("Product:N", title=None),
                    y=alt.Y("Count:Q"),
                    color=alt.Color(
                        "Status:N",
                        scale=alt.Scale(domain=["Found", "Missing"], range=[JPL_BLUE, NASA_RED]),
                        legend=alt.Legend(orient="top"),
                    ),
                    tooltip=["Product", "Status", "Count"],
                )
                .configure(**_altair_theme()["config"])
                .properties(height=220)
            )
            st.altair_chart(stacked_chart, use_container_width=True)

    # Missing bursts detail per product type
    for pt, stats in products.items():
        missing_list = stats.get("missing") or []
        if not missing_list:
            continue

        today = datetime.now().strftime("%Y-%m-%d")
        items = [
            f"{m.get('burst_pattern', m.get('burst_id', '?'))} | "
            f"{str(m.get('acquisition_time', ''))[:10]} | "
            f"{m.get('platform', '?')} {m.get('polarization', '')}"
            for m in missing_list
        ]
        _download_header(
            title=f"Missing {pt} products",
            count=len(missing_list),
            file_base=f"burst_coverage_{pt}_missing_{today}",
            items=items,
            include_json=True,
        )
        with st.expander(f"Preview first {min(50, len(missing_list))} of {len(missing_list):,}"):
            from html import escape as _esc
            preview_rows = []
            for m in missing_list[:50]:
                preview_rows.append([
                    _esc(m.get("burst_pattern", m.get("burst_id", "?"))),
                    _esc(str(m.get("acquisition_time", ""))[:19]),
                    _esc(m.get("platform", "?")),
                    _esc(m.get("polarization", "")),
                    f"<code>{_esc(m.get('slc_native_id', ''))}</code>",
                ])
            _render_html_table(
                ["Burst", "Acquisition", "Platform", "Pol", "Source SLC"],
                preview_rows,
            )
            if len(missing_list) > 50:
                sui.badges(
                    badge_list=[(f"+{len(missing_list) - 50:,} more", "outline")],
                    key=_next_key("badge"),
                )


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------


def main():
    """Main dashboard application."""
    st.set_page_config(
        page_title="OPERA Accountability Dashboard",
        page_icon="🛰️",
        layout="wide",
    )

    # Reset the per-render shadcn key counter.
    st.session_state[_CARD_KEY_COUNTER] = 0

    # Inject theme CSS before widgets so shadcn components inherit it.
    _inject_theme_css()

    # Data directory from CLI args.
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("./output")

    _render_header(data_dir)

    reports = load_reports(data_dir)

    # Navigation — shadcn tabs.
    tab = sui.tabs(
        options=["Overview", "Duplicates", "Accountability", "Burst Coverage"],
        default_value="Overview",
        key="nav_tabs",
    )

    if tab == "Overview":
        _render_overview(reports)
    elif tab == "Duplicates":
        _render_duplicates(reports)
    elif tab == "Accountability":
        _render_accountability(reports)
    elif tab == "Burst Coverage":
        _render_burst_coverage(reports)


if __name__ == "__main__":
    main()
