"""Streamlit dashboard for visualizing OPERA accountability and duplicates.

OPERA / JPL-branded, built on ``streamlit-shadcn-ui`` for modern components
(cards, metric cards, tabs, badges). Supports a user-facing light/dark theme
toggle persisted in :mod:`streamlit.session_state`.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
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
    reports = {'duplicates': {}, 'accountability': {}}

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
                        reports['duplicates'][product_dir.name] = json.load(f)

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
                    reports['accountability'][product] = json.load(f)
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
            summary['_report_dir'] = str(latest)
            reports['accountability'][product] = summary

    return reports


def _unwrap_accountability_results(report: dict) -> dict:
    """Return the canonical results dict regardless of report shape."""
    if 'results' in report and isinstance(report['results'], dict):
        return report['results']
    return report


def _is_dswx_s1_report(report: dict) -> bool:
    """Heuristic: DSWX_S1 summary.json carries pipeline-specific keys."""
    return 'tile_set_count' in report and 'rtc_surveyed' in report


def _extract_generated_at(report: dict) -> str | None:
    """Pull the ``generated_at`` ISO timestamp from whichever report schema.

    - DSWX_HLS / duplicates: ``report['report_metadata']['generated_at']``
    - DSWX_S1 pipeline:      ``report['metadata']['generated_at']``
    """
    if 'report_metadata' in report and isinstance(report['report_metadata'], dict):
        return report['report_metadata'].get('generated_at')
    if 'metadata' in report and isinstance(report['metadata'], dict):
        return report['metadata'].get('generated_at')
    return None


def _format_age(generated_at: str | None) -> str:
    """Format an ISO timestamp as a wall-clock label for the "Generated" column.

    Returns the local-time timestamp as ``YYYY-MM-DD HH:MM`` (operators asked
    for absolute timestamps instead of the original relative labels like
    ``"Today 17:05"`` / ``"3d ago"`` — easier to correlate with log lines and
    cron schedules). Returns ``"unknown"`` if the input is falsy or cannot be
    parsed.
    """
    if not generated_at:
        return "unknown"
    try:
        # Handle both "...Z" and naive ISO strings.
        ts = generated_at.replace('Z', '+00:00')
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
    except (ValueError, TypeError):
        return "unknown"

    return dt.strftime('%Y-%m-%d %H:%M')


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
    body = escape(label) if label is not None else f'{rate_pct:.2f}%'
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
            'Duplicate detection &amp; accountability analysis for OPERA products'
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
    loaded = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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
        '<div class="opera-legend-row"><span class="opera-freshness">YYYY-MM-DD HH:MM</span>'
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
    """Format an ISO timestamp for the meta strip, or return ``None``."""
    if not value:
        return None
    try:
        ts = value.replace('Z', '+00:00')
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
    except (ValueError, TypeError):
        return value
    return dt.strftime('%Y-%m-%d %H:%M')


def _render_report_meta_strip(report: dict) -> None:
    """Render a meta strip (Start / End / Venue / Generated) for any report.

    Handles both schemas:
    - duplicates / DSWX_HLS: ``report['report_metadata']``
    - DSWX_S1 pipeline:      ``report['metadata']``
    """
    meta = {}
    if isinstance(report.get('report_metadata'), dict):
        meta = report['report_metadata']
    elif isinstance(report.get('metadata'), dict):
        meta = report['metadata']

    start = _format_meta_timestamp(meta.get('start_date'))
    end = _format_meta_timestamp(meta.get('end_date'))
    generated = _format_meta_timestamp(meta.get('generated_at'))
    venue = meta.get('venue')

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
        + '</span><span>'.join(parts)
        + '</span></div>',
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

    if not reports['duplicates'] and not reports['accountability']:
        sui.alert(
            title="No reports yet",
            description=(
                "Run `opera-audit duplicates <PRODUCT> --save` or "
                "`opera-audit accountability <PRODUCT> --save` to generate reports."
            ),
            key=_next_key("alert_empty"),
        )
        return

    # Topline metrics — shadcn metric cards.
    total_granules = sum(r['results']['total'] for r in reports['duplicates'].values())
    total_duplicates = sum(r['results']['duplicates'] for r in reports['duplicates'].values())
    total_accountability_products = len(reports['accountability'])

    cols = st.columns(4)
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

    # (Legend now lives in the header popover — see _render_legend_popover.)

    # Duplicate rate bar chart across products.
    if reports['duplicates']:
        _section_label("Duplicate rate by product")
        chart_rows = []
        for product, report in reports['duplicates'].items():
            res = report['results']
            rate = (res['duplicates'] / res['total'] * 100) if res['total'] else 0.0
            chart_rows.append({
                "Product": product,
                "Rate (%)": round(rate, 2),
                "Duplicates": res['duplicates'],
            })
        chart_df = pd.DataFrame(chart_rows).sort_values("Rate (%)", ascending=False)
        chart = (
            alt.Chart(chart_df)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X('Product:N', sort='-y', title=None),
                y=alt.Y('Rate (%):Q'),
                color=alt.value(JPL_BLUE),
                tooltip=['Product', 'Rate (%)', 'Duplicates'],
            )
            .configure(**_altair_theme()["config"])
            .properties(height=220)
        )
        st.altair_chart(chart, use_container_width=True)

    # Per-product duplicate summary with freshness + status columns.
    if reports['duplicates']:
        _section_label("Products — duplicate summary")
        from html import escape as _e
        rows = []
        for product, report in reports['duplicates'].items():
            results = report['results']
            rate = (results['duplicates'] / results['total'] * 100) if results['total'] else 0.0
            rows.append([
                f"<strong>{_e(product)}</strong>",
                f"{results['total']:,}",
                f"{results['unique']:,}",
                f"{results['duplicates']:,}",
                _status_for_duplicate_rate(rate),
                freshness_chip_html(_format_age(_extract_generated_at(report))),
            ])
        _render_html_table(
            ["Product", "Granules", "Unique", "Duplicates", "Status", "Generated"],
            rows,
        )

    # Per-product accountability summary (with freshness + status).
    if reports['accountability']:
        _section_label("Products — accountability summary")
        from html import escape as _e
        acc_rows = []
        for product, report in reports['accountability'].items():
            if _is_dswx_s1_report(report):
                filtered = report.get('filtered_rtc_count', 0)
                missing = report.get('missing_count', 0)
                expected = report.get('expected', filtered)
                actual = report.get('actual', report.get('used_rtc_count', 0))
                # actual / expected is bounded to [0, 100] (used/filtered
                # can overshoot when DSWx references pre-window RTCs).
                rate = (actual / expected * 100) if expected else 0.0
                expected_label = f"{expected:,}"
            else:
                r = _unwrap_accountability_results(report)
                expected = r.get('expected', 0)
                actual = r.get('actual', 0)
                missing = r.get('missing_count', 0)
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


def _render_duplicates(reports: dict) -> None:
    _section_label("Duplicate analysis")

    if not reports['duplicates']:
        sui.alert(
            title="No duplicate reports",
            description="Run `opera-audit duplicates [PRODUCT] --save` to generate reports.",
            key=_next_key("alert"),
        )
        return

    selected = st.selectbox(
        "Product",
        list(reports['duplicates'].keys()),
        key="dup_product_selectbox",
    )
    if not selected:
        return

    report = reports['duplicates'][selected]
    _render_report_meta_strip(report)
    results = report['results']

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
        rate = (results['duplicates'] / results['total'] * 100) if results['total'] else 0.0
        sui.metric_card(title="Duplicate rate", content=f"{rate:.2f}%",
                        description="duplicates ÷ total", key=_next_key("m"))

    # Altair bar chart — one bar per date, two layers (total vs duplicates).
    by_date = results.get('by_date') or {}
    if by_date:
        _section_label("Duplicates by date")
        df = pd.DataFrame([
            {"Date": d, "Total": by_date[d]['total'], "Duplicates": by_date[d]['duplicates']}
            for d in sorted(by_date.keys())
        ])
        melted = df.melt('Date', var_name='Series', value_name='Count')
        chart = (
            alt.Chart(melted)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X('Date:N', title=None),
                y=alt.Y('Count:Q', title='Granules'),
                color=alt.Color('Series:N', legend=alt.Legend(orient='top')),
                xOffset='Series:N',
                tooltip=['Date', 'Series', 'Count'],
            )
            .configure(**_altair_theme()["config"])
            .properties(height=260)
        )
        st.altair_chart(chart, use_container_width=True)

    dup_list = results.get('duplicate_list') or []
    if dup_list:
        today = datetime.now().strftime('%Y-%m-%d')
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

    if not reports['accountability']:
        sui.alert(
            title="No accountability reports",
            description="Run `opera-audit accountability [PRODUCT] --save` to generate reports.",
            key=_next_key("alert"),
        )
        return

    selected = st.selectbox(
        "Product",
        list(reports['accountability'].keys()),
        key="acc_product_selectbox",
    )
    if not selected:
        return

    report = reports['accountability'][selected]
    if _is_dswx_s1_report(report):
        _render_dswx_s1_panel(selected, report)
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
        rate = (results['actual'] / results['expected'] * 100) if results['expected'] else 0.0
        sui.metric_card(title="Accountability rate",
                        content=f"{rate:.2f}%",
                        description="matched ÷ expected",
                        key=_next_key("m"))

    missing = results.get('missing') or []
    if missing:
        today = datetime.now().strftime('%Y-%m-%d')
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


def _render_dswx_s1_panel(product: str, report: dict) -> None:
    _render_report_meta_strip(report)

    _section_label("Accountability")
    filtered = report.get('filtered_rtc_count', 0)
    used = report.get('used_rtc_count', 0)
    missing = report.get('missing_count', 0)
    expected = report.get('expected', filtered)
    actual = report.get('actual', used)

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
    missing_list = report.get('missing') or []
    if missing_list:
        today = datetime.now().strftime('%Y-%m-%d')
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
    report_dir = report.get('_report_dir')
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
        options=["Overview", "Duplicates", "Accountability"],
        default_value="Overview",
        key="nav_tabs",
    )

    if tab == "Overview":
        _render_overview(reports)
    elif tab == "Duplicates":
        _render_duplicates(reports)
    elif tab == "Accountability":
        _render_accountability(reports)


if __name__ == "__main__":
    main()
