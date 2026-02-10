"""Streamlit dashboard for visualizing OPERA accountability and duplicates."""

import json
import sys
from pathlib import Path
from datetime import datetime

import streamlit as st


def load_reports(data_dir: Path) -> dict:
    """
    Load the latest JSON reports from the data directory.

    Args:
        data_dir: Path to output directory

    Returns:
        Dict with loaded reports organized by product and type
    """
    reports_dir = data_dir / "reports"
    reports = {
        'duplicates': {},
        'accountability': {}
    }

    if not reports_dir.exists():
        return reports

    # Load duplicate reports
    dup_dir = reports_dir / "duplicates"
    if dup_dir.exists():
        for product_dir in dup_dir.iterdir():
            if product_dir.is_dir():
                product = product_dir.name
                # Find latest JSON file
                json_files = sorted(product_dir.glob("*.json"), reverse=True)
                if json_files:
                    with open(json_files[0]) as f:
                        reports['duplicates'][product] = json.load(f)

    # Load accountability reports
    acc_dir = reports_dir / "accountability"
    if acc_dir.exists():
        for product_dir in acc_dir.iterdir():
            if product_dir.is_dir():
                product = product_dir.name
                # Find latest JSON file
                json_files = sorted(product_dir.glob("*.json"), reverse=True)
                if json_files:
                    with open(json_files[0]) as f:
                        reports['accountability'][product] = json.load(f)

    return reports


def main():
    """Main dashboard application."""
    st.set_page_config(
        page_title="OPERA Accountability Dashboard",
        page_icon="📊",
        layout="wide"
    )

    # Hide deploy button and sidebar
    st.markdown("""
        <style>
            /* Hide the deploy button in the toolbar */
            [data-testid="stToolbar"] {
                display: none;
            }
            /* Hide sidebar completely */
            [data-testid="stSidebar"] {
                display: none;
            }
            section[data-testid="stSidebar"] {
                display: none;
            }
        </style>
        """, unsafe_allow_html=True)

    st.title("📊 OPERA Accountability Dashboard")

    # Get data directory from command line or use default
    if len(sys.argv) > 1:
        data_dir = Path(sys.argv[1])
    else:
        data_dir = Path("./output")

    # Load reports
    reports = load_reports(data_dir)

    # Navigation in main area (no sidebar)
    st.markdown("---")
    col1, col2, col3, col4 = st.columns([2, 2, 2, 6])

    with col1:
        if st.button("📊 Overview", use_container_width=True):
            st.session_state.page = "Overview"
    with col2:
        if st.button("🔍 Duplicates", use_container_width=True):
            st.session_state.page = "Duplicates"
    with col3:
        if st.button("✅ Accountability", use_container_width=True):
            st.session_state.page = "Accountability"
    with col4:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()

    # Initialize session state
    if 'page' not in st.session_state:
        st.session_state.page = "Overview"

    page = st.session_state.page

    st.markdown(f"**Data Directory:** `{data_dir}` | **Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.markdown("---")

    # OVERVIEW PAGE
    if page == "Overview":
        st.header("Overview")

        # Check if we have any data
        if not reports['duplicates'] and not reports['accountability']:
            st.warning("No reports found. Run `opera-audit duplicates` or `opera-audit accountability` to generate reports.")
            return

        # Overall metrics
        col1, col2, col3 = st.columns(3)

        # Calculate totals from duplicates
        total_granules = sum(
            r['results']['total'] for r in reports['duplicates'].values()
        )
        total_duplicates = sum(
            r['results']['duplicates'] for r in reports['duplicates'].values()
        )

        with col1:
            st.metric("Total Granules", f"{total_granules:,}")

        with col2:
            st.metric("Total Duplicates", f"{total_duplicates:,}")

        with col3:
            if total_granules > 0:
                dup_rate = (total_duplicates / total_granules) * 100
                st.metric("Overall Duplicate Rate", f"{dup_rate:.2f}%")

        # Product summary table
        st.subheader("Products Summary")

        if reports['duplicates']:
            product_data = []
            for product, report in reports['duplicates'].items():
                results = report['results']
                product_data.append({
                    "Product": product,
                    "Granules": f"{results['total']:,}",
                    "Duplicates": f"{results['duplicates']:,}",
                    "Rate": f"{(results['duplicates'] / results['total'] * 100):.2f}%" if results['total'] > 0 else "0%"
                })

            st.dataframe(product_data, use_container_width=True)

    # DUPLICATES PAGE
    elif page == "Duplicates":
        st.header("Duplicate Analysis")

        if not reports['duplicates']:
            st.warning("No duplicate reports found. Run `opera-audit duplicates` to generate reports.")
            return

        # Product selector
        selected_product = st.selectbox(
            "Select Product",
            list(reports['duplicates'].keys())
        )

        if selected_product:
            report = reports['duplicates'][selected_product]
            results = report['results']

            # Metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total Granules", f"{results['total']:,}")

            with col2:
                st.metric("Unique Granules", f"{results['unique']:,}")

            with col3:
                st.metric("Duplicates", f"{results['duplicates']:,}")

            with col4:
                if results['total'] > 0:
                    dup_rate = (results['duplicates'] / results['total']) * 100
                    st.metric("Duplicate Rate", f"{dup_rate:.2f}%")

            # Daily chart
            if results.get('by_date'):
                st.subheader("Duplicates by Date")

                dates = sorted(results['by_date'].keys())
                data = {
                    'Date': dates,
                    'Total': [results['by_date'][d]['total'] for d in dates],
                    'Duplicates': [results['by_date'][d]['duplicates'] for d in dates]
                }

                st.bar_chart(data, x='Date', y=['Total', 'Duplicates'])

            # Duplicate list
            if results.get('duplicate_list'):
                st.subheader(f"Duplicate Granules ({len(results['duplicate_list'])})")

                with st.expander("Show duplicate granule IDs"):
                    for granule_id in results['duplicate_list'][:100]:  # Limit display
                        st.text(granule_id)

                    if len(results['duplicate_list']) > 100:
                        st.info(f"Showing first 100 of {len(results['duplicate_list'])} duplicates")

    # ACCOUNTABILITY PAGE
    elif page == "Accountability":
        st.header("Accountability Analysis")

        if not reports['accountability']:
            st.warning("No accountability reports found. Run `opera-audit accountability` to generate reports.")
            return

        # Currently only DSWX_HLS
        if 'DSWX_HLS' in reports['accountability']:
            report = reports['accountability']['DSWX_HLS']
            results = report['results']

            # Metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Expected HLS Granules", f"{results['expected']:,}")

            with col2:
                st.metric("Matched DSWx Outputs", f"{results['actual']:,}")

            with col3:
                st.metric("Missing Outputs", f"{results['missing_count']:,}")

            with col4:
                if results['expected'] > 0:
                    acc_rate = (results['actual'] / results['expected']) * 100
                    st.metric("Accountability Rate", f"{acc_rate:.2f}%")

            # Missing granules list
            if results.get('missing'):
                st.subheader(f"Missing DSWx-HLS Outputs ({len(results['missing'])})")

                with st.expander("Show missing HLS granule IDs"):
                    for granule_id in results['missing'][:100]:  # Limit display
                        st.text(granule_id)

                    if len(results['missing']) > 100:
                        st.info(f"Showing first 100 of {len(results['missing'])} missing granules")


if __name__ == "__main__":
    main()
