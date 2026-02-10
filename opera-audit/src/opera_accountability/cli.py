"""CLI interface using Typer and Rich."""

import logging
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from . import CONFIG, __version__
from .cmr import query_cmr
from .duplicates import detect_duplicates
from .accountability import analyze_accountability
from .reports import save_reports

# Set up logging (default to WARNING, not INFO)
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Create Typer app
app = typer.Typer(
    name="opera-audit",
    help="OPERA accountability and duplicate detection tool",
    add_completion=False,
    no_args_is_help=True
)

console = Console()


@app.command()
def duplicates(
    product: str = typer.Argument(..., help="Product name (DSWX_HLS, RTC_S1, CSLC_S1, DSWX_S1, DISP_S1)"),
    days_back: int = typer.Option(7, "--days-back", "-d", help="Number of days to look back"),
    start: Optional[str] = typer.Option(None, "--start", "-s", help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, "--end", "-e", help="End date (YYYY-MM-DD)"),
    venue: str = typer.Option("PROD", "--venue", "-v", help="Venue (PROD or UAT)"),
    save: bool = typer.Option(False, "--save", help="Save reports to files (default: stdout only)"),
    output_dir: str = typer.Option("./output", "--output-dir", "-o", help="Output directory (used with --save)"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Minimal output"),
    verbose: bool = typer.Option(False, "--verbose", help="Verbose output")
):
    """Run duplicate detection for a product."""

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate product
    if product not in CONFIG['products']:
        console.print(f"[red]Error: Unknown product '{product}'[/red]")
        console.print(f"Available products: {', '.join(CONFIG['products'].keys())}")
        raise typer.Exit(1)

    # Calculate date range
    if start and end:
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
    else:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

    if not quiet:
        mode_str = "save to files" if save else "stdout only"
        console.print(Panel(
            f"[bold]Duplicate Detection[/bold]\n"
            f"Product: {product}\n"
            f"Venue: {venue}\n"
            f"Date Range: {start_date.date()} to {end_date.date()}\n"
            f"Mode: {mode_str}",
            title="OPERA Audit",
            border_style="cyan"
        ))

    # Get collection ID
    ccid = CONFIG['products'][product]['ccid'][venue]
    if not ccid:
        console.print(f"[red]Error: No collection ID configured for {product} in {venue}[/red]")
        raise typer.Exit(1)

    # Query CMR (progress bar shown by query_cmr)
    cmr_granules = query_cmr(ccid, start_date, end_date, venue)

    if len(cmr_granules) == 0:
        console.print("[yellow]No granules found in date range[/yellow]")
        return

    # Detect duplicates
    if not quiet:
        console.print("\n[cyan]Analyzing for duplicates...[/cyan]")
    results = detect_duplicates(cmr_granules, product)

    # Save reports to files only if --save flag is used
    if save:
        if not quiet:
            console.print("[cyan]Saving reports...[/cyan]")
        files = save_reports(results, output_dir, product, 'duplicates', venue)

    # Display summary (always show unless quiet)
    if not quiet:
        table = Table(title=f"Duplicate Detection Summary - {product}")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")

        table.add_row("Total Granules", f"{results['total']:,}")
        table.add_row("Unique Granules", f"{results['unique']:,}")
        table.add_row("Duplicates", f"{results['duplicates']:,}", style="yellow")

        if results['total'] > 0:
            dup_rate = (results['duplicates'] / results['total']) * 100
            table.add_row("Duplicate Rate", f"{dup_rate:.2f}%")

        console.print(table)

        if save:
            console.print("\n[bold]Files created:[/bold]")
            for file_type, path in files.items():
                console.print(f"  {file_type}: {path}")

    # In quiet mode, just print the numbers
    if quiet:
        print(f"{results['total']},{results['unique']},{results['duplicates']}")

    if not quiet:
        console.print("[green]Done![/green]")


@app.command()
def accountability(
    days_back: int = typer.Option(30, "--days-back", "-d", help="Number of days to look back"),
    start: Optional[str] = typer.Option(None, "--start", "-s", help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, "--end", "-e", help="End date (YYYY-MM-DD)"),
    venue: str = typer.Option("PROD", "--venue", "-v", help="Venue (PROD or UAT)"),
    save: bool = typer.Option(False, "--save", help="Save reports to files (default: stdout only)"),
    output_dir: str = typer.Option("./output", "--output-dir", "-o", help="Output directory (used with --save)"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Minimal output"),
    verbose: bool = typer.Option(False, "--verbose", help="Verbose output")
):
    """Run accountability analysis for DSWX_HLS (HLS input mapping)."""

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    product = 'DSWX_HLS'

    # Calculate date range
    if start and end:
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
    else:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

    if not quiet:
        mode_str = "save to files" if save else "stdout only"
        console.print(Panel(
            f"[bold]Accountability Analysis[/bold]\n"
            f"Product: {product}\n"
            f"Venue: {venue}\n"
            f"Date Range: {start_date.date()} to {end_date.date()}\n"
            f"Mode: {mode_str}",
            title="OPERA Audit",
            border_style="cyan"
        ))

    # Get collection IDs
    dswx_ccid = CONFIG['products'][product]['ccid'][venue]
    hls_s30_ccid = CONFIG['products'][product]['accountability']['hls_s30_ccid'][venue]
    hls_l30_ccid = CONFIG['products'][product]['accountability']['hls_l30_ccid'][venue]

    # Query CMR (progress bars shown by query_cmr)
    dswx_granules = query_cmr(dswx_ccid, start_date, end_date, venue)
    hls_s30_granules = query_cmr(hls_s30_ccid, start_date, end_date, venue)
    hls_l30_granules = query_cmr(hls_l30_ccid, start_date, end_date, venue)

    hls_granules = hls_s30_granules + hls_l30_granules

    if len(dswx_granules) == 0 and len(hls_granules) == 0:
        console.print("[yellow]No granules found in date range[/yellow]")
        return

    # Analyze accountability
    if not quiet:
        console.print("\n[cyan]Analyzing accountability...[/cyan]")
    results = analyze_accountability(dswx_granules, hls_granules)

    # Save reports to files only if --save flag is used
    if save:
        if not quiet:
            console.print("[cyan]Saving reports...[/cyan]")
        files = save_reports(results, output_dir, product, 'accountability', venue)

    # Display summary (always show unless quiet)
    if not quiet:
        table = Table(title=f"Accountability Summary - {product}")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")

        table.add_row("Expected HLS Granules", f"{results['expected']:,}")
        table.add_row("Matched DSWx Granules", f"{results['actual']:,}")
        table.add_row("Missing DSWx Outputs", f"{results['missing_count']:,}", style="yellow")

        if results['expected'] > 0:
            acc_rate = (results['actual'] / results['expected']) * 100
            table.add_row("Accountability Rate", f"{acc_rate:.2f}%")

        console.print(table)

        if save:
            console.print("\n[bold]Files created:[/bold]")
            for file_type, path in files.items():
                console.print(f"  {file_type}: {path}")

    # In quiet mode, just print the numbers
    if quiet:
        print(f"{results['expected']},{results['actual']},{results['missing_count']}")

    if not quiet:
        console.print("[green]Done![/green]")


@app.command()
def dashboard(
    port: int = typer.Option(8501, "--port", "-p", help="Streamlit port"),
    data_dir: str = typer.Option("./output", "--data-dir", "-d", help="Data directory to read reports from")
):
    """Launch Streamlit dashboard."""
    dashboard_script = Path(__file__).parent / "dashboard.py"

    if not dashboard_script.exists():
        console.print("[red]Error: dashboard.py not found[/red]")
        raise typer.Exit(1)

    console.print(f"[cyan]Launching dashboard on port {port}...[/cyan]")
    console.print(f"[cyan]Reading data from: {data_dir}[/cyan]")

    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run",
            str(dashboard_script),
            "--server.port", str(port),
            "--", data_dir
        ])
    except KeyboardInterrupt:
        console.print("\n[yellow]Dashboard stopped[/yellow]")


@app.command()
def version():
    """Show version information."""
    console.print(f"opera-audit version {__version__}")


if __name__ == "__main__":
    app()
