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

from opera_accountability import CONFIG, __version__
from opera_accountability.cmr import query_cmr, query_cmr_by_short_name
from opera_accountability.duplicates import (
    detect_duplicates,
    detect_disp_s1_end_conflicts,
    detect_duplicates_memory_efficient
)
from opera_accountability.reports import save_reports
from .strategies.dswx_hls import analyze_accountability

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
    product: Optional[str] = typer.Argument(None, help="Product name (DSWX_HLS, RTC_S1, CSLC_S1, DSWX_S1, DISP_S1). If omitted, runs for all products."),
    days_back: int = typer.Option(7, "--days-back", "-d", help="Number of days to look back"),
    start: Optional[str] = typer.Option(None, "--start", "-s", help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, "--end", "-e", help="End date (YYYY-MM-DD)"),
    venue: str = typer.Option("PROD", "--venue", "-v", help="Venue (PROD or UAT)"),
    save: bool = typer.Option(False, "--save", help="Save reports to files (default: stdout only)"),
    output_dir: str = typer.Option("./output", "--output-dir", "-o", help="Output directory (used with --save)"),
    check_end_conflicts: bool = typer.Option(False, "--check-end-conflicts", help="Check for DISP-S1 end conflicts (same frame+end date, different begin date)"),
    memory_efficient: bool = typer.Option(True, "--memory-efficient/--no-memory-efficient", help="Use memory-efficient batched processing (default: enabled)"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Minimal output"),
    verbose: bool = typer.Option(False, "--verbose", help="Verbose output")
):
    """Run duplicate detection for a product (or all products if none specified)."""

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # If no product specified, run for all products
    if product is None:
        _run_duplicates_all(
            days_back=days_back,
            start=start,
            end=end,
            venue=venue,
            save=save,
            output_dir=output_dir,
            check_end_conflicts=check_end_conflicts,
            memory_efficient=memory_efficient,
            quiet=quiet,
        )
        return

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

    # Get collection ID (fall back to short_name query if ccid is empty)
    ccid = CONFIG['products'][product]['ccid'][venue]
    has_collection = 'collection' in CONFIG['products'][product] and CONFIG['products'][product]['collection'].get(venue)
    if not ccid and not has_collection:
        console.print(f"[red]Error: No collection ID or short_name configured for {product} in {venue}[/red]")
        raise typer.Exit(1)

    # Query CMR (progress bar shown by query_cmr)
    # End-conflict detection requires the full granule list, so disable
    # memory-efficient mode when both flags are set.
    # Also disable if no CCID (memory-efficient path lacks short-name fallback).
    is_static = CONFIG['products'][product].get('static', False)
    use_memory_efficient = memory_efficient and not (check_end_conflicts and product == 'DISP_S1') and not is_static and bool(ccid)

    if use_memory_efficient:
        if not quiet:
            console.print("\n[cyan]Using memory-efficient batched processing...[/cyan]")
        results = detect_duplicates_memory_efficient(product, start_date, end_date, venue)
    else:
        if ccid:
            cmr_granules = query_cmr(ccid, start_date, end_date, venue, skip_temporal=is_static)
        else:
            coll = CONFIG['products'][product]['collection'][venue]
            cmr_granules = query_cmr_by_short_name(
                coll['short_name'], coll['provider'], start_date, end_date, venue
            )

        if len(cmr_granules) == 0:
            console.print("[yellow]No granules found in date range[/yellow]")
            return

        # Detect duplicates or end conflicts
        if not quiet:
            console.print("\n[cyan]Analyzing for duplicates...[/cyan]")

        if check_end_conflicts and product == 'DISP_S1':
            results = detect_disp_s1_end_conflicts(cmr_granules)
        else:
            results = detect_duplicates(cmr_granules, product)

    # Save reports to files only if --save flag is used
    if save:
        if not quiet:
            console.print("[cyan]Saving reports...[/cyan]")
        files = save_reports(
            results, output_dir, product, 'duplicates', venue,
            start_date=start_date, end_date=end_date,
        )

    # Display summary (always show unless quiet)
    if not quiet:
        if check_end_conflicts and product == 'DISP_S1':
            table = Table(title=f"DISP-S1 End Conflict Summary")
            table.add_column("Metric", style="cyan")
            table.add_column("Count", justify="right", style="green")
            table.add_row("Total granules", f"{results['total']:,}")
            table.add_row("Conflict groups", f"{results['conflict_groups']:,}", style="yellow")
            table.add_row("Conflicting products", f"{results['conflicting_products']:,}", style="yellow")
            if results['total'] > 0:
                conflict_rate = (results['conflicting_products'] / results['total']) * 100
                table.add_row("Conflict rate", f"{conflict_rate:.2f}%")
            console.print(table)
        else:
            table = Table(title=f"Duplicate Summary - {product}")
            table.add_column("Metric", style="cyan")
            table.add_column("Count", justify="right", style="green")

            table.add_row("Total granules", f"{results['total']:,}")
            table.add_row("Unique granules", f"{results['unique']:,}")
            table.add_row("Duplicates", f"{results['duplicates']:,}", style="yellow")

            if results['total'] > 0:
                dup_rate = (results['duplicates'] / results['total']) * 100
                table.add_row("Duplicate rate", f"{dup_rate:.2f}%")

            console.print(table)

        if save:
            console.print("\n[bold]Files created:[/bold]")
            for file_type, path in files.items():
                console.print(f"  {file_type}: {path}")

    # In quiet mode, just print the numbers
    if quiet:
        if check_end_conflicts and product == 'DISP_S1':
            print(f"{results['total']},{results['conflict_groups']},{results['conflicting_products']}")
        else:
            print(f"{results['total']},{results['unique']},{results['duplicates']}")

    if not quiet:
        console.print("[green]Done![/green]")


@app.command()
def accountability(
    product: Optional[str] = typer.Argument(
        None,
        help="Product to analyze (DSWX_HLS, DSWX_S1, DIST_S1, TROPO, DISP_S1, DISP_S1_STATIC). If omitted, runs for all enabled products."
    ),
    strategy: Optional[str] = typer.Option(None, "--strategy", "-s", help="Accountability strategy (forward_map, date_count, delegated_validator, db_based). Auto-detected from config if not specified."),
    days_back: int = typer.Option(7, "--days-back", "-d", help="Number of days to look back"),
    start: Optional[str] = typer.Option(None, "--start", help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, "--end", help="End date (YYYY-MM-DD)"),
    venue: str = typer.Option("PROD", "--venue", "-v", help="Venue (PROD or UAT)"),
    save: bool = typer.Option(False, "--save", help="Save reports to files (default: stdout only)"),
    output_dir: str = typer.Option("./output", "--output-dir", "-o", help="Output directory (used with --save)"),
    recovery_format: Optional[str] = typer.Option(None, "--recovery-format", help="Recovery file format (txt, json). Generates recovery files for missing products."),
    db_path: Optional[str] = typer.Option(None, "--db-path", help="Database path for db_based strategy (e.g., frame-to-burst JSON)"),
    burst_db: Optional[str] = typer.Option(
        None, "--burst-db",
        help=(
            "Path to a DIST-S1 burst DB mapping file (DIST_S1 only). "
            "If omitted, opera-audit runs in CMR-only RTC accountability mode "
            "unless opera-sds-pcm burst DB utilities are available."
        )
    ),
    mgrs_db: Optional[str] = typer.Option(
        None, "--mgrs-db",
        help=(
            "Path to the MGRS tile-collection SQLite DB (DSWX_S1 only). "
            "Falls back to the OPERA_MGRS_DB environment variable when omitted."
        )
    ),
    max_concurrent: Optional[int] = typer.Option(
        None, "--max-concurrent",
        help="Maximum concurrent DIST-S1 iso.xml downloads (DIST_S1 only)."
    ),
    max_retries: Optional[int] = typer.Option(
        None, "--max-retries",
        help="Maximum DIST-S1 iso.xml download retries (DIST_S1 only)."
    ),
    prefer_s3: bool = typer.Option(
        False, "--prefer-s3",
        help="Prefer S3 iso.xml URLs over HTTPS URLs (DIST_S1 only; requires boto3)."
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Minimal output"),
    verbose: bool = typer.Option(False, "--verbose", help="Verbose output")
):
    """Run accountability analysis for a product.

    Supported products (selected via ``accountability.strategy`` in config.yaml):

    - ``DSWX_HLS`` — strategy ``dswx_hls`` (HLS input → DSWx-HLS output mapping)
    - ``DSWX_S1`` — strategy ``dswx_s1`` (RTC-S1 → DSWx-S1 4-step pipeline)
    - ``DIST_S1`` — strategy ``dist_s1`` (RTC-S1 → DIST-S1 ISO XML input mapping)
    
    New strategies (Phase 3):
    - ``TROPO`` — strategy ``date_count`` (date-based counting)
    - ``DISP_S1`` — strategy ``delegated_validator`` (external validator)
    - ``DISP_S1_STATIC`` — strategy ``db_based`` (database-based coverage)
    
    Use --strategy to override the default strategy for a product.
    """

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # If no product specified, run for all enabled products
    if product is None:
        _run_accountability_all(
            days_back=days_back,
            start=start,
            end=end,
            venue=venue,
            save=save,
            output_dir=output_dir,
            quiet=quiet,
            mgrs_db=mgrs_db,
            db_path=db_path,
        )
        return

    # Validate product
    product_cfg = CONFIG['products'].get(product)
    if product_cfg is None:
        console.print(f"[red]Error: Unknown product '{product}'[/red]")
        console.print(f"Available products: {', '.join(CONFIG['products'].keys())}")
        raise typer.Exit(1)

    acc_cfg = product_cfg.get('accountability') or {}

    # Use provided strategy or auto-detect from config
    if strategy:
        strategy_name = strategy
    else:
        if not acc_cfg.get('enabled'):
            console.print(f"[red]Error: accountability not enabled for {product} in config.yaml[/red]")
            console.print("[dim]Tip: use --strategy to force a specific strategy even if not enabled in config[/dim]")
            raise typer.Exit(1)
        strategy_name = acc_cfg.get('strategy', 'dswx_hls')

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
            f"Strategy: {strategy_name}\n"
            f"Venue: {venue}\n"
            f"Date Range: {start_date.date()} to {end_date.date()}\n"
            f"Mode: {mode_str}",
            title="OPERA Audit",
            border_style="cyan"
        ))

    # Dispatch by strategy
    if strategy_name == 'dswx_hls':
        _run_dswx_hls_accountability(
            product, start_date, end_date, venue, save, output_dir, quiet, recovery_format
        )
    elif strategy_name == 'dswx_s1':
        _run_dswx_s1_accountability(
            start_date, end_date, venue, save, output_dir, mgrs_db, quiet, recovery_format
        )
    elif strategy_name == 'dist_s1':
        _run_dist_s1_accountability(
            start_date, end_date, venue, save, output_dir,
            burst_db, max_concurrent, max_retries, prefer_s3, quiet, recovery_format
        )
    elif strategy_name == 'forward_map':
        _run_forward_map_accountability(
            product, start_date, end_date, venue, save, output_dir, quiet, recovery_format
        )
    elif strategy_name == 'date_count':
        _run_date_count_accountability(
            product, start_date, end_date, venue, save, output_dir, quiet, recovery_format
        )
    elif strategy_name == 'delegated_validator':
        _run_delegated_validator_accountability(
            product, start_date, end_date, venue, save, output_dir, quiet, recovery_format
        )
    elif strategy_name == 'db_based':
        _run_db_based_accountability(
            product, start_date, end_date, venue, save, output_dir, quiet, db_path, recovery_format
        )
    else:
        console.print(f"[red]Error: Unknown strategy '{strategy_name}' for {product}[/red]")
        raise typer.Exit(1)


def _run_forward_map_accountability(
    product: str,
    start_date: datetime,
    end_date: datetime,
    venue: str,
    save: bool,
    output_dir: str,
    quiet: bool,
    recovery_format: Optional[str] = None,
) -> None:
    """Run forward-map accountability strategy."""
    from opera_accountability.strategies.forward_map import ForwardMapStrategy
    
    strategy = ForwardMapStrategy(product)
    results = strategy.analyze(start_date, end_date, venue)
    
    if not quiet:
        table = Table(title=f"Forward-Map Accountability - {product}")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")
        
        table.add_row("Expected", f"{results['expected']:,}")
        table.add_row("Actual", f"{results['actual']:,}")
        table.add_row("Missing", f"{results['missing_count']:,}", style="yellow")
        
        console.print(table)
    
    if save:
        save_reports(results, output_dir, product, 'accountability', venue, start_date=start_date, end_date=end_date)
    
    if recovery_format:
        from opera_accountability.recovery_file import write_recovery_file
        output_path = f"{output_dir}/recovery_{product}"
        write_recovery_file(results, output_path, recovery_format)
        if not quiet:
            console.print(f"[cyan]Recovery file written to {output_path}.{recovery_format}[/cyan]")


def _run_date_count_accountability(
    product: str,
    start_date: datetime,
    end_date: datetime,
    venue: str,
    save: bool,
    output_dir: str,
    quiet: bool,
    recovery_format: Optional[str] = None,
) -> dict:
    """Run date-count accountability strategy."""
    from opera_accountability.strategies.date_count import DateCountStrategy
    
    strategy = DateCountStrategy(product)
    results = strategy.analyze(start_date, end_date, venue)
    
    if not quiet:
        table = Table(title=f"Date-Count Accountability - {product}")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")
        
        table.add_row("Expected Per Day", f"{results['expected_per_day']}")
        table.add_row("Total Dates", f"{results['total_dates']}")
        table.add_row("Missing Dates", f"{results['missing_dates']:,}", style="yellow")
        table.add_row("Expected Total", f"{results['expected_total']:,}")
        table.add_row("Actual Total", f"{results['actual_total']:,}")
        
        console.print(table)
    
    if save:
        save_reports(results, output_dir, product, 'accountability', venue, start_date=start_date, end_date=end_date)
    
    if recovery_format:
        from opera_accountability.recovery_file import write_recovery_files_by_date
        files = write_recovery_files_by_date(results, output_dir, recovery_format)
        if not quiet:
            console.print(f"[cyan]Recovery files written: {len(files)} files[/cyan]")
    
    return results


def _run_delegated_validator_accountability(
    product: str,
    start_date: datetime,
    end_date: datetime,
    venue: str,
    save: bool,
    output_dir: str,
    quiet: bool,
    recovery_format: Optional[str] = None,
    **kwargs
) -> None:
    """Run delegated-validator accountability strategy."""
    from opera_accountability.strategies.delegated_validator import DelegatedValidatorStrategy
    
    # Get validator parameters from config
    product_cfg = CONFIG['products'][product]
    validator_cfg = product_cfg.get('accountability', {}).get('delegated_validator', {})
    
    # Pass validator parameters from config and kwargs
    validator_kwargs = {
        'processing_mode': kwargs.get('processing_mode', validator_cfg.get('processing_mode', 'forward')),
        'k': kwargs.get('k', validator_cfg.get('k', 15)),
        'frames_only': kwargs.get('frames_only'),
    }
    
    strategy = DelegatedValidatorStrategy(product)
    results = strategy.analyze(start_date, end_date, venue, **validator_kwargs)
    
    if not quiet:
        table = Table(title=f"Delegated-Validator Accountability - {product}")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")
        
        table.add_row("Expected", f"{results['expected']:,}")
        table.add_row("Actual", f"{results['actual']:,}")
        table.add_row("Missing", f"{results['missing_count']:,}", style="yellow")
        table.add_row("Delegated", "Yes" if results.get('delegated') else "No")
        
        console.print(table)
    
    if save:
        save_reports(results, output_dir, product, 'accountability', venue, start_date=start_date, end_date=end_date)
    
    if recovery_format:
        from opera_accountability.recovery_file import write_recovery_file
        output_path = f"{output_dir}/recovery_{product}"
        write_recovery_file(results, output_path, recovery_format)
        if not quiet:
            console.print(f"[cyan]Recovery file written to {output_path}.{recovery_format}[/cyan]")


def _run_db_based_accountability(
    product: str,
    start_date: datetime,
    end_date: datetime,
    venue: str,
    save: bool,
    output_dir: str,
    quiet: bool,
    db_path: Optional[str] = None,
    recovery_format: Optional[str] = None,
) -> None:
    """Run DB-based accountability strategy."""
    from opera_accountability.strategies.db_based import DBBasedStrategy
    
    strategy = DBBasedStrategy(product)
    results = strategy.analyze(start_date, end_date, venue, db_path=db_path)
    
    if not quiet:
        table = Table(title=f"DB-Based Accountability - {product}")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")
        
        table.add_row("Expected", f"{results['expected']:,}")
        table.add_row("Actual", f"{results['actual']:,}")
        table.add_row("Missing", f"{results['missing_count']:,}", style="yellow")
        table.add_row("Coverage", f"{results['coverage_pct']:.1f}%")
        
        console.print(table)
    
    if save:
        save_reports(results, output_dir, product, 'accountability', venue, start_date=start_date, end_date=end_date)
    
    if recovery_format:
        from opera_accountability.recovery_file import write_recovery_file
        output_path = f"{output_dir}/recovery_{product}"
        write_recovery_file(results, output_path, recovery_format)
        if not quiet:
            console.print(f"[cyan]Recovery file written to {output_path}.{recovery_format}[/cyan]")


def _run_dswx_hls_accountability(
    product: str,
    start_date: datetime,
    end_date: datetime,
    venue: str,
    save: bool,
    output_dir: str,
    quiet: bool,
    recovery_format: Optional[str] = None,
) -> dict:
    """Existing DSWX_HLS pipeline, extracted so the CLI can dispatch by strategy."""
    dswx_ccid = CONFIG['products'][product]['ccid'][venue]
    hls_s30_ccid = CONFIG['products'][product]['accountability']['hls_s30_ccid'][venue]
    hls_l30_ccid = CONFIG['products'][product]['accountability']['hls_l30_ccid'][venue]

    dswx_granules = query_cmr(dswx_ccid, start_date, end_date, venue)
    hls_s30_granules = query_cmr(hls_s30_ccid, start_date, end_date, venue)
    hls_l30_granules = query_cmr(hls_l30_ccid, start_date, end_date, venue)

    hls_granules = hls_s30_granules + hls_l30_granules

    if len(dswx_granules) == 0 and len(hls_granules) == 0:
        console.print("[yellow]No granules found in date range[/yellow]")
        return

    if not quiet:
        console.print("\n[cyan]Analyzing accountability...[/cyan]")
    results = analyze_accountability(dswx_granules, hls_granules)

    files = {}
    if save:
        if not quiet:
            console.print("[cyan]Saving reports...[/cyan]")
        files = save_reports(
            results, output_dir, product, 'accountability', venue,
            start_date=start_date, end_date=end_date,
        )

    if recovery_format and results.get('missing'):
        from opera_accountability.recovery_file import write_recovery_file
        output_path = f"{output_dir}/recovery_{product}"
        write_recovery_file(results, output_path, recovery_format)
        if not quiet:
            console.print(f"[cyan]Recovery file written to {output_path}.{recovery_format}[/cyan]")

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

    if quiet:
        print(f"{results['expected']},{results['actual']},{results['missing_count']}")
    else:
        console.print("[green]Done![/green]")

    return results


def _run_dist_s1_accountability(
    start_date: datetime,
    end_date: datetime,
    venue: str,
    save: bool,
    output_dir: str,
    burst_db: Optional[str],
    max_concurrent: Optional[int],
    max_retries: Optional[int],
    prefer_s3: bool,
    quiet: bool,
    recovery_format: Optional[str] = None,
) -> dict:
    from .strategies.dist_s1 import run as run_dist_s1

    if not quiet:
        console.print("\n[cyan]Running DIST-S1 ISO-XML accountability pipeline...[/cyan]")

    results = run_dist_s1(
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        venue=venue,
        save=save,
        burst_db=burst_db,
        max_concurrent=max_concurrent,
        max_retries=max_retries,
        prefer_s3=prefer_s3,
    )

    if recovery_format and results.get('missing'):
        from opera_accountability.recovery_file import write_recovery_file
        output_path = f"{output_dir}/recovery_DIST_S1"
        write_recovery_file(
            {'missing': results['missing']}, output_path, recovery_format
        )
        if not quiet:
            console.print(f"[cyan]Recovery file written to {output_path}.{recovery_format}[/cyan]")

    if not quiet:
        table = Table(title="DIST-S1 Accountability Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")
        table.add_row("RTC-S1 surveyed", f"{results['rtc_surveyed']:,}")
        table.add_row("DIST-S1 surveyed", f"{results['dist_surveyed']:,}")
        table.add_row("RTCs used in DIST-S1", f"{results['used_rtc_count']:,}")
        table.add_row("Missing RTCs", f"{results['missing_count']:,}", style="yellow")
        if results['expected']:
            rate = results['actual'] / results['expected'] * 100
            table.add_row("Accountability rate", f"{rate:.2f}%")
        table.add_row("Burst DB enabled", "yes" if results['burst_db_enabled'] else "no")
        table.add_row("Missing DIST-S1 product times", f"{results['missing_dist_product_count']:,}")
        console.print(table)

        if save and results['files']:
            console.print("\n[bold]Files created:[/bold]")
            for file_type, path in results['files'].items():
                console.print(f"  {file_type}: {path}")

    if quiet:
        print(
            f"{results['rtc_surveyed']},{results['dist_surveyed']},"
            f"{results['used_rtc_count']},{results['missing_count']},"
            f"{results['missing_dist_product_count']}"
        )
    else:
        console.print("[green]Done![/green]")

    return results


def _run_dswx_s1_accountability(
    start_date: datetime,
    end_date: datetime,
    venue: str,
    save: bool,
    output_dir: str,
    mgrs_db: Optional[str],
    quiet: bool,
    recovery_format: Optional[str] = None,
) -> dict:
    """DSWx-S1 pipeline dispatcher: runs the 4-step strategy and renders results."""
    # Imported lazily so the dswx_s1 package is only loaded when used.
    from .strategies.dswx_s1 import run as run_dswx_s1

    if not quiet:
        console.print("\n[cyan]Running DSWx-S1 4-step accountability pipeline...[/cyan]")

    results = run_dswx_s1(
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        venue=venue,
        save=save,
        mgrs_db_override=mgrs_db,
    )

    if recovery_format and results.get('missing'):
        from opera_accountability.recovery_file import write_recovery_file
        output_path = f"{output_dir}/recovery_DSWX_S1"
        write_recovery_file(
            {'missing': results['missing']}, output_path, recovery_format
        )
        if not quiet:
            console.print(f"[cyan]Recovery file written to {output_path}.{recovery_format}[/cyan]")

    if not quiet:
        table = Table(title="DSWx-S1 Accountability Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")

        table.add_row("RTC-S1 surveyed", f"{results['rtc_surveyed']:,}")
        table.add_row("DSWx-S1 surveyed", f"{results['dswx_surveyed']:,}")
        table.add_row("RTCs after sensor filter", f"{results['filtered_rtc_count']:,}")
        table.add_row("RTCs used in DSWx-S1", f"{results['used_rtc_count']:,}")
        table.add_row("Missing RTCs", f"{results['missing_count']:,}", style="yellow")
        # actual / expected is bounded to [0, 100] — see pipeline._write_summary.
        if results['expected']:
            acc_rate = results['actual'] / results['expected'] * 100
            table.add_row("Accountability rate", f"{acc_rate:.2f}%")
        table.add_row("MGRS tile sets affected", f"{results['tile_set_count']:,}")
        table.add_row("Cycle/sensor buckets", f"{results['cycle_bucket_count']:,}")

        console.print(table)

        if save and results['files']:
            console.print("\n[bold]Files created:[/bold]")
            for file_type, path in results['files'].items():
                console.print(f"  {file_type}: {path}")

    if quiet:
        print(
            f"{results['rtc_surveyed']},{results['dswx_surveyed']},"
            f"{results['used_rtc_count']},{results['missing_count']},"
            f"{results['tile_set_count']},{results['cycle_bucket_count']}"
        )
    else:
        console.print("[green]Done![/green]")

    return results


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


def _run_duplicates_all(
    days_back: int,
    start: Optional[str],
    end: Optional[str],
    venue: str,
    save: bool,
    output_dir: str,
    check_end_conflicts: bool,
    memory_efficient: bool,
    quiet: bool,
) -> None:
    """Internal helper to run duplicate detection for all products."""
    
    # Calculate date range
    if start and end:
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
    else:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
    
    if not quiet:
        console.print(Panel(
            f"[bold]Duplicate Detection - All Products[/bold]\n"
            f"Venue: {venue}\n"
            f"Date Range: {start_date.date()} to {end_date.date()}\n"
            f"Products: {len(CONFIG['products'])}",
            title="OPERA Audit",
            border_style="cyan"
        ))
    
    # Run duplicate detection for each product
    all_results = {}
    for product in CONFIG['products'].keys():
        ccid = CONFIG['products'][product]['ccid'][venue]
        has_collection = 'collection' in CONFIG['products'][product] and CONFIG['products'][product]['collection'].get(venue)
        if not ccid and not has_collection:
            if not quiet:
                console.print(f"[yellow]Skipping {product}: No collection ID or short_name configured[/yellow]")
            continue
        
        if not quiet:
            console.print(f"\n[cyan]Processing {product}...[/cyan]")
        
        try:
            # Query CMR
            # End-conflict detection requires the full granule list.
            # Also disable memory-efficient if no CCID (lacks short-name fallback).
            is_static = CONFIG['products'][product].get('static', False)
            use_mem_eff = memory_efficient and not (check_end_conflicts and product == 'DISP_S1') and not is_static and bool(ccid)
            if use_mem_eff:
                results = detect_duplicates_memory_efficient(product, start_date, end_date, venue)
            else:
                if ccid:
                    cmr_granules = query_cmr(ccid, start_date, end_date, venue, skip_temporal=is_static)
                else:
                    coll = CONFIG['products'][product]['collection'][venue]
                    cmr_granules = query_cmr_by_short_name(
                        coll['short_name'], coll['provider'], start_date, end_date, venue
                    )
                if len(cmr_granules) == 0:
                    if not quiet:
                        console.print(f"  [yellow]No granules found[/yellow]")
                    all_results[product] = {'total': 0, 'unique': 0, 'duplicates': 0}
                    continue
                
                # Detect duplicates
                if check_end_conflicts and product == 'DISP_S1':
                    results = detect_disp_s1_end_conflicts(cmr_granules)
                else:
                    results = detect_duplicates(cmr_granules, product)
            
            all_results[product] = results
            
            if not quiet:
                if check_end_conflicts and product == 'DISP_S1':
                    console.print(f"  Total: {results['total']:,}, Conflicts: {results['conflicting_products']:,}")
                else:
                    console.print(f"  Total: {results['total']:,}, Duplicates: {results['duplicates']:,}")
            
            # Save reports
            if save:
                save_reports(results, output_dir, product, 'duplicates', venue, start_date=start_date, end_date=end_date)
        
        except Exception as e:
            if not quiet:
                console.print(f"  [red]Error: {e}[/red]")
            all_results[product] = {'error': str(e)}
    
    # Display summary table
    if not quiet:
        console.print("\n[bold]Summary:[/bold]")
        table = Table()
        table.add_column("Product", style="cyan")
        table.add_column("Total", justify="right")
        table.add_column("Duplicates", justify="right", style="yellow")
        table.add_column("Rate", justify="right")
        
        for product, results in all_results.items():
            if 'error' in results:
                table.add_row(product, "ERROR", results['error'], "")
            elif 'conflicting_products' in results:  # DISP_S1 end conflicts
                table.add_row(product, f"{results['total']:,}", f"{results['conflicting_products']:,}", "N/A")
            else:
                dup_rate = (results['duplicates'] / results['total'] * 100) if results['total'] > 0 else 0
                table.add_row(product, f"{results['total']:,}", f"{results['duplicates']:,}", f"{dup_rate:.2f}%")
        
        console.print(table)
        console.print("[green]Done![/green]")


def _run_accountability_all(
    days_back: int,
    start: Optional[str],
    end: Optional[str],
    venue: str,
    save: bool,
    output_dir: str,
    quiet: bool,
    mgrs_db: Optional[str] = None,
    db_path: Optional[str] = None,
) -> None:
    """Internal helper to run accountability for all products with accountability enabled."""
    
    # Calculate date range
    if start and end:
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
    else:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
    
    # Get products with accountability enabled
    enabled_products = [p for p, config in CONFIG['products'].items() 
                       if config.get('accountability', {}).get('enabled', False)]
    
    if not quiet:
        console.print(Panel(
            f"[bold]Accountability - All Enabled Products[/bold]\n"
            f"Venue: {venue}\n"
            f"Date Range: {start_date.date()} to {end_date.date()}\n"
            f"Products: {len(enabled_products)}",
            title="OPERA Audit",
            border_style="cyan"
        ))
    
    if not enabled_products:
        console.print("[yellow]No products have accountability enabled[/yellow]")
        return
    
    # Run accountability for each enabled product
    all_results = {}
    for product in enabled_products:
        strategy = CONFIG['products'][product]['accountability']['strategy']
        
        if not quiet:
            console.print(f"\n[cyan]Processing {product} (strategy: {strategy})...[/cyan]")
        
        try:
            if strategy == 'dswx_hls':
                results = _run_dswx_hls_accountability(
                    product, start_date, end_date, venue, save, output_dir, quiet
                )
                if results:
                    all_results[product] = results
            elif strategy == 'dswx_s1':
                results = _run_dswx_s1_accountability(start_date, end_date, venue, save, output_dir, mgrs_db, quiet)
                all_results[product] = {
                    'expected': results.get('expected', results.get('filtered_rtc_count', 0)),
                    'actual': results.get('actual', results.get('used_rtc_count', 0)),
                    'missing_count': results.get('missing_count', 0),
                }
            elif strategy == 'dist_s1':
                prefer_s3 = CONFIG['products'][product]['accountability'].get('prefer_s3_iso_xml', False)
                results = _run_dist_s1_accountability(start_date, end_date, venue, save, output_dir, None, None, None, prefer_s3, quiet)
                all_results[product] = {
                    'expected': results.get('expected', 0),
                    'actual': results.get('actual', 0),
                    'missing_count': results.get('missing_count', 0),
                }
            elif strategy == 'forward_map':
                from opera_accountability.strategies.forward_map import ForwardMapStrategy
                fm_strategy = ForwardMapStrategy(product)
                results = fm_strategy.analyze(start_date, end_date, venue)
                if not quiet:
                    table = Table(title=f"Forward-Map Accountability - {product}")
                    table.add_column("Metric", style="cyan")
                    table.add_column("Count", justify="right", style="green")
                    table.add_row("Expected", f"{results['expected']:,}")
                    table.add_row("Actual", f"{results['actual']:,}")
                    table.add_row("Missing", f"{results['missing_count']:,}", style="yellow")
                    console.print(table)
                if save:
                    save_reports(results, output_dir, product, 'accountability', venue, start_date=start_date, end_date=end_date)
                all_results[product] = results
            elif strategy == 'date_count':
                results = _run_date_count_accountability(product, start_date, end_date, venue, save, output_dir, quiet)
                all_results[product] = {
                    'expected': results.get('expected_total', 0),
                    'actual': results.get('actual_total', 0),
                    'missing_count': results.get('missing_count', 0),
                }
            elif strategy == 'delegated_validator':
                from opera_accountability.strategies.delegated_validator import DelegatedValidatorStrategy
                product_cfg_inner = CONFIG['products'][product]
                validator_cfg = product_cfg_inner.get('accountability', {}).get('delegated_validator', {})
                validator_kwargs = {
                    'processing_mode': validator_cfg.get('processing_mode', 'forward'),
                    'k': validator_cfg.get('k', 15),
                }
                dv_strategy = DelegatedValidatorStrategy(product)
                results = dv_strategy.analyze(start_date, end_date, venue, **validator_kwargs)
                if not quiet:
                    table = Table(title=f"Delegated-Validator Accountability - {product}")
                    table.add_column("Metric", style="cyan")
                    table.add_column("Count", justify="right", style="green")
                    table.add_row("Expected", f"{results['expected']:,}")
                    table.add_row("Actual", f"{results['actual']:,}")
                    table.add_row("Missing", f"{results['missing_count']:,}", style="yellow")
                    console.print(table)
                if save:
                    save_reports(results, output_dir, product, 'accountability', venue, start_date=start_date, end_date=end_date)
                all_results[product] = results
            elif strategy == 'db_based':
                from opera_accountability.strategies.db_based import DBBasedStrategy
                db_strategy = DBBasedStrategy(product)
                results = db_strategy.analyze(start_date, end_date, venue, db_path=db_path)
                if not quiet:
                    table = Table(title=f"DB-Based Accountability - {product}")
                    table.add_column("Metric", style="cyan")
                    table.add_column("Count", justify="right", style="green")
                    table.add_row("Expected", f"{results['expected']:,}")
                    table.add_row("Actual", f"{results['actual']:,}")
                    table.add_row("Missing", f"{results['missing_count']:,}", style="yellow")
                    table.add_row("Coverage", f"{results['coverage_pct']:.1f}%")
                    console.print(table)
                if save:
                    save_reports(results, output_dir, product, 'accountability', venue, start_date=start_date, end_date=end_date)
                all_results[product] = results
            else:
                console.print(f"  [red]Unknown strategy: {strategy}[/red]")
                continue
        
        except Exception as e:
            if not quiet:
                console.print(f"  [red]Error: {e}[/red]")
            all_results[product] = {'error': str(e)}
    
    # Display summary table
    if not quiet:
        console.print("\n[bold]Summary:[/bold]")
        table = Table()
        table.add_column("Product", style="cyan")
        table.add_column("Expected", justify="right")
        table.add_column("Actual", justify="right")
        table.add_column("Missing", justify="right", style="yellow")
        table.add_column("Rate", justify="right")
        
        for product, results in all_results.items():
            if 'error' in results:
                table.add_row(product, "ERROR", results['error'], "", "")
            else:
                expected = results.get('expected', 0)
                actual = results.get('actual', 0)
                missing = results.get('missing_count', 0)
                acc_rate = (actual / expected * 100) if expected > 0 else 0
                table.add_row(product, f"{expected:,}", f"{actual:,}",
                            f"{missing:,}", f"{acc_rate:.2f}%")
        
        console.print(table)
        console.print("[green]Done![/green]")


@app.command()
def version():
    """Show version information."""
    console.print(f"opera-audit version {__version__}")


if __name__ == "__main__":
    app()
