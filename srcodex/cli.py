#!/usr/bin/env python3
"""
srcodex CLI - Semantic code explorer with AI-powered analysis
"""
import click
import sys
import os
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime

from srcodex.tui.app import SrcodexApp
from srcodex.core.config import get_config
from srcodex.indexer.indexer import Indexer
from srcodex.indexer.field_access_analyzer import FieldAccessAnalyzer
from srcodex.indexer.reference_ingestor import ReferenceIngestor
from srcodex.indexer.reference_resolver import ReferenceResolver
from importlib.metadata import version as get_version

@click.command()
@click.argument('path', default='.', type=click.Path(exists=True))
@click.option('--reindex', is_flag=True, help='Force re-indexing')
@click.option('--debug', is_flag=True, help='Enable debug logging')
def main(path, reindex, debug):
    """
    Launch srcodex TUI
    EXAMPLES:
        srcodex                  # Index current directory and launch
        srcodex /path/to/code    # Index specific directory
        srcodex --reindex        # Force re-index and launch
    """

    try:
        pkg_version = get_version("srcodex")
    except Exception:
        pkg_version = "dev"
    click.echo(f"srcodex v{pkg_version} - Semantic code explorer")

    project_path = Path(path).resolve()
    srcodex_dir = project_path / ".srcodex"

    # Check if .srcodex exists
    if not srcodex_dir.exists() or reindex:
        if not reindex:
            # Prompt user
            if not click.confirm(f"No .srcodex/ found in {project_path}\nIndex this directory?"):
                click.echo("Cancelled.")
                return

        click.echo(f"\nIndexing {project_path}...")
        click.echo("This may take a few minutes for large codebases...\n")

        # Run indexer
        try:
            run_indexer(project_path, debug)
            click.echo("\nIndexing complete!")
        except Exception as e:
            click.echo(f"\nError during indexing: {e}", err=True)
            sys.exit(1)

    # Check for API key
    if not check_api_key():
        click.echo("\nError: No API key found!", err=True)
        click.echo("Please set either:")
        click.echo("  - ANTHROPIC_API_KEY (for public API)")
        click.echo("  - AMD_LLM_API_KEY (for AMD enterprise gateway)")
        click.echo("\nSee .env.example for configuration details")
        sys.exit(1)

    # Start backend server
    click.echo("\nStarting backend server...")
    backend_process = start_backend(project_path, debug)

    try:
        # Launch TUI (blocking)
        click.echo("Launching TUI...\n")
        launch_tui(project_path)
    except KeyboardInterrupt:
        click.echo("\n\nShutting down...")
    finally:
        # Cleanup
        if backend_process:
            backend_process.terminate()
            backend_process.wait()


def run_indexer(project_path: Path, debug: bool = False):
    """Run indexer and generate .srcodex/"""
    # Create .srcodex/ directory structure
    srcodex_dir = project_path / ".srcodex"
    data_dir = srcodex_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Database path
    project_name = project_path.name
    db_path = data_dir / f"{project_name}.db"

    indexer = Indexer(str(db_path), verbose=debug)

    try:
        # Connect to database
        indexer.connect_db()

        # Track timing
        start_time = time.time()

        # Stage 1: Index symbols (always force-clear for new indexing)
        click.echo("Stage 1: Extracting symbols with CTags...")
        indexer.index_directory(str(project_path), extensions=['.c', '.h', '.cpp', '.cc', '.py'], force_clear=True)

        # Stage 1.5: Field access analysis
        click.echo("Stage 1.5: Analyzing field access patterns...")
        field_analyzer = FieldAccessAnalyzer(indexer.conn, str(project_path))
        field_analyzer.analyze_all_functions_parallel(clear_existing=True)

        # Stage 2-4: Build cscope and resolve references
        click.echo("Stage 2: Building cscope database...")
        cscope_dir = data_dir / "cscope"
        cscope_dir.mkdir(exist_ok=True)
        indexer.build_cscope_database(str(cscope_dir))

        click.echo("Stage 3: Ingesting references...")
        ingestor = ReferenceIngestor(indexer.conn, str(project_path), cscope_dir)
        ingestor.ingest_callees(clear_existing=True)
        ingestor.ingest_callers(clear_existing=True)
        ingestor.ingest_includes(clear_existing=True)

        click.echo("Stage 4: Resolving semantic edges...")

        resolver = ReferenceResolver(indexer.conn)
        resolver.resolve_callees(clear_existing=True)
        resolver.resolve_includes(clear_existing=True)

        # Collect statistics
        cursor = indexer.conn.cursor()

        files_count = cursor.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        symbols_count = cursor.execute("SELECT COUNT(*) FROM symbols").fetchone()[0]
        calls_count = cursor.execute("SELECT COUNT(*) FROM symbol_edges WHERE edge_type = 'CALLS'").fetchone()[0]
        includes_count = cursor.execute("SELECT COUNT(*) FROM symbol_edges WHERE edge_type = 'INCLUDES'").fetchone()[0]
        accesses_count = cursor.execute("SELECT COUNT(*) FROM symbol_edges WHERE edge_type = 'ACCESSES'").fetchone()[0]
        avg_lines = 500  # Estimate: average C/Python file size

        duration = time.time() - start_time

        # Generate metadata.json
        metadata = {
            "project": {
                "name": project_name,
                "source_root": str(project_path),
                "indexed_at": datetime.now().isoformat(),
                "indexer_version": "0.1.0"
            },
            "stats": {
                "files_indexed": files_count,
                "total_symbols": symbols_count,
                "avg_lines_per_file": int(avg_lines),
                "edges": {
                    "calls": calls_count,
                    "includes": includes_count,
                    "accesses": accesses_count
                },
                "indexing_duration_seconds": round(duration, 2)
            },
            "paths": {
                "source_root": ".",
                "database": f".srcodex/data/{project_name}.db"
            }
        }

        metadata_path = srcodex_dir / "metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        # Summary
        click.echo(f"\n✓ Indexed {files_count:,} files, {symbols_count:,} symbols")
        click.echo(f"✓ Edges: {calls_count:,} CALLS, {includes_count:,} INCLUDES, {accesses_count:,} ACCESSES")
        click.echo(f"✓ Generated .srcodex/metadata.json")
        click.echo(f"✓ Duration: {duration:.1f} seconds")

    finally:
        indexer.close_db()


def start_backend(project_path: Path, debug: bool = False):
    """Start FastAPI backend in background subprocess"""

    # Set environment variable for project root
    env = os.environ.copy()
    env['SRCODEX_PROJECT_ROOT'] = str(project_path)

    # Start uvicorn in background
    stdout = None if debug else subprocess.DEVNULL
    stderr = None if debug else subprocess.DEVNULL

    proc = subprocess.Popen(
        [sys.executable, '-m', 'uvicorn', 'srcodex.backend.chat:app', '--port', '8000'],
        env=env,
        stdout=stdout,
        stderr=stderr,
        cwd=str(project_path)
    )

    time.sleep(2)
    return proc


def launch_tui(project_path: Path):
    """Launch Textual TUI"""
    os.chdir(project_path)
    app = SrcodexApp()
    app.run()


def check_api_key() -> bool:
    """Check if any API key is configured"""
    amd_key = os.getenv("AMD_LLM_API_KEY")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")

    return (amd_key and amd_key != "dummy") or bool(anthropic_key)


@click.command()
@click.argument('path', type=click.Path(exists=True))
def index(path):
    """Index a codebase without launching TUI"""
    project_path = Path(path).resolve()
    click.echo(f"Indexing {project_path}...")
    run_indexer(project_path)
    click.echo("Complete!")


@click.command()
def info():
    """Show project stats from .srcodex/metadata.json"""
    try:
        config = get_config()
        stats = config.stats

        click.echo(f"\nProject: {config.project_name}")
        click.echo(f"Source root: {config.source_root}")
        click.echo(f"Indexed at: {config.indexed_at}")
        click.echo(f"\nStats:")
        click.echo(f"  Files: {stats['files_indexed']:,}")
        click.echo(f"  Symbols: {stats['total_symbols']:,}")
        click.echo(f"  Call edges: {stats['edges']['calls']:,}")
        click.echo(f"  Include edges: {stats['edges']['includes']:,}")
        click.echo(f"  Field access edges: {stats['edges']['accesses']:,}")
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        click.echo("No .srcodex/ found in current directory")
        sys.exit(1)


if __name__ == '__main__':
    main()
