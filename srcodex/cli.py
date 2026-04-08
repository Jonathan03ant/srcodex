#!/usr/bin/env python3
"""
srcodex CLI - Semantic code explorer with AI-powered analysis
"""
import click
import sys
import os
from pathlib import Path


@click.command()
@click.argument('path', default='.', type=click.Path(exists=True))
@click.option('--reindex', is_flag=True, help='Force re-indexing')
@click.option('--debug', is_flag=True, help='Enable debug logging')
def main(path, reindex, debug):
    """
    Launch srcodex TUI (auto-indexes if needed)
    
    EXAMPLES:
    
        srcodex                  # Index current directory and launch
        srcodex /path/to/code    # Index specific directory
        srcodex --reindex        # Force re-index and launch
    """
    
    click.echo("srcodex v0.1.0 - Semantic code explorer")
    click.echo()
    
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
        click.echo("  - AMD_LLM_API_KEY (for enterprise gateway)")
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
    # TODO: Import and run indexer
    # For now, just create placeholder
    srcodex_dir = project_path / ".srcodex"
    srcodex_dir.mkdir(exist_ok=True)
    
    click.echo("ERROR: Indexer not yet implemented in CLI")
    click.echo("This will be completed in Task 1 of Phase 2")
    raise NotImplementedError("Indexer integration pending")


def start_backend(project_path: Path, debug: bool = False):
    """Start FastAPI backend in background subprocess"""
    import subprocess
    
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
    
    # Wait for server to be ready
    import time
    time.sleep(2)
    
    return proc


def launch_tui(project_path: Path):
    """Launch Textual TUI"""
    from srcodex.tui.app import SrcodexApp
    
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
    from srcodex.core.config import get_config
    
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
