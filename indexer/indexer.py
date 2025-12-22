#!/usr/bin/env python3
"""
Code Explorer - Main Indexer
Scans source code and builds a searchable database
"""

import sqlite3
import os
import sys
import hashlib
import subprocess
from pathlib import Path
from typing import List, Optional, Dict
from datetime import datetime
import click
from tqdm import tqdm

from ctags_parser import CTagsParser
from explorer import FileDiscovery
from reference_ingestor import ReferenceIngestor
from reference_resolver import ReferenceResolver


class Indexer:
    def __init__(self, db_path: str, verbose: bool = False):
        """
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        self.verbose = verbose
        self.conn = None
        self.ctags = CTagsParser()
        self.source_root = None 

    def connect_db(self):
        """Connect to database and initialize schema"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row 

        # CRITICAL: Enable foreign keys 
        self.conn.execute("PRAGMA foreign_keys = ON")

        # Read and execute schema
        schema_path = Path(__file__).parent / "db_schema.sql"
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
            self.conn.executescript(schema_sql)

        self.conn.commit()

        if self.verbose:
            print(f"✓ Database initialized: {self.db_path}")

    def close_db(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

    def index_directory(self, source_dir: str, extensions: List[str] = None, force_clear: bool = False):
        """
        Index all files in a directory
        Args:
            source_dir: Root directory to scan
            extensions: File extensions to index (default: ['.c', '.h'])
            force_clear: If True, clear database without prompting
        """
        if extensions is None:
            extensions = ['.c', '.h']

        source_path = Path(source_dir).resolve()  # Convert to absolute for consistent resolution

        # Store source root for relative path computation
        self.source_root = source_path

        print(f"📂 Scanning directory: {source_dir}")

        # Use unified FileDiscovery module
        discovery = FileDiscovery(source_dir, extensions)
        files_to_index = discovery.discover_files_absolute()

        print(f"📝 Found {len(files_to_index)} files to index")

        # Clear database: force or prompt
        if force_clear:
            self._clear_database()
            if self.verbose:
                print("✓ Database cleared (--force)")
        elif click.confirm("Clear existing database?", default=True):
            self._clear_database()

        # Parse ALL files with SINGLE ctags invocation
        print(f"🔍 Running ctags on {len(files_to_index)} files...")
        file_to_symbols = self.ctags.parse_root(str(source_path), extensions, source_root=str(source_path))

        # Index each file (store metadata + symbols) in ONE transaction
        self.conn.execute("BEGIN")
        try:
            total_symbols = 0
            with tqdm(total=len(files_to_index), desc="Indexing", unit="file") as pbar:
                for file_path in files_to_index:
                    try:
                        # Normalize to canonical form: rel_posix (same as parse_root() keys)
                        file_path_canonical = Path(file_path).relative_to(source_path).as_posix()
                        symbols = file_to_symbols.get(file_path_canonical, [])
                        symbols_count = self._index_file_with_symbols(str(file_path), symbols)
                        total_symbols += symbols_count
                        pbar.set_postfix({"symbols": total_symbols})
                    except Exception as e:
                        if self.verbose:
                            print(f"\n⚠ Error indexing {file_path}: {e}")
                    finally:
                        pbar.update(1)

            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise

        # Update metadata
        self._update_metadata(total_symbols, len(files_to_index))

        print(f"\n✅ Indexing complete!")
        print(f"   Files indexed: {len(files_to_index)}")
        print(f"   Symbols found: {total_symbols}")

    def _clear_database(self):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM symbol_edges")
        cursor.execute("DELETE FROM raw_references")
        cursor.execute("DELETE FROM symbols")
        cursor.execute("DELETE FROM files")
        cursor.execute("DELETE FROM symbols_fts")
        self.conn.commit()

        if self.verbose:
            print("✓ Database cleared")

    def _index_file_with_symbols(self, file_path: str, symbols: List[Dict]) -> int:
        """
        Index a single file with PRE-PARSED symbols (from batch ctags call).
        RECOMMENDED method - symbols already parsed by parse_root().

        Args:
            file_path: Path to source file (absolute)
            symbols: Pre-parsed symbols from ctags

        Returns:
            Number of symbols indexed
        """
        # Compute relative path for storage (use POSIX for cross-platform)
        file_path_rel = Path(file_path).relative_to(self.source_root).as_posix()

        # Read file for metadata (sha1, mtime, size)
        with open(file_path, 'rb') as f:
            content_bytes = f.read()

        # Compute metadata
        file_size = len(content_bytes)
        sha1_hash = hashlib.sha1(content_bytes).hexdigest()
        mtime = os.path.getmtime(file_path)

        # Determine language (both .c and .h are C language)
        ext = Path(file_path).suffix
        language = 'c' if ext in ['.c', '.h'] else 'unknown'

        cursor = self.conn.cursor()

        # Delete existing symbols for this file (per-file refresh)
        cursor.execute("DELETE FROM symbols WHERE file_path = ?", (file_path_rel,))

        # Store file metadata
        cursor.execute(
            """INSERT OR REPLACE INTO files (path, size, language, sha1, last_modified)
               VALUES (?, ?, ?, ?, ?)""",
            (file_path_rel, file_size, language, sha1_hash, mtime)
        )

        # Store symbols (already parsed!) - use executemany for batch insert
        symbol_rows = [
            (
                symbol['name'],
                symbol['type'],
                symbol.get('kind_raw'),
                file_path_rel,  # RELATIVE path
                symbol['line'],
                symbol.get('signature'),
                symbol.get('typeref'),
                symbol.get('scope', 'global'),
                symbol.get('scope_kind'),
                symbol.get('scope_name'),
                symbol.get('is_file_scope')
            )
            for symbol in symbols
        ]

        if symbol_rows:
            cursor.executemany(
                """
                INSERT INTO symbols (name, type, kind_raw, file_path, line_number, signature, typeref, scope, scope_kind, scope_name, is_file_scope)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                symbol_rows
            )

        return len(symbols)

    def _index_file(self, file_path: str) -> int:
        """
        Index a single file with per-file ctags invocation.

        DEPRECATED for bulk indexing - use _index_file_with_symbols() instead.
        Kept for:
        - Incremental updates of single files
        - Debugging

        Args:
            file_path: Path to source file (absolute)

        Returns:
            Number of symbols found
        """
        # Convert to relative path for storage
        file_path_obj = Path(file_path)
        if self.source_root:
            try:
                rel_path = file_path_obj.relative_to(self.source_root)
                file_path_rel = rel_path.as_posix()  # POSIX format for cross-platform
            except ValueError:
                # File is outside source_root, use absolute (POSIX)
                file_path_rel = file_path_obj.as_posix()
        else:
            file_path_rel = file_path_obj.as_posix()

        # Read file content (use absolute path for actual file access)
        try:
            with open(file_path, 'rb') as f:  # Binary mode for SHA1
                content_bytes = f.read()
            content = content_bytes.decode('utf-8', errors='ignore')
        except Exception as e:
            if self.verbose:
                print(f"Warning: Could not read {file_path}: {e}")
            return 0

        # Compute metadata
        file_size = len(content_bytes)
        sha1_hash = hashlib.sha1(content_bytes).hexdigest()
        mtime = os.path.getmtime(file_path)

        # Determine language (both .c and .h are C language)
        ext = Path(file_path).suffix
        language = 'c' if ext in ['.c', '.h'] else 'unknown'

        cursor = self.conn.cursor()

        # Delete existing symbols for this file (per-file refresh) - use relative path
        cursor.execute("DELETE FROM symbols WHERE file_path = ?", (file_path_rel,))

        # Store file METADATA only (not content) - use relative path
        cursor.execute(
            """INSERT OR REPLACE INTO files (path, size, language, sha1, last_modified)
               VALUES (?, ?, ?, ?, ?)""",
            (file_path_rel, file_size, language, sha1_hash, mtime)
        )

        # Parse symbols with ctags (use absolute path for ctags)
        symbols = self.ctags.parse_file(file_path)

        # Store symbols - override file_path with relative path
        for symbol in symbols:
            cursor.execute(
                """
                INSERT INTO symbols (name, type, kind_raw, file_path, line_number, signature, typeref, scope, scope_kind, scope_name, is_file_scope)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol['name'],
                    symbol['type'],           # Normalized type
                    symbol.get('kind_raw'),   # Raw ctags kind
                    file_path_rel,            # RELATIVE path for portability
                    symbol['line'],
                    symbol.get('signature'),  # NULL if not available
                    symbol.get('typeref'),    # NULL if not available
                    symbol.get('scope', 'global'),
                    symbol.get('scope_kind'),
                    symbol.get('scope_name'),
                    symbol.get('is_file_scope')
                )
            )

        return len(symbols)

    def _update_metadata(self, total_symbols: int, total_files: int):
        """Update metadata table with indexing statistics"""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            ('total_symbols', str(total_symbols))
        )
        cursor.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            ('total_files', str(total_files))
        )
        cursor.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            ('indexed_at', datetime.now().isoformat())
        )
        # Store source root for path resolution
        if self.source_root:
            cursor.execute(
                "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
                ('source_root', str(self.source_root))
            )
        self.conn.commit()

    def build_cscope_database(self, output_dir: str = None):
        """
        Build cscope database for cross-reference queries.

        CRITICAL: Builds cscope with cwd=source_root and rel_posix paths to ensure
        cscope output paths match DB canonical paths exactly.

        Args:
            output_dir: Directory to store cscope files (default: source_root/.srcodex/)
        """
        print("\n🔍 Building cscope database...")

        if not self.source_root:
            print("❌ Error: source_root not set. Cannot build cscope database.")
            return

        # Determine output directory - default to source_root/.srcodex/
        if output_dir is None:
            output_dir = self.source_root / ".srcodex"
        else:
            output_dir = Path(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)

        # Get all indexed files from database
        cursor = self.conn.cursor()
        cursor.execute("SELECT path FROM files")
        files = cursor.fetchall()

        if not files:
            print("⚠ No files found in database. Run indexing first.")
            return

        # Write cscope.files with RELATIVE paths (same as DB canonical format)
        cscope_files_path = output_dir / "cscope.files"
        with open(cscope_files_path, 'w') as f:
            for file_row in files:
                file_path_rel = file_row['path']  # Already canonical rel_posix!
                f.write(f"{file_path_rel}\n")

        print(f"   Wrote {len(files)} files to {cscope_files_path}")

        # Run cscope with cwd=source_root to force rel_posix output paths
        try:
            result = subprocess.run(
                ['cscope', '-b', '-q', '-k', '-i', str(cscope_files_path.relative_to(self.source_root))],
                cwd=self.source_root,  # KEY: Run from source_root!
                capture_output=True,
                text=True,
                check=True
            )

            # Check that output files were created (in source_root, not output_dir)
            cscope_out = self.source_root / "cscope.out"
            if cscope_out.exists():
                size_mb = cscope_out.stat().st_size / (1024 * 1024)
                print(f"✅ Cscope database built: {cscope_out} ({size_mb:.2f} MB)")
            else:
                print(f"⚠ Warning: cscope.out not found at {cscope_out}")

        except subprocess.CalledProcessError as e:
            print(f"❌ Error building cscope database: {e}")
            if e.stderr:
                print(f"   stderr: {e.stderr}")
        except FileNotFoundError:
            print("❌ Error: cscope command not found. Install with: sudo apt install cscope")

    def ingest_raw_references(self, cscope_dir: Optional[str] = None):
        """
        Stage 2: Ingest raw cscope output into raw_references table

        Args:
            cscope_dir: Directory containing cscope.out (default: source_root where cscope was built)
        """
        if cscope_dir is None:
            cscope_dir = self.source_root  # cscope.out is now in source_root

        cscope_path = Path(cscope_dir)

        if not (cscope_path / "cscope.out").exists():
            print("⚠️  Cscope database not found. Run with --cscope flag first.")
            return

        print("\n🔍 Stage 2: Ingesting raw references from cscope...")

        ingestor = ReferenceIngestor(
            db_conn=self.conn,
            source_root=self.source_root,
            cscope_dir=cscope_path
        )

        # Ingest all three types of references
        total_refs = 0

        # 2a. Callees (functions called by each function)
        callees_count = ingestor.ingest_callees(clear_existing=True)
        total_refs += callees_count

        # 2b. Callers (functions that call each function)
        callers_count = ingestor.ingest_callers(clear_existing=True)
        total_refs += callers_count

        # 2c. Includes (files that include each header)
        includes_count = ingestor.ingest_includes(clear_existing=True)
        total_refs += includes_count

        print(f"\n✅ Ingested {total_refs} total raw references:")
        print(f"   - Callees:  {callees_count}")
        print(f"   - Callers:  {callers_count}")
        print(f"   - Includes: {includes_count}")

    def resolve_semantic_edges(self):
        """
        Stage 3: Resolve raw references into semantic graph edges
        Converts (file, function) names → symbol IDs and stores typed edges
        """
        print("\n🔍 Stage 3: Resolving semantic edges...")

        resolver = ReferenceResolver(db_conn=self.conn)
        stats = resolver.resolve_callees(clear_existing=True)

        print(f"✅ Resolved {stats['resolved_edges']} semantic edges")

    def print_stats(self):
        """Print database statistics"""
        cursor = self.conn.cursor()

        # Get counts
        cursor.execute("SELECT COUNT(*) as count FROM symbols")
        symbol_count = cursor.fetchone()['count']

        cursor.execute("SELECT COUNT(*) as count FROM files")
        file_count = cursor.fetchone()['count']

        cursor.execute("SELECT COUNT(*) as count FROM raw_references")
        raw_ref_count = cursor.fetchone()['count']

        cursor.execute("SELECT COUNT(*) as count FROM symbol_edges")
        edge_count = cursor.fetchone()['count']

        # Get symbol type breakdown
        cursor.execute("SELECT type, COUNT(*) as count FROM symbols GROUP BY type ORDER BY count DESC")
        type_counts = cursor.fetchall()

        print("\n📊 Database Statistics:")
        print(f"   Files:          {file_count}")
        print(f"   Symbols:        {symbol_count}")
        print(f"   Raw refs:       {raw_ref_count}")
        print(f"   Semantic edges: {edge_count}")
        print("\n   Symbol Types:")
        for row in type_counts:
            print(f"      {row['type']:15} {row['count']:6}")

@click.command()
@click.argument('source_dir', type=click.Path(exists=True))
@click.option('--db', default='data/pmfw.db', help='Database path')
@click.option('--extensions', default='.c,.h', help='File extensions (comma-separated)')
@click.option('--refs', is_flag=True, help='[PIPELINE] Build cscope + ingest + resolve (full reference pipeline)')
@click.option('--build-cscope', is_flag=True, help='[STAGE] Build cscope database only')
@click.option('--ingest-refs', is_flag=True, help='[STAGE] Ingest raw references only (requires existing cscope DB)')
@click.option('--resolve-refs', is_flag=True, help='[STAGE] Resolve semantic edges only (requires raw_references)')
@click.option('--force', '-f', is_flag=True, help='Force clear database without prompting')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')

def main(source_dir, db, extensions, refs, build_cscope, ingest_refs, resolve_refs, force, verbose):
    """
    Index source code and build semantic graph

    PIPELINE STAGES:
      1. Index symbols (always runs)
      2. Build cscope DB (optional, --build-cscope or --refs)
      3. Ingest raw refs (optional, --ingest-refs or --refs)
      4. Resolve edges (optional, --resolve-refs or --refs)

    Examples:
        # Symbols only (Stage 1):
        python indexer.py test_code --db data/test.db --force

        # Full pipeline (Stages 1-4):
        python indexer.py test_code --db data/test.db --force --refs

        # Debug Stage 2 only (requires existing cscope DB):
        python indexer.py test_code --db data/test.db --ingest-refs

        # Build pipeline piece by piece:
        python indexer.py test_code --db data/test.db --force
        python indexer.py test_code --db data/test.db --build-cscope
        python indexer.py test_code --db data/test.db --ingest-refs
        python indexer.py test_code --db data/test.db --resolve-refs
    """
    # Parse extensions
    ext_list = [f".{ext.strip().lstrip('.')}" for ext in extensions.split(',')]

    # Create data directory if needed
    db_path = Path(db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"   Source Code Explorer - Indexer")
    print(f"   Source:     {source_dir}")
    print(f"   Database:   {db}")
    print(f"   Extensions: {', '.join(ext_list)}\n")

    # Create indexer
    indexer = Indexer(db, verbose=verbose)

    try:
        # Connect to database
        indexer.connect_db()

        # Determine pipeline stages to run
        run_build_cscope = refs or build_cscope
        run_ingest = refs or ingest_refs
        run_resolve = refs or resolve_refs

        # Stage 1: Index files (CTags → symbols, files tables)
        # Always runs unless we're doing stage-specific operations
        if not (ingest_refs or resolve_refs):
            indexer.index_directory(source_dir, ext_list, force_clear=force)
        else:
            # If skipping Stage 1, still need to set source_root for Stage 2/3
            indexer.source_root = Path(source_dir).resolve()

        # Stage 2a: Build cscope database
        if run_build_cscope:
            indexer.build_cscope_database()

        # Stage 2b: Ingest raw references (cscope → raw_references table)
        if run_ingest:
            indexer.ingest_raw_references()

        # Stage 3: Resolve semantic edges (raw_references → symbol_edges table)
        if run_resolve:
            indexer.resolve_semantic_edges()

        # Print statistics
        indexer.print_stats()

    finally:
        indexer.close_db()

    print(f"\n✅ Done! Database saved to: {db}")


if __name__ == "__main__":
    main()
