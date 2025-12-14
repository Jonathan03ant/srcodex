#!/usr/bin/env python3
"""
PMFW Code Explorer - Main Indexer
Scans PMFW source code and builds a searchable database
"""

import sqlite3
import os
import sys
from pathlib import Path
from typing import List, Optional
from datetime import datetime
import click
from tqdm import tqdm

from ctags_parser import CTagsParser


class PMFWIndexer:
    def __init__(self, db_path: str, verbose: bool = False):
        """
        Args:
            db_path: Path to SQLite database
            verbose: Enable verbose output
        """
        self.db_path = db_path
        self.verbose = verbose
        self.conn = None
        self.ctags = CTagsParser()

    def connect_db(self):
        """Connect to database and initialize schema"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Access columns by name

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

    def index_directory(self, source_dir: str, extensions: List[str] = None):
        """
        Index all files in a directory
        Args:
            source_dir: Root directory to scan
            extensions: File extensions to index (default: ['.c', '.h'])
        """
        if extensions is None:
            extensions = ['.c', '.h']

        source_path = Path(source_dir)
        if not source_path.exists():
            raise FileNotFoundError(f"Directory not found: {source_dir}")

        print(f"📂 Scanning directory: {source_dir}")

        # Find all files
        files_to_index = []     #contains list of path objects
        for ext in extensions:
            files_to_index.extend(source_path.rglob(f'*{ext}'))

        # Filter out .git and other unwanted directories
        files_to_index = [
            f for f in files_to_index
            if '.git' not in f.parts and 'out' not in f.parts
        ]

        print(f"📝 Found {len(files_to_index)} files to index")

        if click.confirm("Clear existing database?", default=True):
            self._clear_database()

        # Index each file
        total_symbols = 0
        with tqdm(total=len(files_to_index), desc="Indexing", unit="file") as pbar:
            for file_path in files_to_index:
                try:
                    symbols_count = self._index_file(str(file_path))
                    total_symbols += symbols_count
                    pbar.set_postfix({"symbols": total_symbols})
                except Exception as e:
                    if self.verbose:
                        print(f"\n⚠ Error indexing {file_path}: {e}")
                finally:
                    pbar.update(1)

        self.conn.commit()

        # Update metadata
        self._update_metadata(total_symbols, len(files_to_index))

        print(f"\n✅ Indexing complete!")
        print(f"   Files indexed: {len(files_to_index)}")
        print(f"   Symbols found: {total_symbols}")

    def _clear_database(self):
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM "references"')
        cursor.execute("DELETE FROM symbols")
        cursor.execute("DELETE FROM files")
        cursor.execute("DELETE FROM symbols_fts")
        self.conn.commit()

        if self.verbose:
            print("✓ Database cleared")

    def _index_file(self, file_path: str) -> int:
        """
        Index a single file
        Args:
            file_path: Path to source file

        Returns:
            Number of symbols found
        """
        # Read file content
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception as e:
            if self.verbose:
                print(f"Warning: Could not read {file_path}: {e}")
            return 0

        # Determine language
        ext = Path(file_path).suffix
        language = 'c' if ext == '.c' else 'h' if ext == '.h' else 'unknown'

        # Store file content
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO files (path, content, size, language) VALUES (?, ?, ?, ?)",
            (file_path, content, len(content), language)
        )

        # Parse symbols with ctags
        symbols = self.ctags.parse_file(file_path)

        # Store symbols
        for symbol in symbols:
            cursor.execute(
                """
                INSERT INTO symbols (name, type, file_path, line_number, signature, typeref, scope, scope_kind, scope_name, is_file_scope)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol['name'],
                    symbol['type'],
                    symbol['file_path'],
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
        self.conn.commit()

    def build_references(self):
        """
        Build cross-references (find where symbols are used)
        This is a simplified version - full reference tracking will be implemented later
        """
        print("\n🔗 Building cross-references...")

        cursor = self.conn.cursor()

        # Get all symbols
        cursor.execute("SELECT id, name FROM symbols")
        symbols = cursor.fetchall()

        # Get all files
        cursor.execute("SELECT path, content FROM files")
        files = cursor.fetchall()

        total_refs = 0
        with tqdm(total=len(files), desc="Scanning", unit="file") as pbar:
            for file_row in files:
                file_path = file_row['path']
                content = file_row['content']
                lines = content.split('\n')

                # Look for symbol usage in each line
                for line_num, line in enumerate(lines, start=1):
                    for symbol_row in symbols:
                        symbol_id = symbol_row['id']
                        symbol_name = symbol_row['name']

                        # Simple check: is symbol name in this line?
                        # (This is basic - could be improved with AST parsing)
                        if symbol_name in line:
                            cursor.execute(
                                """
                                INSERT INTO "references" (symbol_id, file_path, line_number, context)
                                VALUES (?, ?, ?, ?)
                                """,
                                (symbol_id, file_path, line_num, line.strip())
                            )
                            total_refs += 1

                pbar.update(1)

        self.conn.commit()
        print(f"✅ Built {total_refs} cross-references")

    def print_stats(self):
        """Print database statistics"""
        cursor = self.conn.cursor()

        # Get counts
        cursor.execute("SELECT COUNT(*) as count FROM symbols")
        symbol_count = cursor.fetchone()['count']

        cursor.execute("SELECT COUNT(*) as count FROM files")
        file_count = cursor.fetchone()['count']

        cursor.execute('SELECT COUNT(*) as count FROM "references"')
        ref_count = cursor.fetchone()['count']

        # Get symbol type breakdown
        cursor.execute("SELECT type, COUNT(*) as count FROM symbols GROUP BY type ORDER BY count DESC")
        type_counts = cursor.fetchall()

        print("\n📊 Database Statistics:")
        print(f"   Files:      {file_count}")
        print(f"   Symbols:    {symbol_count}")
        print(f"   References: {ref_count}")
        print("\n   Symbol Types:")
        for row in type_counts:
            print(f"      {row['type']:15} {row['count']:6}")

@click.command()
@click.argument('source_dir', type=click.Path(exists=True))
@click.option('--db', default='data/pmfw.db', help='Database path')
@click.option('--extensions', default='.c,.h', help='File extensions (comma-separated)')
@click.option('--no-refs', is_flag=True, help='Skip building cross-references')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')

def main(source_dir, db, extensions, no_refs, verbose):
    """
    Index PMFW source code

    Example:
        python indexer.py /utg/pmfwex/pmfw_source
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
    indexer = PMFWIndexer(db, verbose=verbose)

    try:
        # Connect to database
        indexer.connect_db()

        # Index files
        indexer.index_directory(source_dir, ext_list)

        # Build cross-references 
        if not no_refs:
            indexer.build_references()

        # Print statistics
        indexer.print_stats()

    finally:
        indexer.close_db()

    print(f"\n✅ Done! Database saved to: {db}")


if __name__ == "__main__":
    main()
