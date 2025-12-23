#!/usr/bin/env python3
"""
Interactive Database Explorer
Simple menu-driven interface to explore semantic graph database from a source code
"""

import sqlite3
import os
from pathlib import Path


DB_PATH = "/utg/pmfwex/data/pmfw_main.db"
PMFW_SRC = "/utg/pmfwex/pmfw_source/firmware/main"


def clear_screen():
    """Clear the terminal screen"""
    os.system('clear')


def print_header(title):
    """Print a formatted header"""
    print("=" * 60)
    print(title)
    print("=" * 60)
    print()


def print_table(cursor, headers=None):
    """Print query results as a formatted table"""
    rows = cursor.fetchall()

    if not rows:
        print("No results found.")
        return

    # Get column names from cursor description if headers not provided
    if headers is None:
        headers = [desc[0] for desc in cursor.description]

    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))

    # Print header
    header_line = " | ".join(h.ljust(w) for h, w in zip(headers, col_widths))
    print(header_line)
    print("-" * len(header_line))

    # Print rows
    for row in rows:
        print(" | ".join(str(val).ljust(w) for val, w in zip(row, col_widths)))


def show_statistics(conn):
    """Show database statistics"""
    clear_screen()
    print_header("DATABASE STATISTICS")

    cursor = conn.cursor()
    cursor.execute("""
        SELECT 'Files indexed' as Metric, COUNT(*) as Count FROM files
        UNION ALL
        SELECT 'Total symbols', COUNT(*) FROM symbols
        UNION ALL
        SELECT 'Functions', COUNT(*) FROM symbols WHERE type = 'function'
        UNION ALL
        SELECT 'Macros', COUNT(*) FROM symbols WHERE type = 'macro'
        UNION ALL
        SELECT 'Structs', COUNT(*) FROM symbols WHERE type = 'struct'
        UNION ALL
        SELECT 'CALLS edges', COUNT(*) FROM symbol_edges WHERE edge_type = 'CALLS'
        UNION ALL
        SELECT 'INCLUDES edges', COUNT(*) FROM file_edges WHERE edge_type = 'INCLUDES'
    """)

    print_table(cursor)
    print()
    input("Press Enter to continue...")


def search_symbol(conn):
    """Search for a symbol and show details"""
    clear_screen()
    print_header("SEARCH FOR A SYMBOL")

    symbol_name = input("Enter symbol name (or part of it): ").strip()

    if not symbol_name:
        print("No symbol name provided.")
        return

    cursor = conn.cursor()

    # Find matching symbols (any type)
    cursor.execute("""
        SELECT id, name, type, file_path, line_number
        FROM symbols
        WHERE name LIKE ?
        LIMIT 20
    """, (f'%{symbol_name}%',))

    results = cursor.fetchall()

    clear_screen()
    print_header(f"SEARCH RESULTS FOR: {symbol_name}")

    if not results:
        print(f"No symbols found matching '{symbol_name}'")
        print()
        input("Press Enter to continue...")
        return

    print(f"Found {len(results)} symbol(s):")
    print()

    # Display results
    for i, (sym_id, name, sym_type, file_path, line_num) in enumerate(results, 1):
        print(f"{i}. {name} ({sym_type})")
        print(f"   {file_path}:{line_num}")
        print()

    # Let user select one
    if len(results) == 1:
        selected_idx = 0
    else:
        choice = input("Enter number to see details (or press Enter to skip): ").strip()
        if not choice:
            return
        try:
            selected_idx = int(choice) - 1
            if selected_idx < 0 or selected_idx >= len(results):
                print("Invalid selection.")
                return
        except ValueError:
            print("Invalid input.")
            return

    sym_id, symbol_name, sym_type, file_path, line_num = results[selected_idx]

    # Show symbol details
    clear_screen()
    print_header(f"SYMBOL DETAILS: {symbol_name} ({sym_type})")

    # Definition
    print("DEFINITION:")
    print("-" * 60)
    cursor.execute("SELECT name, type, file_path, line_number, signature FROM symbols WHERE id = ?", (sym_id,))
    print_table(cursor)
    print()

    # If it's a function declared in a header, find the implementation
    if sym_type == 'function' and file_path.endswith('.h'):
        print("IMPLEMENTATION (function defined in header, looking for .c file):")
        print("-" * 60)
        # Look for same function name in .c files
        cursor.execute("""
            SELECT name, file_path, line_number, signature
            FROM symbols
            WHERE name = ? AND type = 'function' AND file_path LIKE '%.c'
            LIMIT 5
        """, (symbol_name,))

        impl_results = cursor.fetchall()
        if impl_results:
            cursor.execute("""
                SELECT name, file_path, line_number, signature
                FROM symbols
                WHERE name = ? AND type = 'function' AND file_path LIKE '%.c'
                LIMIT 5
            """, (symbol_name,))
            print_table(cursor)

            # Update file_path and line_num to the implementation for showing source code
            if len(impl_results) == 1:
                file_path = impl_results[0][1]
                line_num = impl_results[0][2]
        else:
            print("No implementation found in .c files")
        print()

    # Show source code
    source_file = Path(PMFW_SRC) / file_path
    if source_file.exists():
        print("SOURCE CODE (showing 20 lines from definition):")
        print("-" * 60)
        try:
            with open(source_file, 'r') as f:
                lines = f.readlines()
                start = line_num - 1  # 0-indexed
                end = min(start + 20, len(lines))
                for i, line in enumerate(lines[start:end], start=line_num):
                    print(f"{i:6}  {line.rstrip()}")
        except Exception as e:
            print(f"Error reading source: {e}")
        print()

    # Only show call graph for functions
    if sym_type == 'function':
        # What does it call?
        print("CALLS (what this function calls):")
        print("-" * 60)
        cursor.execute("""
            SELECT dst.name as Function, e.source_file as File, e.line_number as Line
            FROM symbol_edges e
            JOIN symbols src ON e.src_symbol_id = src.id
            JOIN symbols dst ON e.dst_symbol_id = dst.id
            WHERE src.id = ?
            ORDER BY e.line_number
            LIMIT 20
        """, (sym_id,))

        results = cursor.fetchall()
        if results:
            cursor.execute("""
                SELECT dst.name as Function, e.source_file as File, e.line_number as Line
                FROM symbol_edges e
                JOIN symbols src ON e.src_symbol_id = src.id
                JOIN symbols dst ON e.dst_symbol_id = dst.id
                WHERE src.id = ?
                ORDER BY e.line_number
                LIMIT 20
            """, (sym_id,))
            print_table(cursor)
        else:
            print("No function calls found (or not resolved)")
        print()

        # Who calls it?
        print("CALLED BY (who calls this function):")
        print("-" * 60)
        cursor.execute("""
            SELECT src.name as Function, e.source_file as File, e.line_number as Line
            FROM symbol_edges e
            JOIN symbols src ON e.src_symbol_id = src.id
            JOIN symbols dst ON e.dst_symbol_id = dst.id
            WHERE dst.id = ?
            ORDER BY src.name
            LIMIT 20
        """, (sym_id,))

        results = cursor.fetchall()
        if results:
            cursor.execute("""
                SELECT src.name as Function, e.source_file as File, e.line_number as Line
                FROM symbol_edges e
                JOIN symbols src ON e.src_symbol_id = src.id
                JOIN symbols dst ON e.dst_symbol_id = dst.id
                WHERE dst.id = ?
                ORDER BY src.name
                LIMIT 20
            """, (sym_id,))
            print_table(cursor)
        else:
            print("No callers found (or not resolved)")
        print()

    input("Press Enter to continue...")


def search_file(conn):
    """Search for a file and show symbols"""
    clear_screen()
    print_header("SEARCH FOR A FILE")

    file_name = input("Enter file name (or part of it): ").strip()

    if not file_name:
        print("No file name provided.")
        return

    cursor = conn.cursor()

    # Find matching files
    cursor.execute("SELECT path FROM files WHERE path LIKE ? LIMIT 20", (f'%{file_name}%',))
    results = cursor.fetchall()

    clear_screen()
    print_header(f"SEARCH RESULTS FOR: {file_name}")

    if not results:
        print(f"No files found matching '{file_name}'")
        print()
        input("Press Enter to continue...")
        return

    print(f"Found {len(results)} file(s):")
    print()

    for i, (path,) in enumerate(results, 1):
        print(f"{i}. {path}")

    print()

    # Let user select one
    if len(results) == 1:
        selected_idx = 0
    else:
        choice = input("Enter number to see details (or press Enter to skip): ").strip()
        if not choice:
            return
        try:
            selected_idx = int(choice) - 1
            if selected_idx < 0 or selected_idx >= len(results):
                print("Invalid selection.")
                return
        except ValueError:
            print("Invalid input.")
            return

    file_path = results[selected_idx][0]

    # Show file details
    clear_screen()
    print_header(f"FILE DETAILS: {file_path}")

    # Symbol count by type
    print("SYMBOLS DEFINED IN THIS FILE:")
    print("-" * 60)
    cursor.execute("SELECT COUNT(*) FROM symbols WHERE file_path = ?", (file_path,))
    total = cursor.fetchone()[0]
    print(f"Total symbols: {total}")
    print()

    cursor.execute("""
        SELECT type, COUNT(*) as count
        FROM symbols
        WHERE file_path = ?
        GROUP BY type
        ORDER BY count DESC
    """, (file_path,))
    print_table(cursor)
    print()

    # Functions
    print("FUNCTIONS:")
    print("-" * 60)
    cursor.execute("""
        SELECT name, line_number
        FROM symbols
        WHERE file_path = ? AND type = 'function'
        ORDER BY line_number
        LIMIT 30
    """, (file_path,))

    results = cursor.fetchall()
    if results:
        cursor.execute("""
            SELECT name, line_number
            FROM symbols
            WHERE file_path = ? AND type = 'function'
            ORDER BY line_number
            LIMIT 30
        """, (file_path,))
        print_table(cursor)
    else:
        print("No functions defined in this file")
    print()

    # Includes
    print("INCLUDES (what this file includes):")
    print("-" * 60)
    cursor.execute("""
        SELECT dst_file as Header, line_number as Line
        FROM file_edges
        WHERE src_file = ? AND edge_type = 'INCLUDES'
        ORDER BY line_number
    """, (file_path,))

    results = cursor.fetchall()
    if results:
        cursor.execute("""
            SELECT dst_file as Header, line_number as Line
            FROM file_edges
            WHERE src_file = ? AND edge_type = 'INCLUDES'
            ORDER BY line_number
        """, (file_path,))
        print_table(cursor)
    else:
        print("No includes found (or not resolved)")
    print()

    input("Press Enter to continue...")


def main():
    """Main menu loop"""
    # Check if database exists
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        print("Please run the indexer first.")
        return

    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Enable column access by name

    try:
        while True:
            clear_screen()
            print_header("PMFW Database Explorer")

            print("1. Show Database Statistics")
            print("2. Search for a Symbol")
            print("3. Search for a File")
            print("4. Exit")
            print()

            choice = input("Choose an option [1-4]: ").strip()

            if choice == '1':
                show_statistics(conn)
            elif choice == '2':
                search_symbol(conn)
            elif choice == '3':
                search_file(conn)
            elif choice == '4':
                clear_screen()
                print("Goodbye!")
                break
            else:
                print("Invalid option. Please try again.")
                input("Press Enter to continue...")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
