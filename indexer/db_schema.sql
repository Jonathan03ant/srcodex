-- PMFW Code Explorer - Database Schema
-- SQLite database for storing symbols, references, and file contents

-- Symbols (function/variable/struct/macro definitions)
CREATE TABLE IF NOT EXISTS symbols (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    type TEXT NOT NULL,           -- Normalized: function, struct, variable, macro, typedef, enum, etc.
    kind_raw TEXT,                -- Raw ctags kind: prototype, function, variable, member, etc.
    file_path TEXT NOT NULL,
    line_number INTEGER NOT NULL,
    signature TEXT,               -- Raw signature from ctags (e.g., "(uint32_t intr_sts, uint8_t device)"), NULL if not available
    typeref TEXT,                 -- Raw typeref from ctags (e.g., "typename:void"), NULL if not available
    scope TEXT,                   -- global, extern (deprecated - use is_file_scope instead)
    scope_kind TEXT,              -- struct, union, enum, class (parent scope type)
    scope_name TEXT,              -- PowerState, Dummy, etc. (parent scope name)
    is_file_scope INTEGER,        -- 1 if file-local (static in C), 0 if not, NULL if unknown
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for fast lookup
CREATE INDEX IF NOT EXISTS idx_symbols_name ON symbols(name);
CREATE INDEX IF NOT EXISTS idx_symbols_file ON symbols(file_path);
CREATE INDEX IF NOT EXISTS idx_symbols_type ON symbols(type);
CREATE INDEX IF NOT EXISTS idx_symbols_kind_raw ON symbols(kind_raw);
CREATE INDEX IF NOT EXISTS idx_symbols_scope ON symbols(scope_kind, scope_name);
CREATE INDEX IF NOT EXISTS idx_symbols_file_scope ON symbols(is_file_scope);

-- References (where symbols are used)
CREATE TABLE IF NOT EXISTS "references" (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    line_number INTEGER NOT NULL,
    context TEXT,                 -- Line of code where it's referenced
    FOREIGN KEY(symbol_id) REFERENCES symbols(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_references_symbol ON "references"(symbol_id);
CREATE INDEX IF NOT EXISTS idx_references_file ON "references"(file_path);

-- Files (source file metadata - content NOT stored for performance/size)
CREATE TABLE IF NOT EXISTS files (
    path TEXT PRIMARY KEY,        -- Relative path from source_root
    size INTEGER NOT NULL,
    language TEXT,                -- c, h, python, makefile, etc.
    sha1 TEXT,                    -- SHA1 hash for change detection
    last_modified REAL,           -- mtime for change detection
    last_indexed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_files_language ON files(language);
CREATE INDEX IF NOT EXISTS idx_files_sha1 ON files(sha1);

-- Full-text search for symbols (FTS5 for fast text search)
CREATE VIRTUAL TABLE IF NOT EXISTS symbols_fts USING fts5(
    name,
    file_path,
    signature,
    content=symbols,
    content_rowid=id
);

-- Triggers to keep FTS table in sync with symbols table
CREATE TRIGGER IF NOT EXISTS symbols_ai AFTER INSERT ON symbols BEGIN
    INSERT INTO symbols_fts(rowid, name, file_path, signature)
    VALUES (new.id, new.name, new.file_path, new.signature);
END;

CREATE TRIGGER IF NOT EXISTS symbols_ad AFTER DELETE ON symbols BEGIN
    DELETE FROM symbols_fts WHERE rowid = old.id;
END;

CREATE TRIGGER IF NOT EXISTS symbols_au AFTER UPDATE ON symbols BEGIN
    DELETE FROM symbols_fts WHERE rowid = old.id;
    INSERT INTO symbols_fts(rowid, name, file_path, signature)
    VALUES (new.id, new.name, new.file_path, new.signature);
END;

-- Metadata table for tracking indexing status
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Store indexing stats
INSERT OR REPLACE INTO metadata (key, value) VALUES ('version', '1.0');
INSERT OR REPLACE INTO metadata (key, value) VALUES ('indexed_at', datetime('now'));
