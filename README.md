# srcodex

**Semantic code explorer with AI-powered search and analysis**

srcodex builds a semantic graph of your codebase and provides AI-powered exploration through natural language queries. Think of it as an intelligent code search that understands relationships, call graphs, and architecture.

## Why srcodex?

Unlike generic code assistants (Claude CLI, GitHub Copilot, etc.) that read entire files to answer questions, srcodex uses a **semantic graph database** to understand your code:

| Question | Generic Assistant | srcodex |
|----------|------------------|---------|
| "Who calls function X?" | Grep entire codebase (20K tokens) | `get_callers('X')` (200 tokens) |
| "Show call chain A→B" | Read multiple files, manual tracing | Graph query (500 tokens) |
| "Find all ioctls" | Grep + read matches (15K tokens) | Database search (300 tokens) |
| "Explain module Y" | Read 10+ files (30K tokens) | Aggregate query (2K tokens) |

**Result:** 99% more token-efficient, instant relationship queries, and unique capabilities impossible for file-based tools (call chains, data flow analysis, architecture visualization).

## Key Features

- **Semantic Indexing Engine**: Extracts symbols, relationships, and cross-references from source code
- **AI-Powered Chat**: Natural language queries about your codebase architecture
- **Call Graph Analysis**: Trace function calls, caller chains, and dependency paths
- **Terminal UI**: Full-featured TUI with file browser, search, and AI chat interface
- **Multi-Language Support**: C, C++, Python, JavaScript, Go, Rust, and more
- **Persistent Graph Database**: SQLite-backed semantic graph with relationship edges
- **Portable**: `.srcodex/` directory makes indexed projects shareable across teams
- **Token Efficient**: 99% reduction in API costs via semantic queries and intelligent caching

## Installation

### From PyPI (Recommended)

```bash
pip install srcodex
```

### From Source

```bash
git clone https://github.com/Jonathan03ant/srcodex.git
cd srcodex
pip install -e .
```

## Prerequisites

Before installing srcodex, you need these system tools:

**Ubuntu/Debian:**
```bash
sudo apt install universal-ctags cscope
```

**macOS:**
```bash
brew install universal-ctags cscope
```

**Arch Linux:**
```bash
sudo pacman -S ctags cscope
```

**Other systems:** Install Universal CTags from https://github.com/universal-ctags/ctags

## Quick Start

```bash
# 1. Install srcodex
pip install srcodex

# 2. Configure API key
export ANTHROPIC_API_KEY="your-api-key"
# Or create .env file with ANTHROPIC_API_KEY=...

# 3. Index your codebase (first time)
cd /path/to/your/project
srcodex

# Output:
# No .srcodex/ found. Index this directory? (y/n) y
# [Indexing happens...]
# [TUI launches]

# 4. Next time - instant launch (uses cached index)
srcodex
```

## Usage

Once indexed, use the TUI to:
- Browse files and symbols
- Search across your codebase
- Chat with AI about your code architecture
- Trace call chains and dependencies

### Example AI Queries

```
"What does the init_system function do?"
"Show me all functions that call malloc"
"Trace the execution path from main to shutdown"
"What structs are defined in config.h?"
```

## Configuration

srcodex requires a Claude API key from Anthropic.

**Option 1: Environment Variable**
```bash
export ANTHROPIC_API_KEY="sk-ant-your-key-here"
```

**Option 2: .env File**
Create a `.env` file in your project directory:
```bash
ANTHROPIC_API_KEY=sk-ant-your-key-here
```

Get your API key from https://console.anthropic.com/

## How It Works

**Indexing Phase:**
1. Analyzes source code to extract symbols, functions, types, and relationships
2. Builds a semantic graph database with typed edges (function calls, includes, data access)
3. Stores everything in a persistent SQLite database

**Query Phase:**
1. You ask questions in natural language via the terminal UI
2. Claude queries the semantic graph database using specialized tools
3. Returns targeted answers without reading entire files

**Why This Is Efficient:**
- Traditional code assistants: Read full files (20K-60K tokens per query)
- srcodex: Semantic graph queries (100-500 tokens per query)
- Intelligent caching: First query builds cache, subsequent queries reuse it
- Result: 99% reduction in API costs

## Project Structure

After indexing, srcodex creates a `.srcodex/` directory in your project:

```
your-project/
├── .srcodex/
│   ├── metadata.json       # Project statistics
│   ├── data/
│   │   └── project.db      # Semantic graph database
│   ├── conversations/      # Chat history
│   └── .debug/             # Debug logs
└── [your source files...]
```

The `.srcodex/` directory is portable - you can commit it to git or share it with your team to avoid re-indexing.

## Performance

**Indexing Speed** (varies by codebase size):
- Small projects (< 100 files): 2-5 seconds
- Medium projects (100-1000 files): 5-20 seconds
- Large projects (1000+ files): 20-60 seconds

**Query Speed:**
- Database queries: < 100ms
- AI responses: 2-10 seconds (depends on complexity)

**Token Usage:**
- First query: 500-2000 tokens (builds cache)
- Subsequent queries: 25-200 tokens (uses cache)

## Development

```bash
# Clone repository
git clone https://github.com/Jonathan03ant/srcodex.git
cd srcodex

# Install in development mode
pip install -e .

# Index and run on srcodex itself
srcodex .
```

## License

MIT License - see LICENSE file for details

## Contributing

Contributions welcome! Please open an issue or pull request.

## Links

- [GitHub Repository](https://github.com/Jonathan03ant/srcodex)
- [Issue Tracker](https://github.com/Jonathan03ant/srcodex/issues)
- [Documentation](https://github.com/Jonathan03ant/srcodex/wiki)
