# srcodex

**Semantic code explorer with AI-powered search and analysis**

srcodex builds a semantic graph of your codebase and provides AI-powered exploration through natural language queries. Think of it as an intelligent code search that understands relationships, call graphs, and architecture.

## Features

- **Semantic Indexing**: Builds a persistent graph of symbols, functions, types, and their relationships
- **AI-Powered Search**: Ask questions in natural language about your code
- **Call Graph Analysis**: Trace function calls, dependencies, and execution paths
- **Terminal UI**: Beautiful terminal interface with file browser and AI chat
- **Multi-Language**: Supports C, C++, Python, and more
- **Fast**: SQLite-backed graph queries with intelligent caching
- **Portable**: `.srcodex/` directory makes indexed projects shareable

## Installation

```bash
pip install srcodex
```

## Quick Start

```bash
# Index your codebase (first time)
cd /path/to/your/project
srcodex

# Output:
# No .srcodex/ found. Index this directory? (y/n) y
# [Indexing happens...]
# [TUI launches]

# Next time - instant launch
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

Copy `.env.example` to `.env` and configure your API key:

```bash
# Public Anthropic API
ANTHROPIC_API_KEY=sk-ant-your-key-here

# Or enterprise gateway (if applicable)
AMD_LLM_API_KEY=your-subscription-key
```

## Requirements

- Python 3.9+
- Universal CTags (`brew install universal-ctags` or `apt install universal-ctags`)
- Cscope (optional, for call graph)
- Claude API key (Anthropic or enterprise gateway)

## How It Works

1. **Indexing**: Extracts symbols, relationships, and metadata using CTags and Cscope
2. **Graph Building**: Creates semantic graph with typed edges (CALLS, INCLUDES, ACCESSES)
3. **AI Integration**: Claude queries the graph using specialized tools (not reading full files)
4. **Token Efficiency**: 99%+ reduction in tokens vs. traditional file-reading approaches

## Project Structure

After indexing, your project will have:

```
your-project/
├── .srcodex/
│   ├── metadata.json       # Project stats
│   ├── config.toml         # Indexing config
│   ├── data/
│   │   └── project.db      # Semantic graph
│   └── logs/               # Debug logs
└── [your source files...]
```

## Development

```bash
# Clone repository
git clone https://github.com/Jonathan03ant/srcodex.git
cd srcodex

# Install in development mode
pip install -e .

# Run tests
pytest
```

## License

MIT License - see LICENSE file for details

## Contributing

Contributions welcome! Please open an issue or pull request.

## Links

- [GitHub Repository](https://github.com/Jonathan03ant/srcodex)
- [Issue Tracker](https://github.com/Jonathan03ant/srcodex/issues)
- [Documentation](https://github.com/Jonathan03ant/srcodex/wiki)
