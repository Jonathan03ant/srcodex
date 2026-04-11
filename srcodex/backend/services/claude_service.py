import os
import logging
from anthropic import Anthropic, APIError, APIStatusError
from .file_access_tools import TOOL_DEFINITIONS as FILE_TOOLS, execute_tool as execute_file_tool
from .graph_tools import TOOLS as GRAPH_TOOLS, execute_graph_tool
from .config_loader import get_config


logger = logging.getLogger(__name__)


class ClaudeService:
    """Wrapper for Claude API - supports both AMD LLM Gateway and public Anthropic API"""
    def __init__(self):
        # internal AMD users
        amd_api_key = os.getenv("AMD_LLM_API_KEY")
        anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")

        if amd_api_key and amd_api_key != "dummy":
            # AMD LLM Gateway mode
            base_url = os.getenv("ANTHROPIC_BASE_URL", "https://llm-api.amd.com/Anthropic")
            self.client = Anthropic(
                base_url=base_url,
                api_key="dummy",
                default_headers={
                    "Ocp-Apim-Subscription-Key": amd_api_key,
                    "user": os.getenv("USER", "unknown")
                }
            )
            self.model = os.getenv("ANTHROPIC_DEFAULT_SONNET_MODEL", "claude-sonnet-4.5")
            logger.info("Using AMD LLM Gateway")

        elif anthropic_api_key:
            # Public Anthropic API mode
            base_url = os.getenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com")
            self.client = Anthropic(
                base_url=base_url,
                api_key=anthropic_api_key
            )
            self.model = os.getenv("ANTHROPIC_DEFAULT_SONNET_MODEL", "claude-sonnet-4-20250514")
            logger.info("Using public Anthropic API")

        else:
            raise ValueError(
                "No API key found! Set either:\n"
                "  - AMD_LLM_API_KEY (for AMD internal users)\n"
                "  - ANTHROPIC_API_KEY (for public API users)"
            )

        # Merge all tools (file tools + graph tools)
        self.tools = FILE_TOOLS + GRAPH_TOOLS

        # Load project configuration and generate system prompt
        config = get_config()
        stats = config.stats

        # System prompt with project context (auto-generated from metadata)
        self.system_prompt = f"""You are analyzing the {config.project_name} project.

  **Project Context:**
  - Source root: {config.metadata['paths']['source_root']}/ (all paths are relative to this)
  - Files indexed: {stats['files_indexed']:,}
  - Total symbols: {stats['total_symbols']:,}
  - Call graph edges: {stats['edges']['calls']:,} CALLS relationships
  - Include edges: {stats['edges']['includes']:,} INCLUDES relationships
  - Field access edges: {stats['edges']['accesses']:,} ACCESSES relationships

  **Path Convention:**
  All file paths are relative to source root. Examples:
  - 'firmware/main/mp1/src/app/power.c'
  - 'firmware/main/mp1/src/app/dpm.h'
  - 'firmware/main/mpccx/src/app/thermal.c'

  **Available Tools:**

  File System Tools:
  - read_file(file_path): Read source code files (path relative to source root)
  - list_directory(dir_path): Browse directory structure (path relative to source root)
  - search_files(pattern, search_path): Find files by glob pattern

  Semantic Graph Tools (use these to save tokens!):
  - get_callers: Find what calls a function (1-hop backward)
  - get_callees: Find what a function calls (1-hop forward)
  - get_call_chain: Trace execution paths from A to B (multi-hop)
  - search_symbols: Search for symbols by name pattern
  - get_symbol_definition: Get ONLY one symbol's definition (not entire file)
  - get_symbols_from_file: Get ALL symbols from a file (replaces read_file for headers)
  - get_file_by_pattern: Find files by name pattern
  - execute_sql: Custom SQL queries on the semantic graph

  **Database Schema (for execute_sql):**

  symbols table:
    - id, name, type (function/struct/macro/variable/enum/typedef)
    - file_path, line_number, signature
    - scope_kind, scope_name (parent scope)

  symbol_edges table:
    - edge_type ('CALLS', 'INCLUDES', 'ACCESSES')
    - src_symbol_id, dst_symbol_id (foreign keys to symbols.id)
    - source_file, line_number (where edge occurs)

  Example SQL:
    SELECT s1.name as caller, s2.name as callee
    FROM symbol_edges e
    JOIN symbols s1 ON e.src_symbol_id = s1.id
    JOIN symbols s2 ON e.dst_symbol_id = s2.id
    WHERE e.edge_type = 'CALLS' AND s2.name = 'FunctionName'

  **Token Optimization Guide (CRITICAL - Follow This!):**

  TWO-STEP APPROACH (Best Practice):
  1. Get metadata FIRST: get_symbols_from_file(file, include_definitions=false)
     → See what symbols exist (~100 tokens for a header with 50 symbols)
  2. Get specific definitions: get_symbol_definition(symbol_name)
     → Only fetch the 2-3 symbols you actually need (~200 tokens each)

  AVOID: get_symbols_from_file(file, include_definitions=true)
  → This returns EVERY symbol's code (can be 5-10x more tokens than the file!)
  → Example: 69 symbols × 50 lines = 3,450 lines (vs 400 lines for the file itself)

  Quick Rules:
  - Exploring directories? Use list_indexed_files() NOT list_directory() (10x faster, database query vs filesystem)
  - Single symbol? Use get_symbol_definition() (keep context_lines ≤ 10)
  - Exploring a file? Use get_symbols_from_file(include_definitions=false), then selective get_symbol_definition()
  - Call graph queries? Use get_callers/get_callees/get_call_chain (fastest!)
  - NEVER use read_file() if the file is indexed (check with get_symbols_from_file first)
  - Large context_lines (>20) defeats the purpose of symbol-based queries - keep it small!

  **Instructions:**
  - PREFER graph tools over filesystem tools (massive token savings!)
  - Use list_indexed_files() instead of list_directory() for indexed code
  - Use graph tools for "what calls X", "how does A reach B", "show call flow"
  - CRITICAL: Once you've used get_symbol_definition(), do NOT follow up with read_file() on the same file
  - Be concise and direct
  - Show file paths when referencing code
  """

    def send_message(self, message):
        """Send Message to Claude and get response"""

        response = self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            system=self.system_prompt,
            messages=[
                {"role": "user", "content": message}
            ]
        )

        for block in response.content:
            if block.type == "text":
                return block.text
        return ""

    def send_message_with_tools(self, message, conversation_history=None):
        """Send message to Claude with tool support"""
        logger.info("=" * 80)
        logger.info(f"📨 User message: {message}")

        # Build messages array with conversation history
        if conversation_history:
            messages = conversation_history.copy()
            logger.info(f"📚 Using conversation history ({len(conversation_history)} messages)")
        else:
            messages = []

        # Add current message
        messages.append({"role": "user", "content": message})

        # Token tracking
        total_input_tokens = 0
        total_output_tokens = 0

        # Tool use loop - allow multiple tool calls
        iteration = 0
        while True:
            iteration += 1
            logger.info(f"\n🔄 Iteration {iteration}: Calling Claude API...")

            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=4096,
                    system=self.system_prompt,
                    tools=self.tools,
                    messages=messages
                )
            except APIStatusError as e:
                logger.error(f"❌ API Error: {e.status_code} {e.message}")
                logger.error(f"   Response body: {e.body}")
                logger.error(f"   Request details:")
                logger.error(f"     - Model: {self.model}")
                logger.error(f"     - Messages count: {len(messages)}")
                logger.error(f"     - Tools count: {len(self.tools)}")
                if messages:
                    logger.error(f"     - Last message: {messages[-1]}")
                raise
            except APIError as e:
                logger.error(f"❌ API Error: {e}")
                raise

            # Check stop reason
            if response.stop_reason == "end_turn":
                # No more tool calls, return final text
                logger.info("✅ Claude finished (no more tools)")
                for block in response.content:
                    if block.type == "text":
                        logger.info(f"📝 Response length: {len(block.text)} chars")
                        logger.info("=" * 80)
                        return block.text
                return ""

            elif response.stop_reason == "tool_use":
                # Claude wants to use tools
                logger.info("🔧 Claude is using tools...")

                # Add assistant's response to messages
                messages.append({
                    "role": "assistant",
                    "content": response.content
                })

                # Execute all tool calls
                tool_results = []
                tool_count = 0
                for block in response.content:
                    if block.type == "tool_use":
                        tool_count += 1
                        logger.info(f"\n  🛠️  Tool #{tool_count}: {block.name}")
                        logger.info(f"      Input: {block.input}")

                        # Route to correct tool handler
                        file_tools = ["read_file", "list_directory", "search_files"]
                        graph_tools = ["get_callers", "get_callees", "get_call_chain", "execute_sql",
                                      "get_file_by_pattern", "get_file_info", "list_indexed_files",
                                      "search_symbols", "get_symbol_definition", "get_symbols_from_file"]

                        if block.name in file_tools:
                            logger.info(f"      Type: FILE SYSTEM TOOL")
                            result = execute_file_tool(block.name, block.input)
                        elif block.name in graph_tools:
                            logger.info(f"      Type: GRAPH TOOL ⚡")
                            result = execute_graph_tool(block.name, block.input)
                        else:
                            logger.warning(f"      Type: UNKNOWN TOOL!")
                            result = {"error": f"Unknown tool: {block.name}"}

                        # Log result summary
                        if isinstance(result, dict):
                            if "error" in result:
                                logger.error(f"      ❌ Error: {result['error']}")
                            elif "count" in result:
                                logger.info(f"      ✅ Returned {result['count']} results")
                            else:
                                logger.info(f"      ✅ Success (keys: {list(result.keys())})")

                        # Add tool result
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": str(result)
                        })

                logger.info(f"\n✅ Executed {tool_count} tool(s), sending results back to Claude...")

                # Send tool results back to Claude
                messages.append({
                    "role": "user",
                    "content": tool_results
                })

                # Continue loop to get Claude's response

            else:
                # Unexpected stop reason
                return f"Unexpected stop reason: {response.stop_reason}"

    def stream_message_with_tools(self, message, conversation_history=None):
        """
        Stream message to Claude with tool support - yields text chunks and metadata

        Args:
            message: User's current message
            conversation_history: Optional list of previous messages [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]

        Yields:
            dict: Either text chunks or token metadata
                {"type": "text", "content": "..."}
                {"type": "tokens", "input": 1234, "output": 56, "total": 1290, "cache_read": 100, "cache_write": 50}
        """
        logger.info("=" * 80)
        logger.info(f"📨 User message (streaming): {message}")

        # Build messages array with conversation history
        if conversation_history:
            messages = conversation_history.copy()
            logger.info(f"📚 Using conversation history ({len(conversation_history)} messages)")
        else:
            messages = []

        # Add current message
        messages.append({"role": "user", "content": message})

        # Token tracking
        total_input_tokens = 0
        total_output_tokens = 0
        total_cache_read_tokens = 0
        total_cache_write_tokens = 0

        # Tool use loop
        iteration = 0
        while True:
            iteration += 1
            logger.info(f"\n🔄 Iteration {iteration}: Calling Claude API...")

            # Build system prompt with cache control
            system_with_cache = [
                {
                    "type": "text",
                    "text": self.system_prompt,
                    "cache_control": {"type": "ephemeral"}
                }
            ]

            # Note: Tool caching disabled due to AMD VertexGenAI TTL ordering constraint
            # (tools get TTL=5m, system gets TTL=1h, but API requires longer TTL first)
            tools_with_cache = self.tools

            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=4096,
                    system=system_with_cache,
                    tools=tools_with_cache,
                    messages=messages
                )
            except APIStatusError as e:
                logger.error(f"❌ API Error: {e.status_code} {e.message}")
                logger.error(f"   Response body: {e.body}")
                logger.error(f"   Request details:")
                logger.error(f"     - Model: {self.model}")
                logger.error(f"     - Messages count: {len(messages)}")
                logger.error(f"     - Tools count: {len(tools_with_cache)}")
                if messages:
                    logger.error(f"     - Last message: {messages[-1]}")
                # Yield error to frontend
                yield {"type": "error", "content": f"API Error {e.status_code}: {e.message}"}
                return
            except APIError as e:
                logger.error(f"❌ API Error: {e}")
                yield {"type": "error", "content": f"API Error: {str(e)}"}
                return

            # Track tokens (including cache metrics)
            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens
            cache_read = getattr(response.usage, 'cache_read_input_tokens', 0)
            cache_write = getattr(response.usage, 'cache_creation_input_tokens', 0)
            total_cache_read_tokens += cache_read
            total_cache_write_tokens += cache_write

            logger.info(f"   📊 Tokens: {response.usage.input_tokens} in / {response.usage.output_tokens} out")
            if cache_read > 0 or cache_write > 0:
                logger.info(f"   💾 Cache: {cache_read} read / {cache_write} write")

            # Check stop reason
            if response.stop_reason == "end_turn":
                # No tools used, stream the final text
                logger.info("Claude finished (no more tools)")
                for block in response.content:
                    if block.type == "text":
                        logger.info(f"Streaming response ({len(block.text)} chars)")
                        # Yield text chunks
                        for char in block.text:
                            yield {"type": "text", "content": char}

                # Yield final token count with cache metrics
                total_tokens = total_input_tokens + total_output_tokens
                logger.info(f"\n💰 TOTAL TOKENS: {total_input_tokens} input + {total_output_tokens} output = {total_tokens} total")
                if total_cache_read_tokens > 0 or total_cache_write_tokens > 0:
                    logger.info(f"   CACHE: {total_cache_read_tokens} read + {total_cache_write_tokens} write")
                logger.info("=" * 80)
                yield {
                    "type": "tokens",
                    "input": total_input_tokens,
                    "output": total_output_tokens,
                    "total": total_tokens,
                    "cache_read": total_cache_read_tokens,
                    "cache_write": total_cache_write_tokens
                }
                return

            elif response.stop_reason == "tool_use":
                # Claude wants to use tools
                logger.info("🔧 Claude is using tools...")

                # Add assistant's response to messages
                messages.append({
                    "role": "assistant",
                    "content": response.content
                })

                # Execute all tool calls (don't stream this part)
                tool_results = []
                tool_count = 0
                for block in response.content:
                    if block.type == "tool_use":
                        tool_count += 1
                        logger.info(f"\n  🛠️  Tool #{tool_count}: {block.name}")
                        logger.info(f"      Input: {block.input}")

                        # Route to correct tool handler
                        file_tools = ["read_file", "list_directory", "search_files"]
                        graph_tools = ["get_callers", "get_callees", "get_call_chain", "execute_sql",
                                      "get_file_by_pattern", "get_file_info", "list_indexed_files",
                                      "search_symbols", "get_symbol_definition", "get_symbols_from_file"]

                        if block.name in file_tools:
                            logger.info(f"      Type: FILE SYSTEM TOOL")
                            result = execute_file_tool(block.name, block.input)
                        elif block.name in graph_tools:
                            logger.info(f"      Type: GRAPH TOOL ⚡")
                            result = execute_graph_tool(block.name, block.input)
                        else:
                            logger.warning(f"      Type: UNKNOWN TOOL!")
                            result = {"error": f"Unknown tool: {block.name}"}

                        # Log result summary
                        if isinstance(result, dict):
                            if "error" in result:
                                logger.error(f"      Error: {result['error']}")
                            elif "count" in result:
                                logger.info(f"      Returned {result['count']} results")
                            else:
                                logger.info(f"      Success (keys: {list(result.keys())})")

                        # Add tool result
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": str(result)
                        })

                logger.info(f"\n✅ Executed {tool_count} tool(s), sending results back to Claude...")

                # Send tool results back to Claude
                messages.append({
                    "role": "user",
                    "content": tool_results
                })

            else:
                yield {"type": "text", "content": f"Unexpected stop reason: {response.stop_reason}"}
                return

    def stream_message(self, message):
        """Stream message to Claude and yield text chunks"""
        with self.client.messages.stream(
            model=self.model,
            max_tokens=16000,
            messages=[{"role": "user", "content": message}]
        ) as stream:
            for text in stream.text_stream:
                yield text