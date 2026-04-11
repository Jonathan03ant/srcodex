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

  **EFFICIENCY RULES (CRITICAL - You have max 12 iterations!):**

  SEARCH STRATEGY - Always use targeted search FIRST:
  1. NEVER start with list_directory or search_files for code questions
  2. ALWAYS use search_symbols() or execute_sql as your FIRST tool
  3. Think: "What pattern would find this?" then search the database directly

  Examples of SMART searching:
  - "find ioctl calls" → search_symbols('%ioctl%', 'macro') or search_symbols('%ioctl%', 'function')
  - "how does X work" → search_symbols('X') → get_callees('X') → get_symbol_definition()
  - "struct for Y" → search_symbols('Y', 'struct') or execute_sql("SELECT * FROM symbols WHERE name LIKE '%Y%' AND type='struct'")
  - "all functions in file.c" → get_symbols_from_file('file.c', include_definitions=false)

  WRONG approach (wastes tokens):
  ❌ list_directory → search_files → read_file → search_symbols

  RIGHT approach (efficient):
  ✅ search_symbols (find it!) → get_symbol_definition (get details) → done in 2-3 iterations

  TOKEN OPTIMIZATION:
  - TWO-STEP: get_symbols_from_file(include_definitions=false) THEN get_symbol_definition() for specific symbols
  - AVOID: get_symbols_from_file(include_definitions=true) - returns ALL code (5-10x more tokens)
  - Keep context_lines ≤ 10 in get_symbol_definition()
  - Use execute_sql for complex queries (finds multiple related symbols in 1 call)

  TOOL PREFERENCES (fastest to slowest):
  1. search_symbols, execute_sql (database query - instant, ~500 tokens)
  2. get_symbol_definition, get_callees, get_callers (targeted fetch - ~200 tokens)
  3. get_symbols_from_file (file metadata - ~100-500 tokens)
  4. list_indexed_files (directory listing - ~1000 tokens)
  5. NEVER: list_directory, search_files, read_file on code (blocked/expensive)

  **Instructions:**
  - Answer in 3-5 iterations when possible (you have max 12)
  - Start with database search, not filesystem browsing
  - Be concise and direct
  - Show file paths when referencing code
  """

    def _truncate_conversation_history(self, conversation_history, max_messages=6):
        """
        Truncate conversation history to reduce token usage while preserving context.
        Keeps last N messages and strips tool_use/tool_result blocks from assistant messages.
        """
        # Keep only last N messages
        recent = conversation_history[-max_messages:] if len(conversation_history) > max_messages else conversation_history

        # Strip tool blocks from messages (keep only text responses)
        cleaned = []
        for msg in recent:
            if msg["role"] == "user":
                # Keep user messages as-is
                cleaned.append(msg)
            elif msg["role"] == "assistant":
                # Extract only text content from assistant messages
                content = msg.get("content", "")
                if isinstance(content, list):
                    # Filter out tool_use blocks, keep only text
                    text_blocks = [block.get("text", "") for block in content if isinstance(block, dict) and block.get("type") == "text"]
                    if text_blocks:
                        cleaned.append({"role": "assistant", "content": " ".join(text_blocks)})
                elif isinstance(content, str):
                    cleaned.append(msg)

        return cleaned

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
            messages = self._truncate_conversation_history(conversation_history)
            logger.info(f"📚 Using conversation history ({len(conversation_history)} messages, truncated to {len(messages)})")
        else:
            messages = []

        # Add current message
        messages.append({"role": "user", "content": message})

        # Token tracking
        total_input_tokens = 0
        total_output_tokens = 0

        # Tool use loop - max 12 iterations to prevent runaway token usage
        iteration = 0
        max_iterations = 12
        while True:
            iteration += 1

            # Check iteration limit
            if iteration > max_iterations:
                logger.warning(f"⚠️  Reached max iterations ({max_iterations}), stopping tool loop")
                logger.info("💡 Tip: Try breaking complex questions into smaller parts")
                # Return whatever we have from the last assistant response
                for msg in reversed(messages):
                    if msg["role"] == "assistant":
                        content = msg.get("content", "")
                        if isinstance(content, list):
                            for block in content:
                                if isinstance(block, dict) and block.get("type") == "text":
                                    return block.get("text", "Reached iteration limit without completing analysis.")
                        elif isinstance(content, str):
                            return content
                return f"Reached maximum iterations ({max_iterations}). Please try a more specific question or break this into smaller parts."

            logger.info(f"\n🔄 Iteration {iteration}/{max_iterations}: Calling Claude API...")

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
            messages = self._truncate_conversation_history(conversation_history)
            logger.info(f"📚 Using conversation history ({len(conversation_history)} messages, truncated to {len(messages)})")
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

            # Track tokens
            cache_read = getattr(response.usage, 'cache_read_input_tokens', 0)
            cache_write = getattr(response.usage, 'cache_creation_input_tokens', 0)

            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens
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

                # Yield final token count
                total_tokens = total_input_tokens + total_output_tokens
                logger.info(f"\n💰 TOTAL: {total_input_tokens} input, {total_output_tokens} output, {total_cache_read_tokens} cache read, {total_cache_write_tokens} cache write (total {total_tokens})")
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