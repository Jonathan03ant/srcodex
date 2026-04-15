import os
import logging
from anthropic import Anthropic, APIError, APIStatusError
from .file_access_tools import TOOL_DEFINITIONS as FILE_TOOLS, execute_tool as execute_file_tool
from .graph_tools import TOOLS as GRAPH_TOOLS, execute_graph_tool
from .config_loader import get_config
from .status_tracker import StatusTracker


logger = logging.getLogger(__name__)


class ClaudeService:
    """Wrapper for Claude API - supports both AMD LLM Gateway and public Anthropic API"""
    def __init__(self):
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
        self.system_prompt = f""" CORE PRINCIPLE: THINK AHEAD, BATCH AGGRESSIVELY 

Before calling ANY tools, think: "What will I need in the NEXT iteration? Fetch it ALL NOW!"

You are analyzing the {config.project_name} project.

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

  WARN: WARN: WARN: CRITICAL: TARGET 3 ITERATIONS (4 iterations MAX) WARN: WARN: WARN:

  **WHY 3 ITERATIONS?**
  - Iterations 1-3 are CACHED (free to access later)
  - Iteration 4+ is NOT CACHED (every tool result costs tokens)
  - Solution: Get EVERYTHING in iterations 1-3, then answer in iteration 4

  **THINK AHEAD! Predict what you'll need in future iterations and fetch it NOW!**

  **MANDATORY ITERATION PLAN:**

  **Iteration 1 (BROAD EXPLORATION - 15-25 tools):**
  Think: "What are ALL the patterns, files, and areas I might need to explore?"
  Then call EVERY exploration tool in ONE batch:
  - search_symbols() with 5-10 different patterns ('%foo%', '%bar%', '%init%', '%process%', etc.)
  - execute_sql() for 3-5 aggregate queries (file counts, symbol types, etc.)
  - get_symbols_from_file() for 5-10 key files you predict will matter
  - list_indexed_files() if exploring file structure
  **THINK PREDICTIVELY:** If the question is "how does X work?", you'll need X's definition, callees, callers, related files - so search for ALL of those patterns NOW!

  **Iteration 2 (FETCH EVERYTHING - 20-30 tools):**
  Think: "From iteration 1, what are ALL the symbols/functions I found? I'll need ALL their details!"
  Then fetch EVERYTHING in ONE batch:
  - get_symbol_definition() for EVERY relevant symbol (15-25 symbols, not just 3-4!)
  - get_callees() for EVERY function found
  - get_callers() for EVERY function found
  - execute_sql() for relationships between symbols
  **BE GREEDY:** If iteration 1 found 20 symbols, fetch ALL 20 definitions NOW! Don't cherry-pick 5 and come back later!

  **Iteration 3 (DEEP DIVE - 10-20 tools, LAST CACHED ITERATION!):**
  Think: "What are ALL the remaining details I need to answer completely?"
  WARN: THIS IS YOUR LAST CACHED ITERATION! Get EVERYTHING you need NOW!
  - get_symbol_definition() with context_lines=20 for ALL core symbols
  - get_call_chain() for ALL execution paths
  - execute_sql() for ALL complex relationship queries
  - get_symbols_from_file() with include_definitions=True for ALL critical files
  **CRITICAL:** If you're missing ANYTHING, fetch it NOW! Iteration 4 is NOT cached - every tool wastes tokens!

  **Iteration 4 (ANSWER - ZERO tools):**
  Synthesize everything from iterations 1-3 into your complete answer.
  WARN: DO NOT call tools in iteration 4 - they're not cached and waste tokens!
  You have ALL the information from iterations 1-3 (cached). Use it to answer fully.

  **Iterations 5-6 (EMERGENCY FALLBACK - SHOULD NOT REACH):**
  You failed to complete in 4 iterations. Answer with what you have.

  **EXAMPLES:**

   PERFECT (4 iterations):
  Q: "How does the indexer work?"
  Iteration 1: [search_symbols('%index%'), search_symbols('%parse%'), search_symbols('%ctags%'),
                execute_sql("SELECT * FROM symbols WHERE name LIKE '%index%'"),
                execute_sql("SELECT * FROM symbols WHERE type='class'"),
                get_symbols_from_file('indexer/indexer.py'),
                get_symbols_from_file('indexer/ctags_parser.py'),
                ... 40 more tools] (50 tools total)
  Iteration 2: [get_symbol_definition('Indexer'), get_symbol_definition('parse_symbols'),
                get_callees('index_directory'), get_callers('parse_symbols'),
                ... 45 more tools] (60 tools total)
  Iteration 3: [execute_sql("SELECT * FROM symbol_edges WHERE src_symbol_id=123"),
                get_call_chain('main', 'parse_symbols'), ... 15 more tools] (20 tools total)
  Iteration 4: "The indexer works in 3 stages..." (ANSWER, 0 tools)

  ERROR: FAILURE (6+ iterations):
  Iteration 1: 5 tools
  Iteration 2: 3 tools
  Iteration 3: 4 tools
  ... YOU FAILED. Start over and batch properly!

  **If you call fewer than 20 tools in iterations 1-2, you are doing it WRONG.**

  **IMPORTANT: NEVER mention iterations, caching, or your tool-gathering strategy in your final answer.**
  The user doesn't need to know about your internal process. Just answer their question directly and professionally.
  """

    def _truncate_conversation_history(self, conversation_history, max_messages=10):
        """Keep last N messages for cache efficiency"""
        if len(conversation_history) > max_messages:
            return conversation_history[-max_messages:]
        return conversation_history

    def _calculate_savings(self, user_input_tokens, files_accessed_count):
        """
        Calculate token savings vs traditional manual approach

        Traditional: User pastes N files × avg_lines × tokens_per_line
        srcodex: User types short queries, tools fetch only needed data

        Returns: (traditional_tokens, savings_percentage)
        """
        config = get_config()

        # Get average file size from metadata
        avg_lines_per_file = config.stats.get('avg_lines_per_file', 500)
        tokens_per_line = 4  # Industry standard

        # Traditional approach: paste entire files
        traditional_tokens = files_accessed_count * avg_lines_per_file * tokens_per_line

        # Avoid division by zero
        if traditional_tokens == 0:
            return 0, 0.0

        # Calculate savings percentage
        savings_percentage = (1 - user_input_tokens / traditional_tokens) * 100

        # Cap at 99.9% (avoid showing 100%)
        savings_percentage = min(savings_percentage, 99.9)

        return traditional_tokens, savings_percentage

    def send_message(self, message):
        """Send Message to Claude and get response"""

        response = self.client.messages.create(
            model=self.model,
            max_tokens=8192,
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
        logger.info(f"MSG: User message: {message}")

        # Build messages array with conversation history
        if conversation_history:
            messages = self._truncate_conversation_history(conversation_history)
            logger.info(f"HISTORY: Using conversation history ({len(conversation_history)} messages, truncated to {len(messages)})")
        else:
            messages = []

        # Add current message
        messages.append({"role": "user", "content": message})

        # Token tracking
        total_input_tokens = 0
        total_output_tokens = 0

        # File access tracking for savings calculation
        files_accessed = set()

        # Cache breakpoint tracking (max 4 total: 1 for system + 3 for messages)
        cache_breakpoints_used = 0
        max_cache_breakpoints = 3

        # Tool use loop - max 6 iterations (target: 4, absolute emergency: 5)
        iteration = 0
        max_iterations = 6
        while True:
            iteration += 1

            # At iteration 6, FORCE final answer (disable tools completely)
            if iteration > max_iterations:
                logger.error(f"CRITICAL: ITERATION {iteration} - EXCEEDED MAX! Forcing answer with available context.")

            # Warnings for iterations past target
            if iteration == 5:
                logger.warning("WARN: ITERATION 5/6 - Should have finished in 4! One more iteration left.")
            elif iteration == 6:
                logger.error("CRITICAL: ITERATION 6/6 - FINAL ITERATION! Must answer NOW.")

            logger.info(f"\nITER Iteration {iteration}/6: Calling Claude API...")

            # Build system prompt with cache control
            system_with_cache = [
                {
                    "type": "text",
                    "text": self.system_prompt,
                    "cache_control": {"type": "ephemeral"}
                }
            ]

            # Keep tools constant to preserve cache
            tools_to_use = self.tools

            # At iteration 6, inject urgent message to FORCE answer without tools
            messages_to_send = messages
            if iteration >= 6:
                # Add urgent instruction as last message
                messages_to_send = messages + [{
                    "role": "user",
                    "content": "WARN: CRITICAL: This is iteration 6/6. You MUST provide your final answer NOW using everything you've gathered. DO NOT call any more tools. Synthesize your findings and answer the user's question completely."
                }]

            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=8192,
                    system=system_with_cache,
                    tools=tools_to_use,
                    messages=messages_to_send,
                    extra_headers={
                        "anthropic-beta": "context-management-2025-06-27,prompt-caching-2024-07-31"
                    }
                )
            except APIStatusError as e:
                logger.error(f"ERROR: API Error: {e.status_code} {e.message}")
                logger.error(f"   Response body: {e.body}")
                logger.error(f"   Request details:")
                logger.error(f"     - Model: {self.model}")
                logger.error(f"     - Messages count: {len(messages)}")
                logger.error(f"     - Tools count: {len(self.tools)}")
                if messages:
                    logger.error(f"     - Last message: {messages[-1]}")
                raise
            except APIError as e:
                logger.error(f"ERROR: API Error: {e}")
                raise

            # Check stop reason
            if response.stop_reason == "end_turn":
                # No more tool calls, return final text
                logger.info(" Claude finished (no more tools)")
                for block in response.content:
                    if block.type == "text":
                        logger.info(f" Response length: {len(block.text)} chars")
                        logger.info("=" * 80)
                        return block.text
                return ""

            elif response.stop_reason == "tool_use":
                # Block tools after iteration 3 (cache is full)
                if iteration > 3:
                    logger.error(f"BLOCKED: BLOCKED: Claude tried to call {sum(1 for b in response.content if b.type == 'tool_use')} tools in iteration {iteration}!")
                    logger.error("   Tools are ONLY allowed in iterations 1-3 (cached). Forcing answer with cached data.")

                    # Skip appending assistant message with tool_use to avoid API error
                    # Inject user message to force answer
                    messages.append({
                        "role": "user",
                        "content": "BLOCKED: TOOL CALLS BLOCKED! You are in iteration 4+. Tools are ONLY allowed in iterations 1-3. You have ALL the data from cached iterations. Provide your complete answer NOW. DO NOT call any more tools."
                    })
                    # Loop back to get answer
                    continue

                # Claude wants to use tools
                logger.info(" Claude is using tools...")

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
                        logger.info(f"\n  Tool  Tool #{tool_count}: {block.name}")
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
                            logger.info(f"      Type: GRAPH TOOL ")
                            result = execute_graph_tool(block.name, block.input)
                        else:
                            logger.warning(f"      Type: UNKNOWN TOOL!")
                            result = {"error": f"Unknown tool: {block.name}"}

                        # Track files accessed for savings calculation
                        if block.name in ["read_file", "get_symbols_from_file", "get_file_info"]:
                            file_path = block.input.get("file_path")
                            if file_path:
                                files_accessed.add(file_path)

                        # Log result summary
                        if isinstance(result, dict):
                            if "error" in result:
                                logger.error(f"      ERROR: Error: {result['error']}")
                            elif "count" in result:
                                logger.info(f"       Returned {result['count']} results")
                            else:
                                logger.info(f"       Success (keys: {list(result.keys())})")

                        # Add tool result (no truncation - iterations 1-3 are cached)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": str(result)
                        })

                logger.info(f"\n Executed {tool_count} tool(s), sending results back to Claude...")

                # Send tool results back to Claude
                # Cache tool results if under breakpoint limit (max 4 total: system + 3 messages)
                if tool_results and cache_breakpoints_used < max_cache_breakpoints:
                    tool_results[-1]["cache_control"] = {"type": "ephemeral"}
                    cache_breakpoints_used += 1
                    logger.info(f"CACHE: Cache breakpoint set on last tool result (iteration {iteration}, breakpoint {cache_breakpoints_used}/{max_cache_breakpoints})")
                elif tool_results and cache_breakpoints_used >= max_cache_breakpoints:
                    logger.info(f"WARN:  Skipping cache (already at {cache_breakpoints_used}/{max_cache_breakpoints} breakpoints) - rely on parallel tools to finish quickly!")

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
        logger.info(f"MSG: User message (streaming): {message}")

        # Initialize status tracker
        status = StatusTracker()
        status.start_query()

        # Build messages array with conversation history
        if conversation_history:
            messages = self._truncate_conversation_history(conversation_history)
            logger.info(f"HISTORY: Using conversation history ({len(conversation_history)} messages, truncated to {len(messages)})")
        else:
            messages = []

        # Estimate conversation history tokens BEFORE adding current message
        # Each previous message ~10-50 tokens (use 20 as conservative estimate to avoid going negative)
        conversation_history_tokens = len(messages) * 20

        # NOTE: Conversation history caching disabled to stay within 4 cache breakpoint limit
        # We use: 1=system, 2=iter1 tools, 3=iter2 tools, 4=iter3 tools

        # Add current message
        messages.append({"role": "user", "content": message})

        # Token tracking
        total_input_tokens = 0
        total_output_tokens = 0
        total_cache_read_tokens = 0
        total_cache_write_tokens = 0
        user_message_tokens = 0
        iteration_1_cache_write = 0  # Track iteration 1 cache (system + tools)
        cached_iterations_input = 0  # Track input tokens from iterations 1-3 (when we're caching)
        PROMPT_OVERHEAD = 300  # System prompt tokens (constant across queries)

        # File access tracking for savings calculation
        files_accessed = set()

        # Cache breakpoint tracking (max 4 total: 1 for system + 3 for messages)
        cache_breakpoints_used = 0
        max_cache_breakpoints = 3

        # Tool use loop - max 6 iterations (target: 4)
        iteration = 0
        max_iterations = 6
        while True:
            iteration += 1
            status.start_iteration(iteration)

            # At iteration 6, FORCE final answer (disable tools completely)
            if iteration > max_iterations:
                logger.error(f"CRITICAL: ITERATION {iteration} - EXCEEDED MAX! Forcing answer with available context.")

            # Warnings for iterations past target
            if iteration == 5:
                logger.warning("WARN: ITERATION 5/6 - Should have finished in 4! One more iteration left.")
            elif iteration == 6:
                logger.error("CRITICAL: ITERATION 6/6 - FINAL ITERATION! Must answer NOW or fail.")

            logger.info(f"\nITER Iteration {iteration}/6: Calling Claude API...")

            # Build system prompt with cache control
            system_with_cache = [
                {
                    "type": "text",
                    "text": self.system_prompt,
                    "cache_control": {"type": "ephemeral"}
                }
            ]

            # Keep tools constant to preserve cache
            tools_with_cache = self.tools

            # At iteration 6, inject urgent message to FORCE answer without tools
            messages_to_send = messages
            if iteration >= 6:
                # Add urgent instruction as last message (doesn't break cache since it's a NEW iteration)
                messages_to_send = messages + [{
                    "role": "user",
                    "content": "WARN: CRITICAL: This is iteration 6/6. You MUST provide your final answer NOW using everything you've gathered. DO NOT call any more tools. Synthesize your findings and answer the user's question completely."
                }]

            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=8192,
                    system=system_with_cache,
                    tools=tools_with_cache,
                    messages=messages_to_send
                )
            except APIStatusError as e:
                logger.error(f"ERROR: API Error: {e.status_code} {e.message}")
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
                logger.error(f"ERROR: API Error: {e}")
                yield {"type": "error", "content": f"API Error: {str(e)}"}
                return

            # Track tokens
            cache_read = getattr(response.usage, 'cache_read_input_tokens', 0)
            cache_write = getattr(response.usage, 'cache_creation_input_tokens', 0)

            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens
            total_cache_read_tokens += cache_read
            total_cache_write_tokens += cache_write

            # Track input tokens from cached iterations (1-3)
            if iteration <= 3:
                cached_iterations_input += response.usage.input_tokens

            # Calculate user message tokens in iteration 1
            # Iteration 1: input = user_message + system_prompt + tools
            #              cache_write = system_prompt + tools (if caching)
            #              user_message = input - cache_write
            if iteration == 1:
                if cache_write > 0:
                    # Session with existing cache: input includes cache write
                    user_message_tokens = response.usage.input_tokens - cache_write
                    iteration_1_cache_write = cache_write
                else:
                    # First ever query (no cache): all input is user message + system + tools
                    # We don't cache on first query, so total_input IS the cost
                    user_message_tokens = response.usage.input_tokens
                logger.info(f"    User message (+ system/tools if no cache): ~{user_message_tokens} tokens")

            logger.info(f"   FILES: Tokens: {response.usage.input_tokens} in / {response.usage.output_tokens} out")
            if cache_read > 0 or cache_write > 0:
                logger.info(f"   💾 Cache: {cache_read} read / {cache_write} write")

            # Check stop reason
            if response.stop_reason == "end_turn":
                # No tools used, stream the final text
                logger.info("Claude finished (no more tools)")

                # Update status to "Preparing answer..." if this is iteration 5 or final iteration
                if iteration >= 4:
                    status.set_preparing_answer()
                    yield status.get_status_message()

                for block in response.content:
                    if block.type == "text":
                        logger.info(f"Streaming response ({len(block.text)} chars)")
                        # Yield text chunks
                        for char in block.text:
                            yield {"type": "text", "content": char}

                # Calculate token savings
                # User input = all input tokens from iterations 1-3 minus overhead
                # Overhead = system prompt + conversation history
                # Example: 3349 input - 300 prompt - 1050 history = 1999 tokens
                user_input_only = max(0, cached_iterations_input - PROMPT_OVERHEAD - conversation_history_tokens)
                traditional_equiv, new_savings_pct = self._calculate_savings(user_input_only, len(files_accessed))

                # If no files accessed, keep previous savings percentage (don't reset to 0%)
                if len(files_accessed) == 0 and hasattr(self, 'last_savings_pct'):
                    savings_pct = self.last_savings_pct
                else:
                    savings_pct = new_savings_pct
                    self.last_savings_pct = new_savings_pct  # Save for next time

                # Yield final token count
                total_tokens = total_input_tokens + total_output_tokens
                logger.info(f"\nTOTAL: {total_input_tokens} input, {total_output_tokens} output, {total_cache_read_tokens} cache read, {total_cache_write_tokens} cache write (total {total_tokens})")
                logger.info(f"FILES: {len(files_accessed)} accessed, traditional: {traditional_equiv} tokens, savings: {savings_pct:.1f}%")
                logger.info("=" * 80)

                # Mark query as complete
                status.set_complete()
                yield status.get_status_message()

                # End status tracking
                status.end_query()

                yield {
                    "type": "tokens",
                    "input": total_input_tokens,
                    "output": total_output_tokens,
                    "total": total_tokens,
                    "cache_read": total_cache_read_tokens,
                    "cache_write": total_cache_write_tokens,
                    "user_input_only": user_input_only,
                    "files_accessed": len(files_accessed),
                    "traditional_equivalent": traditional_equiv,
                    "savings_percentage": savings_pct
                }
                return

            elif response.stop_reason == "tool_use":
                # Block tools after iteration 3 (cache is full)
                if iteration > 3:
                    logger.error(f"BLOCKED: BLOCKED: Claude tried to call {sum(1 for b in response.content if b.type == 'tool_use')} tools in iteration {iteration}!")
                    logger.error("   Tools are ONLY allowed in iterations 1-3 (cached). Forcing answer with cached data.")

                    # Skip appending assistant message with tool_use to avoid API error
                    # Inject user message to force answer
                    messages.append({
                        "role": "user",
                        "content": "BLOCKED: TOOL CALLS BLOCKED! You are in iteration 4+. Tools are ONLY allowed in iterations 1-3. You have ALL the data from cached iterations. Provide your complete answer NOW. DO NOT call any more tools."
                    })
                    # Loop back to get answer
                    continue

                # Claude wants to use tools
                logger.info(" Claude is using tools...")

                # Add assistant's response to messages
                messages.append({
                    "role": "assistant",
                    "content": response.content
                })

                # Count total tools first
                total_tools = sum(1 for block in response.content if block.type == "tool_use")

                # Execute all tool calls (don't stream this part)
                tool_results = []
                tool_count = 0
                first_tool_name = None
                for block in response.content:
                    if block.type == "tool_use":
                        tool_count += 1
                        logger.info(f"\n  Tool  Tool #{tool_count}: {block.name}")
                        logger.info(f"      Input: {block.input}")

                        # Capture first tool name for status
                        if tool_count == 1:
                            first_tool_name = block.name
                            # Update status and yield it
                            status.set_tool_status(first_tool_name, total_tools)
                            yield status.get_status_message()

                        # Route to correct tool handler
                        file_tools = ["read_file", "list_directory", "search_files"]
                        graph_tools = ["get_callers", "get_callees", "get_call_chain", "execute_sql",
                                      "get_file_by_pattern", "get_file_info", "list_indexed_files",
                                      "search_symbols", "get_symbol_definition", "get_symbols_from_file"]

                        if block.name in file_tools:
                            logger.info(f"      Type: FILE SYSTEM TOOL")
                            result = execute_file_tool(block.name, block.input)
                        elif block.name in graph_tools:
                            logger.info(f"      Type: GRAPH TOOL ")
                            result = execute_graph_tool(block.name, block.input)
                        else:
                            logger.warning(f"      Type: UNKNOWN TOOL!")
                            result = {"error": f"Unknown tool: {block.name}"}

                        # Track files accessed for savings calculation
                        if block.name in ["read_file", "get_symbols_from_file", "get_file_info"]:
                            file_path = block.input.get("file_path")
                            if file_path:
                                files_accessed.add(file_path)

                        # Log result summary
                        if isinstance(result, dict):
                            if "error" in result:
                                logger.error(f"      Error: {result['error']}")
                            elif "count" in result:
                                logger.info(f"      Returned {result['count']} results")
                            else:
                                logger.info(f"      Success (keys: {list(result.keys())})")

                        # Add tool result (no truncation - iterations 1-3 are cached)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": str(result)
                        })

                logger.info(f"\n Executed {tool_count} tool(s), sending results back to Claude...")

                # Send tool results back to Claude
                # Cache tool results if under breakpoint limit (max 4 total: system + 3 messages)
                if tool_results and cache_breakpoints_used < max_cache_breakpoints:
                    tool_results[-1]["cache_control"] = {"type": "ephemeral"}
                    cache_breakpoints_used += 1
                    logger.info(f"CACHE: Cache breakpoint set on last tool result (iteration {iteration}, breakpoint {cache_breakpoints_used}/{max_cache_breakpoints})")
                elif tool_results and cache_breakpoints_used >= max_cache_breakpoints:
                    logger.info(f"WARN:  Skipping cache (already at {cache_breakpoints_used}/{max_cache_breakpoints} breakpoints) - rely on parallel tools to finish quickly!")

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