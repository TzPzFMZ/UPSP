/* Generated from src/showcase/conversation-components.ts. Do not edit directly. */
"use strict";
(() => {
  // src/showcase/conversation-components.ts
  var STORAGE_KEY = "upsp.conversationShowcase.v1";
  var STATE_SCHEMA = "upsp_conversation_showcase_state.v1";
  var SELECTION_SCHEMA = "upsp_conversation_showcase_selection.v1";
  var COMPONENTS = ["thinking", "tools", "activity", "streaming"];
  var componentLabels = {
    thinking: "\u601D\u8003\u6298\u53E0",
    tools: "\u5DE5\u5177\u8C03\u7528",
    activity: "\u6B63\u5728\u5904\u7406",
    streaming: "\u6D41\u5F0F\u8F93\u51FA"
  };
  var variants = {
    thinking: [
      { id: "thinking_inline_disclosure", marker: "A", title: "\u6781\u7B80\u6298\u53E0\u884C", description: "\u601D\u8003\u5757\u59CB\u7EC8\u6536\u8D77\uFF1B\u53EA\u5728\u624B\u52A8\u70B9\u51FB\u65F6\u5C55\u5F00\u5168\u6587\u3002" },
      { id: "thinking_auto_card", marker: "B", title: "\u81EA\u52A8\u6536\u675F\u9884\u89C8\u5361", description: "\u6D3B\u52A8\u5757\u5C55\u5F00\uFF1B\u79BB\u5F00\u540E\u6536\u6210\u4E24\u884C\u9884\u89C8\uFF0C\u4ECD\u53EF\u624B\u52A8\u5C55\u5F00\u3002" },
      { id: "thinking_phase_timeline", marker: "C", title: "\u5168\u4E8B\u4EF6\u65F6\u95F4\u7EBF", description: "\u7528\u8FDE\u7EED\u8F68\u9053\u4FDD\u7559\u601D\u8003\u3001\u8FDB\u5C55\u3001\u5DE5\u5177\u4E0E\u56DE\u590D\u7684\u524D\u540E\u5173\u7CFB\u3002" }
    ],
    tools: [
      { id: "tools_compact_records", marker: "A", title: "\u7D27\u51D1\u8BB0\u5F55", description: "\u4E00\u4E2A\u5DE5\u5177\u4E00\u884C\uFF1B\u53C2\u6570\u4E0E\u7ED3\u679C\u6309\u9700\u5C55\u5F00\u3002" },
      { id: "tools_execution_cards", marker: "B", title: "\u6267\u884C\u5361\u7247", description: "\u8C03\u7528\u548C\u8FD4\u56DE\u4E0A\u4E0B\u5206\u533A\uFF0C\u72B6\u6001\u4E00\u773C\u53EF\u8FA8\u3002" },
      { id: "tools_execution_timeline", marker: "C", title: "\u6267\u884C\u65F6\u95F4\u7EBF", description: "\u9010\u9879\u4FDD\u7559\u8C03\u7528\u3001\u5931\u8D25\u3001\u91CD\u8BD5\u4E0E\u5B8C\u6210\u987A\u5E8F\u3002" }
    ],
    activity: [
      { id: "activity_pulse_dots", marker: "A", title: "\u4E09\u70B9\u8109\u51B2", description: "\u6700\u8F7B\u91CF\u7684\u6D3B\u52A8\u53CD\u9988\uFF0C\u7D27\u90BB assistant \u8EAB\u4EFD\u3002" },
      { id: "activity_breath_timer", marker: "B", title: "\u547C\u5438\u73AF\u8BA1\u65F6", description: "\u7528\u5F53\u524D\u9636\u6BB5\u4E0E\u7D2F\u8BA1\u7528\u65F6\u51CF\u5C11\u7B49\u5F85\u7684\u4E0D\u786E\u5B9A\u611F\u3002" },
      { id: "activity_stage_rail", marker: "C", title: "\u56DB\u9636\u6BB5\u8F68", description: "\u663E\u5F0F\u533A\u5206\u8FDE\u63A5\u3001\u601D\u8003\u3001\u5DE5\u5177\u4E0E\u56DE\u590D\u3002" }
    ],
    streaming: [
      { id: "streaming_direct_delta", marker: "A", title: "\u539F\u59CB\u589E\u91CF", description: "\u6309 provider \u6279\u6B21\u539F\u6837\u8FFD\u52A0\uFF0C\u4FDD\u7559\u771F\u5B9E\u7A81\u53D1\u611F\u3002" },
      { id: "streaming_smoothed_phrases", marker: "B", title: "\u77ED\u8BED\u5E73\u6ED1", description: "\u5C06\u6279\u6B21\u653E\u5165\u961F\u5217\uFF0C\u4EE5\u7A33\u5B9A\u8282\u62CD\u91CA\u653E\u3002" },
      { id: "streaming_block_commit", marker: "C", title: "\u5757\u7EA7\u63D0\u4EA4", description: "\u6D3B\u52A8\u6BB5\u4FDD\u6301\u7EAF\u6587\u672C\uFF0C\u5B8C\u6210\u540E\u518D\u63D0\u4EA4 Markdown\u3002" }
    ]
  };
  var BASELINE_VARIANTS = {
    thinking: "thinking_inline_disclosure",
    tools: "tools_compact_records",
    activity: "activity_pulse_dots",
    streaming: "streaming_direct_delta"
  };
  function answerEvents(startAtMs, intervalMs, chunks, blockId = "final-answer") {
    return chunks.map((text, index) => ({
      atMs: startAtMs + index * intervalMs,
      type: "answer",
      blockId,
      text
    }));
  }
  function progressEvents(startAtMs, intervalMs, blockId, chunks) {
    return chunks.map((text, index) => ({
      atMs: startAtMs + index * intervalMs,
      type: "progress",
      blockId,
      text
    }));
  }
  var scenarios = [
    {
      id: "multi_tool",
      label: "\u6B63\u5E38\u591A\u5DE5\u5177\u56DE\u590D",
      userText: "\u5E2E\u6211\u6838\u5BF9\u8FD9\u4E2A\u5224\u65AD\uFF0C\u5E76\u628A\u8BC1\u636E\u8BF4\u6E05\u695A\u3002",
      durationMs: 11800,
      events: [
        { atMs: 0, type: "stage", stage: "connecting" },
        { atMs: 650, type: "stage", stage: "thinking" },
        { atMs: 900, type: "reasoning", blockId: "reasoning-1", text: "\u5148\u62C6\u5F00\u95EE\u9898\u4E2D\u7684\u7ED3\u8BBA\u4E0E\u8BC1\u636E\u8981\u6C42\u3002" },
        { atMs: 1400, type: "reasoning", blockId: "reasoning-1", text: "\u73B0\u6709\u6458\u8981\u4E0D\u8DB3\u4EE5\u652F\u6491\u7CBE\u786E\u7ED3\u8BBA\uFF0C\u9700\u8981\u5148\u627E\u5019\u9009\u3002" },
        { atMs: 1600, type: "stage", stage: "progressing" },
        ...progressEvents(1650, 180, "progress-1", ["\u6211\u5148\u4ECE\u76F8\u5173\u8BB0\u5FC6\u91CC", "\u627E\u5B9A\u4F4D\u5019\u9009\u3002"]),
        { atMs: 2100, type: "stage", stage: "tool_running" },
        { atMs: 2100, type: "tool_start", callId: "call-search", toolId: "memory_search", args: 'query_terms=["\u7F13\u5B58\u547D\u4E2D", "\u6D4B\u8BD5\u7ED3\u8BBA"]' },
        { atMs: 2850, type: "tool_result", callId: "call-search", result: "\u627E\u5230 3 \u4E2A\u5B9A\u4F4D\u5019\u9009\uFF1B\u7247\u6BB5\u4E0D\u662F\u8BC1\u636E\u3002" },
        { atMs: 3e3, type: "stage", stage: "thinking" },
        { atMs: 3100, type: "reasoning", blockId: "reasoning-2", text: "\u5019\u9009\u6307\u5411\u540C\u4E00\u6761\u8BB0\u5FC6\uFF0C\u7EE7\u7EED\u8BFB\u53D6\u5B8C\u6574\u6B63\u6587\u3002" },
        { atMs: 3300, type: "stage", stage: "progressing" },
        ...progressEvents(3350, 180, "progress-2", ["\u6709\u4E09\u6761\u5019\u9009\uFF0C\u4F46\u7247\u6BB5\u4E0D\u662F\u8BC1\u636E\uFF1B", "\u6211\u7EE7\u7EED\u6253\u5F00\u5B8C\u6574\u6B63\u6587\u3002"]),
        { atMs: 3800, type: "stage", stage: "tool_running" },
        { atMs: 3800, type: "tool_start", callId: "call-read", toolId: "memory_content_read", args: "mem_id=MEM-7A10C2D4, mount_mode=temporary" },
        { atMs: 4650, type: "tool_result", callId: "call-read", result: "\u5DF2\u8BFB\u53D6\u5B8C\u6574\u6B63\u6587\uFF1B\u6765\u6E90\u5750\u6807\u4E3A meta / R000619\u3002" },
        { atMs: 4850, type: "stage", stage: "thinking" },
        { atMs: 5e3, type: "reasoning", blockId: "reasoning-3", text: "\u6B63\u6587\u7ED9\u51FA\u4E86\u8303\u56F4\u548C\u9650\u5B9A\uFF0C\u4F46\u65E5\u671F\u4ECD\u9700\u56DE\u67E5\u539F\u59CB\u8BED\u6599\u3002" },
        { atMs: 5250, type: "stage", stage: "progressing" },
        ...progressEvents(5300, 180, "progress-3", ["\u6B63\u6587\u8FD8\u7F3A\u7CBE\u786E\u65E5\u671F\uFF1B", "\u6211\u7EE7\u7EED\u56DE\u67E5\u521B\u5EFA\u5206\u8EAB\u7684\u539F\u59CB\u8F6E\u5BA1\u8BA1\u3002"]),
        { atMs: 5900, type: "stage", stage: "tool_running" },
        { atMs: 5900, type: "tool_start", callId: "call-grep", toolId: "file_grep", args: "root=persona://active, query=\u7F13\u5B58\u8BFB\u53D6\u6BD4\u4F8B" },
        { atMs: 6750, type: "tool_result", callId: "call-grep", result: "\u547D\u4E2D\u539F\u59CB\u8F6E\u5BA1\u8BA1 2 \u5904\uFF0C\u8986\u76D6\u5B8C\u6574\u3002" },
        { atMs: 7e3, type: "stage", stage: "answering" },
        ...answerEvents(7150, 520, [
          "\u53EF\u4EE5\u786E\u8BA4\uFF1A",
          "\u8FD9\u6B21\u63D0\u5347\u4E3B\u8981\u53D1\u751F\u5728\u8FDE\u7EED\u8C03\u7528\u7684\u7A33\u5B9A\u524D\u7F00\uFF0C",
          "\u5E76\u4E0D\u7B49\u4E8E\u6240\u6709\u573A\u666F\u90FD\u4F1A\u5F97\u5230\u540C\u6837\u7684\u547D\u4E2D\u7387\u3002\n\n",
          "\u8BC1\u636E\u94FE\u662F\uFF1A\u8BB0\u5FC6\u5B9A\u4F4D \u2192 \u5B8C\u6574\u6B63\u6587 \u2192 \u539F\u59CB\u8F6E\u5BA1\u8BA1\u3002"
        ]),
        { atMs: 9750, type: "complete" }
      ]
    },
    {
      id: "first_byte_wait",
      label: "\u9996\u5B57\u7B49\u5F85",
      userText: "\u5148\u4ED4\u7EC6\u60F3\u6E05\u695A\u518D\u56DE\u7B54\u3002",
      durationMs: 10800,
      events: [
        { atMs: 0, type: "stage", stage: "connecting" },
        { atMs: 3600, type: "stage", stage: "thinking" },
        { atMs: 4200, type: "reasoning", blockId: "reasoning-1", text: "\u6B63\u5728\u5EFA\u7ACB\u56DE\u7B54\u7ED3\u6784\uFF0C\u5E76\u6838\u5BF9\u662F\u5426\u9700\u8981\u5916\u90E8\u8BC1\u636E\u3002" },
        { atMs: 6600, type: "stage", stage: "answering" },
        ...answerEvents(6900, 650, ["\u6211\u5148\u7ED9\u7ED3\u8BBA\uFF1A", "\u5F53\u524D\u8BC1\u636E\u53EA\u652F\u6301\u6709\u9650\u5224\u65AD\uFF0C", "\u8FD8\u4E0D\u80FD\u628A\u5B83\u5916\u63A8\u6210\u4E00\u822C\u89C4\u5F8B\u3002"]),
        { atMs: 9300, type: "complete" }
      ]
    },
    {
      id: "no_reasoning",
      label: "\u6A21\u578B\u672A\u8FD4\u56DE reasoning",
      userText: "\u628A\u8FD9\u6BB5\u4FE1\u606F\u6574\u7406\u6210\u4E24\u70B9\u3002",
      durationMs: 7800,
      events: [
        { atMs: 0, type: "stage", stage: "connecting" },
        { atMs: 550, type: "stage", stage: "progressing" },
        ...progressEvents(650, 180, "progress-1", ["\u6211\u5148\u5C55\u5F00\u7D22\u5F15\uFF0C", "\u786E\u8BA4\u5DF2\u6709\u4FE1\u606F\u7684\u7ED3\u6784\u3002"]),
        { atMs: 1250, type: "stage", stage: "tool_running" },
        { atMs: 1250, type: "tool_start", callId: "call-index", toolId: "index_view", args: "scope=ltm_inverted, offset=0, limit=8" },
        { atMs: 2150, type: "tool_result", callId: "call-index", result: "\u8FD4\u56DE 8 \u4E2A\u7D22\u5F15\u9879\u3002" },
        { atMs: 2500, type: "stage", stage: "answering" },
        ...answerEvents(2650, 600, ["\u4E00\u662F\u4FDD\u7559\u4E8B\u5B9E\u8FB9\u754C\uFF1B", "\u4E8C\u662F\u628A\u5C1A\u672A\u6838\u9A8C\u7684\u5185\u5BB9\u660E\u786E\u6807\u6210\u5F85\u786E\u8BA4\u3002"]),
        { atMs: 5300, type: "complete" }
      ]
    },
    {
      id: "tool_retry",
      label: "\u5DE5\u5177\u5931\u8D25\u3001\u5BA1\u6279\u4E0E\u91CD\u8BD5",
      userText: "\u67E5\u4E00\u4E0B\u539F\u59CB\u6587\u4EF6\uFF0C\u5931\u8D25\u7684\u8BDD\u6362\u8DEF\u5F84\u7EE7\u7EED\u3002",
      durationMs: 13200,
      events: [
        { atMs: 0, type: "stage", stage: "thinking" },
        { atMs: 400, type: "reasoning", blockId: "reasoning-1", text: "\u5148\u5C1D\u8BD5\u6B63\u6587\u68C0\u7D22\uFF1B\u5982\u679C\u8986\u76D6\u4E0D\u5B8C\u6574\uFF0C\u518D\u7528\u53D7\u63A7\u547D\u4EE4\u6838\u9A8C\u3002" },
        { atMs: 750, type: "stage", stage: "progressing" },
        { atMs: 800, type: "progress", blockId: "progress-1", text: "\u6211\u5148\u4ECE\u539F\u59CB\u8BED\u6599\u505A\u4E00\u6B21\u5B57\u9762\u68C0\u7D22\u3002" },
        { atMs: 1300, type: "stage", stage: "tool_running" },
        { atMs: 1300, type: "tool_start", callId: "call-grep-fail", toolId: "file_grep", args: "root=persona://active, query=\u539F\u59CB\u7ED3\u8BBA" },
        { atMs: 2250, type: "tool_error", callId: "call-grep-fail", result: "\u76EE\u6807\u6587\u4EF6\u65E0\u6CD5\u89E3\u7801\uFF1B\u8986\u76D6\u4E0D\u5B8C\u6574\u3002" },
        { atMs: 2500, type: "stage", stage: "thinking" },
        { atMs: 2650, type: "reasoning", blockId: "reasoning-2", text: "\u96F6\u547D\u4E2D\u4E0D\u80FD\u5F53\u4F5C\u4E0D\u5B58\u5728\uFF1B\u6539\u67E5\u6587\u672C\u5BFC\u51FA\u76EE\u5F55\u3002" },
        { atMs: 2900, type: "stage", stage: "progressing" },
        ...progressEvents(2950, 160, "progress-2", ["\u7B2C\u4E00\u6B21\u68C0\u7D22\u8986\u76D6\u4E0D\u5B8C\u6574\uFF0C", "\u6211\u6362\u4E00\u6761\u9700\u8981\u5BA1\u6279\u7684\u8DEF\u5F84\u7EE7\u7EED\u3002"]),
        { atMs: 3350, type: "stage", stage: "tool_running" },
        { atMs: 3350, type: "tool_start", callId: "call-shell", toolId: "shell_command", args: "command=rg --text \u539F\u59CB\u7ED3\u8BBA export/, purpose=\u6838\u9A8C\u5386\u53F2\u6587\u672C" },
        { atMs: 4e3, type: "stage", stage: "tool_approval" },
        { atMs: 4e3, type: "tool_approval", callId: "call-shell", message: "\u7B49\u5F85\u7528\u6237\u5141\u8BB8\u672C\u6B21\u6267\u884C\u3002" },
        { atMs: 5800, type: "stage", stage: "tool_running" },
        { atMs: 6500, type: "tool_result", callId: "call-shell", result: "\u9000\u51FA\u7801 0\uFF1B\u547D\u4E2D 2 \u884C\u5E76\u8FD4\u56DE\u6765\u6E90\u8DEF\u5F84\u3002" },
        { atMs: 6800, type: "stage", stage: "answering" },
        ...answerEvents(7e3, 600, ["\u7B2C\u4E00\u6B21\u68C0\u7D22\u8986\u76D6\u4E0D\u5B8C\u6574\uFF0C", "\u5207\u6362\u5230\u5DF2\u5BA1\u6279\u7684\u6587\u672C\u6838\u9A8C\u540E\u627E\u5230\u4E86\u4E24\u5904\u6765\u6E90\u3002", "\u6240\u4EE5\u7ED3\u8BBA\u5E94\u4EE5\u7B2C\u4E8C\u6B21\u7ED3\u679C\u4E3A\u51C6\u3002"]),
        { atMs: 10700, type: "complete" }
      ]
    },
    {
      id: "user_stop",
      label: "\u7528\u6237\u4E3B\u52A8\u505C\u6B62",
      userText: "\u5148\u67E5\u8D44\u6599\uFF1B\u5982\u679C\u6211\u505C\u6B62\uFF0C\u5C31\u522B\u7EE7\u7EED\u3002",
      durationMs: 8400,
      events: [
        { atMs: 0, type: "stage", stage: "thinking" },
        { atMs: 500, type: "reasoning", blockId: "reasoning-1", text: "\u9700\u8981\u8BFB\u53D6\u4E00\u4EFD\u8F83\u957F\u8D44\u6599\uFF0C\u518D\u6C47\u603B\u7ED3\u8BBA\u3002" },
        { atMs: 850, type: "stage", stage: "progressing" },
        { atMs: 900, type: "progress", blockId: "progress-1", text: "\u6211\u5148\u8BFB\u53D6\u8FD9\u4EFD\u957F\u8D44\u6599\u3002" },
        { atMs: 1400, type: "stage", stage: "tool_running" },
        { atMs: 1400, type: "tool_start", callId: "call-long-read", toolId: "file_read", args: "path=persona://active/files/raw/long-note.md" },
        { atMs: 3100, type: "tool_result", callId: "call-long-read", result: "\u5DF2\u8FD4\u56DE\u7B2C\u4E00\u7A97\u53E3\uFF1B\u6B63\u6587\u5C1A\u672A\u8BFB\u5B8C\u3002" },
        { atMs: 3300, type: "stage", stage: "progressing" },
        { atMs: 3450, type: "progress", blockId: "progress-2", text: "\u6211\u5DF2\u7ECF\u8BFB\u5230\u524D\u534A\u90E8\u5206\uFF0C\u521D\u6B65\u770B\u2014\u2014" },
        { atMs: 4450, type: "stopped" }
      ]
    },
    {
      id: "long_markdown",
      label: "\u957F Markdown \u56DE\u590D",
      userText: "\u7528\u5217\u8868\u3001\u4EE3\u7801\u548C\u8868\u683C\u7ED9\u6211\u4E00\u4E2A\u5B8C\u6574\u793A\u4F8B\u3002",
      durationMs: 17200,
      events: [
        { atMs: 0, type: "stage", stage: "thinking" },
        { atMs: 500, type: "reasoning", blockId: "reasoning-1", text: "\u6309\u7ED3\u8BBA\u3001\u6267\u884C\u6B65\u9AA4\u3001\u793A\u4F8B\u4EE3\u7801\u548C\u5BF9\u7167\u8868\u7EC4\u7EC7\u3002" },
        { atMs: 1500, type: "stage", stage: "answering" },
        ...answerEvents(1700, 720, [
          "### \u5EFA\u8BAE\u65B9\u6848\n\n",
          "\u5148\u628A\u72B6\u6001\u4E0E\u6B63\u6587\u5206\u5F00\uFF1A\n\n",
          "- \u601D\u8003\u8FC7\u7A0B\u72EC\u7ACB\u6298\u53E0\n- \u5DE5\u5177\u8C03\u7528\u9010\u9879\u663E\u793A\n- \u56DE\u590D\u6B63\u6587\u5355\u72EC\u6D41\u5165\n\n",
          '```ts\nconst state = { phase: "answering" };\nrenderFrame(state);\n```\n\n',
          "| \u9636\u6BB5 | \u5BF9\u7528\u6237\u53EF\u89C1 |\n| --- | --- |\n| \u601D\u8003 | \u53EF\u6298\u53E0 |\n| \u5DE5\u5177 | \u9010\u9879\u5361\u7247 |\n| \u56DE\u590D | \u6D41\u5F0F\u6B63\u6587 |\n\n",
          "\u8FD9\u6837\u65E2\u4FDD\u7559\u900F\u660E\u5EA6\uFF0C\u4E5F\u4E0D\u4F1A\u628A\u5BA1\u8BA1\u7ED3\u6784\u76F4\u63A5\u503E\u5012\u8FDB\u804A\u5929\u7A97\u53E3\u3002"
        ]),
        { atMs: 12e3, type: "complete" }
      ]
    }
  ];
  var scenarioById = new Map(scenarios.map((scenario) => [scenario.id, scenario]));
  var stageLabels = {
    idle: "\u7B49\u5F85\u64AD\u653E",
    connecting: "\u6B63\u5728\u8FDE\u63A5\u6A21\u578B",
    thinking: "\u6B63\u5728\u601D\u8003",
    progressing: "\u6B63\u5728\u8F93\u51FA\u8F6E\u4E2D\u8FDB\u5C55",
    tool_running: "\u6B63\u5728\u6267\u884C\u5DE5\u5177",
    tool_approval: "\u7B49\u5F85\u5DE5\u5177\u5BA1\u6279",
    answering: "\u6B63\u5728\u7EC4\u7EC7\u56DE\u590D",
    completed: "\u672C\u6B21\u6F14\u793A\u5DF2\u5B8C\u6210",
    stopped: "\u5DF2\u6309\u7528\u6237\u8981\u6C42\u505C\u6B62"
  };
  function requiredElement(id) {
    const node = document.getElementById(id);
    if (!(node instanceof HTMLElement)) throw new Error("missing showcase element: ".concat(id));
    return node;
  }
  function createElement(tagName, className = "", text = "") {
    const node = document.createElement(tagName);
    if (className) node.className = className;
    if (text) node.textContent = text;
    return node;
  }
  function formatSeconds(milliseconds) {
    return "".concat((Math.max(0, milliseconds) / 1e3).toFixed(1), " \u79D2");
  }
  function normalizeSpeed(value) {
    const speed = Number(value);
    return [0.5, 1, 2].includes(speed) ? speed : 1;
  }
  function isScenarioId(value) {
    return typeof value === "string" && scenarioById.has(value);
  }
  function validVariant(component, value) {
    return typeof value === "string" && variants[component].some((variant) => variant.id === value);
  }
  function deriveSnapshot(scenario, elapsedMs2) {
    let stage = elapsedMs2 > 0 ? "connecting" : "idle";
    let terminalMessage = "";
    const reasoningOrder = [];
    const reasoningMap = /* @__PURE__ */ new Map();
    const textOrder = [];
    const textMap = /* @__PURE__ */ new Map();
    const toolMap = /* @__PURE__ */ new Map();
    const timelineRefs = [];
    const timelineKeys = /* @__PURE__ */ new Set();
    const appendTimeline = (kind, id, atMs) => {
      const key = "".concat(kind, ":").concat(id);
      if (timelineKeys.has(key)) return;
      timelineKeys.add(key);
      timelineRefs.push({ kind, id, atMs });
    };
    for (const event of scenario.events) {
      if (elapsedMs2 <= 0 || event.atMs > elapsedMs2) break;
      if (event.type === "stage") {
        stage = event.stage;
      } else if (event.type === "reasoning") {
        let segment = reasoningMap.get(event.blockId);
        if (!segment) {
          reasoningOrder.push(event.blockId);
          segment = {
            id: event.blockId,
            label: "\u601D\u8003\u7247\u6BB5 ".concat(reasoningOrder.length),
            text: "",
            startedAtMs: event.atMs,
            endedAtMs: null,
            active: false
          };
          reasoningMap.set(event.blockId, segment);
          appendTimeline("reasoning", event.blockId, event.atMs);
        }
        segment.text += event.text;
      } else if (event.type === "progress" || event.type === "answer") {
        const key = "".concat(event.type, ":").concat(event.blockId);
        let segment = textMap.get(key);
        if (!segment) {
          textOrder.push(key);
          segment = {
            id: key,
            kind: event.type,
            text: "",
            active: false
          };
          textMap.set(key, segment);
          appendTimeline(event.type, key, event.atMs);
        }
        segment.text += event.text;
      } else if (event.type === "tool_start") {
        toolMap.set(event.callId, {
          callId: event.callId,
          toolId: event.toolId,
          args: event.args,
          result: "",
          state: "running",
          startedAtMs: event.atMs,
          endedAtMs: null
        });
        appendTimeline("tool", event.callId, event.atMs);
      } else if (event.type === "tool_approval") {
        const tool = toolMap.get(event.callId);
        if (tool) {
          tool.state = "approval";
          tool.result = event.message;
        }
      } else if (event.type === "tool_result" || event.type === "tool_error") {
        const tool = toolMap.get(event.callId);
        if (tool) {
          tool.state = event.type === "tool_result" ? "succeeded" : "failed";
          tool.result = event.result;
          tool.endedAtMs = event.atMs;
        }
      } else if (event.type === "complete") {
        stage = "completed";
      } else if (event.type === "stopped") {
        stage = "stopped";
        terminalMessage = "\u7528\u6237\u5DF2\u505C\u6B62\u672C\u8F6E\uFF1B\u53EA\u4FDD\u7559\u505C\u6B62\u524D\u5DF2\u7ECF\u663E\u793A\u7684\u5185\u5BB9\u3002";
      }
    }
    if (stage === "stopped") {
      toolMap.forEach((tool) => {
        if (["running", "approval"].includes(tool.state)) {
          tool.state = "stopped";
          tool.endedAtMs = elapsedMs2;
          tool.result = terminalMessage || "\u5904\u7406\u5DF2\u505C\u6B62\u3002";
        }
      });
    }
    const terminal = stage === "completed" || stage === "stopped";
    timelineRefs.forEach((ref, index) => {
      const next = timelineRefs[index + 1];
      const end = next?.atMs ?? (terminal ? elapsedMs2 : null);
      const isLast = index === timelineRefs.length - 1;
      if (ref.kind === "reasoning") {
        const segment = reasoningMap.get(ref.id);
        if (segment) {
          segment.endedAtMs = end;
          segment.active = isLast && stage === "thinking";
        }
      } else if (ref.kind === "progress" || ref.kind === "answer") {
        const segment = textMap.get(ref.id);
        if (segment) {
          segment.active = isLast && stage === (ref.kind === "progress" ? "progressing" : "answering");
        }
      }
    });
    const reasoningSegments = reasoningOrder.map((id) => reasoningMap.get(id)).filter((value) => Boolean(value));
    const textSegments = textOrder.map((id) => textMap.get(id)).filter((value) => Boolean(value));
    const timeline = [];
    timelineRefs.forEach((ref) => {
      if (ref.kind === "reasoning") {
        const value = reasoningMap.get(ref.id);
        if (value) timeline.push({ kind: "reasoning", id: ref.id, value });
      } else if (ref.kind === "tool") {
        const value = toolMap.get(ref.id);
        if (value) timeline.push({ kind: "tool", id: ref.id, value });
      } else {
        const value = textMap.get(ref.id);
        if (value) timeline.push({ kind: ref.kind, id: ref.id, value });
      }
    });
    return {
      elapsedMs: elapsedMs2,
      stage,
      reasoningSegments,
      tools: [...toolMap.values()],
      textSegments,
      timeline,
      terminalMessage
    };
  }
  var scenarioSelect = requiredElement("scenarioSelect");
  var playButton = requiredElement("playButton");
  var pauseButton = requiredElement("pauseButton");
  var resetButton = requiredElement("resetButton");
  var timelineStatus = requiredElement("timelineStatus");
  var timelineProgress = requiredElement("timelineProgress");
  var thinkingVariants = requiredElement("thinkingVariants");
  var toolVariants = requiredElement("toolVariants");
  var activityVariants = requiredElement("activityVariants");
  var streamingVariants = requiredElement("streamingVariants");
  var combinationMissing = requiredElement("combinationMissing");
  var combinationPreview = requiredElement("combinationPreview");
  var selectionSummary = requiredElement("selectionSummary");
  var selectionFeedback = requiredElement("selectionFeedback");
  var clearSelectionButton = requiredElement("clearSelectionButton");
  var copySelectionButton = requiredElement("copySelectionButton");
  var exportSelectionButton = requiredElement("exportSelectionButton");
  var focusedSurfaces = {
    thinking: /* @__PURE__ */ new Map(),
    tools: /* @__PURE__ */ new Map(),
    activity: /* @__PURE__ */ new Map(),
    streaming: /* @__PURE__ */ new Map()
  };
  var combinationSurface = null;
  var selections = {};
  var currentScenarioId = "multi_tool";
  var playbackSpeed = 1;
  var elapsedMs = 0;
  var playing = false;
  var animationFrame = 0;
  var previousTimestamp = null;
  var smoothedText = /* @__PURE__ */ new Map();
  var activeTab = "thinking";
  function currentScenario() {
    return scenarioById.get(currentScenarioId) || scenarios[0];
  }
  function readStoredState() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return null;
      const data = JSON.parse(raw);
      if (data.schema_version !== STATE_SCHEMA || !isScenarioId(data.scenario)) return null;
      const safeSelections = {};
      COMPONENTS.forEach((component) => {
        const candidate = data.selections?.[component];
        if (validVariant(component, candidate)) safeSelections[component] = candidate;
      });
      return {
        schema_version: STATE_SCHEMA,
        scenario: data.scenario,
        speed: normalizeSpeed(data.speed),
        selections: safeSelections
      };
    } catch {
      return null;
    }
  }
  function persistState() {
    const state = {
      schema_version: STATE_SCHEMA,
      scenario: currentScenarioId,
      speed: playbackSpeed,
      selections
    };
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    } catch {
      selectionFeedback.textContent = "\u6D4F\u89C8\u5668\u672A\u5141\u8BB8\u4FDD\u5B58\u672C\u5730\u9009\u578B\uFF1B\u672C\u9875\u4ECD\u53EF\u7EE7\u7EED\u6BD4\u8F83\u3002";
    }
  }
  function selectionPayload() {
    if (!COMPONENTS.every((component) => validVariant(component, selections[component]))) return null;
    return {
      schema_version: SELECTION_SCHEMA,
      thinking: selections.thinking || "",
      tools: selections.tools || "",
      activity: selections.activity || "",
      streaming: selections.streaming || ""
    };
  }
  function selectedDefinition(component) {
    return variants[component].find((variant) => variant.id === selections[component]) || null;
  }
  function readableSelection() {
    const chosen = COMPONENTS.map((component) => {
      const definition = selectedDefinition(component);
      return definition ? "".concat(componentLabels[component], "\uFF1A").concat(definition.marker, "\uFF5C").concat(definition.title) : "";
    }).filter(Boolean);
    return chosen.length ? chosen.join("\uFF1B") : "\u5C1A\u672A\u9009\u62E9\u4EFB\u4F55\u65B9\u6848\u3002";
  }
  function buildMiniThread(root) {
    const thread = createElement("div", "mini-thread");
    const user = createElement("div", "mini-user");
    user.dataset.showcaseUser = "true";
    thread.append(user);
    const assistant = createElement("div", "mini-assistant");
    assistant.append(createElement("div", "mini-assistant-label", "UPSP \xB7 ASSISTANT"));
    const timeline = createElement("div", "round-event-stream");
    timeline.setAttribute("aria-label", "\u672C\u8F6E\u53EF\u89C2\u5BDF\u4E8B\u4EF6\u65F6\u95F4\u7EBF");
    const activity = createElement("div", "activity-mount round-activity-tail");
    assistant.append(timeline, activity);
    thread.append(assistant);
    root.append(thread);
    return { timeline, activity };
  }
  function buildVariantCard(component, definition) {
    const card = createElement("article", "variant-card");
    card.dataset.component = component;
    card.dataset.variantId = definition.id;
    const header = createElement("header");
    header.append(createElement("span", "", "\u65B9\u6848 ".concat(definition.marker)));
    header.append(createElement("h3", "", definition.title));
    header.append(createElement("p", "", definition.description));
    const preview = createElement("div", "variant-preview");
    focusedSurfaces[component].set(definition.id, buildMiniThread(preview));
    const footer = createElement("footer");
    const select = createElement("button", "variant-select", "\u9009\u62E9\u6B64\u65B9\u6848");
    select.type = "button";
    select.dataset.selectComponent = component;
    select.dataset.selectVariant = definition.id;
    select.setAttribute("aria-pressed", "false");
    footer.append(select);
    card.append(header, preview, footer);
    return card;
  }
  function buildVariantGrids() {
    const roots = {
      thinking: thinkingVariants,
      tools: toolVariants,
      activity: activityVariants,
      streaming: streamingVariants
    };
    COMPONENTS.forEach((component) => {
      roots[component].replaceChildren(...variants[component].map((definition) => buildVariantCard(component, definition)));
    });
  }
  function updateUserText() {
    document.querySelectorAll("[data-showcase-user]").forEach((node) => {
      node.textContent = currentScenario().userText;
    });
  }
  function disclosureOpen(toggle, automatic) {
    if (toggle.dataset.manual === "true") return toggle.getAttribute("aria-expanded") === "true";
    return automatic;
  }
  function createThinkingUnit(variantId, segmentId) {
    const variantClass = variantId === "thinking_auto_card" ? "variant-b" : variantId === "thinking_phase_timeline" ? "variant-c" : "";
    const unit = createElement("section", "thinking-unit ".concat(variantClass));
    unit.dataset.thinkingVariant = variantId;
    unit.dataset.reasoningId = segmentId;
    const toggle = createElement("button", "thinking-toggle");
    toggle.type = "button";
    toggle.setAttribute("aria-expanded", "false");
    toggle.append(createElement("strong", "", "\u601D\u8003\u7247\u6BB5"));
    toggle.append(createElement("span", "thinking-meta", "\u7B49\u5F85 reasoning"));
    const content = createElement("p", "thinking-content");
    content.hidden = true;
    toggle.addEventListener("click", () => {
      const open = toggle.getAttribute("aria-expanded") !== "true";
      toggle.dataset.manual = "true";
      toggle.setAttribute("aria-expanded", String(open));
      content.classList.remove("is-preview");
      content.hidden = !open;
    });
    unit.append(toggle, content);
    return unit;
  }
  function renderThinkingUnit(unit, variantId, segment, snapshot) {
    const toggle = unit.querySelector(".thinking-toggle");
    const meta = unit.querySelector(".thinking-meta");
    const title = unit.querySelector(".thinking-toggle strong");
    const content = unit.querySelector(".thinking-content");
    if (!toggle || !meta || !content) return;
    const automaticOpen = variantId !== "thinking_inline_disclosure" && segment.active;
    const expanded = disclosureOpen(toggle, automaticOpen);
    const preview = variantId === "thinking_auto_card" && !segment.active && toggle.dataset.manual !== "true";
    toggle.setAttribute("aria-expanded", String(expanded));
    content.hidden = !expanded && !preview;
    content.classList.toggle("is-preview", preview);
    const end = segment.endedAtMs ?? snapshot.elapsedMs;
    if (title) title.textContent = segment.label;
    const stateLabel = segment.active ? "\u601D\u8003\u4E2D" : preview ? "\u4E24\u884C\u9884\u89C8" : "\u5DF2\u7ED3\u675F";
    meta.textContent = "".concat(stateLabel, " \xB7 ").concat(formatSeconds(end - segment.startedAtMs));
    content.textContent = segment.text;
    unit.classList.toggle("is-active", segment.active);
    unit.dataset.state = segment.active ? "active" : "settled";
  }
  function renderThinking(root, variantId, snapshot, segments = snapshot.reasoningSegments) {
    root.classList.add("thinking-mount", "thinking-list");
    root.classList.toggle("thinking-timeline", variantId === "thinking_phase_timeline");
    segments.forEach((segment) => {
      let unit = [...root.querySelectorAll("[data-reasoning-id]")].find((candidate) => candidate.dataset.reasoningId === segment.id);
      if (!unit || unit.dataset.thinkingVariant !== variantId) {
        unit?.remove();
        unit = createThinkingUnit(variantId, segment.id);
        root.append(unit);
      }
      renderThinkingUnit(unit, variantId, segment, snapshot);
    });
    const activeIds = new Set(segments.map((segment) => segment.id));
    root.querySelectorAll("[data-reasoning-id]").forEach((unit) => {
      if (!activeIds.has(unit.dataset.reasoningId || "")) unit.remove();
    });
    if (!segments.length) {
      let empty = root.querySelector(".runtime-empty-copy");
      if (!empty) {
        empty = createElement("p", "runtime-empty-copy");
        root.append(empty);
      }
      empty.textContent = snapshot.stage === "completed" || snapshot.stage === "stopped" ? "Provider \u672A\u8FD4\u56DE reasoning\uFF1B\u672A\u751F\u6210\u601D\u8003\u8282\u70B9\u3002" : "\u5F53\u524D\u5C1A\u672A\u6536\u5230 reasoning\uFF1B\u4E0D\u751F\u6210\u5360\u4F4D\u601D\u8003\u8282\u70B9\u3002";
    }
    if (segments.length) root.querySelector(".runtime-empty-copy")?.remove();
  }
  function toolStateLabel(tool, elapsedMsValue) {
    const labels = {
      running: "\u6267\u884C\u4E2D",
      approval: "\u7B49\u5F85\u5BA1\u6279",
      succeeded: "\u5DF2\u5B8C\u6210",
      failed: "\u6267\u884C\u5931\u8D25",
      stopped: "\u5DF2\u505C\u6B62"
    };
    const end = tool.endedAtMs ?? elapsedMsValue;
    return "".concat(labels[tool.state], " \xB7 ").concat(formatSeconds(end - tool.startedAtMs));
  }
  function createToolItem(variantId, tool) {
    if (variantId === "tools_execution_cards") {
      const card = createElement("article", "tool-item tool-card-b");
      card.dataset.callId = tool.callId;
      const header = createElement("header");
      header.append(createElement("code"), createElement("span", "tool-state"));
      const detail2 = createElement("div", "tool-detail");
      const call2 = createElement("div");
      call2.append(createElement("b", "", "\u8C03\u7528\u53C2\u6570"), createElement("pre", "tool-args"));
      const result2 = createElement("div");
      result2.append(createElement("b", "", "\u6267\u884C\u7ED3\u679C"), createElement("p", "tool-result"));
      detail2.append(call2, result2);
      card.append(header, detail2);
      return card;
    }
    const details = createElement("details", "tool-item");
    details.dataset.callId = tool.callId;
    const summary = createElement("summary");
    summary.append(createElement("strong"), createElement("span", "tool-state"));
    const detail = createElement("div", "tool-detail");
    const call = createElement("div");
    call.append(createElement("b", "", "\u8C03\u7528\u53C2\u6570"), createElement("pre", "tool-args"));
    const result = createElement("div");
    result.append(createElement("b", "", "\u6267\u884C\u7ED3\u679C"), createElement("p", "tool-result"));
    detail.append(call, result);
    details.append(summary, detail);
    return details;
  }
  function renderToolNode(root, variantId, tool, snapshot) {
    let item = root.querySelector("[data-call-id]");
    if (!item || item.dataset.callId !== tool.callId || item.dataset.toolVariant !== variantId) {
      item?.remove();
      item = createToolItem(variantId, tool);
      item.dataset.toolVariant = variantId;
      root.append(item);
    }
    item.dataset.state = tool.state;
    const name = item.querySelector("strong, code");
    const state = item.querySelector(".tool-state");
    const args = item.querySelector(".tool-args");
    const result = item.querySelector(".tool-result");
    if (name) name.textContent = tool.toolId;
    if (state) state.textContent = toolStateLabel(tool, snapshot.elapsedMs);
    if (args) args.textContent = tool.args;
    if (result) result.textContent = tool.result || "\u7B49\u5F85\u8FD4\u56DE\u2026\u2026";
  }
  function createActivitySurface(variantId) {
    const surface = createElement("div", "activity-surface");
    surface.dataset.activityVariant = variantId;
    if (variantId === "activity_pulse_dots") {
      const dots = createElement("div", "pulse-dots");
      dots.setAttribute("aria-hidden", "true");
      dots.append(createElement("i"), createElement("i"), createElement("i"));
      surface.append(dots, createElement("div", "activity-copy"));
    } else if (variantId === "activity_breath_timer") {
      surface.append(createElement("div", "breath-ring"), createElement("div", "activity-copy"));
    } else {
      const rail = createElement("div", "stage-rail");
      [
        ["connecting", "\u8FDE\u63A5\u6A21\u578B"],
        ["thinking", "\u601D\u8003"],
        ["tool_running", "\u6267\u884C\u5DE5\u5177"],
        ["answering", "\u7EC4\u7EC7\u56DE\u590D"]
      ].forEach(([stage, label]) => {
        const row = createElement("div", "stage-node");
        row.dataset.processStage = stage;
        row.append(createElement("i"), createElement("span", "", label), createElement("em", "", "\u7B49\u5F85"));
        rail.append(row);
      });
      surface.append(rail);
    }
    return surface;
  }
  function stageRank(stage) {
    if (stage === "connecting" || stage === "idle") return 0;
    if (stage === "thinking") return 1;
    if (stage === "tool_running" || stage === "tool_approval") return 2;
    if (stage === "progressing") return 3;
    return 3;
  }
  function renderActivity(root, variantId, snapshot) {
    let surface = root.querySelector("[data-activity-variant]");
    if (!surface || surface.dataset.activityVariant !== variantId) {
      root.replaceChildren(createActivitySurface(variantId));
      surface = root.querySelector("[data-activity-variant]");
    }
    if (!surface) return;
    const label = stageLabels[snapshot.stage];
    surface.classList.toggle("is-active", !["idle", "completed", "stopped"].includes(snapshot.stage));
    if (variantId === "activity_pulse_dots") {
      const copy = surface.querySelector(".activity-copy");
      if (copy) copy.textContent = label;
    } else if (variantId === "activity_breath_timer") {
      const ring = surface.querySelector(".breath-ring");
      const copy = surface.querySelector(".activity-copy");
      if (ring) ring.textContent = "".concat((snapshot.elapsedMs / 1e3).toFixed(0), "s");
      if (copy) copy.textContent = label;
    } else {
      const currentRank = stageRank(snapshot.stage);
      surface.querySelectorAll("[data-process-stage]").forEach((node, index) => {
        let state = index < currentRank ? "done" : index === currentRank ? "active" : "waiting";
        if (snapshot.stage === "idle") state = "waiting";
        if (["completed", "stopped"].includes(snapshot.stage)) state = snapshot.stage === "completed" ? "done" : index < currentRank ? "done" : "waiting";
        node.dataset.stageState = state;
        const status = node.querySelector("em");
        if (status) status.textContent = state === "done" ? "\u5B8C\u6210" : state === "active" ? label : "\u7B49\u5F85";
      });
    }
    surface.setAttribute("aria-label", "".concat(label, "\uFF0C\u5DF2\u7528 ").concat(formatSeconds(snapshot.elapsedMs)));
    surface.setAttribute("role", "status");
  }
  function createStreamingSurface(variantId) {
    if (variantId === "streaming_block_commit") {
      const surface = createElement("section", "reply-object stream-output block-stream-output");
      surface.dataset.streamingVariant = variantId;
      surface.append(createElement("div", "committed-blocks"), createElement("div", "active-stream-block"));
      return surface;
    }
    const output = createElement("section", "reply-object stream-output is-empty");
    output.dataset.streamingVariant = variantId;
    return output;
  }
  function splitStreamBlocks(text, terminal) {
    if (!text) return { complete: [], active: "" };
    if (terminal) return { complete: text.split(/\n\n+/).filter(Boolean), active: "" };
    const boundary = text.lastIndexOf("\n\n");
    if (boundary < 0) return { complete: [], active: text };
    return {
      complete: text.slice(0, boundary).split(/\n\n+/).filter(Boolean),
      active: text.slice(boundary + 2)
    };
  }
  function appendCommittedBlock(root, block) {
    const trimmed = block.trim();
    if (!trimmed) return;
    if (trimmed.startsWith("```") && trimmed.endsWith("```")) {
      const lines2 = trimmed.split("\n");
      const pre = createElement("pre");
      const code = createElement("code", "", lines2.slice(1, -1).join("\n"));
      pre.append(code);
      root.append(pre);
      return;
    }
    if (trimmed.startsWith("### ")) {
      root.append(createElement("h3", "", trimmed.slice(4)));
      return;
    }
    if (trimmed.split("\n").every((line) => line.startsWith("- "))) {
      const list = createElement("ul");
      trimmed.split("\n").forEach((line) => list.append(createElement("li", "", line.slice(2))));
      root.append(list);
      return;
    }
    const lines = trimmed.split("\n");
    if (lines.length >= 3 && lines[0].includes("|") && /^\|?\s*---/.test(lines[1])) {
      const table = createElement("table");
      const splitRow = (line) => line.replace(/^\||\|$/g, "").split("|").map((cell) => cell.trim());
      const thead = createElement("thead");
      const headRow = createElement("tr");
      splitRow(lines[0]).forEach((cell) => headRow.append(createElement("th", "", cell)));
      thead.append(headRow);
      const tbody = createElement("tbody");
      lines.slice(2).forEach((line) => {
        const row = createElement("tr");
        splitRow(line).forEach((cell) => row.append(createElement("td", "", cell)));
        tbody.append(row);
      });
      table.append(thead, tbody);
      root.append(table);
      return;
    }
    root.append(createElement("p", "", trimmed));
  }
  function createTextEvent(segment) {
    const event = createElement("section", "text-event text-event-".concat(segment.kind));
    event.dataset.textId = segment.id;
    const header = createElement("header");
    header.append(
      createElement("strong", "", segment.kind === "progress" ? "\u8F6E\u4E2D\u8FDB\u5C55" : "\u6700\u7EC8\u56DE\u590D"),
      createElement("span", "text-event-state")
    );
    event.append(header, createElement("div", "text-event-mount"));
    return event;
  }
  function renderTextNode(root, variantId, segment) {
    let event = root.querySelector("[data-text-id]");
    if (!event || event.dataset.textId !== segment.id) {
      event?.remove();
      event = createTextEvent(segment);
      root.append(event);
    }
    event.className = "text-event text-event-".concat(segment.kind);
    event.dataset.state = segment.active ? "streaming" : "settled";
    const state = event.querySelector(".text-event-state");
    const mount = event.querySelector(".text-event-mount");
    if (!mount) return;
    if (state) state.textContent = segment.active ? "\u8F93\u51FA\u4E2D" : "\u5DF2\u7ED3\u7B97";
    let surface = mount.querySelector("[data-streaming-variant]");
    if (!surface || surface.dataset.streamingVariant !== variantId) {
      mount.replaceChildren(createStreamingSurface(variantId));
      surface = mount.querySelector("[data-streaming-variant]");
    }
    if (!surface) return;
    const terminal = !segment.active;
    const text = variantId === "streaming_smoothed_phrases" ? smoothedText.get(segment.id) || "" : segment.text;
    if (variantId === "streaming_block_commit") {
      const blocksRoot = surface.querySelector(".committed-blocks");
      const active = surface.querySelector(".active-stream-block");
      if (!blocksRoot || !active) return;
      const blocks = splitStreamBlocks(text, terminal);
      const signature = JSON.stringify(blocks.complete);
      if (blocksRoot.dataset.signature !== signature) {
        blocksRoot.replaceChildren();
        blocks.complete.forEach((block) => appendCommittedBlock(blocksRoot, block));
        blocksRoot.dataset.signature = signature;
      }
      active.textContent = blocks.active;
      active.hidden = !blocks.active;
      active.classList.toggle("is-streaming", !terminal && Boolean(blocks.active));
    } else {
      surface.textContent = text || "\u7B49\u5F85\u6587\u672C\u2026\u2026";
      surface.classList.toggle("is-empty", !text);
      surface.classList.toggle("is-streaming", segment.active && Boolean(text));
    }
    event.setAttribute("aria-label", "".concat(segment.kind === "progress" ? "\u8F6E\u4E2D\u8FDB\u5C55" : "\u6700\u7EC8\u56DE\u590D", "\uFF0C").concat(segment.active ? "\u8F93\u51FA\u4E2D" : "\u5DF2\u7ED3\u7B97"));
  }
  function renderStreaming(root, variantId, snapshot, segments = snapshot.textSegments, includeTerminal = true) {
    root.classList.add("streaming-mount", "text-event-list");
    segments.forEach((segment) => {
      let mount = [...root.querySelectorAll("[data-text-mount]")].find((candidate) => candidate.dataset.textMount === segment.id);
      if (!mount) {
        mount = createElement("div", "text-node-mount");
        mount.dataset.textMount = segment.id;
        root.append(mount);
      }
      renderTextNode(mount, variantId, segment);
    });
    const activeIds = new Set(segments.map((segment) => segment.id));
    root.querySelectorAll("[data-text-mount]").forEach((node) => {
      if (!activeIds.has(node.dataset.textMount || "")) node.remove();
    });
    if (!segments.length && !root.querySelector(".runtime-empty-copy")) {
      root.append(createElement("p", "runtime-empty-copy", "\u7B49\u5F85\u8F6E\u4E2D\u8FDB\u5C55\u6216\u6700\u7EC8\u56DE\u590D\u2026\u2026"));
    }
    if (segments.length) root.querySelector(".runtime-empty-copy")?.remove();
    let terminal = root.querySelector(".stream-terminal-note");
    if (includeTerminal && snapshot.terminalMessage) {
      if (!terminal) {
        terminal = createElement("p", "stream-terminal-note");
        root.append(terminal);
      }
      terminal.textContent = snapshot.terminalMessage;
    } else {
      terminal?.remove();
    }
  }
  function resetDynamicSurfaces() {
    document.querySelectorAll("[data-reasoning-id]").forEach((node) => node.remove());
    document.querySelectorAll("[data-text-mount]").forEach((node) => node.remove());
    document.querySelectorAll("[data-round-event-key]").forEach((node) => node.remove());
    document.querySelectorAll("[data-activity-variant]").forEach((node) => node.remove());
    document.querySelectorAll(".runtime-empty-copy").forEach((node) => node.remove());
    document.querySelectorAll(".stream-terminal-note").forEach((node) => node.remove());
  }
  function buildCombinationPreview() {
    combinationSurface = null;
    const missing = COMPONENTS.filter((component) => !validVariant(component, selections[component]));
    if (missing.length) {
      combinationMissing.hidden = false;
      combinationPreview.hidden = true;
      combinationPreview.replaceChildren();
      combinationMissing.textContent = "\u8FD8\u9700\u9009\u62E9\uFF1A".concat(missing.map((component) => componentLabels[component]).join("\u3001"), "\u3002");
      return;
    }
    combinationMissing.hidden = true;
    combinationPreview.hidden = false;
    combinationPreview.replaceChildren();
    combinationSurface = buildMiniThread(combinationPreview);
    updateUserText();
  }
  function createRoundEventNode(item) {
    const node = createElement("section", "round-event-node round-event-".concat(item.kind));
    node.dataset.roundEventKey = "".concat(item.kind, ":").concat(item.id);
    node.dataset.eventKind = item.kind;
    node.append(createElement("div", "round-event-marker"), createElement("div", "round-event-mount"));
    return node;
  }
  function renderRoundStream(surface, snapshot, variantSet) {
    const { timeline, activity } = surface;
    const useTimeline = variantSet.thinking === "thinking_phase_timeline" || variantSet.tools === "tools_execution_timeline";
    timeline.classList.toggle("is-timeline", useTimeline);
    timeline.dataset.thinkingVariant = variantSet.thinking;
    timeline.dataset.toolsVariant = variantSet.tools;
    timeline.dataset.streamingVariant = variantSet.streaming;
    snapshot.timeline.forEach((item) => {
      const key = "".concat(item.kind, ":").concat(item.id);
      let node = [...timeline.querySelectorAll("[data-round-event-key]")].find((candidate) => candidate.dataset.roundEventKey === key);
      if (!node) {
        node = createRoundEventNode(item);
        timeline.append(node);
      }
      const mount = node.querySelector(".round-event-mount");
      if (!mount) return;
      if (item.kind === "reasoning") {
        renderThinking(mount, variantSet.thinking, snapshot, [item.value]);
      } else if (item.kind === "tool") {
        mount.className = "round-event-mount tool-list ".concat(variantSet.tools === "tools_execution_timeline" ? "tool-timeline" : "");
        renderToolNode(mount, variantSet.tools, item.value, snapshot);
      } else if (item.kind === "progress" || item.kind === "answer") {
        renderStreaming(mount, variantSet.streaming, snapshot, [item.value], false);
      }
    });
    const activeKeys = new Set(snapshot.timeline.map((item) => "".concat(item.kind, ":").concat(item.id)));
    timeline.querySelectorAll("[data-round-event-key]").forEach((node) => {
      if (!activeKeys.has(node.dataset.roundEventKey || "")) node.remove();
    });
    let terminal = timeline.querySelector(".round-terminal-note");
    if (snapshot.terminalMessage) {
      if (!terminal) {
        terminal = createElement("p", "round-terminal-note");
        timeline.append(terminal);
      }
      terminal.textContent = snapshot.terminalMessage;
    } else {
      terminal?.remove();
    }
    renderActivity(activity, variantSet.activity, snapshot);
  }
  function renderCombination(snapshot) {
    if (!combinationSurface) return;
    const variantSet = {
      thinking: selections.thinking || BASELINE_VARIANTS.thinking,
      tools: selections.tools || BASELINE_VARIANTS.tools,
      activity: selections.activity || BASELINE_VARIANTS.activity,
      streaming: selections.streaming || BASELINE_VARIANTS.streaming
    };
    renderRoundStream(combinationSurface, snapshot, variantSet);
  }
  function updateSelectionUi() {
    document.querySelectorAll("[data-variant-id]").forEach((card) => {
      const component = card.dataset.component;
      const selected = selections[component] === card.dataset.variantId;
      card.classList.toggle("is-selected", selected);
      const button = card.querySelector(".variant-select");
      if (button) {
        button.setAttribute("aria-pressed", String(selected));
        button.textContent = selected ? "\u5DF2\u9009\u62E9" : "\u9009\u62E9\u6B64\u65B9\u6848";
      }
    });
    selectionSummary.textContent = readableSelection();
    const complete = Boolean(selectionPayload());
    copySelectionButton.disabled = !complete;
    exportSelectionButton.disabled = !complete;
    buildCombinationPreview();
    persistState();
    renderAll(deriveSnapshot(currentScenario(), elapsedMs));
  }
  function renderFocused(component, snapshot) {
    focusedSurfaces[component].forEach((surface, variantId) => {
      renderRoundStream(surface, snapshot, { ...BASELINE_VARIANTS, [component]: variantId });
    });
  }
  function renderAll(snapshot) {
    COMPONENTS.forEach((component) => renderFocused(component, snapshot));
    renderCombination(snapshot);
    updateUserText();
    timelineStatus.textContent = "".concat(stageLabels[snapshot.stage], " \xB7 ").concat(formatSeconds(snapshot.elapsedMs));
    timelineProgress.value = snapshot.stage === "completed" ? 1 : Math.min(1, snapshot.elapsedMs / currentScenario().durationMs);
    playButton.disabled = playing;
    pauseButton.disabled = !playing;
  }
  function resetPlayback() {
    playing = false;
    cancelAnimationFrame(animationFrame);
    previousTimestamp = null;
    elapsedMs = 0;
    smoothedText.clear();
    resetDynamicSurfaces();
    renderAll(deriveSnapshot(currentScenario(), elapsedMs));
  }
  function advanceSmoothedText(snapshot, deltaMs) {
    const release = Math.max(1, Math.floor(deltaMs * playbackSpeed * 0.12));
    snapshot.textSegments.forEach((segment) => {
      let current = smoothedText.get(segment.id) || "";
      if (!segment.text.startsWith(current)) current = "";
      if (current.length < segment.text.length) {
        current += segment.text.slice(current.length, current.length + release);
        smoothedText.set(segment.id, current);
      }
    });
  }
  function animationTick(timestamp) {
    if (!playing) return;
    const last = previousTimestamp ?? timestamp;
    const delta = Math.min(120, timestamp - last);
    previousTimestamp = timestamp;
    const scenario = currentScenario();
    elapsedMs = Math.min(scenario.durationMs, elapsedMs + delta * playbackSpeed);
    const snapshot = deriveSnapshot(scenario, elapsedMs);
    advanceSmoothedText(snapshot, delta);
    renderAll(snapshot);
    const sourceComplete = ["completed", "stopped"].includes(snapshot.stage) || elapsedMs >= scenario.durationMs;
    const smoothedComplete = snapshot.textSegments.every((segment) => (smoothedText.get(segment.id) || "").length >= segment.text.length);
    if (sourceComplete && smoothedComplete) {
      playing = false;
      previousTimestamp = null;
      renderAll(snapshot);
      return;
    }
    animationFrame = requestAnimationFrame(animationTick);
  }
  function startPlayback() {
    const stage = deriveSnapshot(currentScenario(), elapsedMs).stage;
    if (["completed", "stopped"].includes(stage) || elapsedMs >= currentScenario().durationMs) resetPlayback();
    if (playing) return;
    playing = true;
    previousTimestamp = null;
    animationFrame = requestAnimationFrame(animationTick);
    renderAll(deriveSnapshot(currentScenario(), elapsedMs));
  }
  function pausePlayback() {
    playing = false;
    cancelAnimationFrame(animationFrame);
    previousTimestamp = null;
    renderAll(deriveSnapshot(currentScenario(), elapsedMs));
  }
  function setActiveTab(tabId, focus = false) {
    const tabs = [...document.querySelectorAll("[data-showcase-tab]")];
    if (!tabs.some((tab) => tab.dataset.showcaseTab === tabId)) return;
    activeTab = tabId;
    tabs.forEach((tab) => {
      const selected = tab.dataset.showcaseTab === activeTab;
      tab.setAttribute("aria-selected", String(selected));
      tab.tabIndex = selected ? 0 : -1;
      if (selected && focus) tab.focus();
    });
    document.querySelectorAll("[data-showcase-panel]").forEach((panel) => {
      panel.hidden = panel.dataset.showcasePanel !== activeTab;
    });
  }
  async function copyText(text) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      const field = createElement("textarea");
      field.value = text;
      field.style.position = "fixed";
      field.style.opacity = "0";
      document.body.append(field);
      field.select();
      const copied = document.execCommand("copy");
      field.remove();
      return copied;
    }
  }
  function exportSelection() {
    const payload = selectionPayload();
    if (!payload) return;
    const blob = new Blob(["".concat(JSON.stringify(payload, null, 2), "\n")], { type: "application/json;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = createElement("a");
    link.href = url;
    link.download = "upsp-conversation-showcase-selection.json";
    document.body.append(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    selectionFeedback.textContent = "\u5DF2\u5BFC\u51FA\u9009\u578B JSON\uFF1B\u5B83\u5C1A\u672A\u5199\u5165\u4EA7\u54C1\u914D\u7F6E\u3002";
  }
  function bindEvents() {
    playButton.addEventListener("click", startPlayback);
    pauseButton.addEventListener("click", pausePlayback);
    resetButton.addEventListener("click", resetPlayback);
    scenarioSelect.addEventListener("change", () => {
      if (!isScenarioId(scenarioSelect.value)) return;
      currentScenarioId = scenarioSelect.value;
      persistState();
      resetPlayback();
    });
    document.querySelectorAll('input[name="speed"]').forEach((input) => {
      input.addEventListener("change", () => {
        if (!input.checked) return;
        playbackSpeed = normalizeSpeed(input.value);
        persistState();
      });
    });
    document.addEventListener("click", (event) => {
      const target = event.target instanceof Element ? event.target : null;
      const select = target?.closest("[data-select-component][data-select-variant]");
      if (select) {
        const component = select.dataset.selectComponent;
        const variantId = select.dataset.selectVariant;
        if (COMPONENTS.includes(component) && validVariant(component, variantId)) {
          selections = { ...selections, [component]: variantId };
          selectionFeedback.textContent = "".concat(componentLabels[component], "\u5DF2\u9009\u62E9\u3002");
          updateSelectionUi();
        }
        return;
      }
      const tab = target?.closest("[data-showcase-tab]");
      if (tab?.dataset.showcaseTab) setActiveTab(tab.dataset.showcaseTab);
    });
    const tabList = document.querySelector('[role="tablist"]');
    tabList?.addEventListener("keydown", (event) => {
      if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
      const tabs = [...tabList.querySelectorAll("[data-showcase-tab]")];
      const current = tabs.findIndex((tab) => tab.dataset.showcaseTab === activeTab);
      let next = current;
      if (event.key === "ArrowLeft") next = (current - 1 + tabs.length) % tabs.length;
      if (event.key === "ArrowRight") next = (current + 1) % tabs.length;
      if (event.key === "Home") next = 0;
      if (event.key === "End") next = tabs.length - 1;
      event.preventDefault();
      setActiveTab(tabs[next].dataset.showcaseTab || "thinking", true);
    });
    clearSelectionButton.addEventListener("click", () => {
      selections = {};
      selectionFeedback.textContent = "\u5DF2\u6E05\u7A7A\u56DB\u7EC4\u9009\u62E9\u3002";
      updateSelectionUi();
    });
    copySelectionButton.addEventListener("click", async () => {
      const payload = selectionPayload();
      if (!payload) return;
      const text = "".concat(readableSelection(), "\n\n").concat(JSON.stringify(payload, null, 2));
      selectionFeedback.textContent = await copyText(text) ? "\u5DF2\u590D\u5236\u9009\u578B\u6458\u8981\u3002" : "\u590D\u5236\u5931\u8D25\uFF0C\u8BF7\u4F7F\u7528\u5BFC\u51FA JSON\u3002";
    });
    exportSelectionButton.addEventListener("click", exportSelection);
  }
  function initialize() {
    scenarioSelect.replaceChildren(...scenarios.map((scenario) => {
      const option = createElement("option", "", scenario.label);
      option.value = scenario.id;
      return option;
    }));
    const stored = readStoredState();
    if (stored) {
      currentScenarioId = stored.scenario;
      playbackSpeed = stored.speed;
      selections = stored.selections;
    }
    scenarioSelect.value = currentScenarioId;
    document.querySelectorAll('input[name="speed"]').forEach((input) => {
      input.checked = Number(input.value) === playbackSpeed;
    });
    buildVariantGrids();
    bindEvents();
    setActiveTab("thinking");
    updateSelectionUi();
    resetPlayback();
  }
  initialize();
})();
