"""
UPSP Base V2 runtime orchestration layer.

Core modules:
- heartbeat.py: heartbeat clock and trigger classification; no LLM call.
- executor.py: API execution, endpoint routing, circuit breaker, timeout handling.
- runtime.py: Base round/step transaction coordinator and component wiring.
- reaction_loop.py: reaction-step loop runner compatibility boundary.
- protocol_tool_dispatcher.py: protocol-tool request/submission receipt helper.
- cleanup_pipeline.py: cleanup-step pipeline compatibility boundary.

engines/ owns round/step ordering, try/finally discipline, and cross-layer
coordination. Domain content remains in data/, logic/, and assembly/.
"""
