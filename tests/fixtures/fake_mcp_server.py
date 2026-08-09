"""Tiny stdio MCP server used by integration tests."""

import json
import sys


TOOLS = [
    {
        "name": "echo",
        "description": "Echo text",
        "inputSchema": {
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
        },
    },
    {
        "name": "fail",
        "description": "Return an MCP tool error",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


for line in sys.stdin:
    try:
        request = json.loads(line)
    except ValueError:
        continue
    if "id" not in request:
        continue
    method = request.get("method")
    if method == "initialize":
        send(
            {
                "jsonrpc": "2.0",
                "id": request["id"],
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fake", "version": "1"},
                },
            }
        )
    elif method == "tools/list":
        send({"jsonrpc": "2.0", "id": request["id"], "result": {"tools": TOOLS}})
    elif method == "tools/call":
        params = request.get("params") or {}
        if params.get("name") == "fail":
            result = {
                "content": [{"type": "text", "text": "expected failure"}],
                "isError": True,
            }
        else:
            result = {
                "content": [
                    {
                        "type": "text",
                        "text": str((params.get("arguments") or {}).get("text", "")),
                    }
                ]
            }
        send({"jsonrpc": "2.0", "id": request["id"], "result": result})
