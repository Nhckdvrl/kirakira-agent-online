"""Model Context Protocol stdio client and declarative workspace servers."""

from agent.mcp.admin import WorkspaceMcpAdmin
from agent.mcp.client import McpClient, McpToolInfo
from agent.mcp.declarations import (
    WorkspaceMcpDeclarations,
    declarations_input_revision,
    load_workspace_mcp_declarations,
)
from agent.mcp.host import McpGenerationHost, PreparedMcpCatalog
from agent.mcp.publisher import McpCatalogPublisher
from agent.mcp.watcher import WorkspaceMcpWatcher

__all__ = [
    "McpCatalogPublisher",
    "McpClient",
    "McpGenerationHost",
    "McpToolInfo",
    "PreparedMcpCatalog",
    "WorkspaceMcpAdmin",
    "WorkspaceMcpDeclarations",
    "WorkspaceMcpWatcher",
    "declarations_input_revision",
    "load_workspace_mcp_declarations",
]
