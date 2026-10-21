permit(
  principal == Agent::"memory-concierge-pg",
  action in [Action::"McpServer.initialize", Action::"McpServer.ping", Action::"McpServer.tools_list", Action::"McpServerTool.call"],
  resource == McpServer::"pg-agent-memory"
);
