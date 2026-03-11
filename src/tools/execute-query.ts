import { CallToolResult, ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import sql from 'mssql';
import { ToolDefinition } from '../types.js';
import { BaseSQLTool } from './base-tool.js';

export class ExecuteQueryTool extends BaseSQLTool {
  getDefinition(): ToolDefinition {
    return {
      name: 'execute_query',
      description: 'Execute a read-only SQL SELECT query and return the results as JSON',
      inputSchema: {
        type: 'object',
        properties: {
          query: {
            type: 'string',
            description: 'SQL SELECT query to execute (read-only; only SELECT statements are allowed)'
          }
        },
        required: ['query']
      }
    };
  }

  async execute(pool: sql.ConnectionPool, args?: Record<string, any>): Promise<CallToolResult> {
    const query = args?.query as string;

    if (!query || !query.trim()) {
      throw new McpError(ErrorCode.InvalidParams, 'query parameter is required');
    }

    if (!query.trim().toUpperCase().startsWith('SELECT')) {
      throw new McpError(ErrorCode.InvalidParams, 'Only SELECT queries are allowed');
    }

    const request = pool.request();
    const result = await request.query(query);

    return this.formatResponse(result.recordset);
  }
}
