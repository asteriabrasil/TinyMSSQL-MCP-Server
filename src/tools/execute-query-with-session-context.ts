import { CallToolResult, ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import sql from 'mssql';
import { ToolDefinition } from '../types.js';
import { BaseSQLTool } from './base-tool.js';

function buildConfig(): sql.config {
  const serverString = process.env.MSSQL_SERVER || 'localhost';
  const [server, instanceName] = serverString.includes('\\')
    ? serverString.split('\\')
    : [serverString, process.env.MSSQL_INSTANCE];

  const config: sql.config = {
    server,
    database: process.env.MSSQL_DATABASE || 'master',
    pool: { max: 1, min: 0, idleTimeoutMillis: 5000 },
    options: {
      encrypt: process.env.MSSQL_ENCRYPT === 'true',
      trustServerCertificate: process.env.MSSQL_TRUST_CERT === 'true',
      connectTimeout: 30000,
      requestTimeout: 30000,
      instanceName: instanceName || undefined
    }
  };

  if (process.env.MSSQL_TRUSTED_CONNECTION !== 'true') {
    config.user = process.env.MSSQL_USER;
    config.password = process.env.MSSQL_PASSWORD;
  }

  return config;
}

export class ExecuteQueryWithSessionContextTool extends BaseSQLTool {
  getDefinition(): ToolDefinition {
    return {
      name: 'execute_query_with_session_context',
      description:
        'Execute a read-only SQL SELECT query after setting arbitrary SQL Server session context ' +
        'variables via sp_set_session_context. Use this when the database has Row-Level Security (RLS) ' +
        'or other logic that reads SESSION_CONTEXT() variables. ' +
        'Pass any key-value pairs needed by the target database — no keys are hardcoded.',
      inputSchema: {
        type: 'object',
        properties: {
          query: {
            type: 'string',
            description: 'SQL SELECT query to execute (read-only; only SELECT statements are allowed)'
          },
          sessionContext: {
            type: 'object',
            description:
              'Key-value pairs to set as session context before executing the query. ' +
              'All values are passed as NVARCHAR strings. ' +
              'Example: { "TenantId": "C47B2E42-...", "UserId": "34E341D7-...", "IsAdmin": "1" }',
            additionalProperties: { type: 'string' }
          }
        },
        required: ['query', 'sessionContext']
      }
    };
  }

  // pool parameter is kept for interface compatibility but not used —
  // a dedicated single-connection pool is created per call so that session
  // context set via sp_set_session_context is guaranteed visible to the SELECT
  // and is not affected by stale read_only context on shared pooled connections.
  async execute(_pool: sql.ConnectionPool, args?: Record<string, any>): Promise<CallToolResult> {
    const query = args?.query as string;
    const sessionContext = args?.sessionContext as Record<string, string> | undefined;

    if (!query || !query.trim()) {
      throw new McpError(ErrorCode.InvalidParams, 'query parameter is required');
    }

    if (!sessionContext || typeof sessionContext !== 'object' || Object.keys(sessionContext).length === 0) {
      throw new McpError(ErrorCode.InvalidParams, 'sessionContext must be a non-empty object of key-value string pairs');
    }

    if (!query.trim().toUpperCase().startsWith('SELECT')) {
      throw new McpError(ErrorCode.InvalidParams, 'Only SELECT queries are allowed');
    }

    const dedicatedPool = new sql.ConnectionPool(buildConfig());
    await dedicatedPool.connect();

    try {
      // Use a Transaction to pin all requests to the same physical connection.
      const transaction = new sql.Transaction(dedicatedPool);
      await transaction.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);

      try {
        for (const [key, value] of Object.entries(sessionContext)) {
          const req = new sql.Request(transaction);
          req.input('key', sql.NVarChar, key);
          req.input('value', sql.NVarChar, String(value));
          // read_only = 0: do not lock the key so future calls on this connection can set it again
          await req.query('EXEC sp_set_session_context @key, @value, 0;');
        }

        const selectRequest = new sql.Request(transaction);
        const result = await selectRequest.query(query);

        await transaction.rollback();
        return this.formatResponse(result.recordset);
      } catch (error) {
        try { await transaction.rollback(); } catch { /* ignore */ }
        throw error;
      }
    } finally {
      await dedicatedPool.close();
    }
  }
}
