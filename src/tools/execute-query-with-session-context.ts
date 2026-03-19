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
        'Execute a read-only SQL SELECT query with SQL Server session context set via sp_set_session_context. ' +
        'Use this when the database has Row-Level Security (RLS) that requires session context variables such as TenantId.',
      inputSchema: {
        type: 'object',
        properties: {
          query: {
            type: 'string',
            description: 'SQL SELECT query to execute (read-only; only SELECT statements are allowed)'
          },
          tenantId: {
            type: 'string',
            description: 'Tenant GUID to set as session context TenantId (required for RLS)'
          },
          userId: {
            type: 'string',
            description: 'User GUID to set as session context UserId (optional, defaults to empty string)'
          },
          isAdmin: {
            type: 'boolean',
            description: 'Whether the user is a system admin (optional, defaults to false)'
          },
          isTenantAdmin: {
            type: 'boolean',
            description: 'Whether the user is a tenant admin (optional, defaults to false)'
          },
          tenantName: {
            type: 'string',
            description: 'Tenant name to set as session context TenantName (optional, defaults to empty string)'
          }
        },
        required: ['query', 'tenantId']
      }
    };
  }

  // pool parameter is kept for interface compatibility but not used here —
  // we create a dedicated single-connection pool so that session context
  // set via sp_set_session_context is guaranteed to be visible to the SELECT
  // and is not affected by stale read_only context from pooled connections.
  async execute(_pool: sql.ConnectionPool, args?: Record<string, any>): Promise<CallToolResult> {
    const query = args?.query as string;
    const tenantId = args?.tenantId as string;
    const userId = (args?.userId as string) ?? '';
    const isAdmin = args?.isAdmin === true ? '1' : '0';
    const isTenantAdmin = args?.isTenantAdmin === true ? '1' : '0';
    const tenantName = (args?.tenantName as string) ?? '';

    if (!query || !query.trim()) {
      throw new McpError(ErrorCode.InvalidParams, 'query parameter is required');
    }

    if (!tenantId || !tenantId.trim()) {
      throw new McpError(ErrorCode.InvalidParams, 'tenantId parameter is required');
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
        const sessionVars: [string, string][] = [
          ['UserId', userId],
          ['TenantId', tenantId],
          ['IsAdmin', isAdmin],
          ['IsTenantAdmin', isTenantAdmin],
          ['TenantName', tenantName]
        ];

        for (const [key, value] of sessionVars) {
          const req = new sql.Request(transaction);
          req.input('key', sql.NVarChar, key);
          req.input('value', sql.NVarChar, value);
          // Use read_only = 0 so we can set context without conflicts on fresh connections
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
