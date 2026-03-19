#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { CallToolRequestSchema, ErrorCode, ListToolsRequestSchema, McpError } from '@modelcontextprotocol/sdk/types.js';
import sql from 'mssql';
import {
  ExecuteQueryTool,
  ExecuteQueryWithSessionContextTool,
  GetTableDetailsTool,
  ListFunctionsTool,
  ListIndexesTool,
  ListProceduresTool,
  ListSchemasTool,
  ListTablesTool
} from './tools/index.js';
import { BaseTool } from './types.js';

class SQLMCPServer {
  private server: Server;
  private pool: sql.ConnectionPool | null = null;
  private tools: Map<string, BaseTool>;

  constructor() {
    this.tools = new Map();
    this.initializeTools();

    this.server = new Server(
      {
        name: 'sql-mcp-server',
        version: '0.1.0'
      },
      {
        capabilities: {
          tools: {}
        }
      }
    );

    this.setupToolHandlers();
    this.setupErrorHandling();
  }

  private initializeTools() {
    const toolInstances = [
      new ListSchemasTool(),
      new ListTablesTool(),
      new ListProceduresTool(),
      new ListFunctionsTool(),
      new ListIndexesTool(),
      new GetTableDetailsTool(),
      new ExecuteQueryTool(),
      new ExecuteQueryWithSessionContextTool()
    ];

    for (const tool of toolInstances) {
      this.tools.set(tool.getDefinition().name, tool);
    }
  }

  private async connectToDatabase() {
    if (this.pool && this.pool.connected) {
      return this.pool;
    }

    const useTrustedConnection = process.env.MSSQL_TRUSTED_CONNECTION === 'true';
    if (!useTrustedConnection && (!process.env.MSSQL_USER || !process.env.MSSQL_PASSWORD)) {
      throw new Error(
        'Database credentials not provided. Set MSSQL_USER and MSSQL_PASSWORD environment variables, or set MSSQL_TRUSTED_CONNECTION=true for Windows Authentication.'
      );
    }

    const serverString = process.env.MSSQL_SERVER || 'localhost';
    const [server, instanceName] = serverString.includes('\\')
      ? serverString.split('\\')
      : [serverString, process.env.MSSQL_INSTANCE];

    const config: sql.config = {
      server,
      database: process.env.MSSQL_DATABASE || 'master',
      options: {
        encrypt: process.env.MSSQL_ENCRYPT === 'true',
        trustServerCertificate: process.env.MSSQL_TRUST_CERT === 'true',
        connectTimeout: 30000,
        requestTimeout: 30000,
        instanceName: instanceName || undefined
      }
    };

    if (!useTrustedConnection) {
      config.user = process.env.MSSQL_USER;
      config.password = process.env.MSSQL_PASSWORD;
    }
    // Note: For true Windows Authentication (Trusted Connection) with mssql package,
    // you need to install msnodesqlv8 driver and use a connection string instead.
    // The default tedious driver doesn't support integrated Windows Authentication.

    try {
      this.pool = new sql.ConnectionPool(config);
      await this.pool.connect();
      return this.pool;
    } catch (error) {
      throw new Error(`Failed to connect to database: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private setupToolHandlers() {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      const tools = Array.from(this.tools.values()).map((tool) => tool.getDefinition());
      return { tools };
    });

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;

      try {
        const tool = this.tools.get(name);
        if (!tool) {
          throw new McpError(ErrorCode.MethodNotFound, `Unknown tool: ${name}`);
        }

        const pool = await this.connectToDatabase();
        return await tool.execute(pool, args);
      } catch (error) {
        if (error instanceof McpError) {
          throw error;
        }
        throw new McpError(
          ErrorCode.InternalError,
          `Database operation failed: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    });
  }

  private setupErrorHandling() {
    this.server.onerror = (error) => {
      console.error('[MCP Error]', error);
    };

    process.on('SIGINT', async () => {
      if (this.pool) {
        await this.pool.close();
      }
      await this.server.close();
      process.exit(0);
    });
  }

  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    console.error('SQL MCP server running on stdio');
  }
}

const server = new SQLMCPServer();
server.run().catch(console.error);
