# Spark MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io) server for Apache
Spark, implemented as a thin client over [Spark
Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html).

> Status: **early scaffold (v0.1 PoC)**. Not yet wired into Spark releases.

## Why

LLM clients can already talk to MCP servers; Spark Connect already separates
client from cluster. This module connects the two: a Spark cluster shows up
to an LLM as a set of safe, paginated tools — `list_tables`, `describe_table`,
`execute_sql`, `explain_query`, etc. — without giving the LLM a JVM driver.

## Run

The server speaks the `stdio` MCP transport (other transports may be added
later) and connects to a Spark cluster via Spark Connect:

```bash
python -m pyspark.sql.mcp \
  --connect-url "sc://localhost:15002"
```

Equivalent via environment variable:

```bash
export SPARK_REMOTE="sc://localhost:15002"
python -m pyspark.sql.mcp
```

The server is **read-only by default**. Pass `--no-read-only` to allow DDL/DML
(use with care; the read-only filter is a guardrail, not a security boundary).

## Tools (planned for v0.1)

Catalog exploration, query execution, and query plans:

- `get_session_info`
- `list_catalogs`, `list_databases`, `list_tables`, `describe_table`,
  `list_functions`
- `execute_sql`, `preview_table`
- `explain_query`, `analyze_query`

See `dev/design-docs/spark-mcp-server/02-design.md` for the full design.

## Dependencies

- `pyspark` with the Spark Connect client extras.
- The official [`mcp`](https://github.com/modelcontextprotocol/python-sdk)
  Python SDK.
