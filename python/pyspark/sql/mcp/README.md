# Spark MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io) server for Apache
Spark, implemented as a thin client over [Spark
Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html).

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

## Configuring an MCP client

The server speaks `stdio` MCP transport, so a client launches it as a
subprocess. The exact configuration step is client-specific; the moving
parts are always:

1. The command (`python -m pyspark.sql.mcp`).
2. The Spark Connect URL, supplied via `--connect-url` or the
   `SPARK_REMOTE` environment variable.
3. Optionally, `--no-read-only` to permit DDL/DML.

### Claude Code

```bash
claude mcp add spark \
  -e SPARK_REMOTE=sc://localhost:15002 \
  -- python -m pyspark.sql.mcp
```

Pass `--scope user` to register the server for all projects, or
`--scope project` to write a shared `.mcp.json` into the current
repository. Restart the Claude Code session and the LLM gains
`mcp__spark__*` tools.

### Claude Desktop

Edit the application's `claude_desktop_config.json` (location is
documented in the Claude Desktop docs) and add an entry under
`mcpServers`:

```json
{
  "mcpServers": {
    "spark": {
      "command": "python",
      "args": ["-m", "pyspark.sql.mcp"],
      "env": {
        "SPARK_REMOTE": "sc://localhost:15002"
      }
    }
  }
}
```

Restart Claude Desktop.

### Other MCP clients

Any MCP-compliant client that can launch a stdio subprocess can use the
same shape:

```json
{
  "command": "python",
  "args": ["-m", "pyspark.sql.mcp"],
  "env": {"SPARK_REMOTE": "sc://localhost:15002"}
}
```

### Environment variables

| Variable | Meaning |
| --- | --- |
| `SPARK_REMOTE` | Spark Connect URL (`sc://host:port[/;params]`). |
| `SPARK_MCP_READ_ONLY` | `true` (default) or `false`. |
| `SPARK_MCP_USER_ID` | Optional Spark Connect session user id. |
| `SPARK_MCP_TRANSPORT` | Reserved; only `stdio` is implemented. |

To verify the wiring, ask the LLM to list catalogs, or to call
`get_session_info` — the response should include the Spark version
reported by the cluster.

## Tools

Catalog exploration, query execution, and query plans:

- `get_session_info`
- `list_catalogs`, `list_databases`, `list_tables`, `describe_table`,
  `list_functions`
- `execute_sql`, `preview_table`
- `explain_query`, `analyze_query`

## Motivating example: LLM-assisted plan analysis

`explain_query` and `analyze_query` are the highest-leverage tools the
server exposes: an LLM that can read a structured Spark plan can reason
about performance the way a Spark engineer would, without running the
query.

A user prompts an LLM client (which has the Spark MCP server attached):

> "I want to understand the execution plan for
> `SELECT count(*) FROM range(1000000) WHERE id % 7 = 0 GROUP BY id % 3` —
> but **don't actually execute it**. From the plan, where is the predicate
> handled?"

The LLM calls `analyze_query`, receives the optimized logical and physical
plans, and produces an answer like:

```
Optimized Logical Plan
Aggregate [_groupingexpression#50L], [count(1) AS count(1)]
+- Project [(id % 3) AS _groupingexpression]
   +- Filter ((id % 7) = 0)
      +- Range (0, 1000000, step=1)

Physical Plan
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[_groupingexpression], functions=[count(1)])
   +- Exchange hashpartitioning(_groupingexpression, 200)
      +- HashAggregate(keys=[_groupingexpression], functions=[partial_count(1)])
         +- Project [(id % 3) AS _groupingexpression]
            +- Filter ((id % 7) = 0)
               +- Range (0, 1000000, step=1, splits=16)
```

> 1. **Range (splits=16)** generates 0..999999 across 16 partitions — no I/O.
> 2. **Filter `(id % 7) = 0`** sits directly above the Range, so only ~1/7 of
>    the rows survive; everything above it sees the smaller stream.
> 3. **Project `id % 3`** materializes the group key once so it isn't
>    recomputed on either side of the aggregate.
> 4. **HashAggregate (partial_count)** does map-side combine inside each
>    partition, emitting `(group_key, partial_count)`.
> 5. **Exchange hashpartitioning(200)** shuffles by group key. Since the key
>    only has 3 distinct values, only 3 reducers actually receive data.
> 6. **HashAggregate (final count)** sums the partial counts into the final
>    3-row result.
> 7. **AdaptiveSparkPlan `isFinalPlan=false`** means AQE can still adjust at
>    runtime (coalesce empty partitions, change join strategies, etc.).
>
> **Where is `id % 7 = 0` handled?** In the `Filter` operator immediately
> above the source — the lowest, earliest point in the plan. The
> `PushDownPredicates` optimizer rule moves filters as close to the source
> as possible. `Range` isn't a file source, so there is no storage-level
> pushdown, but the effect is identical: `id % 3`, the partial aggregate,
> and the shuffle all see only the post-filter rows. For a Parquet source
> the same filter would also be pushed down as a data-source filter and
> drive partition pruning.

This kind of explanation is what makes the plan tools valuable: the LLM
turns Spark's verbose plan output into a step-by-step narrative grounded in
operator semantics, without any of the query's data leaving the cluster.

## Dependencies

- `pyspark` with the Spark Connect client extras.
- The official [`mcp`](https://github.com/modelcontextprotocol/python-sdk)
  Python SDK.
