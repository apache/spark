#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Entry point for the Spark MCP server.

The :func:`build_server` function is the seam used by tests; :func:`run` is
the CLI entry point used by ``python -m pyspark.sql.mcp``.

The official ``mcp`` Python SDK is imported lazily so that importing this
module does not pull the SDK in on machines that only need the configuration
objects (e.g. for unit testing the safety layer).
"""

from __future__ import annotations

import argparse
import json
from typing import Any, TYPE_CHECKING

from pyspark.sql.mcp.config import ServerConfig
from pyspark.sql.mcp.session import SessionHolder
from pyspark.sql.mcp.tools.registry import all_tools

if TYPE_CHECKING:
    from mcp.server import Server


def build_server(config: ServerConfig) -> "Server":
    """Build an MCP server instance bound to a Spark Connect session.

    The server registers tool handlers but does not start a transport. Use
    :func:`run` (or the SDK's transport runners) to serve.
    """
    from mcp.server import Server

    holder = SessionHolder(config)
    server: "Server[Any]" = Server("spark-mcp")
    _register_tools(server, holder)
    return server


def _register_tools(server: "Server", holder: SessionHolder) -> None:
    import mcp.types as types

    specs = {spec.name: spec for spec in all_tools()}

    @server.list_tools()
    async def _list_tools() -> list:
        return [
            types.Tool(
                name=spec.name,
                description=spec.description,
                inputSchema=spec.input_schema,
            )
            for spec in specs.values()
        ]

    @server.call_tool()
    async def _call_tool(name: str, arguments: dict) -> list:
        spec = specs.get(name)
        if spec is None:
            raise ValueError(f"unknown tool: {name}")
        result = await spec.handler(arguments or {}, holder)
        return [types.TextContent(type="text", text=json.dumps(result, default=str))]


def run(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    config = ServerConfig.from_env(
        connect_url=args.connect_url,
        read_only=args.read_only,
        transport=args.transport,
        max_rows=args.max_rows,
        query_timeout_seconds=args.query_timeout_seconds,
        user_id=args.user_id,
    )
    _serve(build_server(config), config)


def _serve(server: "Server", config: ServerConfig) -> None:
    if config.transport == "stdio":
        import anyio
        from mcp.server.stdio import stdio_server

        async def main() -> None:
            async with stdio_server() as (read, write):
                await server.run(read, write, server.create_initialization_options())

        anyio.run(main)
    else:
        raise NotImplementedError(
            f"transport {config.transport!r} is not implemented yet"
        )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m pyspark.sql.mcp",
        description="Apache Spark MCP server (Spark Connect thin client).",
    )
    parser.add_argument(
        "--connect-url",
        help="Spark Connect URL (sc://host:port[/;params]). "
        "Defaults to the SPARK_REMOTE environment variable.",
    )
    ro = parser.add_mutually_exclusive_group()
    ro.add_argument(
        "--read-only",
        dest="read_only",
        action="store_true",
        default=None,
        help="Reject DDL/DML statements (default).",
    )
    ro.add_argument(
        "--no-read-only",
        dest="read_only",
        action="store_false",
        help="Allow arbitrary SQL. Use with care.",
    )
    parser.add_argument(
        "--transport",
        choices=["stdio"],
        default=None,
        help="MCP transport (currently only stdio is implemented).",
    )
    parser.add_argument(
        "--user-id",
        dest="user_id",
        default=None,
        help="Spark Connect session user id. Defaults to $SPARK_MCP_USER_ID.",
    )
    parser.add_argument(
        "--max-rows",
        dest="max_rows",
        default=None,
        type=int,
        help=(
            "Hard cap on rows returned per row-producing tool call. Defaults "
            "to $SPARK_MCP_MAX_ROWS or 1000."
        ),
    )
    parser.add_argument(
        "--query-timeout-seconds",
        dest="query_timeout_seconds",
        default=None,
        type=int,
        help=(
            "Wall-clock timeout for individual tool invocations that touch "
            "the cluster. Defaults to $SPARK_MCP_QUERY_TIMEOUT_SECONDS or 60."
        ),
    )
    return parser.parse_args(argv)


if __name__ == "__main__":  # pragma: no cover - thin CLI wrapper
    run()
