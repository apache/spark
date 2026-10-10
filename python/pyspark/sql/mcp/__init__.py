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

"""Spark MCP (Model Context Protocol) server.

Thin client over Spark Connect that exposes Spark capabilities as MCP tools
for LLM consumption. Catalog browsing, SQL execution, and query plan tools
are read-only by default.
"""

from pyspark.sql.mcp.config import ServerConfig
from pyspark.sql.mcp.server import build_server, run

__all__ = ["ServerConfig", "build_server", "run"]
