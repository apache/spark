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

from typing import TYPE_CHECKING, Optional

from pyspark.sql.mcp.config import ServerConfig

if TYPE_CHECKING:
    from pyspark.sql.session import SparkSession


class SessionHolder:
    """Lazily creates and caches a Spark Connect session for the MCP server.

    One MCP client connection maps to one Spark Connect session.
    """

    def __init__(self, config: ServerConfig):
        self._config = config
        self._session: Optional["SparkSession"] = None

    @property
    def config(self) -> ServerConfig:
        return self._config

    def get(self) -> "SparkSession":
        if self._session is None:
            from pyspark.sql import SparkSession

            builder = SparkSession.builder.remote(self._config.connect_url)
            for key, value in self._config.extra_session_configs.items():
                builder = builder.config(key, value)
            if self._config.user_id is not None:
                builder = builder.config("spark.connect.session.userId", self._config.user_id)
            self._session = builder.getOrCreate()
        assert self._session is not None
        return self._session

    def close(self) -> None:
        if self._session is not None:
            try:
                self._session.stop()
            finally:
                self._session = None
