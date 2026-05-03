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

import re

from pyspark.errors import PySparkValueError

# Read-only allow-list. Comments and leading whitespace are stripped before
# matching. We deliberately keep this conservative — anything not on the list
# is rejected when read_only is on.
_READ_ONLY_LEAD = re.compile(
    r"^(select|with|show|describe|desc|explain|use|set|reset)\b",
    re.IGNORECASE,
)

# Strip /* ... */ block comments and -- line comments before matching.
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT = re.compile(r"--[^\n]*")


class ReadOnlyViolation(PySparkValueError):
    """Raised when a SQL statement violates the read-only policy."""


def assert_read_only(query: str) -> None:
    """Reject queries that mutate state when the server is in read-only mode.

    The check is best-effort and string-based; it is a guardrail, not a
    security boundary. Real isolation belongs at the Spark cluster (catalog
    permissions, workspace policies, etc.).
    """
    stripped = _LINE_COMMENT.sub("", _BLOCK_COMMENT.sub("", query)).strip()
    if not stripped:
        raise ReadOnlyViolation(
            errorClass="MCP_EMPTY_QUERY",
            messageParameters={},
        )
    if not _READ_ONLY_LEAD.match(stripped):
        first_token = stripped.split(None, 1)[0]
        raise ReadOnlyViolation(
            errorClass="MCP_READ_ONLY_VIOLATION",
            messageParameters={"statement": first_token},
        )
