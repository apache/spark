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

"""Experimental PySpark functions for remote model inference."""

from typing import TYPE_CHECKING, Dict, Optional, Union

from pyspark.sql._ai_client import _AgentEndpoint, _ModelEndpoint, _query_batches
from pyspark.sql.column import Column

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName

__all__ = ["ai_query", "register_agent", "register_model"]

_endpoints: Dict[str, Union[_AgentEndpoint, _ModelEndpoint]] = {}


def register_model(
    name: str,
    model_id: str,
    *,
    provider: str,
    profile: Optional[str] = None,
    region: Optional[str] = None,
    auth_type: Optional[str] = None,
    max_concurrency: int = 4,
    timeout: float = 60.0,
    max_retries: int = 2,
    max_tokens: int = 2048,
    temperature: float = 0.0,
) -> None:
    """Register a model alias for subsequent calls to :func:`ai_query`.

    Use ``provider="databricks"`` with a serving endpoint name or ``provider="bedrock"`` with
    a model or inference profile ID. Workers resolve credentials through the provider SDK.
    Bedrock accepts an optional AWS region and profile. Databricks accepts a profile and
    either ``oauth-m2m`` (the default) or ``databricks-cli`` for OAuth.
    Concurrency is limited per Python task. Retries and repeated Spark actions can make
    additional billable calls.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError("name must be a non-empty string")
    _endpoints[name] = _ModelEndpoint(
        model_id=model_id,
        provider=provider,
        profile=profile,
        region=region,
        auth_type=auth_type,
        max_concurrency=max_concurrency,
        timeout=timeout,
        max_retries=max_retries,
        max_tokens=max_tokens,
        temperature=temperature,
    )


def register_agent(
    name: str,
    app_name: str,
    *,
    profile: Optional[str] = None,
    output_field: Optional[str] = None,
    max_concurrency: int = 4,
    timeout: float = 60.0,
    max_retries: int = 2,
    auth_type: str = "oauth-m2m",
) -> None:
    """Register an agent configuration in this Python process, without contacting Databricks.

    The model is configured in the deployed agent. Workers require ``databricks-sdk``,
    ``requests``, and OAuth M2M credentials, supplied through their environment or a profile.
    Credentials are resolved on the workers and are never stored in this registry.
    Set ``auth_type="databricks-cli"`` to reuse a CLI OAuth login for local development;
    the CLI and its authenticated profile must then be available on every worker.

    ``output_field`` selects a top-level string field in a custom agent's output. Otherwise,
    the agent must return a string or the assistant messages produced by AgentKit templates.
    ``timeout`` is the invocation time budget in seconds, excluding initial client setup.
    ``max_retries`` limits transient HTTP and connection retries. ``max_concurrency`` bounds
    requests per active Python task; total concurrency grows with the number of Spark tasks.

    Registering the same name replaces its configuration for subsequently constructed columns.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError("name must be a non-empty string")
    _endpoints[name] = _AgentEndpoint(
        app_name, profile, output_field, max_concurrency, timeout, max_retries, auth_type
    )


def ai_query(
    endpoint: str, request: "ColumnOrName", *, invocation_id: Optional["ColumnOrName"] = None
) -> Column:
    """Return a text column by invoking a registered model or agent for each non-null request.

    Direct model inference needs only ``endpoint`` and ``request``. ``invocation_id`` is required
    only for agent endpoints and is rejected for direct models, which offer no deduplication.

    For agent endpoints, ``request`` and ``invocation_id`` are string columns or column names.
    The latter must
    contain stable, non-empty identifiers, unique per logical input within the job namespace.
    Include a job or dataset version in these identifiers to control reuse across actions.
    Do not generate them with a random expression or task/partition identifiers.

    The app name, input identifier, and request determine a stable UUID. Retries use this UUID
    as the invocation ID and as an independent session ID. Idempotency applies only while
    Databricks retains the invocation record; this is not an exactly-once guarantee for tools.

    Null requests return null without a network call. Invalid identifiers and invocation errors
    fail the task. Spark may repeat calls on retries or subsequent actions. This prototype
    supports synchronous text generation only, without conversational or human-input workflows.
    """
    from pyspark.sql.functions import PandasUDFType, lit, pandas_udf
    from pyspark.sql.types import StringType

    if not isinstance(endpoint, str) or endpoint not in _endpoints:
        raise ValueError("Register the endpoint before ai_query")
    config = _endpoints[endpoint]
    if isinstance(config, _ModelEndpoint):
        if invocation_id is not None:
            raise ValueError("invocation_id is only supported by agent endpoints")
        invocation_id = lit("")
    elif invocation_id is None:
        raise ValueError("Agent endpoints require a stable invocation_id column")

    def query(batches):
        yield from _query_batches(config, batches)

    query_udf = pandas_udf(
        query, returnType=StringType(), functionType=PandasUDFType.SCALAR_ITER
    ).asNondeterministic()
    return query_udf(request, invocation_id)
