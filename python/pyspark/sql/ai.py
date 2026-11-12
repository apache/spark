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

from typing import TYPE_CHECKING, Dict, Iterator, Optional, cast

from pyspark.errors import PySparkTypeError, PySparkValueError
from pyspark.sql._ai_client import (
    _ModelEndpoint,
    _query_batches,
    _validate_non_empty_string,
)
from pyspark.sql.column import Column

if TYPE_CHECKING:
    import pandas as pd

    from pyspark.sql._typing import ColumnOrName
    from pyspark.sql.session import SparkSession

__all__ = ["ai_query", "register_model"]


def _model_registry(session: Optional["SparkSession"]) -> Dict[str, _ModelEndpoint]:
    if session is None:
        from pyspark.sql.session import SparkSession

        session = SparkSession.active()
    return cast(Dict[str, _ModelEndpoint], session.__dict__.setdefault("_ai_model_registry", {}))


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
    session: Optional["SparkSession"] = None,
) -> None:
    """Register a model alias for subsequent calls to :func:`ai_query`.

    Workers resolve credentials through the provider SDK. Aliases belong to the selected
    Spark session. Registering the same name replaces its configuration for subsequently
    constructed columns in that session.

    .. versionadded:: 4.4.0

    Parameters
    ----------
    name : str
        Alias to use in :func:`ai_query`.
    model_id : str
        Databricks serving endpoint name or Bedrock model or inference profile ID.
    provider : str
        Either ``databricks`` or ``bedrock``.
    profile : str, optional
        Credential profile available on each worker.
    region : str, optional
        AWS region for Bedrock.
    auth_type : str, optional
        Databricks OAuth authentication type, either ``oauth-m2m`` (the default)
        or ``databricks-cli``.
    max_concurrency : int, optional
        Maximum concurrent invocations per Python task.
    timeout : float, optional
        Databricks per-row budget across authentication, HTTP attempts, and retry delays,
        or Bedrock SDK connection and read timeout, in seconds. Databricks checks the
        budget after authentication and before returning a successful response. Its
        HTTP timeout limits connection and read inactivity. Authentication and an active
        response download can exceed the budget before an error is reported.
    max_retries : int, optional
        Maximum retries for a transient failure. Retries can incur additional charges.
    max_tokens : int, optional
        Maximum number of generated tokens.
    temperature : float, optional
        Sampling temperature. The maximum is 2 for Databricks and 1 for Bedrock.
    session : :class:`~pyspark.sql.SparkSession`, optional
        Session that owns the alias. Defaults to the active or default session.

    Examples
    --------
    >>> from pyspark.sql.ai import register_model
    >>> register_model("summarizer", "databricks-gpt-oss-20b", provider="databricks")
    """
    _validate_non_empty_string("name", name)
    _model_registry(session)[name] = _ModelEndpoint(
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


def ai_query(
    endpoint: str,
    request: "ColumnOrName",
    *,
    on_error: str = "raise",
    session: Optional["SparkSession"] = None,
) -> Column:
    """Return a text column by invoking a registered model for each non-null request.

    The column captures the endpoint configuration when it is constructed. Spark may repeat
    calls on task retries or subsequent actions. Apply filters and limits to the input before
    adding this column to control how many rows can reach the model.

    .. versionadded:: 4.4.0

    Parameters
    ----------
    endpoint : str
        Alias registered with :func:`register_model`.
    request : :class:`~pyspark.sql.Column` or str
        String column or column name containing the prompt. Nulls produce null results
        without a network call. Use :meth:`~pyspark.sql.Column.cast` for other input types.
    on_error : str, optional
        How to handle a model invocation error. ``"raise"`` (the default) fails the task.
        ``"null"`` returns null for known inference and transient failures. Invalid input,
        client initialization, authentication, and permission errors fail the task.
    session : :class:`~pyspark.sql.SparkSession`, optional
        Session that owns the model alias. Defaults to the active or default session.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        String column containing the response to each request.

    Examples
    --------
    >>> from pyspark.sql.ai import ai_query, register_model
    >>> from pyspark.sql.functions import col, concat, lit
    >>> register_model("summarizer", "databricks-gpt-oss-20b", provider="databricks")
    >>> df = spark.createDataFrame([("A document",)], ["description"])
    >>> result = df.withColumn(
    ...     "summary",
    ...     ai_query("summarizer", concat(lit("Summarize: "), col("description"))),
    ... )
    """
    from pyspark.sql.functions import PandasUDFType, pandas_udf
    from pyspark.sql.types import StringType

    if not isinstance(endpoint, str):
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "endpoint",
                "expected_type": "str",
                "arg_type": type(endpoint).__name__,
            },
        )
    registry = _model_registry(session)
    if endpoint not in registry:
        raise PySparkValueError(
            errorClass="AI_QUERY_ENDPOINT_NOT_REGISTERED",
            messageParameters={"endpoint": endpoint},
        )
    if not isinstance(on_error, str):
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "on_error",
                "expected_type": "str",
                "arg_type": type(on_error).__name__,
            },
        )
    if on_error not in ("raise", "null"):
        raise PySparkValueError(
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={"arg_name": "on_error", "allowed_values": "raise, null"},
        )
    config = registry[endpoint]

    def query(batches: Iterator["pd.Series"]) -> Iterator["pd.Series"]:
        yield from _query_batches(config, batches, on_error=on_error)

    query_udf = pandas_udf(
        query, returnType=StringType(), functionType=PandasUDFType.SCALAR_ITER
    ).asNondeterministic()
    return query_udf(request)
