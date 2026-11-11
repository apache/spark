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

"""Worker-side transport for the experimental AI query API."""

import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import islice
from typing import TYPE_CHECKING, Any, Iterator, Mapping, Optional, Union
from urllib.parse import quote, urlsplit

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError

if TYPE_CHECKING:
    import pandas as pd
    from requests import Session


@dataclass(frozen=True)
class _ModelEndpoint:
    model_id: str
    provider: str
    profile: Optional[str] = None
    region: Optional[str] = None
    max_concurrency: int = 4
    timeout: float = 60.0
    max_retries: int = 2
    auth_type: Optional[str] = None
    max_tokens: int = 2048
    temperature: float = 0.0

    def __post_init__(self) -> None:
        _validate_endpoint_options(self)
        _validate_non_empty_string("model_id", self.model_id)
        if self.provider == "databricks":
            if self.auth_type is not None:
                _validate_oauth(self.auth_type)
            if self.region is not None:
                raise PySparkValueError(
                    errorClass="AI_QUERY_UNSUPPORTED_OPTION",
                    messageParameters={"arg_name": "region", "target": "Databricks models"},
                )
            max_temperature = 2
        elif self.provider == "bedrock":
            if self.auth_type is not None:
                raise PySparkValueError(
                    errorClass="AI_QUERY_UNSUPPORTED_OPTION",
                    messageParameters={"arg_name": "auth_type", "target": "Bedrock models"},
                )
            if self.region is not None:
                _validate_non_empty_string("region", self.region)
            max_temperature = 1
        else:
            raise PySparkValueError(
                errorClass="VALUE_NOT_ALLOWED",
                messageParameters={"arg_name": "provider", "allowed_values": "databricks, bedrock"},
            )
        _validate_integer("max_tokens", self.max_tokens, 1)
        if (
            isinstance(self.temperature, bool)
            or not isinstance(self.temperature, (int, float))
            or not math.isfinite(self.temperature)
            or not 0 <= self.temperature <= max_temperature
        ):
            raise PySparkValueError(
                errorClass="VALUE_NOT_BETWEEN",
                messageParameters={
                    "arg_name": "temperature",
                    "min": "0",
                    "max": str(max_temperature),
                },
            )


def _validate_non_empty_string(name: str, value: object) -> None:
    if not isinstance(value, str):
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": name,
                "expected_type": "str",
                "arg_type": type(value).__name__,
            },
        )
    if not value.strip():
        raise PySparkValueError(
            errorClass="VALUE_NOT_NON_EMPTY_STR",
            messageParameters={"arg_name": name, "arg_value": value},
        )


def _validate_integer(name: str, value: object, minimum: int) -> None:
    if type(value) is not int:
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": name,
                "expected_type": "int",
                "arg_type": type(value).__name__,
            },
        )
    if value < minimum:
        raise PySparkValueError(
            errorClass="VALUE_NOT_POSITIVE" if minimum else "NEGATIVE_VALUE",
            messageParameters={"arg_name": name, "arg_value": str(value)},
        )


def _invocation_error(target: str, condition: str, **parameters: str) -> PySparkRuntimeError:
    return PySparkRuntimeError(
        errorClass=f"AI_QUERY_INVOCATION_FAILED.{condition}",
        messageParameters={"target": target, **parameters},
    )


def _validate_oauth(auth_type: str) -> None:
    if auth_type not in ("oauth-m2m", "databricks-cli"):
        raise PySparkValueError(
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={
                "arg_name": "auth_type",
                "allowed_values": "oauth-m2m, databricks-cli",
            },
        )


def _validate_endpoint_options(endpoint: _ModelEndpoint) -> None:
    if endpoint.profile is not None:
        _validate_non_empty_string("profile", endpoint.profile)
    for name, minimum in (("max_concurrency", 1), ("max_retries", 0)):
        _validate_integer(name, getattr(endpoint, name), minimum)
    if (
        isinstance(endpoint.timeout, bool)
        or not isinstance(endpoint.timeout, (int, float))
        or not math.isfinite(endpoint.timeout)
        or endpoint.timeout <= 0
    ):
        raise PySparkValueError(errorClass="AI_QUERY_INVALID_TIMEOUT", messageParameters={})


def _retry_delay(headers: Mapping[str, str], failures: int, deadline: float) -> float:
    try:
        retry_after = float(headers.get("Retry-After", "0"))
        if not math.isfinite(retry_after):
            retry_after = 0.0
    except ValueError:
        retry_after = 0.0
    return min(
        max(2 ** min(failures, 3), retry_after),
        max(0, deadline - time.monotonic()),
    )


def _databricks_session() -> "Session":
    import requests

    session = requests.Session()
    # Keep the SDK's Authorization header even when a matching netrc entry exists.
    session.auth = lambda request: request
    return session


def _databricks_url(url: Optional[str]) -> str:
    try:
        parsed = urlsplit(url or "")
        port = parsed.port
    except ValueError:
        raise _invocation_error("Databricks model", "INVALID_URL") from None
    if (
        not url
        or parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or "?" in url
        or "#" in url
        or port == 0
    ):
        raise _invocation_error("Databricks model", "INVALID_URL")
    return url.rstrip("/")


class _ChatCompletionsClient:
    def __init__(self, endpoint: _ModelEndpoint) -> None:
        from databricks.sdk import WorkspaceClient

        self.endpoint = endpoint
        self.workspace = WorkspaceClient(
            profile=endpoint.profile, auth_type=endpoint.auth_type or "oauth-m2m"
        )
        host = _databricks_url(self.workspace.config.host)
        self.url = host + "/serving-endpoints/" + quote(endpoint.model_id, safe="") + "/invocations"
        self.http = _databricks_session()

    def close(self) -> None:
        self.http.close()

    def invoke(self, request: str) -> str:
        import requests

        body: dict[str, Any] = {
            "messages": [{"role": "user", "content": request}],
            "max_tokens": self.endpoint.max_tokens,
            "temperature": self.endpoint.temperature,
        }
        deadline = time.monotonic() + self.endpoint.timeout
        attempt = 0
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise _invocation_error("Databricks model", "TIMEOUT")
            retry_headers: Mapping[str, str] = {}
            try:
                with self.http.post(
                    self.url,
                    headers=self.workspace.config.authenticate(),
                    json=body,
                    timeout=remaining,
                    allow_redirects=False,
                ) as response:
                    status = response.status_code
                    if status == 429 or status in (500, 502, 503, 504):
                        if attempt == self.endpoint.max_retries:
                            raise _invocation_error(
                                "Databricks model", "HTTP_RETRIES_EXHAUSTED", status=str(status)
                            )
                        retry_headers = response.headers
                    elif not 200 <= status < 300:
                        raise _invocation_error(
                            "Databricks model", "HTTP_ERROR", status=str(status)
                        )
                    else:
                        try:
                            text = response.json()["choices"][0]["message"]["content"]
                        except (KeyError, IndexError, TypeError, ValueError):
                            raise _invocation_error(
                                "Databricks model", "INVALID_RESPONSE"
                            ) from None
                        if not isinstance(text, str) or not text.strip():
                            raise _invocation_error("Databricks model", "EMPTY_RESPONSE")
                        return text
            except requests.exceptions.InvalidProxyURL:
                raise
            except requests.exceptions.InvalidURL:
                raise _invocation_error("Databricks model", "INVALID_URL") from None
            except (requests.Timeout, requests.ConnectionError):
                if attempt == self.endpoint.max_retries:
                    raise _invocation_error("Databricks model", "CONNECTION_ERROR") from None
            time.sleep(_retry_delay(retry_headers, attempt, deadline))
            attempt += 1


class _ConverseClient:
    def __init__(self, endpoint: _ModelEndpoint) -> None:
        import boto3
        from botocore.config import Config

        self.endpoint = endpoint
        session = boto3.Session(profile_name=endpoint.profile, region_name=endpoint.region)
        self.client = session.client(
            "bedrock-runtime",
            config=Config(
                connect_timeout=min(5, endpoint.timeout),
                read_timeout=endpoint.timeout,
                retries={"mode": "adaptive", "total_max_attempts": endpoint.max_retries + 1},
            ),
        )

    def close(self) -> None:
        self.client.close()

    def invoke(self, request: str) -> str:
        response = self.client.converse(
            modelId=self.endpoint.model_id,
            messages=[{"role": "user", "content": [{"text": request}]}],
            inferenceConfig={
                "maxTokens": self.endpoint.max_tokens,
                "temperature": self.endpoint.temperature,
            },
        )
        content = response.get("output", {}).get("message", {}).get("content", [])
        text = "".join(block["text"] for block in content if "text" in block)
        if not text.strip():
            raise _invocation_error("Bedrock model", "EMPTY_RESPONSE")
        return text


def _query_batches(
    endpoint: _ModelEndpoint,
    batches: Iterator["pd.Series"],
) -> Iterator["pd.Series"]:
    import pandas as pd

    local = threading.local()
    clients: list[Union[_ConverseClient, _ChatCompletionsClient]] = []
    lock = threading.Lock()
    factory = _ConverseClient if endpoint.provider == "bedrock" else _ChatCompletionsClient

    def invoke(request: Any) -> Optional[str]:
        if pd.api.types.is_scalar(request) and pd.isna(request):
            return None
        if not isinstance(request, str):
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": "request",
                    "expected_type": "str",
                    "arg_type": type(request).__name__,
                },
            )
        if not hasattr(local, "client"):
            local.client = factory(endpoint)
            with lock:
                clients.append(local.client)
        return local.client.invoke(request)

    batch_failed = False
    try:
        with ThreadPoolExecutor(max_workers=endpoint.max_concurrency) as pool:
            for requests in batches:
                # Chunk submission as well as execution to bound outstanding work.
                rows = iter(requests)
                values: list[Optional[str]] = []
                while True:
                    chunk = list(islice(rows, endpoint.max_concurrency))
                    if not chunk:
                        break
                    values.extend(pool.map(invoke, chunk))
                yield pd.Series(values, dtype="object")
    except BaseException:
        batch_failed = True
        raise
    finally:
        close_error = None
        for client in clients:
            try:
                client.close()
            except Exception as error:
                if close_error is None:
                    close_error = error
        if not batch_failed and close_error is not None:
            raise close_error
