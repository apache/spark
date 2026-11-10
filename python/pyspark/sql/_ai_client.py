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

import json
import math
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import islice
from typing import TYPE_CHECKING, Any, Iterator, Mapping, Optional, Tuple, Union
from urllib.parse import quote, urlsplit

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError

if TYPE_CHECKING:
    import pandas as pd


@dataclass(frozen=True)
class _AgentEndpoint:
    app_name: str
    profile: Optional[str] = None
    output_field: Optional[str] = None
    max_concurrency: int = 4
    timeout: float = 60.0
    max_retries: int = 2
    auth_type: str = "oauth-m2m"

    def __post_init__(self) -> None:
        _validate_endpoint_options(self)
        _validate_oauth(self.auth_type)
        for name in ("app_name", "output_field"):
            value = getattr(self, name)
            if name == "app_name" or value is not None:
                _validate_non_empty_string(name, value)


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


def _validate_endpoint_options(endpoint: Union[_AgentEndpoint, _ModelEndpoint]) -> None:
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


class _ChatCompletionsClient:
    def __init__(self, endpoint: _ModelEndpoint) -> None:
        import requests
        from databricks.sdk import WorkspaceClient

        self.endpoint = endpoint
        self.workspace = WorkspaceClient(
            profile=endpoint.profile, auth_type=endpoint.auth_type or "oauth-m2m"
        )
        host = self.workspace.config.host
        if urlsplit(host).scheme != "https":
            raise _invocation_error("Databricks model", "INVALID_URL")
        self.url = host.rstrip("/") + "/serving-endpoints/" + quote(endpoint.model_id, safe="")
        self.url += "/invocations"
        self.http = requests.Session()

    def close(self) -> None:
        self.http.close()

    def invoke(self, request: str, row_id: Optional[str] = None) -> str:
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

    def invoke(self, request: str, row_id: Optional[str] = None) -> str:
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


def _request_id(app_name: str, row_id: str, request: str) -> str:
    _validate_non_empty_string("invocation_id", row_id)
    if not isinstance(request, str):
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "request",
                "expected_type": "str",
                "arg_type": type(request).__name__,
            },
        )
    key = json.dumps([app_name, row_id, request], ensure_ascii=True, separators=(",", ":"))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def _extract_text(output: Any, output_field: Optional[str]) -> str:
    if output_field is not None:
        if not isinstance(output, dict):
            raise _invocation_error("Databricks agent", "INVALID_RESPONSE")
        output = output.get(output_field)
    if isinstance(output, str):
        return output
    if isinstance(output, dict) and output_field is None:
        if output.get("status") == "interrupted":
            raise _invocation_error("Databricks agent", "HUMAN_INPUT_REQUIRED")
        messages = output.get("output")
        if isinstance(messages, list):
            texts = []
            for message in messages:
                if not isinstance(message, dict) or message.get("role") != "assistant":
                    continue
                content = message.get("content")
                if isinstance(content, str):
                    texts.append(content)
                elif isinstance(content, list):
                    for item in content:
                        if (
                            isinstance(item, dict)
                            and item.get("type") in ("text", "output_text")
                            and isinstance(item.get("text"), str)
                        ):
                            texts.append(item["text"])
            if texts:
                return "\n".join(texts)
    raise _invocation_error("Databricks agent", "EMPTY_RESPONSE")


class _AgentClient:
    def __init__(self, endpoint: _AgentEndpoint) -> None:
        import requests
        from databricks.sdk import WorkspaceClient

        # Resolve credentials on the worker, never while building the Spark plan.
        self.endpoint = endpoint
        self.workspace = WorkspaceClient(profile=endpoint.profile, auth_type=endpoint.auth_type)
        app_url = self.workspace.apps.get(endpoint.app_name).url
        parsed = urlsplit(app_url or "")
        if (
            not app_url
            or parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise _invocation_error("Databricks agent", "INVALID_URL")
        self.url = app_url.rstrip("/") + "/api/invocations"
        self.http = requests.Session()

    def close(self) -> None:
        self.http.close()

    def invoke(self, request: str, row_id: str) -> str:
        import requests

        invocation_id = _request_id(self.endpoint.app_name, row_id, request)
        body: dict[str, Any] = {
            "id": invocation_id,
            "session_id": invocation_id,
            "input": {"messages": [{"role": "user", "content": request}]},
        }
        deadline = time.monotonic() + self.endpoint.timeout
        failures = 0
        poll = False
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise _invocation_error("Databricks agent", "TIMEOUT")
            headers = self.workspace.config.authenticate()
            headers["X-Routing-Key"] = invocation_id
            retry_headers: Mapping[str, str] = {}
            try:
                with self.http.request(
                    "GET" if poll else "POST",
                    self.url + "/" + invocation_id if poll else self.url,
                    headers=headers,
                    json=None if poll else body,
                    timeout=remaining,
                    allow_redirects=False,
                ) as response:
                    status = response.status_code
                    if status == 429 or status in (500, 502, 503, 504):
                        if failures >= self.endpoint.max_retries:
                            raise _invocation_error(
                                "Databricks agent", "HTTP_RETRIES_EXHAUSTED", status=str(status)
                            )
                        retry_headers = response.headers
                    elif not 200 <= status < 300:
                        # Do not include response bodies, prompts, or credentials in task errors.
                        raise _invocation_error(
                            "Databricks agent", "HTTP_ERROR", status=str(status)
                        )
                    else:
                        try:
                            result = response.json()
                        except ValueError:
                            raise _invocation_error(
                                "Databricks agent", "INVALID_RESPONSE"
                            ) from None
                        if not isinstance(result, dict) or result.get("id") != invocation_id:
                            raise _invocation_error("Databricks agent", "INVALID_RESPONSE")
                        state = result.get("status")
                        if state == "completed":
                            return _extract_text(result.get("output"), self.endpoint.output_field)
                        if state not in ("pending", "queued", "running"):
                            raise _invocation_error("Databricks agent", "AGENT_RUN_FAILED")
                        poll = True
                        time.sleep(min(0.2, max(0, deadline - time.monotonic())))
                        continue
            except (requests.Timeout, requests.ConnectionError):
                if failures >= self.endpoint.max_retries:
                    raise _invocation_error("Databricks agent", "CONNECTION_ERROR") from None
            delay = _retry_delay(retry_headers, failures, deadline)
            failures += 1
            time.sleep(delay)


def _query_batches(
    endpoint: Union[_AgentEndpoint, _ModelEndpoint],
    batches: Iterator[Tuple["pd.Series", "pd.Series"]],
) -> Iterator["pd.Series"]:
    import pandas as pd

    local = threading.local()
    clients: list[Union[_ConverseClient, _ChatCompletionsClient, _AgentClient]] = []
    lock = threading.Lock()

    def invoke(row: Tuple[Any, Any]) -> Optional[str]:
        request, row_id = row
        if pd.api.types.is_scalar(request) and pd.isna(request):
            return None
        # Validate before resolving credentials or making any request.
        if isinstance(endpoint, _ModelEndpoint):
            if not isinstance(request, str):
                raise PySparkTypeError(
                    errorClass="NOT_EXPECTED_TYPE",
                    messageParameters={
                        "arg_name": "request",
                        "expected_type": "str",
                        "arg_type": type(request).__name__,
                    },
                )
        else:
            _request_id(endpoint.app_name, row_id, request)
        if not hasattr(local, "client"):
            if isinstance(endpoint, _AgentEndpoint):
                local.client = _AgentClient(endpoint)
            elif endpoint.provider == "bedrock":
                local.client = _ConverseClient(endpoint)
            else:
                local.client = _ChatCompletionsClient(endpoint)
            with lock:
                clients.append(local.client)
        return local.client.invoke(request, row_id)

    try:
        with ThreadPoolExecutor(max_workers=endpoint.max_concurrency) as pool:
            for requests, row_ids in batches:
                # Chunk submission as well as execution to bound outstanding work.
                rows = zip(requests, row_ids)
                values: list[Optional[str]] = []
                while True:
                    chunk = list(islice(rows, endpoint.max_concurrency))
                    if not chunk:
                        break
                    values.extend(pool.map(invoke, chunk))
                yield pd.Series(values, dtype="object")
    finally:
        for client in clients:
            client.close()
