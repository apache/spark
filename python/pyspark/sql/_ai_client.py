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
from typing import Any, Optional
from urllib.parse import quote, urlsplit


@dataclass(frozen=True)
class _AgentEndpoint:
    app_name: str
    profile: Optional[str] = None
    output_field: Optional[str] = None
    max_concurrency: int = 4
    timeout: float = 60.0
    max_retries: int = 2
    auth_type: str = "oauth-m2m"

    def __post_init__(self):
        _validate_endpoint_options(self)
        _validate_oauth(self.auth_type)
        for name in ("app_name", "output_field"):
            value = getattr(self, name)
            if (name == "app_name" or value is not None) and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(f"{name} must be a non-empty string")


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

    def __post_init__(self):
        _validate_endpoint_options(self)
        if not isinstance(self.model_id, str) or not self.model_id.strip():
            raise ValueError("model_id must be a non-empty string")
        if self.provider == "databricks":
            if self.auth_type is not None:
                _validate_oauth(self.auth_type)
            if self.region is not None:
                raise ValueError("region is only supported by Bedrock")
            max_temperature = 2
        elif self.provider == "bedrock":
            if self.auth_type is not None:
                raise ValueError("Bedrock resolves authentication through the AWS SDK")
            if self.region is not None and (
                not isinstance(self.region, str) or not self.region.strip()
            ):
                raise ValueError("region must be a non-empty string")
            max_temperature = 1
        else:
            raise ValueError("provider must be databricks or bedrock")
        if type(self.max_tokens) is not int or self.max_tokens < 1:
            raise ValueError("max_tokens must be a positive integer")
        if (
            isinstance(self.temperature, bool)
            or not isinstance(self.temperature, (int, float))
            or not math.isfinite(self.temperature)
            or not 0 <= self.temperature <= max_temperature
        ):
            raise ValueError(f"temperature must be a finite number between 0 and {max_temperature}")


def _validate_oauth(auth_type):
    if auth_type not in ("oauth-m2m", "databricks-cli"):
        raise ValueError("auth_type must be oauth-m2m or databricks-cli")


def _validate_endpoint_options(endpoint):
    if endpoint.profile is not None and (
        not isinstance(endpoint.profile, str) or not endpoint.profile.strip()
    ):
        raise ValueError("profile must be a non-empty string")
    for name, minimum in (("max_concurrency", 1), ("max_retries", 0)):
        value = getattr(endpoint, name)
        if type(value) is not int or value < minimum:
            raise ValueError(f"{name} must be an integer >= {minimum}")
    if (
        isinstance(endpoint.timeout, bool)
        or not isinstance(endpoint.timeout, (int, float))
        or not math.isfinite(endpoint.timeout)
        or endpoint.timeout <= 0
    ):
        raise ValueError("timeout must be a finite positive number")


def _retry_delay(headers, failures, deadline):
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
    def __init__(self, endpoint: _ModelEndpoint):
        import requests
        from databricks.sdk import WorkspaceClient

        self.endpoint = endpoint
        self.workspace = WorkspaceClient(
            profile=endpoint.profile, auth_type=endpoint.auth_type or "oauth-m2m"
        )
        host = self.workspace.config.host
        if urlsplit(host).scheme != "https":
            raise ValueError("Databricks workspace URL must use HTTPS")
        self.url = host.rstrip("/") + "/serving-endpoints/" + quote(endpoint.model_id, safe="")
        self.url += "/invocations"
        self.http = requests.Session()

    def close(self):
        self.http.close()

    def invoke(self, request: str, row_id=None) -> str:
        import requests

        body = {
            "messages": [{"role": "user", "content": request}],
            "max_tokens": self.endpoint.max_tokens,
            "temperature": self.endpoint.temperature,
        }
        deadline = time.monotonic() + self.endpoint.timeout
        attempt = 0
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Databricks model invocation timed out")
            retry_headers = {}
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
                            raise RuntimeError(f"Databricks model HTTP {status}; retries exhausted")
                        retry_headers = response.headers
                    elif not 200 <= status < 300:
                        raise RuntimeError(f"Databricks model HTTP {status}")
                    else:
                        try:
                            text = response.json()["choices"][0]["message"]["content"]
                        except (KeyError, IndexError, TypeError, ValueError):
                            raise ValueError("Invalid Databricks model response") from None
                        if not isinstance(text, str) or not text.strip():
                            raise ValueError("Model returned no text; check max_tokens")
                        return text
            except (requests.Timeout, requests.ConnectionError):
                if attempt == self.endpoint.max_retries:
                    raise RuntimeError(
                        "Databricks model connection failed; retries exhausted"
                    ) from None
            time.sleep(_retry_delay(retry_headers, attempt, deadline))
            attempt += 1


class _ConverseClient:
    def __init__(self, endpoint: _ModelEndpoint):
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

    def close(self):
        self.client.close()

    def invoke(self, request: str, row_id=None) -> str:
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
            raise ValueError("Model returned no text; check max_tokens")
        return text


def _request_id(app_name: str, row_id: str, request: str) -> str:
    if not isinstance(row_id, str) or not row_id.strip():
        raise ValueError("invocation_id must contain non-empty stable strings")
    if not isinstance(request, str):
        raise TypeError("request must contain strings or nulls")
    key = json.dumps([app_name, row_id, request], ensure_ascii=True, separators=(",", ":"))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def _extract_text(output: Any, output_field: Optional[str]) -> str:
    if output_field is not None:
        if not isinstance(output, dict):
            raise ValueError("Agent output must be an object when output_field is configured")
        output = output.get(output_field)
    if isinstance(output, str):
        return output
    if isinstance(output, dict) and output_field is None:
        if output.get("status") == "interrupted":
            raise RuntimeError("Agent requires human input; ai_query cannot resume it")
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
    raise ValueError("Agent did not return text; configure output_field for a custom output")


class _AgentClient:
    def __init__(self, endpoint: _AgentEndpoint):
        import requests
        from databricks.sdk import WorkspaceClient

        # Resolve credentials on the worker, never while building the Spark plan.
        self.endpoint = endpoint
        self.workspace = WorkspaceClient(profile=endpoint.profile, auth_type=endpoint.auth_type)
        app_url = self.workspace.apps.get(endpoint.app_name).url
        parsed = urlsplit(app_url or "")
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Databricks must return an HTTPS app URL without credentials or query")
        self.url = app_url.rstrip("/") + "/api/invocations"
        self.http = requests.Session()

    def close(self):
        self.http.close()

    def invoke(self, request: str, row_id: str) -> str:
        import requests

        invocation_id = _request_id(self.endpoint.app_name, row_id, request)
        body = {
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
                raise TimeoutError("Databricks agent invocation timed out")
            headers = self.workspace.config.authenticate()
            headers["X-Routing-Key"] = invocation_id
            retry_headers = {}
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
                            raise RuntimeError(f"Databricks agent HTTP {status}; retries exhausted")
                        retry_headers = response.headers
                    elif not 200 <= status < 300:
                        # Do not include response bodies, prompts, or credentials in task errors.
                        raise RuntimeError(f"Databricks agent HTTP {status}")
                    else:
                        result = response.json()
                        if not isinstance(result, dict) or result.get("id") != invocation_id:
                            raise ValueError("Invalid Databricks invocation response")
                        state = result.get("status")
                        if state == "completed":
                            return _extract_text(result.get("output"), self.endpoint.output_field)
                        if state not in ("pending", "queued", "running"):
                            raise RuntimeError("Databricks agent did not complete successfully")
                        poll = True
                        time.sleep(min(0.2, max(0, deadline - time.monotonic())))
                        continue
            except (requests.Timeout, requests.ConnectionError):
                if failures >= self.endpoint.max_retries:
                    raise RuntimeError(
                        "Databricks agent connection failed; retries exhausted"
                    ) from None
            delay = _retry_delay(retry_headers, failures, deadline)
            failures += 1
            time.sleep(delay)


def _query_batches(endpoint, batches):
    import pandas as pd

    local = threading.local()
    clients = []
    lock = threading.Lock()
    if isinstance(endpoint, _ModelEndpoint):
        factory = _ConverseClient if endpoint.provider == "bedrock" else _ChatCompletionsClient
    else:
        factory = _AgentClient

    def invoke(row):
        request, row_id = row
        if pd.isna(request):
            return None
        # Validate before resolving credentials or making any request.
        if isinstance(endpoint, _ModelEndpoint):
            if not isinstance(request, str):
                raise TypeError("request must contain strings or nulls")
        else:
            _request_id(endpoint.app_name, row_id, request)
        if not hasattr(local, "client"):
            local.client = factory(endpoint)
            with lock:
                clients.append(local.client)
        return local.client.invoke(request, row_id)

    try:
        with ThreadPoolExecutor(max_workers=endpoint.max_concurrency) as pool:
            for requests, row_ids in batches:
                # Chunk submission as well as execution to bound outstanding work.
                rows = zip(requests, row_ids)
                values = []
                while True:
                    chunk = list(islice(rows, endpoint.max_concurrency))
                    if not chunk:
                        break
                    values.extend(pool.map(invoke, chunk))
                yield pd.Series(values, dtype="object")
    finally:
        for client in clients:
            client.close()
