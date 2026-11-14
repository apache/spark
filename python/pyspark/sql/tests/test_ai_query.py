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

import gzip
import json
import threading
import time
import traceback
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, Mock, patch

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError, PythonException
from pyspark.sql._ai_client import (
    _ChatCompletionsClient,
    _ConverseClient,
    _ModelEndpoint,
    _query_batches,
    _retry_delay,
)
from pyspark.sql.ai import ai_query, register_model, register_sql_function
from pyspark.testing import sqlutils
from pyspark.testing.utils import PySparkErrorTestUtils, have_pyarrow

try:
    import pandas as pd
    import requests
except ImportError:
    have_ai_query_dependencies = False
else:
    have_ai_query_dependencies = True


class _BotoCoreError(Exception):
    pass


class _ClientError(Exception):
    pass


def _local_query_functions(origin):
    import requests

    session_type = requests.Session

    @contextmanager
    def local_session(endpoint, *, on_error="raise"):
        from urllib.parse import urlsplit

        import requests

        from pyspark.sql import _ai_client

        config = SimpleNamespace(
            host="https://example.test",
            authenticate=lambda: {"Authorization": "Bearer test"},
        )
        sdk = SimpleNamespace(WorkspaceClient=lambda **kwargs: SimpleNamespace(config=config))

        class LocalAdapter(requests.adapters.HTTPAdapter):
            def send(self, request, **kwargs):
                request.url = origin + urlsplit(request.url).path
                kwargs["proxies"] = {}
                return super().send(request, **kwargs)

        def make_session():
            session = session_type()
            session.mount(config.host, LocalAdapter())
            return session

        with (
            patch.dict("sys.modules", {"databricks.sdk": sdk}),
            patch("requests.Session", side_effect=make_session),
            _ai_client._query_session(endpoint, on_error=on_error) as query,
        ):
            yield query

    def local_batches(endpoint, batches, *, on_error="raise"):
        with local_session(endpoint, on_error=on_error) as query:
            for batch in batches:
                yield query(batch)

    return local_session, local_batches


@contextmanager
def _model_http_server():
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            prompt = body["messages"][0]["content"]
            model = self.path.split("/")[2]
            assert self.path == f"/serving-endpoints/{model}/invocations"
            assert self.headers.get("Authorization") == "Bearer test"
            received.append((model, prompt))
            status = {"reject": 400, "forbidden": 403, "unavailable": 503}.get(prompt, 200)
            response = json.dumps(
                {"choices": [{"message": {"content": f"{model}:{prompt.upper()}"}}]}
            ).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}", received
        finally:
            server.shutdown()
            thread.join()


@unittest.skipUnless(have_ai_query_dependencies, "AI query tests require pandas and requests")
class AIQueryTests(unittest.TestCase, PySparkErrorTestUtils):
    def setUp(self):
        super().setUp()
        active = patch("pyspark.sql.session.SparkSession.active", return_value=SimpleNamespace())
        active.start()
        self.addCleanup(active.stop)

    def client(self, **options):
        client = object.__new__(_ChatCompletionsClient)
        client.endpoint = _ModelEndpoint("model", provider="databricks", **options)
        client.url = "https://example.test/serving-endpoints/model/invocations"
        client.workspace = Mock()
        client.workspace.config.authenticate.side_effect = lambda: {"Authorization": "Bearer test"}
        client.http = Mock()
        return client

    def response(self, http_status=200, **body):
        response = MagicMock()
        response.__enter__.return_value = response
        response.status_code = http_status
        response.headers = {}
        response.json.return_value = body
        response.iter_content.return_value = [json.dumps(body).encode()]
        return response

    def sql_query(self):
        session = SimpleNamespace(udf=Mock())
        register_model("model", "endpoint", provider="databricks", session=session)
        register_sql_function(session=session)
        return session.udf.register.call_args.args[1].func

    def test_invalid_configuration(self):
        for options, condition in (
            ({"model_id": ""}, "VALUE_NOT_NON_EMPTY_STR"),
            ({"profile": ""}, "VALUE_NOT_NON_EMPTY_STR"),
            ({"model_id": 12}, "NOT_EXPECTED_TYPE"),
            ({"timeout": 0}, "AI_QUERY_INVALID_TIMEOUT"),
            ({"timeout": float("nan")}, "AI_QUERY_INVALID_TIMEOUT"),
            ({"timeout": float("inf")}, "AI_QUERY_INVALID_TIMEOUT"),
            ({"max_concurrency": 0}, "VALUE_NOT_POSITIVE"),
            ({"max_concurrency": True}, "NOT_EXPECTED_TYPE"),
            ({"max_retries": -1}, "NEGATIVE_VALUE"),
            ({"max_retries": 1.5}, "NOT_EXPECTED_TYPE"),
            ({"auth_type": "pat"}, "VALUE_NOT_ALLOWED"),
        ):
            error_type = PySparkTypeError if condition == "NOT_EXPECTED_TYPE" else PySparkValueError
            with self.subTest(options=options), self.assertRaises(error_type) as raised:
                _ModelEndpoint(**{"model_id": "model", "provider": "databricks", **options})
            self.assertEqual(raised.exception.getCondition(), condition)

    def test_oauth_initialized_only_on_worker(self):
        workspace = Mock()
        workspace.config.host = "https://example.test"
        factory = Mock(return_value=workspace)
        with patch.dict(
            "sys.modules", {"databricks.sdk": SimpleNamespace(WorkspaceClient=factory)}
        ):
            client = _ChatCompletionsClient(
                _ModelEndpoint("model", provider="databricks", profile="production")
            )
            try:
                factory.assert_called_once_with(profile="production", auth_type="oauth-m2m")
                self.assertEqual(
                    client.url, "https://example.test/serving-endpoints/model/invocations"
                )
            finally:
                client.close()
            client = _ChatCompletionsClient(
                _ModelEndpoint(
                    "model", provider="databricks", profile="local", auth_type="databricks-cli"
                )
            )
            try:
                factory.assert_called_with(profile="local", auth_type="databricks-cli")
            finally:
                client.close()

    def test_invalid_databricks_urls(self):
        workspace = Mock()
        sdk = SimpleNamespace(WorkspaceClient=Mock(return_value=workspace))
        endpoint = _ModelEndpoint("model", provider="databricks")
        with patch.dict("sys.modules", {"databricks.sdk": sdk}):
            for url in (
                None,
                "http://example.test",
                "https://",
                "https://user:password@example.test",
                "https://@example.test",
                "https://example.test?key=value",
                "https://example.test?",
                "https://example.test#fragment",
                "https://example.test#",
                "https://example.test:broken",
                "https://[broken",
            ):
                workspace.config.host = url
                with self.subTest(url=url), self.assertRaises(PySparkRuntimeError) as raised:
                    _ChatCompletionsClient(endpoint)
                self.check_error(
                    raised.exception,
                    "AI_QUERY_INVOCATION_FAILED.INVALID_URL",
                    {"target": "Databricks model"},
                )

    def test_invalid_databricks_url_rejected_by_requests(self):
        workspace = Mock()
        workspace.config.host = "https://.example.test"
        workspace.config.authenticate.return_value = {"Authorization": "Bearer test"}
        sdk = SimpleNamespace(WorkspaceClient=Mock(return_value=workspace))

        with patch.dict("sys.modules", {"databricks.sdk": sdk}):
            client = _ChatCompletionsClient(_ModelEndpoint("model", provider="databricks"))
        try:
            with self.assertRaises(PySparkRuntimeError) as raised:
                client.invoke("hello")
            self.check_error(
                raised.exception,
                "AI_QUERY_INVOCATION_FAILED.INVALID_URL",
                {"target": "Databricks model"},
            )
            self.assertNotIn(".example.test", str(raised.exception))
            workspace.config.authenticate.assert_called_once()
        finally:
            client.close()

    def test_netrc_does_not_override_databricks_authentication(self):
        workspace = Mock()
        workspace.config.host = "https://example.test"
        workspace.config.authenticate.return_value = {"Authorization": "Bearer test"}
        sdk = SimpleNamespace(WorkspaceClient=Mock(return_value=workspace))

        with TemporaryDirectory() as directory:
            netrc = Path(directory) / "netrc"
            netrc.write_text("default login test-user password test-password\n", encoding="utf-8")
            netrc.chmod(0o600)
            with (
                patch.dict("os.environ", {"NETRC": str(netrc)}),
                patch.dict("sys.modules", {"databricks.sdk": sdk}),
            ):
                client = _ChatCompletionsClient(_ModelEndpoint("model", provider="databricks"))
                try:
                    request = requests.Request(
                        "POST", client.url, headers=workspace.config.authenticate()
                    )
                    prepared = client.http.prepare_request(request)
                    self.assertEqual(prepared.headers["Authorization"], "Bearer test")
                    self.assertTrue(client.http.trust_env)
                finally:
                    client.close()

    def test_transient_retry_keeps_body_and_refreshes_auth(self):
        client = self.client(max_retries=3)
        client.http.post.side_effect = [
            self.response(429),
            requests.ConnectionError("sensitive diagnostic"),
            requests.exceptions.ChunkedEncodingError("sensitive framing diagnostic"),
            self.response(choices=[{"message": {"content": "answer"}}]),
        ]
        with patch("pyspark.sql._ai_client.time.sleep"):
            self.assertEqual(client.invoke("hello"), "answer")
        calls = client.http.post.call_args_list
        self.assertEqual(len(calls), 4)
        self.assertEqual(calls[0].kwargs["json"], calls[3].kwargs["json"])
        self.assertFalse(calls[0].kwargs["allow_redirects"])
        self.assertEqual(client.workspace.config.authenticate.call_count, 4)

    def test_retry_after_is_respected(self):
        now = 1700000000
        date = format_datetime(datetime.fromtimestamp(now + 3, timezone.utc), usegmt=True)
        for value in ("3", date):
            with self.subTest(retry_after=value):
                client = self.client()
                retry = self.response(429)
                retry.headers = {"Retry-After": value}
                client.http.post.side_effect = [
                    retry,
                    self.response(choices=[{"message": {"content": "answer"}}]),
                ]
                with (
                    patch("pyspark.sql._ai_client.time.time", return_value=now),
                    patch("pyspark.sql._ai_client.time.sleep") as sleep,
                ):
                    self.assertEqual(client.invoke("hello"), "answer")
                sleep.assert_called_once_with(3)

    def test_retry_after_exceeding_budget_preserves_http_status(self):
        now = 1700000000
        date = format_datetime(datetime.fromtimestamp(now + 30, timezone.utc), usegmt=True)
        for status in (429, 503):
            for value in ("30", date):
                with self.subTest(status=status, retry_after=value):
                    client = self.client(timeout=5)
                    response = self.response(status)
                    response.headers = {"Retry-After": value}
                    client.http.post.return_value = response
                    with (
                        patch("pyspark.sql._ai_client.time.time", return_value=now),
                        patch("pyspark.sql._ai_client.time.monotonic", return_value=0),
                        patch("pyspark.sql._ai_client.time.sleep") as sleep,
                        self.assertRaises(PySparkRuntimeError) as raised,
                    ):
                        client.invoke("private prompt")
                    self.check_error(
                        raised.exception,
                        "AI_QUERY_INVOCATION_FAILED.RETRY_AFTER_EXCEEDS_TIMEOUT",
                        {
                            "target": "Databricks model",
                            "status": str(status),
                            "retry_after": "30.0",
                        },
                    )
                    sleep.assert_not_called()
                    client.http.post.assert_called_once()

    def test_retry_after_invalid_and_past_values_use_backoff(self):
        now = 1700000000
        past = format_datetime(datetime.fromtimestamp(now - 30, timezone.utc), usegmt=True)
        for value in (past, "invalid", "", "-3", "0", "nan", "inf"):
            with (
                self.subTest(retry_after=value),
                patch("pyspark.sql._ai_client.time.time", return_value=now),
                patch("pyspark.sql._ai_client.time.monotonic", return_value=0),
                patch("pyspark.sql._ai_client.random.uniform", return_value=1.0),
            ):
                self.assertEqual(_retry_delay({"Retry-After": value}, 0, 5, 429), 1)

    def test_retry_jitter_respects_server_delay_and_budget(self):
        with (
            patch("pyspark.sql._ai_client.time.monotonic", return_value=0),
            patch("pyspark.sql._ai_client.random.uniform", return_value=0.5),
        ):
            self.assertEqual(_retry_delay({}, 0, 10, None), 0.5)
            self.assertEqual(_retry_delay({"Retry-After": "3"}, 1, 10, 429), 3)
            self.assertEqual(_retry_delay({}, 3, 0.25, None), 0.25)

    def test_response_size_limit_closes_the_response_and_sanitizes_errors(self):
        client = self.client()
        response = self.response(choices=[{"message": {"content": "answer"}}])
        response.iter_content.return_value = [b"synthetic-sensitive-response"]
        client.http.post.return_value = response
        with (
            patch("pyspark.sql._ai_client._MAX_DATABRICKS_RESPONSE_BYTES", 8),
            self.assertRaises(PySparkRuntimeError) as raised,
        ):
            client.invoke("hello")
        self.check_error(
            raised.exception,
            "AI_QUERY_INVOCATION_FAILED.INVALID_RESPONSE",
            {"target": "Databricks model"},
        )
        self.assertNotIn(
            "synthetic-sensitive-response", "".join(traceback.format_exception(raised.exception))
        )
        response.__exit__.assert_called_once()
        client.http.post.assert_called_once()
        self.assertTrue(client.http.post.call_args.kwargs["stream"])

    def test_response_at_the_size_limit_is_accepted(self):
        client = self.client()
        response = self.response(choices=[{"message": {"content": "answer"}}])
        chunks = list(response.iter_content.return_value)
        payload = b"".join(chunks)
        response.iter_content.return_value = [payload[:10], payload[10:]]
        client.http.post.return_value = response
        with patch("pyspark.sql._ai_client._MAX_DATABRICKS_RESPONSE_BYTES", len(payload)):
            self.assertEqual(client.invoke("hello"), "answer")

    def test_gzip_response_is_bounded_before_the_size_check(self):
        from urllib3.response import HTTPResponse

        payload = gzip.compress(
            json.dumps({"choices": [{"message": {"content": "x" * (8 * 1024 * 1024)}}]}).encode()
        )
        response = requests.Response()
        response.status_code = 200
        response.raw = HTTPResponse(
            body=BytesIO(payload), headers={"Content-Encoding": "gzip"}, preload_content=False
        )
        buffered = []
        iter_content = response.iter_content

        def observed_chunks(*args, **kwargs):
            for chunk in iter_content(*args, **kwargs):
                # Older urllib3 versions expand the entire gzip into this buffer first.
                buffered.append(len(response.raw._decoded_buffer))
                yield chunk

        response.iter_content = observed_chunks
        client = self.client()
        client.http.post.return_value = response
        with (
            patch("pyspark.sql._ai_client._MAX_DATABRICKS_RESPONSE_BYTES", 128 * 1024),
            self.assertRaises(PySparkRuntimeError) as raised,
        ):
            client.invoke("hello")
        self.assertEqual(
            raised.exception.getCondition(), "AI_QUERY_INVOCATION_FAILED.INVALID_RESPONSE"
        )
        self.assertLessEqual(max(buffered), 64 * 1024)
        self.assertTrue(response.raw.closed)

    def test_first_row_budget_includes_client_initialization(self):
        for on_error in ("raise", "null"):
            now = 0.0
            config = SimpleNamespace(
                host="https://example.test", authenticate=Mock(return_value={})
            )

            def make_workspace(**kwargs):
                nonlocal now
                now += 2
                return SimpleNamespace(config=config)

            sdk = SimpleNamespace(WorkspaceClient=make_workspace)
            http = Mock()
            http.post.return_value = self.response(choices=[{"message": {"content": "answer"}}])
            endpoint = _ModelEndpoint(
                "model", provider="databricks", timeout=1, max_retries=0, max_concurrency=1
            )
            with (
                self.subTest(on_error=on_error),
                patch.dict("sys.modules", {"databricks.sdk": sdk}),
                patch("pyspark.sql._ai_client._databricks_session", return_value=http),
                patch("pyspark.sql._ai_client.time.monotonic", side_effect=lambda: now),
            ):
                batches = _query_batches(
                    endpoint, iter([pd.Series(["first", "later"])]), on_error=on_error
                )
                if on_error == "null":
                    self.assertEqual(list(batches)[0].tolist(), [None, "answer"])
                    http.post.assert_called_once()
                    config.authenticate.assert_called_once()
                else:
                    with self.assertRaises(PySparkRuntimeError) as raised:
                        list(batches)
                    self.assertEqual(
                        raised.exception.getCondition(), "AI_QUERY_INVOCATION_FAILED.TIMEOUT"
                    )
                    http.post.assert_not_called()
                    config.authenticate.assert_not_called()
                http.close.assert_called_once()

    def test_sdk_constructor_errors_are_sanitized(self):
        sdk = SimpleNamespace(WorkspaceClient=Mock(side_effect=ValueError("synthetic-auth-body")))
        with (
            patch.dict("sys.modules", {"databricks.sdk": sdk}),
            self.assertRaises(PySparkRuntimeError) as raised,
        ):
            _ChatCompletionsClient(_ModelEndpoint("model", provider="databricks"))
        self.check_error(
            raised.exception,
            "AI_QUERY_INVOCATION_FAILED.PERMISSION_DENIED",
            {"target": "Databricks model", "code": "SDK_AUTHENTICATION_ERROR"},
        )
        self.assertNotIn(
            "synthetic-auth-body", "".join(traceback.format_exception(raised.exception))
        )

    def test_authentication_errors_fail_even_with_null_policy(self):
        client = self.client()
        client.workspace.config.authenticate.side_effect = ValueError("synthetic-auth-body")
        with (
            patch("pyspark.sql._ai_client._ChatCompletionsClient", return_value=client),
            self.assertRaises(PySparkRuntimeError) as raised,
        ):
            list(_query_batches(client.endpoint, iter([pd.Series(["hello"])]), on_error="null"))
        self.check_error(
            raised.exception,
            "AI_QUERY_INVOCATION_FAILED.PERMISSION_DENIED",
            {"target": "Databricks model", "code": "SDK_AUTHENTICATION_ERROR"},
        )
        self.assertNotIn(
            "synthetic-auth-body", "".join(traceback.format_exception(raised.exception))
        )
        client.http.post.assert_not_called()
        client.http.close.assert_called_once()

    def test_real_oauth_non_json_error_is_sanitized(self):
        try:
            from databricks.sdk.oauth import ClientCredentials
        except ImportError:
            self.skipTest("OAuth regression requires databricks-sdk")

        marker = b"synthetic-private-token-response"

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(401)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(marker)))
                self.end_headers()
                self.wfile.write(marker)

            def log_message(self, *args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                tokens = ClientCredentials(
                    client_id="synthetic-client",
                    client_secret="synthetic-secret",
                    token_url=f"http://127.0.0.1:{server.server_port}/token",
                    use_header=True,
                )
                client = self.client()
                client.workspace.config.authenticate.side_effect = tokens.token
                with self.assertRaises(PySparkRuntimeError) as raised:
                    client.invoke("hello")
                self.check_error(
                    raised.exception,
                    "AI_QUERY_INVOCATION_FAILED.PERMISSION_DENIED",
                    {"target": "Databricks model", "code": "SDK_AUTHENTICATION_ERROR"},
                )
                self.assertNotIn(
                    marker.decode(), "".join(traceback.format_exception(raised.exception))
                )
                client.http.post.assert_not_called()
            finally:
                server.shutdown()
                thread.join()

    def test_transient_authentication_transport_errors_are_retried(self):
        client = self.client(max_retries=1)
        client.workspace.config.authenticate.side_effect = requests.ConnectionError(
            "private diagnostic"
        )
        with (
            patch("pyspark.sql._ai_client.time.sleep"),
            self.assertRaises(PySparkRuntimeError) as raised,
        ):
            client.invoke("hello")
        self.check_error(
            raised.exception,
            "AI_QUERY_INVOCATION_FAILED.CONNECTION_ERROR",
            {"target": "Databricks model"},
        )
        self.assertEqual(client.workspace.config.authenticate.call_count, 2)
        client.http.post.assert_not_called()

    def test_response_size_limit_respects_null_policy(self):
        client = self.client()
        client.http.post.return_value = self.response(choices=[{"message": {"content": "answer"}}])
        with (
            patch("pyspark.sql._ai_client._MAX_DATABRICKS_RESPONSE_BYTES", 8),
            patch("pyspark.sql._ai_client._ChatCompletionsClient", return_value=client),
        ):
            result = list(
                _query_batches(client.endpoint, iter([pd.Series(["hello"])]), on_error="null")
            )
        self.assertEqual(result[0].tolist(), [None])
        client.http.close.assert_called_once()

    def test_http_body_failures_are_sanitized_and_follow_null_policy(self):
        received = []
        marker = "synthetic-body-diagnostic"

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                received.append(self.path)
                self.send_response(200)
                if self.path == "/chunked":
                    self.send_header("Transfer-Encoding", "chunked")
                    body = marker.encode() + b"\r\ninvalid\r\n"
                elif self.path == "/truncated":
                    self.send_header("Content-Length", "1000")
                    body = marker.encode()
                else:
                    body = marker.encode()
                    self.send_header("Content-Encoding", "gzip")
                    self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                self.wfile.flush()
                self.close_connection = True

            def log_message(self, *args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                for path, condition, attempts in (
                    ("chunked", "CONNECTION_ERROR", 3),
                    ("truncated", "CONNECTION_ERROR", 3),
                    ("encoding", "INVALID_RESPONSE", 1),
                ):
                    for on_error in ("raise", "null"):
                        with self.subTest(path=path, on_error=on_error):
                            client = self.client(timeout=5, max_retries=2)
                            client.url = f"http://127.0.0.1:{server.server_port}/{path}"
                            client.http = requests.Session()
                            client.http.trust_env = False
                            before = len(received)
                            with (
                                patch("pyspark.sql._ai_client.time.sleep"),
                                patch(
                                    "pyspark.sql._ai_client._ChatCompletionsClient",
                                    return_value=client,
                                ),
                                patch.object(client, "close", wraps=client.close) as close,
                            ):
                                batches = iter([pd.Series(["private prompt"])])
                                if on_error == "null":
                                    result = list(
                                        _query_batches(client.endpoint, batches, on_error=on_error)
                                    )
                                    self.assertEqual(result[0].tolist(), [None])
                                else:
                                    with self.assertRaises(PySparkRuntimeError) as raised:
                                        list(_query_batches(client.endpoint, batches))
                                    self.check_error(
                                        raised.exception,
                                        f"AI_QUERY_INVOCATION_FAILED.{condition}",
                                        {"target": "Databricks model"},
                                    )
                                    rendered = "".join(traceback.format_exception(raised.exception))
                                    self.assertNotIn(marker, rendered)
                                    self.assertNotIn("private prompt", rendered)
                                close.assert_called_once()
                            self.assertEqual(len(received) - before, attempts)
            finally:
                server.shutdown()
                thread.join()

    def test_framing_errors_respect_retry_deadline(self):
        client = self.client(timeout=1, max_retries=2)
        client.http.post.side_effect = requests.exceptions.ChunkedEncodingError(
            "private diagnostic"
        )
        with (
            patch("pyspark.sql._ai_client.time.monotonic", side_effect=[0, 0, 0, 2, 2]),
            patch("pyspark.sql._ai_client.time.sleep") as sleep,
            self.assertRaises(PySparkRuntimeError) as raised,
        ):
            client.invoke("private prompt")
        self.check_error(
            raised.exception, "AI_QUERY_INVOCATION_FAILED.TIMEOUT", {"target": "Databricks model"}
        )
        client.http.post.assert_called_once()
        sleep.assert_called_once_with(0)

    def test_http_errors_do_not_leak_or_retry(self):
        for status in (302, 400, 409):
            client = self.client()
            client.http.post.return_value = self.response(status)
            with (
                self.subTest(status=status),
                self.assertRaises(PySparkRuntimeError) as raised,
            ):
                client.invoke("private prompt")
            self.check_error(
                raised.exception,
                "AI_QUERY_INVOCATION_FAILED.HTTP_ERROR",
                {"target": "Databricks model", "status": str(status)},
            )
            self.assertEqual(raised.exception.getSqlState(), "58000")
            self.assertNotIn("private prompt", str(raised.exception))

    def test_permission_denied_http_statuses_do_not_leak_or_retry(self):
        for status in (401, 403):
            client = self.client()
            client.http.post.return_value = self.response(status)
            with (
                self.subTest(status=status),
                self.assertRaises(PySparkRuntimeError) as raised,
            ):
                client.invoke("private prompt")
            self.check_error(
                raised.exception,
                "AI_QUERY_INVOCATION_FAILED.PERMISSION_DENIED",
                {"target": "Databricks model", "code": str(status)},
            )
            self.assertEqual(raised.exception.getSqlState(), "58000")
            self.assertNotIn("private prompt", str(raised.exception))
            self.assertEqual(client.http.post.call_count, 1)

    def test_direct_model_rejects_non_text_output(self):
        client = self.client()
        client.http.post.return_value = self.response(choices=[{"message": {"content": None}}])
        with self.assertRaises(PySparkRuntimeError) as raised:
            client.invoke("hello")
        self.check_error(
            raised.exception,
            "AI_QUERY_INVOCATION_FAILED.EMPTY_RESPONSE",
            {"target": "Databricks model"},
        )
        client.http.post.assert_called_once()

    def bedrock_modules(self, session_factory):
        return {
            "boto3": SimpleNamespace(Session=session_factory),
            "botocore.config": SimpleNamespace(Config=Mock()),
            "botocore.exceptions": SimpleNamespace(
                BotoCoreError=_BotoCoreError, ClientError=_ClientError
            ),
        }

    def test_bedrock_registered_model_batches(self):
        session = Mock()
        client = session.client.return_value
        client.converse.return_value = {
            "output": {"message": {"content": [{"text": "hello"}, {"text": " world"}]}}
        }
        create_session = Mock(return_value=session)
        with (
            patch.dict("sys.modules", self.bedrock_modules(create_session)),
            patch("pyspark.sql.functions.pandas_udf") as udf,
        ):
            register_model("test-bedrock", "model", provider="bedrock", region="us-east-1")
            ai_query("test-bedrock", "prompt")
            create_session.assert_not_called()
            query = udf.call_args.args[0]
            batches = [pd.Series(["prompt", None])]
            self.assertEqual(list(query(iter(batches)))[0].tolist(), ["hello world", None])
        create_session.assert_called_once_with(profile_name=None, region_name="us-east-1")
        client.converse.assert_called_once_with(
            modelId="model",
            messages=[{"role": "user", "content": [{"text": "prompt"}]}],
            inferenceConfig={"maxTokens": 2048, "temperature": 0.0},
        )
        client.close.assert_called_once()

    def test_bedrock_sdk_errors_are_structured(self):
        endpoint = _ModelEndpoint("model", provider="bedrock")
        session = Mock()
        sdk_client = session.return_value.client.return_value
        arn = "arn:aws:bedrock:us-east-1:000000000000:inference-profile/synthetic"
        failure = _ClientError(arn)
        failure.response = {"Error": {"Code": "AccessDeniedException", "Message": arn}}
        sdk_client.converse.side_effect = failure

        with patch.dict("sys.modules", self.bedrock_modules(session)):
            with self.assertRaises(PySparkRuntimeError) as raised:
                _ConverseClient(endpoint).invoke("prompt")
            self.check_error(
                raised.exception,
                "AI_QUERY_INVOCATION_FAILED.PERMISSION_DENIED",
                {"target": "Bedrock model", "code": "AccessDeniedException"},
            )
            self.assertNotIn(arn, str(raised.exception))
            self.assertTrue(raised.exception.__suppress_context__)

            failure.response = {"Error": {"Code": "ThrottlingException", "Message": arn}}
            with self.assertRaises(PySparkRuntimeError) as raised:
                _ConverseClient(endpoint).invoke("prompt")
            self.check_error(
                raised.exception,
                "AI_QUERY_INVOCATION_FAILED.SDK_ERROR",
                {"target": "Bedrock model", "code": "ThrottlingException"},
            )
            self.assertNotIn(arn, str(raised.exception))

            session.side_effect = _BotoCoreError(arn)
            with self.assertRaises(PySparkRuntimeError) as raised:
                _ConverseClient(endpoint)
            self.check_error(
                raised.exception,
                "AI_QUERY_INVOCATION_FAILED.SDK_ERROR",
                {"target": "Bedrock model", "code": "_BotoCoreError"},
            )
            self.assertNotIn(arn, str(raised.exception))

    def test_retries_are_bounded(self):
        client = self.client(max_retries=1)
        client.http.post.side_effect = requests.Timeout("private diagnostic")
        with (
            patch("pyspark.sql._ai_client.time.sleep"),
            self.assertRaises(PySparkRuntimeError) as raised,
        ):
            client.invoke("private prompt")
        self.check_error(
            raised.exception,
            "AI_QUERY_INVOCATION_FAILED.CONNECTION_ERROR",
            {"target": "Databricks model"},
        )
        self.assertNotIn("private diagnostic", str(raised.exception))
        self.assertEqual(client.http.post.call_count, 2)

    def test_read_timeout_is_distinguished_from_connection_failures(self):
        from urllib3.exceptions import ReadTimeoutError

        for error, condition in (
            (requests.ReadTimeout("private diagnostic"), "TIMEOUT"),
            (
                requests.ConnectionError(ReadTimeoutError(None, None, "private diagnostic")),
                "TIMEOUT",
            ),
            (requests.ConnectTimeout("private diagnostic"), "CONNECTION_ERROR"),
        ):
            client = self.client(max_retries=0)
            client.http.post.side_effect = error
            with (
                self.subTest(error=type(error).__name__),
                self.assertRaises(PySparkRuntimeError) as raised,
            ):
                client.invoke("hello")
            self.check_error(
                raised.exception,
                f"AI_QUERY_INVOCATION_FAILED.{condition}",
                {"target": "Databricks model"},
            )
            self.assertNotIn(
                "private diagnostic", "".join(traceback.format_exception(raised.exception))
            )
            client.http.post.assert_called_once()

    def test_throttling_status_survives_deadline_expiry_with_fallback_backoff(self):
        now = 1700000000
        past = format_datetime(datetime.fromtimestamp(now - 30, timezone.utc), usegmt=True)
        for header in ("-5", "nan", "invalid", past, "0.5"):
            client = self.client(timeout=1)
            response = self.response(429)
            response.headers = {"Retry-After": header}
            client.http.post.return_value = response
            with (
                self.subTest(retry_after=header),
                patch("pyspark.sql._ai_client.time.time", return_value=now),
                patch("pyspark.sql._ai_client.time.monotonic", side_effect=[0, 0, 0, 0.25, 1]),
                patch("pyspark.sql._ai_client.time.sleep"),
                self.assertRaises(PySparkRuntimeError) as raised,
            ):
                client.invoke("hello")
            self.check_error(
                raised.exception,
                "AI_QUERY_INVOCATION_FAILED.HTTP_RETRIES_EXHAUSTED",
                {"target": "Databricks model", "status": "429"},
            )
            client.http.post.assert_called_once()

    def test_authentication_time_reduces_http_timeout(self):
        client = self.client(timeout=5)
        client.http.post.return_value = self.response(choices=[{"message": {"content": "answer"}}])
        with patch("pyspark.sql._ai_client.time.monotonic", side_effect=[0, 0, 2, 3]):
            self.assertEqual(client.invoke("hello"), "answer")
        self.assertEqual(client.http.post.call_args.kwargs["timeout"], 3)

    def test_slow_authentication_exhausts_budget_before_post(self):
        client = self.client(timeout=0.1, max_retries=0)

        def authenticate():
            time.sleep(0.2)
            return {"Authorization": "Bearer test"}

        client.workspace.config.authenticate.side_effect = authenticate
        client.http.post.return_value = self.response(choices=[{"message": {"content": "answer"}}])
        with self.assertRaises(PySparkRuntimeError) as raised:
            client.invoke("hello")
        self.check_error(
            raised.exception, "AI_QUERY_INVOCATION_FAILED.TIMEOUT", {"target": "Databricks model"}
        )
        client.workspace.config.authenticate.assert_called_once()
        client.http.post.assert_not_called()

    def test_success_at_or_after_deadline_is_rejected(self):
        for arrival in (1, 2):
            with self.subTest(arrival=arrival):
                client = self.client(timeout=1)
                response = self.response(choices=[{"message": {"content": "private answer"}}])
                client.http.post.return_value = response
                with (
                    patch("pyspark.sql._ai_client.time.monotonic", side_effect=[0, 0, 0, arrival]),
                    self.assertRaises(PySparkRuntimeError) as raised,
                ):
                    client.invoke("private prompt")
                self.check_error(
                    raised.exception,
                    "AI_QUERY_INVOCATION_FAILED.TIMEOUT",
                    {"target": "Databricks model"},
                )
                client.http.post.assert_called_once()
                response.__exit__.assert_called_once()

    def test_slow_http_download_returns_null_and_closes_client(self):
        received = []
        body = json.dumps({"choices": [{"message": {"content": "answer"}}]}).encode()

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                received.append(self.path)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                for offset in range(0, len(body), 2):
                    self.wfile.write(body[offset : offset + 2])
                    self.wfile.flush()
                    if offset + 2 < len(body):
                        time.sleep(0.05)

            def log_message(self, *args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            client = self.client(timeout=0.5, max_retries=0)
            client.url = f"http://127.0.0.1:{server.server_port}/invocations"
            client.http = requests.Session()
            client.http.trust_env = False
            try:
                with (
                    patch("pyspark.sql._ai_client._ChatCompletionsClient", return_value=client),
                    patch.object(client, "close", wraps=client.close) as close,
                ):
                    started = time.monotonic()
                    result = list(
                        _query_batches(
                            client.endpoint, iter([pd.Series(["hello"])]), on_error="null"
                        )
                    )
                    elapsed = time.monotonic() - started
                    self.assertEqual(result[0].tolist(), [None])
                    self.assertGreaterEqual(elapsed, client.endpoint.timeout)
                    close.assert_called_once()
                self.assertEqual(received, ["/invocations"])
            finally:
                client.close()
                server.shutdown()
                thread.join()

    def test_retry_deadline(self):
        client = self.client(timeout=1)
        client.http.post.return_value = self.response(429)
        with (
            patch("pyspark.sql._ai_client.time.monotonic", side_effect=[0, 0, 0, 2, 2]),
            patch("pyspark.sql._ai_client.time.sleep"),
            self.assertRaises(PySparkRuntimeError) as raised,
        ):
            client.invoke("hello")
        self.check_error(
            raised.exception,
            "AI_QUERY_INVOCATION_FAILED.HTTP_RETRIES_EXHAUSTED",
            {"target": "Databricks model", "status": "429"},
        )

    def test_sliding_window_starts_later_rows_while_an_earlier_row_is_running(self):
        later_started = threading.Event()
        client = Mock()

        def invoke(request, *, deadline=None):
            if request == "slow":
                self.assertTrue(later_started.wait(timeout=10))
            elif request == "later":
                later_started.set()
            return request.upper()

        client.invoke.side_effect = invoke
        with patch("pyspark.sql._ai_client._ChatCompletionsClient", return_value=client):
            result = list(
                _query_batches(
                    _ModelEndpoint("model", provider="databricks", max_concurrency=2),
                    iter([pd.Series(["slow", "fast", "later"])]),
                )
            )
        self.assertEqual(result[0].tolist(), ["SLOW", "FAST", "LATER"])
        self.assertEqual(client.invoke.call_count, 3)

    def test_batch_order_nulls_concurrency_and_cleanup(self):
        clients = []
        lock = threading.Lock()
        active = 0
        maximum = 0
        started = threading.Barrier(2)
        fast_finished = threading.Event()

        def make_client(endpoint):
            nonlocal active, maximum
            client = Mock()

            def invoke(request, *, deadline=None):
                nonlocal active, maximum
                with lock:
                    active += 1
                    maximum = max(maximum, active)
                if request in ("slow", "fast"):
                    started.wait(timeout=10)
                if request == "slow":
                    self.assertTrue(fast_finished.wait(timeout=10))
                with lock:
                    active -= 1
                if request == "fast":
                    fast_finished.set()
                return request.upper()

            client.invoke.side_effect = invoke
            clients.append(client)
            return client

        batches = [pd.Series(["slow", "fast", None]), pd.Series(["last"])]
        with patch("pyspark.sql._ai_client._ChatCompletionsClient", side_effect=make_client):
            result = list(
                _query_batches(
                    _ModelEndpoint("model", provider="databricks", max_concurrency=2), iter(batches)
                )
            )
        self.assertEqual(result[0].tolist(), ["SLOW", "FAST", None])
        self.assertEqual(result[1].tolist(), ["LAST"])
        self.assertEqual(maximum, 2)
        self.assertLessEqual(len(clients), 2)
        self.assertEqual(sum(c.invoke.call_count for c in clients), 3)
        for client in clients:
            client.close.assert_called_once()

    def test_cleanup_continues_after_close_error(self):
        batches = [pd.Series(["hello", "world"])]
        for batch_error in (None, RuntimeError("invocation failed")):
            with self.subTest(batch_error=batch_error):
                ready = threading.Barrier(2)
                clients = [Mock(), Mock()]
                close_error = RuntimeError("close failed")

                def invoke(request, *, deadline=None):
                    ready.wait(timeout=5)
                    if batch_error is not None:
                        raise batch_error
                    return request.upper()

                for client in clients:
                    client.invoke.side_effect = invoke
                    client.close.side_effect = close_error
                endpoint = _ModelEndpoint("model", provider="databricks", max_concurrency=2)
                with (
                    patch("pyspark.sql._ai_client._ChatCompletionsClient", side_effect=clients),
                    self.assertLogs("pyspark.sql._ai_client", level="WARNING") as logs,
                ):
                    if batch_error is None:
                        result = list(_query_batches(endpoint, iter(batches)))
                        self.assertEqual(result[0].tolist(), ["HELLO", "WORLD"])
                    else:
                        with self.assertRaises(RuntimeError) as raised:
                            list(_query_batches(endpoint, iter(batches)))
                        self.assertIs(raised.exception, batch_error)
                self.assertEqual(len(logs.output), 2)
                self.assertNotIn("close failed", " ".join(logs.output))
                for client in clients:
                    client.close.assert_called_once()

    def test_nulls_and_invalid_requests_do_not_initialize_clients(self):
        endpoint = _ModelEndpoint("model", provider="databricks")
        with patch("pyspark.sql._ai_client._ChatCompletionsClient") as client:
            result = list(_query_batches(endpoint, iter([pd.Series([None])])))
            self.assertEqual(result[0].tolist(), [None])
            with self.assertRaises(PySparkTypeError) as raised:
                list(_query_batches(endpoint, iter([pd.Series([12])])))
            self.check_error(
                raised.exception,
                "NOT_EXPECTED_TYPE",
                {"arg_name": "request", "expected_type": "str", "arg_type": "int"},
            )
            client.assert_not_called()

    def test_null_on_invocation_error_preserves_validation_and_setup_errors(self):
        endpoint = _ModelEndpoint("model", provider="databricks")
        with patch("pyspark.sql._ai_client._ChatCompletionsClient") as factory:
            factory.side_effect = PySparkRuntimeError(
                errorClass="AI_QUERY_INVOCATION_FAILED.INVALID_URL",
                messageParameters={"target": "Databricks model"},
            )
            with self.assertRaises(PySparkRuntimeError) as raised:
                list(_query_batches(endpoint, iter([pd.Series(["hello"])]), on_error="null"))
            self.assertEqual(
                raised.exception.getCondition(), "AI_QUERY_INVOCATION_FAILED.INVALID_URL"
            )
            with self.assertRaises(PySparkTypeError):
                list(_query_batches(endpoint, iter([pd.Series([12])]), on_error="null"))

    def test_null_on_invocation_error_does_not_hide_permission_denied(self):
        endpoint = _ModelEndpoint("model", provider="databricks")
        client = Mock()
        client.invoke.side_effect = PySparkRuntimeError(
            errorClass="AI_QUERY_INVOCATION_FAILED.PERMISSION_DENIED",
            messageParameters={"target": "Databricks model", "code": "403"},
        )
        with patch("pyspark.sql._ai_client._ChatCompletionsClient", return_value=client):
            with self.assertRaises(PySparkRuntimeError) as raised:
                list(_query_batches(endpoint, iter([pd.Series(["hello"])]), on_error="null"))
            self.assertEqual(
                raised.exception.getCondition(), "AI_QUERY_INVOCATION_FAILED.PERMISSION_DENIED"
            )

    def test_bedrock_null_mode_distinguishes_transient_and_auth_errors(self):
        endpoint = _ModelEndpoint("model", provider="bedrock")
        session = Mock()
        sdk_client = session.return_value.client.return_value
        failure = _ClientError("synthetic")
        sdk_client.converse.side_effect = failure
        with patch.dict("sys.modules", self.bedrock_modules(session)):
            for code, status, condition in (
                ("AccessDeniedException", 403, "PERMISSION_DENIED"),
                ("ExpiredToken", 400, "SDK_ERROR"),
                ("InvalidClientTokenId", 400, "SDK_ERROR"),
                ("UnknownForbidden", 403, "PERMISSION_DENIED"),
            ):
                with self.subTest(code=code):
                    failure.response = {
                        "Error": {"Code": code},
                        "ResponseMetadata": {"HTTPStatusCode": status},
                    }
                    with self.assertRaises(PySparkRuntimeError) as raised:
                        list(
                            _query_batches(endpoint, iter([pd.Series(["prompt"])]), on_error="null")
                        )
                    self.check_error(
                        raised.exception,
                        f"AI_QUERY_INVOCATION_FAILED.{condition}",
                        {"target": "Bedrock model", "code": code},
                    )
            failure.response = {"Error": {"Code": "ThrottlingException"}}
            result = list(_query_batches(endpoint, iter([pd.Series(["prompt"])]), on_error="null"))
            self.assertEqual(result[0].tolist(), [None])

    def test_on_error_type_is_validated(self):
        register_model("on-error-type-test", "model", provider="databricks")
        with self.assertRaises(PySparkTypeError) as raised:
            ai_query("on-error-type-test", "prompt", on_error=["null"])
        self.check_error(
            raised.exception,
            "NOT_EXPECTED_TYPE",
            {"arg_name": "on_error", "expected_type": "str", "arg_type": "list"},
        )

    def test_registration_is_lazy_and_columns_snapshot_configuration(self):
        with patch("pyspark.sql.functions.pandas_udf") as factory:
            register_model("test", "original", provider="databricks")
            ai_query("test", "prompt")
            factory.return_value.asNondeterministic.assert_called_once()
            factory.return_value.asNondeterministic.return_value.assert_called_once_with("prompt")
            query = factory.call_args.args[0]
            register_model("test", "replacement", provider="bedrock")
            with patch("pyspark.sql.ai._query_batches", return_value=iter([])) as batches:
                list(query(iter([])))
                self.assertEqual(batches.call_args.args[0].model_id, "original")
        with self.assertRaises(PySparkValueError) as raised:
            ai_query("not-registered", "prompt")
        self.check_error(
            raised.exception,
            "AI_QUERY_ENDPOINT_NOT_REGISTERED",
            {"endpoint": "not-registered"},
        )
        self.assertEqual(raised.exception.getSqlState(), "22023")

    def test_sql_function_resolves_per_row_alias_and_snapshots_registry(self):
        session = SimpleNamespace(udf=Mock())
        with patch("pyspark.sql.session.SparkSession.active", return_value=session):
            register_model("alias1", "model1", provider="databricks")
            register_model("alias2", "model2", provider="bedrock", region="us-east-1")
            register_sql_function()
            query_udf = session.udf.register.call_args.args[1]
            query = query_udf.func
            # A registration made after register_sql_function must not affect this snapshot.
            register_model("alias3", "model3", provider="databricks")

        @contextmanager
        def fake_query_session(config, *, on_error):
            def query(series):
                return pd.Series([f"{config.model_id}:{r}" for r in series], index=series.index)

            yield query

        with patch("pyspark.sql.ai._query_session", side_effect=fake_query_session):
            batches = [(pd.Series(["alias1", "alias2", "alias1"]), pd.Series(["x", "y", "z"]))]
            self.assertEqual(
                list(query(iter(batches)))[0].tolist(),
                ["model1:x", "model2:y", "model1:z"],
            )
            with self.assertRaises(PySparkValueError) as raised:
                list(query(iter([(pd.Series(["alias3"]), pd.Series(["x"]))])))
            self.check_error(
                raised.exception, "AI_QUERY_ENDPOINT_NOT_REGISTERED", {"endpoint": "alias3"}
            )

        import pyspark.cloudpickle as cloudpickle

        cloudpickle.loads(cloudpickle.dumps(query))

    def test_invalid_request_batches_fail_before_creating_clients(self):
        endpoint = _ModelEndpoint("model", provider="databricks", max_concurrency=1)
        for batch in (pd.DataFrame({"field": ["value"]}), pd.Series(["valid", 42])):
            with (
                self.subTest(batch_type=type(batch).__name__),
                patch("pyspark.sql._ai_client._ChatCompletionsClient") as factory,
                self.assertRaises(PySparkTypeError) as raised,
            ):
                list(_query_batches(endpoint, iter([batch])))
            self.assertEqual(raised.exception.getCondition(), "NOT_EXPECTED_TYPE")
            factory.assert_not_called()

    def test_sql_validates_all_aliases_before_invoking_models(self):
        query = self.sql_query()
        alias = "missing" * 500
        batches = [(pd.Series(["model", alias]), pd.Series(["first", "second"]))]
        with (
            patch("pyspark.sql.ai._query_session") as factory,
            self.assertRaises(PySparkValueError) as raised,
        ):
            list(query(iter(batches)))
        self.check_error(
            raised.exception,
            "AI_QUERY_ENDPOINT_NOT_REGISTERED",
            {"endpoint": alias[:100] + "..."},
        )
        factory.assert_not_called()

    def test_sql_null_alias_does_not_invoke_a_model(self):
        query = self.sql_query()
        client = Mock()
        client.invoke.return_value = "answer"
        with patch("pyspark.sql._ai_client._ChatCompletionsClient", return_value=client):
            result = list(query(iter([(pd.Series([None, "model"]), pd.Series(["skip", "hello"]))])))
        self.assertEqual(result[0].tolist(), [None, "answer"])
        client.invoke.assert_called_once_with("hello", deadline=ANY)
        client.close.assert_called_once()

    def test_sql_wrong_arity_has_a_structured_error(self):
        query = self.sql_query()
        for batch, count in ((pd.Series(["one"]), 1), ((pd.Series(["one"]),), 1), ((), 0)):
            with self.subTest(count=count), self.assertRaises(PySparkValueError) as raised:
                list(query(iter([batch])))
            self.check_error(
                raised.exception,
                "AI_QUERY_WRONG_NUM_ARGUMENTS",
                {"function_name": "ai_query", "num_args": str(count)},
            )

    def test_sql_alias_allowlist_excludes_other_credentials_from_the_snapshot(self):
        import pyspark.cloudpickle as cloudpickle

        session = SimpleNamespace(udf=Mock())
        register_model("safe", "model", provider="databricks", session=session)
        register_model(
            "prod",
            "private-model",
            provider="bedrock",
            profile="private-prod-profile",
            region="us-east-2",
            session=session,
        )
        register_sql_function(session=session, aliases=["safe"])
        query = session.udf.register.call_args.args[1].func
        self.assertNotIn(b"private-prod-profile", cloudpickle.dumps(query))
        with (
            patch("pyspark.sql.ai._query_session") as factory,
            self.assertRaises(PySparkValueError) as raised,
        ):
            list(query(iter([(pd.Series(["safe", "prod"]), pd.Series(["a", "b"]))])))
        self.check_error(raised.exception, "AI_QUERY_ENDPOINT_NOT_REGISTERED", {"endpoint": "prod"})
        factory.assert_not_called()

    def test_sql_alias_allowlist_is_validated_at_registration(self):
        session = SimpleNamespace(udf=Mock())
        register_model("known", "model", provider="databricks", session=session)
        for aliases, error_type in (
            ("known", PySparkTypeError),
            ([None], PySparkTypeError),
            (["missing"], PySparkValueError),
        ):
            with self.subTest(aliases=aliases), self.assertRaises(error_type):
                register_sql_function(session=session, aliases=aliases)
        session.udf.register.assert_not_called()

    def test_sql_reuses_clients_across_batches_and_closes_them(self):
        session = SimpleNamespace(udf=Mock())
        for alias in ("first", "second"):
            register_model(alias, alias, provider="databricks", max_concurrency=1, session=session)
        register_sql_function(session=session)
        query = session.udf.register.call_args.args[1].func
        clients = []

        def make_client(endpoint):
            client = Mock()
            client.invoke.side_effect = lambda prompt, **kwargs: f"{endpoint.model_id}:{prompt}"
            clients.append(client)
            return client

        batches = [
            (pd.Series(["first", "second", "first"]), pd.Series(["a", "b", None])),
            (pd.Series(["second", "first"]), pd.Series(["c", "d"])),
            (pd.Series(["first"]), pd.Series(["e"])),
        ]
        with patch("pyspark.sql._ai_client._ChatCompletionsClient", side_effect=make_client):
            results = [series.tolist() for series in query(iter(batches))]
        self.assertEqual(
            results, [["first:a", "second:b", None], ["second:c", "first:d"], ["first:e"]]
        )
        self.assertEqual(len(clients), 2)
        for client in clients:
            client.close.assert_called_once()

    def test_sql_closes_existing_clients_after_an_unknown_alias(self):
        session = SimpleNamespace(udf=Mock())
        register_model("known", "model", provider="databricks", session=session)
        register_sql_function(session=session)
        query = session.udf.register.call_args.args[1].func
        client = Mock()
        client.invoke.return_value = "answer"
        batches = [
            (pd.Series(["known"]), pd.Series(["hello"])),
            (pd.Series(["missing"]), pd.Series(["hello"])),
        ]
        with patch("pyspark.sql._ai_client._ChatCompletionsClient", return_value=client):
            results = query(iter(batches))
            self.assertEqual(next(results).tolist(), ["answer"])
            with self.assertRaises(PySparkValueError) as raised:
                next(results)
        self.check_error(
            raised.exception, "AI_QUERY_ENDPOINT_NOT_REGISTERED", {"endpoint": "missing"}
        )
        client.close.assert_called_once()

    def test_sql_error_policy_is_validated_before_registration(self):
        session = SimpleNamespace(udf=Mock())
        for value, error_type in ((None, PySparkTypeError), ("ignore", PySparkValueError)):
            with self.subTest(on_error=value), self.assertRaises(error_type):
                register_sql_function(session=session, on_error=value)
        session.udf.register.assert_not_called()

    def test_sql_null_policy_handles_timeouts(self):
        session = SimpleNamespace(udf=Mock())
        register_model("model", "endpoint", provider="databricks", session=session)
        register_sql_function(session=session, on_error="null")
        query = session.udf.register.call_args.args[1].func
        client = Mock()
        client.invoke.side_effect = PySparkRuntimeError(
            errorClass="AI_QUERY_INVOCATION_FAILED.TIMEOUT",
            messageParameters={"target": "Databricks model"},
        )
        with patch("pyspark.sql._ai_client._ChatCompletionsClient", return_value=client):
            result = list(query(iter([(pd.Series(["model"]), pd.Series(["hello"]))])))
        self.assertEqual(result[0].tolist(), [None])
        client.close.assert_called_once()


@unittest.skipUnless(
    have_ai_query_dependencies and have_pyarrow,
    "Spark AI query tests require pandas, requests and pyarrow",
)
class AIQuerySparkTests(sqlutils.ReusedSQLTestCase):
    def test_sql_alias_allowlist_limits_routing(self):
        with _model_http_server() as (origin, received):
            local_session, _ = _local_query_functions(origin)
            with patch("pyspark.sql.ai._query_session", new=local_session):
                for alias in ("allowed", "excluded"):
                    register_model(alias, alias, provider="databricks", session=self.spark)
                register_sql_function("restricted_query", session=self.spark, aliases=["allowed"])
                self.assertEqual(
                    self.spark.sql("SELECT restricted_query('allowed', 'hello')").first()[0],
                    "allowed:HELLO",
                )
                for aliases, alias in ((["allowed"], "excluded"), ([], "allowed")):
                    register_sql_function("restricted_query", session=self.spark, aliases=aliases)
                    with self.assertRaises(PythonException) as raised:
                        self.spark.sql(f"SELECT restricted_query('{alias}', 'hello')").collect()
                    self.assertIn("AI_QUERY_ENDPOINT_NOT_REGISTERED", str(raised.exception))
            self.assertEqual(received, [("allowed", "hello")])

    def test_non_string_columns_fail_without_http_calls(self):
        from pyspark.sql.functions import col, struct

        with _model_http_server() as (origin, received):
            local_session, local_batches = _local_query_functions(origin)
            with (
                patch("pyspark.sql.ai._query_session", new=local_session),
                patch("pyspark.sql.ai._query_batches", new=local_batches),
            ):
                register_model("typed", "model", provider="databricks", session=self.spark)
                register_sql_function("typed_query", session=self.spark)
                base = self.spark.createDataFrame([(1, "hello")], "id int, text string")
                for request in (col("id"), struct("text")):
                    frame = base.select(request.alias("request"))
                    frame.createOrReplaceTempView("typed_documents")
                    results = (
                        frame.select(ai_query("typed", "request")),
                        self.spark.sql("SELECT typed_query('typed', request) FROM typed_documents"),
                    )
                    for result in results:
                        with (
                            self.subTest(schema=frame.schema),
                            self.assertRaises(PythonException) as raised,
                        ):
                            result.collect()
                        self.assertIn("NOT_EXPECTED_TYPE", str(raised.exception))
            self.assertEqual(received, [])

    def test_sql_wrong_arity_fails_with_a_structured_error(self):
        register_sql_function("arity_query", session=self.spark)
        for arguments in ("'model'", "'model', 'hello', 'extra'"):
            result = self.spark.sql(f"SELECT arity_query({arguments})")
            with self.subTest(arguments=arguments), self.assertRaises(PythonException) as raised:
                result.collect()
            self.assertIn("AI_QUERY_WRONG_NUM_ARGUMENTS", str(raised.exception))

    def test_sql_null_alias_returns_null_without_http_calls(self):
        with _model_http_server() as (origin, received):
            local_session, _ = _local_query_functions(origin)
            with patch("pyspark.sql.ai._query_session", new=local_session):
                register_sql_function("null_alias_query", session=self.spark)
                result = self.spark.sql(
                    "SELECT null_alias_query(CAST(NULL AS STRING), 'hello') AS answer"
                )
                self.assertIsNone(result.collect()[0].answer)
            self.assertEqual(received, [])

    def test_sql_registration_snapshots_and_refreshes_each_session_independently(self):
        other = self.spark.newSession()
        statement = "SELECT session_query('shared', 'hello') AS answer"
        with _model_http_server() as (origin, _):
            local_session, _ = _local_query_functions(origin)
            with patch("pyspark.sql.ai._query_session", new=local_session):
                register_model("shared", "original", provider="databricks", session=self.spark)
                register_model("shared", "other", provider="databricks", session=other)
                register_sql_function("session_query", session=self.spark)
                register_sql_function("session_query", session=other)
                register_model("shared", "replacement", provider="databricks", session=self.spark)

                original = self.spark.sql(statement)
                self.assertEqual(original.collect()[0].answer, "original:HELLO")
                self.assertEqual(
                    other.sql(statement).collect()[0].answer,
                    "other:HELLO",
                )
                register_sql_function("session_query", session=self.spark)
                self.assertEqual(
                    self.spark.sql(statement).collect()[0].answer,
                    "replacement:HELLO",
                )
                self.assertEqual(original.collect()[0].answer, "original:HELLO")
                self.assertEqual(
                    other.sql(statement).collect()[0].answer,
                    "other:HELLO",
                )

    def test_sql_null_policy_preserves_successes_and_nulls_in_multiple_batches(self):
        with _model_http_server() as (origin, received):
            local_session, _ = _local_query_functions(origin)
            with (
                patch("pyspark.sql.ai._query_session", new=local_session),
                self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": "1"}),
            ):
                register_model(
                    "nullable", "model", provider="databricks", max_retries=0, session=self.spark
                )
                register_sql_function("nullable_query", session=self.spark, on_error="null")
                frame = self.spark.createDataFrame(
                    [(1, "hello"), (2, "reject"), (3, None), (4, "unavailable"), (5, "world")],
                    "id int, prompt string",
                ).coalesce(1)
                frame.createOrReplaceTempView("nullable_documents")
                result = self.spark.sql(
                    "SELECT id, nullable_query('nullable', prompt) AS answer "
                    "FROM nullable_documents"
                )
                self.assertEqual(
                    sorted((row.id, row.answer) for row in result.collect()),
                    [(1, "model:HELLO"), (2, None), (3, None), (4, None), (5, "model:WORLD")],
                )
            self.assertCountEqual(
                received,
                [("model", prompt) for prompt in ("hello", "reject", "unavailable", "world")],
            )

    def test_sql_default_error_policy_fails_on_inference_errors(self):
        with _model_http_server() as (origin, received):
            local_session, _ = _local_query_functions(origin)
            with patch("pyspark.sql.ai._query_session", new=local_session):
                register_model("strict", "model", provider="databricks", session=self.spark)
                register_sql_function("strict_query", session=self.spark)
                with self.assertRaises(PythonException) as raised:
                    self.spark.sql("SELECT strict_query('strict', 'reject')").collect()
                self.assertIn("AI_QUERY_INVOCATION_FAILED.HTTP_ERROR", str(raised.exception))
            self.assertEqual(received, [("model", "reject")])

    def test_sql_null_policy_preserves_permission_failures(self):
        with _model_http_server() as (origin, received):
            local_session, _ = _local_query_functions(origin)
            with patch("pyspark.sql.ai._query_session", new=local_session):
                register_model("forbidden", "model", provider="databricks", session=self.spark)
                register_sql_function("forbidden_query", session=self.spark, on_error="null")
                with self.assertRaises(PythonException) as raised:
                    self.spark.sql("SELECT forbidden_query('forbidden', 'forbidden')").collect()
                self.assertIn("AI_QUERY_INVOCATION_FAILED.PERMISSION_DENIED", str(raised.exception))
            self.assertEqual(received, [("model", "forbidden")])

    def test_registrations_are_session_scoped(self):
        other = self.spark.newSession()
        register_model("session-only", "first", provider="databricks", session=self.spark)
        with self.assertRaises(PySparkValueError) as raised:
            ai_query("session-only", "prompt", session=other)
        self.check_error(
            raised.exception,
            "AI_QUERY_ENDPOINT_NOT_REGISTERED",
            {"endpoint": "session-only"},
        )
        register_model("session-only", "second", provider="bedrock", session=other)
        with patch("pyspark.sql.functions.pandas_udf") as factory:
            ai_query("session-only", "prompt", session=self.spark)
            first_query = factory.call_args.args[0]
            ai_query("session-only", "prompt", session=other)
            second_query = factory.call_args.args[0]
        with patch("pyspark.sql.ai._query_batches", return_value=iter([])) as batches:
            list(first_query(iter([])))
            self.assertEqual(batches.call_args.args[0].model_id, "first")
            list(second_query(iter([])))
            self.assertEqual(batches.call_args.args[0].model_id, "second")

    def test_model_query_with_arrow_and_local_endpoint(self):
        with _model_http_server() as (origin, received):
            _, local_batches = _local_query_functions(origin)
            register_model("local-model", "model", provider="databricks", timeout=5)
            frame = self.spark.createDataFrame(
                [(1, "hello"), (2, None), (3, "world"), (4, "reject")], "id int, prompt string"
            )
            with patch("pyspark.sql.ai._query_batches", new=local_batches):
                result = frame.withColumn(
                    "answer", ai_query("local-model", "prompt", on_error="null")
                )
                self.assertIn(
                    "ArrowEvalPython", result._jdf.queryExecution().executedPlan().toString()
                )
                self.assertEqual(
                    sorted((row.id, row.answer) for row in result.collect()),
                    [(1, "model:HELLO"), (2, None), (3, "model:WORLD"), (4, None)],
                )
            self.assertCountEqual(
                received, [("model", prompt) for prompt in ("hello", "world", "reject")]
            )

    def test_sql_and_column_api_share_the_same_execution_path(self):
        with _model_http_server() as (origin, received):
            local_session, local_batches = _local_query_functions(origin)
            register_model("sql-and-column-model", "model", provider="databricks", timeout=5)
            frame = self.spark.createDataFrame([("hello",), ("world",)], ["prompt"])
            frame.createOrReplaceTempView("sql_and_column_api_documents")
            with (
                patch("pyspark.sql.ai._query_batches", new=local_batches),
                patch("pyspark.sql.ai._query_session", new=local_session),
            ):
                register_sql_function()
                sql_result = self.spark.sql(
                    "SELECT ai_query('sql-and-column-model', prompt) AS answer "
                    "FROM sql_and_column_api_documents"
                )
                column_result = frame.select(
                    ai_query("sql-and-column-model", "prompt").alias("answer")
                )
                expected = ["model:HELLO", "model:WORLD"]
                for result in (sql_result, column_result):
                    self.assertIn(
                        "ArrowEvalPython", result._jdf.queryExecution().executedPlan().toString()
                    )
                    self.assertEqual(sorted(row.answer for row in result.collect()), expected)
            self.assertCountEqual(
                received, [("model", prompt) for prompt in ("hello", "world", "hello", "world")]
            )

    def test_sql_function_fails_at_execution_for_unregistered_alias(self):
        register_sql_function(name="unused_alias_check")
        result = self.spark.sql("SELECT unused_alias_check('not-registered', 'hello')")
        with self.assertRaises(PythonException) as raised:
            result.collect()
        self.assertIn("AI_QUERY_ENDPOINT_NOT_REGISTERED", str(raised.exception))


if __name__ == "__main__":
    unittest.main(verbosity=2)
