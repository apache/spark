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

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.sql._ai_client import (
    _ChatCompletionsClient,
    _ConverseClient,
    _ModelEndpoint,
    _query_batches,
)
from pyspark.sql.ai import ai_query, register_model
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
        return response

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
        client = self.client()
        client.http.post.side_effect = [
            self.response(429),
            requests.ConnectionError("sensitive diagnostic"),
            self.response(choices=[{"message": {"content": "answer"}}]),
        ]
        with patch("pyspark.sql._ai_client.time.sleep"):
            self.assertEqual(client.invoke("hello"), "answer")
        calls = client.http.post.call_args_list
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[0].kwargs["json"], calls[2].kwargs["json"])
        self.assertFalse(calls[0].kwargs["allow_redirects"])
        self.assertEqual(client.workspace.config.authenticate.call_count, 3)

    def test_retry_after_is_respected(self):
        client = self.client()
        retry = self.response(429)
        retry.headers = {"Retry-After": "3"}
        client.http.post.side_effect = [
            retry,
            self.response(choices=[{"message": {"content": "answer"}}]),
        ]
        with patch("pyspark.sql._ai_client.time.sleep") as sleep:
            self.assertEqual(client.invoke("hello"), "answer")
        sleep.assert_called_once_with(3)

    def test_retry_after_exceeding_budget_preserves_http_status(self):
        for status in (429, 503):
            with self.subTest(status=status):
                client = self.client(timeout=5)
                response = self.response(status)
                response.headers = {"Retry-After": "30"}
                client.http.post.return_value = response
                with (
                    patch("pyspark.sql._ai_client.time.monotonic", return_value=0),
                    patch("pyspark.sql._ai_client.time.sleep") as sleep,
                    self.assertRaises(PySparkRuntimeError) as raised,
                ):
                    client.invoke("private prompt")
                self.check_error(
                    raised.exception,
                    "AI_QUERY_INVOCATION_FAILED.RETRY_AFTER_EXCEEDS_TIMEOUT",
                    {"target": "Databricks model", "status": str(status), "retry_after": "30.0"},
                )
                sleep.assert_not_called()
                client.http.post.assert_called_once()

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

    def test_retry_deadline(self):
        client = self.client(timeout=1)
        client.http.post.return_value = self.response(429)
        with (
            patch("pyspark.sql._ai_client.time.monotonic", side_effect=[0, 0, 2, 2]),
            patch("pyspark.sql._ai_client.time.sleep"),
            self.assertRaises(PySparkRuntimeError) as raised,
        ):
            client.invoke("hello")
        self.check_error(
            raised.exception,
            "AI_QUERY_INVOCATION_FAILED.TIMEOUT",
            {"target": "Databricks model"},
        )

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

            def invoke(request):
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

                def invoke(request):
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


@unittest.skipUnless(
    have_ai_query_dependencies and have_pyarrow,
    "Spark AI query tests require pandas, requests and pyarrow",
)
class AIQuerySparkTests(sqlutils.ReusedSQLTestCase):
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
        received = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                prompt = body["messages"][0]["content"]
                received.append((self.path, prompt, self.headers["Authorization"]))
                result = {"choices": [{"message": {"content": prompt.upper()}}]}
                response = json.dumps(result).encode()
                self.send_response(400 if prompt == "reject" else 200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)

            def log_message(self, *args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            origin = f"http://127.0.0.1:{server.server_port}"

            def local_batches(endpoint, batches, *, on_error):
                from urllib.parse import urlsplit

                import requests

                from pyspark.sql import _ai_client

                session_type = requests.Session
                config = SimpleNamespace(
                    host="https://example.test",
                    authenticate=lambda: {"Authorization": "Bearer test"},
                )
                sdk = SimpleNamespace(
                    WorkspaceClient=lambda **kwargs: SimpleNamespace(config=config)
                )

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
                ):
                    yield from _ai_client._query_batches(endpoint, batches, on_error=on_error)

            try:
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
                        [(1, "HELLO"), (2, None), (3, "WORLD"), (4, None)],
                    )
                self.assertCountEqual(
                    received,
                    [
                        ("/serving-endpoints/model/invocations", prompt, "Bearer test")
                        for prompt in ("hello", "world", "reject")
                    ],
                )
            finally:
                server.shutdown()
                thread.join()


if __name__ == "__main__":
    unittest.main(verbosity=2)
