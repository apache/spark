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
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.sql._ai_client import (
    _ChatCompletionsClient,
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


@unittest.skipUnless(have_ai_query_dependencies, "AI query tests require pandas and requests")
class AIQueryTests(unittest.TestCase, PySparkErrorTestUtils):
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

    def test_http_errors_do_not_leak_or_retry(self):
        for status in (302, 400, 401, 403, 409):
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

    def test_bedrock_registered_model_batches(self):
        session = Mock()
        client = session.client.return_value
        client.converse.return_value = {
            "output": {"message": {"content": [{"text": "hello"}, {"text": " world"}]}}
        }
        create_session = Mock(return_value=session)
        config = Mock()
        sdk_modules = {
            "boto3": SimpleNamespace(Session=create_session),
            "botocore.config": SimpleNamespace(Config=config),
        }
        with (
            patch.dict("sys.modules", sdk_modules),
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

        def make_client(endpoint):
            nonlocal active, maximum
            client = Mock()

            def invoke(request):
                nonlocal active, maximum
                with lock:
                    active += 1
                    maximum = max(maximum, active)
                time.sleep(0.01 if request == "slow" else 0.001)
                with lock:
                    active -= 1
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
                with (
                    patch("pyspark.sql._ai_client._ChatCompletionsClient", side_effect=clients),
                    self.assertRaises(RuntimeError) as raised,
                ):
                    list(
                        _query_batches(
                            _ModelEndpoint("model", provider="databricks", max_concurrency=2),
                            iter(batches),
                        )
                    )
                self.assertIs(raised.exception, batch_error or close_error)
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
    def test_model_query_with_arrow_and_local_endpoint(self):
        received = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                prompt = body["messages"][0]["content"]
                received.append((prompt, self.headers["Authorization"]))
                result = {"choices": [{"message": {"content": prompt.upper()}}]}
                response = json.dumps(result).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)

            def log_message(self, *args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            url = f"http://127.0.0.1:{server.server_port}/invocations"

            def local_batches(endpoint, batches):
                # Install the local transport in the worker after the UDF is deserialized.
                from pyspark.sql import _ai_client

                client_type = _ai_client._ChatCompletionsClient

                def make_client(config):
                    client = object.__new__(client_type)
                    client.endpoint = config
                    client.url = url
                    client.workspace = SimpleNamespace(
                        config=SimpleNamespace(
                            authenticate=lambda: {"Authorization": "Bearer test"}
                        )
                    )
                    client.http = _ai_client._databricks_session()
                    return client

                with patch.object(_ai_client, "_ChatCompletionsClient", side_effect=make_client):
                    yield from _ai_client._query_batches(endpoint, batches)

            try:
                register_model("local-model", "model", provider="databricks", timeout=5)
                frame = self.spark.createDataFrame(
                    [(1, "hello"), (2, None), (3, "world")], "id int, prompt string"
                )
                with patch("pyspark.sql.ai._query_batches", new=local_batches):
                    result = frame.withColumn("answer", ai_query("local-model", "prompt"))
                    self.assertIn(
                        "ArrowEvalPython", result._jdf.queryExecution().executedPlan().toString()
                    )
                    self.assertEqual(
                        sorted((row.id, row.answer) for row in result.collect()),
                        [(1, "HELLO"), (2, None), (3, "WORLD")],
                    )
                self.assertCountEqual(
                    received, [("hello", "Bearer test"), ("world", "Bearer test")]
                )
            finally:
                server.shutdown()
                thread.join()


if __name__ == "__main__":
    unittest.main(verbosity=2)
