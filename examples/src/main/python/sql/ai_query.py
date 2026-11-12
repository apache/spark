#!/usr/bin/env python3
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

"""Run DataFrame and SQL inference locally or against Databricks or Bedrock."""

import argparse
import json
import os
import threading
from collections import Counter
from contextlib import AbstractContextManager, contextmanager, nullcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator, Optional
from unittest.mock import patch

import pyspark.sql._ai_client as client_module
import pyspark.sql.ai as ai_module
from pyspark.errors import PythonException

# $example on:imports$
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.ai import ai_query, register_model, register_sql_function

# $example off:imports$


@contextmanager
def local_endpoint() -> Iterator[tuple[dict[str, str], list[tuple[str, str]]]]:
    received: list[tuple[str, str]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            prompt = body["messages"][0]["content"][0]["text"]
            received.append((self.path, prompt))
            response = json.dumps(
                {
                    "output": {
                        "message": {"role": "assistant", "content": [{"text": prompt.upper()}]}
                    }
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, format: str, *args: Any) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        environment = {
            "AWS_ACCESS_KEY_ID": "demo-local",
            "AWS_SECRET_ACCESS_KEY": "demo-local",
            "AWS_SESSION_TOKEN": "demo-local",
            "AWS_CONFIG_FILE": os.devnull,
            "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_ENDPOINT_URL_BEDROCK_RUNTIME": f"http://127.0.0.1:{server.server_port}",
            "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "false",
            "NO_PROXY": "127.0.0.1,localhost",
        }
        try:
            yield environment, received
        finally:
            server.shutdown()
            thread.join()


def check_result(result: DataFrame, expected: list[tuple[int, Optional[str]]]) -> dict[str, Any]:
    plan = result._jdf.queryExecution().executedPlan().toString()
    if "ArrowEvalPython" not in plan:
        raise AssertionError("The query did not use Arrow Python execution")
    rows = sorted(result.collect(), key=lambda row: row.row_id)
    answers = [
        (row.row_id, row.answer.strip() if isinstance(row.answer, str) else row.answer)
        for row in rows
    ]
    if answers != expected:
        raise AssertionError("The responses did not match the synthetic prompts")
    return {"rows": [row.asDict() for row in rows], "plan": plan}


def run_checks(
    spark: SparkSession, args: argparse.Namespace, received: Optional[list[tuple[str, str]]]
) -> dict[str, Any]:
    local = args.provider == "local"
    provider = "bedrock" if local else args.provider
    model_id = "demo-model" if local else args.model_id
    profile = None if local else args.profile
    region = "us-east-1" if local else args.region
    auth_type = None if local else args.auth_type
    max_retries = 0 if local else 2
    # $example on:registration$
    register_model(
        "model",
        model_id,
        provider=provider,
        profile=profile,
        region=region,
        auth_type=auth_type,
        timeout=120,
        max_retries=max_retries,
        max_tokens=256,
        session=spark,
    )
    # $example off:registration$
    if local:
        register_model("other", "other-model", provider="bedrock", region="us-east-1")
    # $example on:sql$
    register_sql_function(session=spark)
    # $example off:sql$
    if local:
        register_model("other", "replacement", provider="bedrock", region="us-east-1")
        assert not spark.newSession().catalog.functionExists("ai_query")
        inputs = [
            (1, "model", "apple", "APPLE"),
            (2, "other", "banana", "BANANA"),
            (3, "model", None, None),
            (4, "model", "cherry", "CHERRY"),
            (5, "other", "date", "DATE"),
        ]
    else:
        inputs = [
            (1, "model", "Reply with exactly the word APPLE, without any other text.", "APPLE"),
            (2, "model", "Reply with exactly the word BANANA, without any other text.", "BANANA"),
            (3, "model", None, None),
        ]
    frame = spark.createDataFrame(
        inputs, "row_id int, endpoint string, prompt string, expected string"
    )
    frame = frame.coalesce(1) if local else frame.repartition(2)

    # $example on:column$
    column_result = frame.select(
        "row_id", "prompt", "expected", ai_query("model", "prompt", session=spark).alias("answer")
    )
    # $example off:column$
    # $example on:sql$
    frame.createOrReplaceTempView("documents")
    sql_result = spark.sql(
        "SELECT row_id, prompt, expected, ai_query('model', prompt) AS answer FROM documents"
    )
    # $example off:sql$
    expected = [(row_id, answer) for row_id, _, _, answer in inputs]
    report = {
        "spark_version": spark.version,
        "provider": args.provider,
        "model_id": model_id,
        "auth_type": args.auth_type,
        "master": spark.sparkContext.master,
        "arrow_execution": True,
        "column_api": check_result(column_result, expected),
        "sql": check_result(sql_result, expected),
    }
    if local:
        assert received is not None
        mixed = spark.sql(
            "SELECT row_id, prompt, expected, ai_query(endpoint, prompt) AS answer FROM documents"
        )
        report["sql_mixed_aliases"] = check_result(mixed, expected)
        expected_calls: Counter[tuple[str, str]] = Counter()
        for _, alias, prompt, _ in inputs:
            if prompt is not None:
                expected_calls[("/model/demo-model/converse", prompt)] += 2
                model = "other-model" if alias == "other" else "demo-model"
                expected_calls[(f"/model/{model}/converse", prompt)] += 1
        assert Counter(received) == expected_calls, received
        invalid = spark.sql("SELECT ai_query('missing', 'apple')")
        try:
            invalid.collect()
        except PythonException as error:
            assert "AI_QUERY_ENDPOINT_NOT_REGISTERED" in str(error), str(error)
        else:
            raise AssertionError("The unknown alias unexpectedly succeeded")
        assert Counter(received) == expected_calls, received
        report["unknown_alias"] = "analysis succeeds; execution fails before HTTP invocation"
        report["http_requests"] = len(received)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=("local", "databricks", "bedrock"), default="local")
    parser.add_argument("--model-id")
    parser.add_argument("--profile")
    parser.add_argument("--region")
    parser.add_argument("--auth-type", choices=("databricks-cli", "oauth-m2m"))
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if args.provider != "local" and not args.model_id:
        parser.error("--model-id is required for a live provider")

    source = Path(__file__).resolve().parents[5] / "python/pyspark/sql"
    if source.is_dir():
        for module in (ai_module, client_module):
            assert module.__file__ is not None
            loaded = Path(module.__file__)
            if loaded.read_bytes() != (source / loaded.name).read_bytes():
                raise RuntimeError(f"Runtime module differs from this checkout: {loaded.name}")

    context: AbstractContextManager[tuple[dict[str, str], Optional[list[tuple[str, str]]]]]
    if args.provider == "local":
        context = local_endpoint()
    else:
        context = nullcontext(({}, None))
    with context as (environment, received), patch.dict(os.environ, environment):
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("ai-query-example")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "2")
        )
        for name, value in environment.items():
            builder = builder.config(f"spark.executorEnv.{name}", value)
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        try:
            report = json.dumps(run_checks(spark, args, received), indent=2)
            if args.report is not None:
                args.report.write_text(report + "\n")
            print(report)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
