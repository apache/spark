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

"""Run SQL and PySpark inference through the existing Python backend, entirely locally.

Run with the prototype installed in the driver and Python workers, as described in
docs/sql-ai-query.md: python dev/ai_query_sql_demo.py

The HTTP server simulates Bedrock Converse by uppercasing prompts. The real SDK,
Spark workers, Arrow, and _query_batches execute with synthetic local credentials.

The SQL adapter snapshots aliases when registered. Alias lookup happens in workers,
and accepts row-dependent aliases. Analysis-time validation, SQL-only initialization,
Spark Connect, and live cloud inference remain outside this demo's validation.
"""

import json
import os
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import pyspark.sql._ai_client as client_module
import pyspark.sql.ai as ai_module
from pyspark.errors import PythonException
from pyspark.sql import SparkSession
from pyspark.sql.ai import ai_query, register_model, register_sql_function
from pyspark.sql.functions import col


def run_demo(spark: SparkSession, received: list) -> dict:
    register_model(
        "summarizer", "demo-model", provider="bedrock", region="us-east-1", max_retries=0
    )
    register_model("other", "other-model", provider="bedrock", region="us-east-1", max_retries=0)
    register_sql_function(session=spark)
    # Demonstrate that later registrations do not change the SQL adapter's snapshot.
    register_model("other", "replacement", provider="bedrock", region="us-east-1")
    assert not spark.newSession().catalog.functionExists("ai_query")

    inputs = [
        (1, "summarizer", "apple"),
        (2, "other", "banana"),
        (3, "summarizer", None),
        (4, "summarizer", "cherry"),
        (5, "other", "date"),
    ]
    documents = spark.createDataFrame(inputs, "id int, model string, description string")
    documents = documents.coalesce(1)
    documents.createOrReplaceTempView("documents")
    results = {
        "pyspark": documents.select(
            "id", ai_query("summarizer", col("description")).alias("summary")
        ),
        "sql": spark.sql(
            "SELECT id, ai_query('summarizer', description) AS summary FROM documents"
        ),
        "sql_mixed_aliases": spark.sql(
            "SELECT id, ai_query(model, description) AS summary FROM documents"
        ),
    }
    expected = [(1, "APPLE"), (2, "BANANA"), (3, None), (4, "CHERRY"), (5, "DATE")]
    report = {"spark_version": spark.version, "backend": "local simulated Bedrock Converse"}
    for name, result in results.items():
        plan = result._jdf.queryExecution().executedPlan().toString()
        assert "ArrowEvalPython" in plan, plan
        rows = sorted((row.id, row.summary) for row in result.collect())
        assert rows == expected, (name, rows)
        report[name] = {"rows": rows, "plan": plan}

    expected_calls = Counter()
    for _, alias, prompt in inputs:
        if prompt is not None:
            expected_calls[("/model/demo-model/converse", prompt)] += 2
            model = "other-model" if alias == "other" else "demo-model"
            expected_calls[(f"/model/{model}/converse", prompt)] += 1
    assert Counter(received) == expected_calls, received

    invalid = spark.sql("SELECT ai_query('missing', 'apple')")
    invalid._jdf.queryExecution().analyzed()
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
    source = Path(__file__).resolve().parents[1] / "python/pyspark/sql"
    for module in (ai_module, client_module):
        loaded = Path(module.__file__)
        if loaded.read_bytes() != (source / loaded.name).read_bytes():
            raise RuntimeError(f"Runtime module differs from this checkout: {loaded.name}")

    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
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

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        worker_env = {
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
            with patch.dict(os.environ, worker_env):
                builder = (
                    SparkSession.builder.master("local[2]")
                    .appName("ai-query-sql-demo")
                    .config("spark.ui.enabled", "false")
                    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10")
                )
                for name, value in worker_env.items():
                    builder = builder.config(f"spark.executorEnv.{name}", value)
                spark = builder.getOrCreate()
                spark.sparkContext.setLogLevel("OFF")
                try:
                    print(json.dumps(run_demo(spark, received), indent=2))
                finally:
                    spark.stop()
        finally:
            server.shutdown()
            thread.join()


if __name__ == "__main__":
    main()
