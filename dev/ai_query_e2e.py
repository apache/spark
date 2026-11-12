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

"""Opt-in live Spark/Arrow test against a registered model."""

import argparse
import json
from pathlib import Path

import pyspark
import pyspark.sql._ai_client as client_module
import pyspark.sql.ai as ai_module
from pyspark.sql import SparkSession
from pyspark.sql.ai import ai_query, register_model, register_sql_function


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=("databricks", "bedrock"), required=True)
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--profile")
    parser.add_argument("--region")
    parser.add_argument("--auth-type", choices=("databricks-cli", "oauth-m2m"))
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()

    source = Path(__file__).resolve().parents[1] / "python/pyspark/sql"
    for module in (ai_module, client_module):
        loaded = Path(module.__file__)
        if loaded.read_bytes() != (source / loaded.name).read_bytes():
            raise RuntimeError(f"Runtime module differs from this checkout: {loaded.name}")

    inputs = [
        (1, "Reply with exactly the word APPLE, without any other text.", "APPLE"),
        (2, "Reply with exactly the word BANANA, without any other text.", "BANANA"),
        (3, None, None),
    ]
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("ai-query-e2e")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        register_model(
            "live",
            args.model_id,
            provider=args.provider,
            profile=args.profile,
            region=args.region,
            auth_type=args.auth_type,
            timeout=120,
            max_tokens=256,
        )
        frame = spark.createDataFrame(inputs, "row_id int, prompt string, expected string")

        def check(result, label):
            plan = result._jdf.queryExecution().executedPlan().toString()
            if "ArrowEvalPython" not in plan:
                raise AssertionError(f"{label}: the query did not use Spark's Arrow Python execution")
            rows = sorted(result.collect(), key=lambda row: row.row_id)
            if len(rows) != len(inputs):
                raise AssertionError(f"{label}: the output row count changed")
            for row in rows:
                answer = row.answer.strip() if isinstance(row.answer, str) else row.answer
                if answer != row.expected:
                    raise AssertionError(f"{label}: the model response did not match the synthetic prompt")
            return plan, rows

        column_plan, column_rows = check(
            frame.repartition(2).withColumn("answer", ai_query("live", "prompt")), "Column API"
        )

        register_sql_function()
        frame.repartition(2).createOrReplaceTempView("ai_query_e2e_inputs")
        sql_plan, sql_rows = check(
            spark.sql(
                "SELECT row_id, prompt, expected, ai_query('live', prompt) AS answer "
                "FROM ai_query_e2e_inputs"
            ),
            "SQL",
        )

        report = {
            "spark_version": pyspark.__version__,
            "provider": args.provider,
            "model_id": args.model_id,
            "auth_type": args.auth_type,
            "master": spark.sparkContext.master,
            "arrow_execution": True,
            "column_api": {"rows": [row.asDict() for row in column_rows], "plan": column_plan},
            "sql": {"rows": [row.asDict() for row in sql_rows], "plan": sql_plan},
        }
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
