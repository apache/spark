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

"""
Verify that the pipelined Python UDF code path is actually being used.

Checks:
  1. JVM-side: PipelinedWriterThread is created (evidenced by SPARK_PIPELINED_UDF env var
     being propagated to the Python worker when the config is enabled).
  2. Python-side: pipelined_process() is called (evidenced by SPARK_PIPELINED_UDF_ACTIVE
     env var being set inside the function).
  3. When config is disabled, neither env var is set.

Each test runs in a separate Python process to ensure a fresh SparkContext.
"""

import os
import subprocess
import sys

# Allow running from the Spark root directory.
SPARK_HOME = os.path.join(os.path.dirname(__file__), "../../../../..")
sys.path.insert(0, SPARK_HOME)


WORKER_SCRIPT = '''
import os, sys
sys.path.insert(0, "{spark_home}")

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType

spark = (
    SparkSession.builder.master("local[1]")
    .appName("VerifyPipelinedPath")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.python.worker.reuse", "false")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.python.udf.pipelined.enabled", "{pipelined}")
    .getOrCreate()
)

@pandas_udf(StringType())
def report_env(x: pd.Series) -> pd.Series:
    # SPARK_PIPELINED_UDF: set by JVM when pipelinedEnabled=true
    # SPARK_PIPELINED_UDF_ACTIVE: set by Python pipelined_process() at the start
    jvm_flag = os.environ.get("SPARK_PIPELINED_UDF", "<not set>")
    py_flag = os.environ.get("SPARK_PIPELINED_UDF_ACTIVE", "<not set>")
    queue_depth = os.environ.get("SPARK_PIPELINED_UDF_QUEUE_DEPTH", "<not set>")
    report = (
        f"jvm_flag={{jvm_flag}}"
        f"|py_active={{py_flag}}"
        f"|queue_depth={{queue_depth}}"
    )
    return pd.Series([report] * len(x))

df = spark.range(100, numPartitions=1).select(
    col("id"), report_env(col("id")).alias("report")
)
row = df.select("report").head()
print("WORKER_REPORT:" + row[0])
spark.stop()
'''


def run_check(pipelined):
    """Run a separate Python process with the given pipelined setting."""
    script = WORKER_SCRIPT.format(
        spark_home=os.path.abspath(SPARK_HOME),
        pipelined="true" if pipelined else "false",
    )
    env = os.environ.copy()
    env["SPARK_HOME"] = os.path.abspath(SPARK_HOME)
    py4j_zip = os.path.join(os.path.abspath(SPARK_HOME), "python/lib/py4j-0.10.9.9-src.zip")
    pyspark_path = os.path.join(os.path.abspath(SPARK_HOME), "python")
    env["PYTHONPATH"] = f"{pyspark_path}:{py4j_zip}:" + env.get("PYTHONPATH", "")

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, env=env, timeout=120
    )

    for line in result.stdout.splitlines():
        if line.startswith("WORKER_REPORT:"):
            return line[len("WORKER_REPORT:"):]

    print("  STDOUT:", result.stdout[-500:] if result.stdout else "<empty>")
    print("  STDERR:", result.stderr[-1000:] if result.stderr else "<empty>")
    raise RuntimeError(f"WORKER_REPORT not found in output (exit code {result.returncode})")


def parse_report(report):
    return dict(kv.split("=", 1) for kv in report.split("|") if "=" in kv)


def verify():
    print()
    print("=" * 70)
    print("  Verify Pipelined Code Path")
    print("=" * 70)

    # --- Test 1: pipelined=false ---
    print()
    print("--- Test 1: pipelined=false ---")
    report = run_check(pipelined=False)
    print(f"  Worker report: {report}")
    p = parse_report(report)

    assert p["jvm_flag"] == "<not set>", \
        f"JVM should NOT set SPARK_PIPELINED_UDF, got: {p['jvm_flag']}"
    assert p["py_active"] == "<not set>", \
        f"Python pipelined_process should NOT run, got: {p['py_active']}"
    print("  PASS: JVM flag not set, Python pipelined_process not called")

    # --- Test 2: pipelined=true ---
    print()
    print("--- Test 2: pipelined=true ---")
    report = run_check(pipelined=True)
    print(f"  Worker report: {report}")
    p = parse_report(report)

    assert p["jvm_flag"] == "1", \
        f"JVM should set SPARK_PIPELINED_UDF=1, got: {p['jvm_flag']}"
    assert p["py_active"] == "1", \
        f"Python pipelined_process should set ACTIVE=1, got: {p['py_active']}"
    assert p["queue_depth"] == "2", \
        f"Queue depth should be 2, got: {p['queue_depth']}"
    print("  PASS: JVM flag=1, Python pipelined_process active, queue_depth=2")

    print()
    print("=" * 70)
    print("  All checks passed! Both JVM and Python pipelined paths confirmed.")
    print("=" * 70)
    print()


if __name__ == "__main__":
    verify()
