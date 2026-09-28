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

import io
import json
import os
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pyspark.serializers import SpecialLengths, read_int

# The worker can be imported only under the Python worker runtime.
with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker import report_metrics


class WorkerMetricsProtocolTests(unittest.TestCase):
    def test_metrics_report_is_length_prefixed_json(self):
        outfile = io.BytesIO()
        report_metrics(outfile, 1.25, 2.5, 3.75, 42, 7, 9)
        stream = io.BytesIO(outfile.getvalue())

        self.assertEqual(read_int(stream), SpecialLengths.METRICS_DATA)
        length = read_int(stream)
        self.assertGreater(length, 0)
        self.assertEqual(
            json.loads(stream.read(length)),
            {
                "bootTimestampMs": 1250,
                "initTimestampMs": 2500,
                "finishTimestampMs": 3750,
                "pythonExecutionDurationMs": 42,
                "memoryBytesSpilled": 7,
                "diskBytesSpilled": 9,
            },
        )
        self.assertEqual(stream.read(), b"")


class WorkerMetricsSocketTests(unittest.TestCase):
    def test_live_worker_reports_spill_metrics_to_jvm(self):
        if os.name == "nt":
            self.skipTest("the custom worker module requires the Python daemon")

        from pyspark import SparkConf, SparkContext

        with tempfile.TemporaryDirectory() as temp_dir:
            marker_path = Path(temp_dir) / "marker"
            conf = (
                SparkConf()
                .set("spark.python.use.daemon", "true")
                .set("spark.python.worker.module", "pyspark.tests.test_worker_metrics")
                .set("spark.executorEnv.PYSPARK_METRICS_TEST_MARKER_PATH", str(marker_path))
            )
            sc = SparkContext("local[1]", "worker-metrics-json", conf=conf)
            try:
                accumulator = sc.accumulator(0)

                def increment(value):
                    from pyspark import TaskContext, shuffle

                    # Supply known spill totals to check their transport into JVM task metrics.
                    shuffle.MemoryBytesSpilled += 7
                    shuffle.DiskBytesSpilled += 9
                    accumulator.add(1)
                    return TaskContext.get().stageId(), value + 1

                result = sc.parallelize([1, 2], 1).map(increment).collect()
                self.assertEqual([value for _, value in result], [2, 3])
                self.assertEqual(accumulator.value, 2)
                sc._jsc.sc().listenerBus().waitUntilEmpty(10000)
                stage = sc._jsc.sc().statusStore().lastStageAttempt(result[0][0])
                self.assertEqual(stage.memoryBytesSpilled(), 14)
                self.assertEqual(stage.diskBytesSpilled(), 18)
                self.assertEqual(
                    marker_path.read_bytes(), struct.pack("!i", SpecialLengths.METRICS_DATA)
                )
            finally:
                sc.stop()

    def test_live_arrow_udf_sends_json_to_jvm(self):
        if os.name == "nt":
            self.skipTest("the custom worker module requires the Python daemon")

        from pyspark.testing.utils import have_pandas, have_pyarrow

        if not (have_pandas and have_pyarrow):
            self.skipTest("pandas and pyarrow are required for Arrow UDFs")

        from pyspark.sql import SparkSession
        from pyspark.sql.functions import pandas_udf

        with tempfile.TemporaryDirectory() as temp_dir:
            marker_path = Path(temp_dir) / "marker"
            spark = (
                SparkSession.builder.master("local[1]")
                .appName("worker-metrics-json-arrow")
                .config("spark.python.use.daemon", "true")
                .config("spark.python.worker.module", "pyspark.tests.test_worker_metrics")
                .config("spark.executorEnv.PYSPARK_METRICS_TEST_MARKER_PATH", str(marker_path))
                .getOrCreate()
            )
            try:

                @pandas_udf("long")
                def increment(values):
                    import time

                    time.sleep(0.02)
                    return values + 1

                result = spark.range(2).select(increment("id"))
                self.assertEqual([row[0] for row in result.collect()], [1, 2])
                self.assertIn(
                    "ArrowEvalPython", result._jdf.queryExecution().executedPlan().toString()
                )
                self.assertEqual(
                    marker_path.read_bytes(), struct.pack("!i", SpecialLengths.METRICS_DATA)
                )
            finally:
                spark.stop()

    def test_reused_worker_sends_one_json_report_per_task(self):
        if os.name == "nt":
            self.skipTest("the custom worker module requires the Python daemon")

        from pyspark import SparkConf, SparkContext

        with tempfile.TemporaryDirectory() as temp_dir:
            marker_path = Path(temp_dir) / "markers"
            conf = (
                SparkConf()
                .set("spark.python.use.daemon", "true")
                .set("spark.python.worker.reuse", "true")
                .set("spark.python.worker.module", "pyspark.tests.test_worker_metrics")
                .set("spark.executorEnv.PYSPARK_METRICS_TEST_MARKER_PATH", str(marker_path))
            )
            sc = SparkContext("local[1]", "worker-metrics-json-reuse", conf=conf)
            try:
                result = (
                    sc.parallelize([1, 2], 2).map(lambda value: (os.getpid(), value + 1)).collect()
                )
                self.assertEqual([value for _, value in result], [2, 3])
                self.assertEqual(result[0][0], result[1][0])
                self.assertEqual(
                    marker_path.read_bytes(),
                    struct.pack("!ii", SpecialLengths.METRICS_DATA, SpecialLengths.METRICS_DATA),
                )
            finally:
                sc.stop()


def _worker_main(infile, outfile):
    """Check the task report before forwarding it to the JVM socket."""
    from pyspark import worker

    original_report = worker.report_metrics

    def checked_report(
        out, boot, init, finish, execution_duration_ms, memory_bytes_spilled, disk_bytes_spilled
    ):
        frame = io.BytesIO()
        original_report(
            frame,
            boot,
            init,
            finish,
            execution_duration_ms,
            memory_bytes_spilled,
            disk_bytes_spilled,
        )
        data = frame.getvalue()
        marker, length = struct.unpack("!ii", data[:8])
        if marker != SpecialLengths.METRICS_DATA:
            raise AssertionError(f"unexpected metrics marker: {marker}")
        if len(data) != length + 8:
            raise AssertionError("metrics frame length does not match its payload")
        report = json.loads(data[8:])
        expected = {
            "bootTimestampMs": int(1000 * boot),
            "initTimestampMs": int(1000 * init),
            "finishTimestampMs": int(1000 * finish),
            "pythonExecutionDurationMs": execution_duration_ms,
            "memoryBytesSpilled": memory_bytes_spilled,
            "diskBytesSpilled": disk_bytes_spilled,
        }
        if report != expected:
            raise AssertionError(f"unexpected metrics report: {report}")
        with Path(os.environ["PYSPARK_METRICS_TEST_MARKER_PATH"]).open("ab") as marker_file:
            marker_file.write(data[:4])
        out.write(data)

    worker.report_metrics = checked_report
    try:
        worker.main(infile, outfile)
    finally:
        worker.report_metrics = original_report


# The configured Python daemon looks up main on this module.
main = _worker_main


if __name__ == "__main__":
    from pyspark.testing import main

    main()
