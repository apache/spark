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

# These reporting helpers live in worker-only modules. Allow their import for this test module.
with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker import WorkerMetrics, report_metrics, report_worker_metrics
    from pyspark.worker_util import RunnerConf


class WorkerMetricsTests(unittest.TestCase):
    def test_only_recorded_values_are_exported(self):
        metrics = WorkerMetrics()
        metrics.register("rowsProcessed", unit="count")
        metrics.register("inputBytes", unit="bytes")
        self.assertEqual(metrics.to_report(), {})

        metrics.set("rowsProcessed", 0)
        self.assertEqual(
            metrics.to_report(),
            {
                "rowsProcessed": {
                    "value": 0,
                    "unit": "count",
                }
            },
        )
        metrics.set("rowsProcessed", 42)
        self.assertEqual(metrics.to_report()["rowsProcessed"]["value"], 42)
        self.assertNotIn("inputBytes", metrics.to_report())

    def test_duplicate_registration_preserves_existing_metric(self):
        metrics = WorkerMetrics()
        metrics.register("counter", unit="count")
        metrics.set("counter", 42)
        original = metrics.to_report()
        for unit in ("count", "bytes"):
            with self.subTest(unit=unit):
                with self.assertRaisesRegex(ValueError, "already registered"):
                    metrics.register("counter", unit=unit)
                self.assertEqual(metrics.to_report(), original)

    def test_set_requires_registration(self):
        metrics = WorkerMetrics()
        with self.assertRaisesRegex(ValueError, "not registered"):
            metrics.set("counter", 42)
        self.assertEqual(metrics.to_report(), {})

    def test_invalid_definitions_are_rejected(self):
        for definition in (
            {"name": "", "unit": "count"},
            {"name": "counter", "unit": ""},
            {"name": "counter", "unit": None},
        ):
            with self.subTest(definition=definition):
                metrics = WorkerMetrics()
                with self.assertRaisesRegex(ValueError, "nonempty string"):
                    metrics.register(**definition)
                metrics.register("counter", unit="count")
                metrics.set("counter", 42)
                self.assertEqual(metrics.to_report()["counter"]["value"], 42)

    def test_json_values_keep_their_types(self):
        metrics = WorkerMetrics()
        metrics.register("sample", unit="sample")
        for value in (None, True, "ready", 0, 1 << 80, 0.75, [1, None], {"count": 42}):
            with self.subTest(value=value):
                metrics.set("sample", value)
                recorded = metrics.to_report()["sample"]["value"]
                self.assertIs(type(recorded), type(value))
                self.assertEqual(recorded, value)

    def test_reports_and_tasks_have_independent_state(self):
        metrics = WorkerMetrics()
        metrics.register("counter", unit="count")
        metrics.set("counter", 42)
        snapshot = metrics.to_report()
        metrics.set("counter", 99)
        self.assertEqual(snapshot["counter"]["value"], 42)
        snapshot["counter"]["unit"] = "bytes"
        self.assertEqual(metrics.to_report()["counter"]["unit"], "count")
        self.assertEqual(metrics.to_report()["counter"]["value"], 99)

        next_task = WorkerMetrics()
        next_task.register("counter", unit="count")
        self.assertEqual(next_task.to_report(), {})

    def test_nested_values_are_independent_copies(self):
        metrics = WorkerMetrics()
        metrics.register("sample", unit="sample")
        value = {"counts": [1, 2]}
        metrics.set("sample", value)
        value["counts"].append(3)
        snapshot = metrics.to_report()
        self.assertEqual(snapshot["sample"]["value"], {"counts": [1, 2]})
        snapshot["sample"]["value"]["counts"].append(4)
        self.assertEqual(metrics.to_report()["sample"]["value"], {"counts": [1, 2]})


class WorkerMetricsProtocolTests(unittest.TestCase):
    protocol_version_key = "spark.python.worker.metrics.protocol.version"

    def report(self, conf):
        outfile = io.BytesIO()
        report_worker_metrics(outfile, 1.25, 2.5, 3.75, 42, RunnerConf(conf))
        return outfile.getvalue()

    def test_legacy_bytes_without_v1_capability(self):
        expected = struct.pack("!iqqqq", SpecialLengths.TIMING_DATA, 1250, 2500, 3750, 42)
        self.assertEqual(self.report({}), expected)
        self.assertEqual(self.report({self.protocol_version_key: "2"}), expected)

    def test_v1_named_metrics_frame(self):
        stream = io.BytesIO(self.report({self.protocol_version_key: "1"}))
        self.assertEqual(read_int(stream), SpecialLengths.METRICS_DATA)
        length = read_int(stream)
        self.assertGreater(length, 0)
        self.assertLessEqual(length, 64 * 1024)
        self.assertEqual(
            json.loads(stream.read(length)),
            {
                "kind": "spark.python.worker.metrics",
                "version": 1,
                "metrics": {
                    "bootTimestampMs": {
                        "value": 1250,
                        "unit": "timestampMillis",
                    },
                    "initTimestampMs": {
                        "value": 2500,
                        "unit": "timestampMillis",
                    },
                    "finishTimestampMs": {
                        "value": 3750,
                        "unit": "timestampMillis",
                    },
                    "processingDurationMs": {
                        "value": 42,
                        "unit": "milliseconds",
                    },
                },
            },
        )
        self.assertEqual(stream.read(), b"")

    def read_metrics(self, stream):
        self.assertEqual(read_int(stream), SpecialLengths.METRICS_DATA)
        length = read_int(stream)
        payload = json.loads(stream.read(length))
        self.assertEqual(stream.read(), b"")
        return payload["metrics"]

    def test_additional_metric_is_preserved(self):
        entries = self.read_metrics(io.BytesIO(self.report({self.protocol_version_key: "1"})))
        metrics = WorkerMetrics()
        for name, entry in entries.items():
            metrics.register(name, unit=entry["unit"])
            metrics.set(name, entry["value"])
        metrics.register("futureRatio", unit="ratio")
        metrics.set("futureRatio", 0.75)
        outfile = io.BytesIO()
        report_metrics(outfile, metrics.to_report())
        decoded = self.read_metrics(io.BytesIO(outfile.getvalue()))
        self.assertEqual(len(decoded), 5)
        self.assertEqual(decoded, metrics.to_report())

    def test_generic_report_does_not_require_timing_fields(self):
        counter = {
            "rowsProcessed": {
                "value": 9007199254740993,
                "unit": "count",
            }
        }
        for metrics in ({}, counter):
            with self.subTest(metrics=metrics):
                outfile = io.BytesIO()
                report_metrics(outfile, metrics)
                self.assertEqual(self.read_metrics(io.BytesIO(outfile.getvalue())), metrics)

    def test_json_values_are_preserved(self):
        for value in (None, True, "ready", -(1 << 80), 1 << 80, 0.75, [1, None], {"count": 42}):
            with self.subTest(value=value):
                outfile = io.BytesIO()
                report_metrics(outfile, {"sample": {"value": value, "unit": "sample"}})
                decoded = self.read_metrics(io.BytesIO(outfile.getvalue()))["sample"]["value"]
                self.assertIs(type(decoded), type(value))
                self.assertEqual(decoded, value)

    def test_invalid_records_do_not_start_a_frame(self):
        metric = {"value": 42, "unit": "count"}
        invalid_metrics = [{"": metric}, {"counter": None}]
        invalid_metrics.extend(
            {"counter": {k: v for k, v in metric.items() if k != field}} for field in metric
        )
        invalid_metrics.extend({"counter": {**metric, "unit": unit}} for unit in ("", None, 1))
        for metrics in invalid_metrics:
            with self.subTest(metrics=metrics):
                outfile = io.BytesIO()
                with self.assertRaises(ValueError):
                    report_metrics(outfile, {"validCounter": metric, **metrics})
                self.assertEqual(outfile.getvalue(), b"")

    def test_non_json_values_do_not_start_a_frame(self):
        for value in (object(), {1, 2}, float("nan"), float("inf"), -float("inf"), [float("nan")]):
            with self.subTest(value=value):
                outfile = io.BytesIO()
                with self.assertRaises((TypeError, ValueError)):
                    report_metrics(outfile, {"sample": {"value": value, "unit": "sample"}})
                self.assertEqual(outfile.getvalue(), b"")

    def test_oversized_report_does_not_start_a_frame(self):
        outfile = io.BytesIO()
        metrics = {
            "x" * (64 * 1024): {
                "value": 42,
                "unit": "count",
            }
        }
        with self.assertRaisesRegex(ValueError, "exceeds 64 KiB"):
            report_metrics(outfile, metrics)
        self.assertEqual(outfile.getvalue(), b"")


class WorkerMetricsSocketTests(unittest.TestCase):
    def test_live_arrow_udf_sends_legacy_marker_to_jvm(self):
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
                .appName("worker-metrics-legacy-arrow")
                .config("spark.python.use.daemon", "true")
                .config("spark.python.worker.module", "pyspark.tests.test_worker_metrics")
                .config("spark.executorEnv.PYSPARK_METRICS_TEST_MARKER_PATH", str(marker_path))
                .config("spark.executorEnv.PYSPARK_METRICS_TEST_MODE", "legacy")
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

                def find_arrow_exec(plan):
                    if plan.getClass().getSimpleName() == "ArrowEvalPythonExec":
                        return plan
                    children = plan.children()
                    for index in range(children.size()):
                        found = find_arrow_exec(children.apply(index))
                        if found is not None:
                            return found
                    return None

                arrow_exec = find_arrow_exec(result._jdf.queryExecution().executedPlan())
                self.assertIsNotNone(arrow_exec)
                self.assertGreater(arrow_exec.metrics().apply("pythonProcessingTime").value(), 0)
                self.assertEqual(
                    marker_path.read_bytes(), struct.pack("!i", SpecialLengths.TIMING_DATA)
                )
            finally:
                spark.stop()

    def test_live_arrow_udf_sends_v1_marker_to_jvm(self):
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
                .appName("worker-metrics-v1-arrow")
                .config("spark.python.use.daemon", "true")
                .config("spark.python.worker.module", "pyspark.tests.test_worker_metrics")
                .config("spark.executorEnv.PYSPARK_METRICS_TEST_MARKER_PATH", str(marker_path))
                .config("spark.executorEnv.PYSPARK_METRICS_TEST_MODE", "v1")
                .getOrCreate()
            )
            try:

                @pandas_udf("long")
                def increment(values):
                    return values + 1

                result = spark.range(2).select(increment("id"))
                self.assertIn(
                    "ArrowEvalPython", result._jdf.queryExecution().executedPlan().toString()
                )
                self.assertEqual([row[0] for row in result.collect()], [1, 2])
                self.assertEqual(
                    marker_path.read_bytes(), struct.pack("!i", SpecialLengths.METRICS_DATA)
                )
            finally:
                spark.stop()

    def test_live_worker_sends_v1_marker_to_jvm(self):
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
                .set("spark.executorEnv.PYSPARK_METRICS_TEST_MODE", "v1")
            )
            sc = SparkContext("local[1]", "worker-metrics-v1-marker", conf=conf)
            try:
                result = sc.parallelize([1, 2], 1).map(lambda value: value + 1).collect()
                self.assertEqual(result, [2, 3])
                self.assertEqual(
                    marker_path.read_bytes(), struct.pack("!i", SpecialLengths.METRICS_DATA)
                )
            finally:
                sc.stop()

    def test_live_worker_sends_legacy_marker_to_jvm(self):
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
                .set("spark.executorEnv.PYSPARK_METRICS_TEST_MODE", "legacy")
            )
            sc = SparkContext("local[1]", "worker-metrics-legacy-marker", conf=conf)
            try:
                result = sc.parallelize([1, 2], 1).map(lambda value: value + 1).collect()
                self.assertEqual(result, [2, 3])
                self.assertEqual(
                    marker_path.read_bytes(), struct.pack("!i", SpecialLengths.TIMING_DATA)
                )
            finally:
                sc.stop()

    def test_reused_worker_sends_v1_marker_for_each_task(self):
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
                .set("spark.executorEnv.PYSPARK_METRICS_TEST_MODE", "v1")
            )
            sc = SparkContext("local[1]", "worker-metrics-v1-reuse", conf=conf)
            try:
                result = (
                    sc.parallelize([1, 2], 2).map(lambda value: (os.getpid(), value + 1)).collect()
                )
                self.assertEqual([value for _, value in result], [2, 3])
                self.assertEqual(result[0][0], result[1][0])
                self.assertEqual(
                    marker_path.read_bytes(),
                    struct.pack("!ii", *([SpecialLengths.METRICS_DATA] * 2)),
                )
            finally:
                sc.stop()


def _worker_main(infile, outfile):
    """Check the actual task report before forwarding it to the JVM socket."""
    from pyspark import worker

    original_report = worker.report_worker_metrics

    def checked_report(out, boot, init, finish, processing_time_ms, runner_conf):
        if runner_conf.get("spark.python.worker.metrics.protocol.version") != "1":
            raise AssertionError("JVM did not advertise worker metrics v1")
        mode = os.environ.get("PYSPARK_METRICS_TEST_MODE", "v1")
        if mode == "legacy":
            report_conf = RunnerConf()
            expected_marker = SpecialLengths.TIMING_DATA
        elif mode == "v1":
            report_conf = runner_conf
            expected_marker = SpecialLengths.METRICS_DATA
        else:
            raise AssertionError(f"unknown metrics test mode: {mode}")
        frame = io.BytesIO()
        original_report(frame, boot, init, finish, processing_time_ms, report_conf)
        marker = struct.unpack("!i", frame.getvalue()[:4])[0]
        if marker != expected_marker:
            raise AssertionError(f"expected marker {expected_marker}, received {marker}")
        with Path(os.environ["PYSPARK_METRICS_TEST_MARKER_PATH"]).open("ab") as marker_file:
            marker_file.write(frame.getvalue()[:4])
        out.write(frame.getvalue())

    worker.report_worker_metrics = checked_report
    try:
        worker.main(infile, outfile)
    finally:
        worker.report_worker_metrics = original_report


# The configured Python daemon looks up main on this module.
main = _worker_main


if __name__ == "__main__":
    from pyspark.testing import main

    main()
