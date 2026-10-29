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
import signal
import sys
import tempfile
import threading
import time
import unittest
from typing import Any, Optional
from unittest.mock import patch

has_resource_module = True
try:
    import resource
except ImportError:
    has_resource_module = False

from py4j.protocol import Py4JJavaError

from pyspark import SparkConf, SparkContext
from pyspark.accumulators import _accumulatorRegistry
from pyspark.core.files import SparkFiles
from pyspark.serializers import (
    CloudPickleSerializer,
    CPickleSerializer,
    SpecialLengths,
    read_int,
    write_int,
    write_long,
    write_with_length,
)
from pyspark.sql.types import LongType, StructField, StructType
from pyspark.taskcontext import TaskContext
from pyspark.testing.utils import QuietTest, ReusedPySparkTestCase, eventually
from pyspark.util import PythonEvalType
from pyspark.worker_metrics import WorkerMetrics

with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker import main as worker_main


class WorkerMetricsTests(unittest.TestCase):
    def setUp(self):
        metrics = WorkerMetrics()
        metrics.reset()
        self.addCleanup(metrics.reset)

    def test_constructor_reuses_collector_without_reset(self):
        metrics = WorkerMetrics()
        metrics.set("bootTimestampMs", 123)
        metrics.increment("rows", 2)
        self.assertIs(WorkerMetrics(), metrics)
        self.assertEqual(WorkerMetrics().to_dict(), {"bootTimestampMs": 123, "rows": 2})

    def test_reset_isolates_new_task_from_existing_timer(self):
        metrics = WorkerMetrics()
        metrics.set("previousTaskRows", 10)
        previous_timer = metrics.measure("previousTaskDuration")
        with patch(
            "pyspark.worker_metrics.time.perf_counter_ns",
            side_effect=[0, 5_000_000, 10_000_000, 12_000_000],
        ):
            with previous_timer:
                metrics.reset()
                metrics.increment("currentTaskRows")
            with metrics.measure("currentTaskDuration"):
                pass
        previous_timer.add_duration_ns(3_000_000)
        self.assertIs(WorkerMetrics(), metrics)
        self.assertEqual(metrics.to_dict(), {"currentTaskRows": 1, "currentTaskDuration": 2})

    def test_set_current_timestamp_uses_epoch_milliseconds(self):
        metrics = WorkerMetrics()
        with patch("pyspark.worker_metrics.time.time_ns", return_value=1_700_000_000_123_456_789):
            metrics.set_current_timestamp("bootTimestampMs")
        self.assertEqual(metrics.to_dict(), {"bootTimestampMs": 1_700_000_000_123})

    def test_accumulate_before_converting_to_milliseconds(self):
        metrics = WorkerMetrics()
        # Control the clock to retain fractional milliseconds and exclude gaps between blocks.
        with patch(
            "pyspark.worker_metrics.time.perf_counter_ns",
            side_effect=[10_000_000, 10_600_000, 100_000_000, 100_700_000],
        ):
            with metrics.measure("duration"):
                pass
            self.assertEqual(metrics.to_dict(), {"duration": 0})
            with metrics.measure("duration"):
                pass
        self.assertEqual(metrics.to_dict(), {"duration": 1})

    def test_nested_timings_are_inclusive(self):
        metrics = WorkerMetrics()
        with patch(
            "pyspark.worker_metrics.time.perf_counter_ns",
            side_effect=[0, 1_000_000, 3_000_000, 5_000_000],
        ):
            with metrics.measure("outer"):
                with metrics.measure("inner"):
                    pass
        self.assertEqual(metrics.to_dict(), {"outer": 5, "inner": 2})

    def test_reuse_timer_scope(self):
        metrics = WorkerMetrics()
        timer = metrics.measure("duration")
        with patch(
            "pyspark.worker_metrics.time.perf_counter_ns",
            side_effect=[0, 2_000_000, 3_000_000, 4_000_000],
        ):
            with timer:
                with self.assertRaisesRegex(RuntimeError, "already running"):
                    with timer:
                        pass
            with timer:
                pass
        self.assertEqual(metrics.to_dict(), {"duration": 3})

    def test_accumulate_and_propagate_exception(self):
        metrics = WorkerMetrics()
        with patch(
            "pyspark.worker_metrics.time.perf_counter_ns",
            side_effect=[0, 2_000_000, 3_000_000, 4_000_000],
        ):
            with self.assertRaisesRegex(ValueError, "worker failure"):
                with metrics.measure("duration"):
                    raise ValueError("worker failure")
            with metrics.measure("duration"):
                pass
        self.assertEqual(metrics.to_dict(), {"duration": 3})


def _write_utf8(s: str, buf: io.BytesIO) -> None:
    write_with_length(s.encode("utf-8"), buf)


def _worker_input(
    udf_info: bytes,
    items: list,
    python_version: Optional[str] = None,
    eval_type: int = PythonEvalType.NON_UDF,
) -> bytes:
    """Build the socket input of a task of ``eval_type`` whose init message ends with
    ``udf_info``, followed by ``items`` as pickled input."""
    init = io.BytesIO()
    write_int(0, init)  # split_index
    _write_utf8(python_version or "%d.%d" % sys.version_info[:2], init)
    task_context = {
        "isBarrier": False,
        "connInfo": None,
        "secret": None,
        "stageId": 0,
        "partitionId": 0,
        "attemptNumber": 0,
        "taskAttemptId": 0,
        "cpus": 1,
        "cpuAmount": "1",
        "resources": {},
        "localProperties": {},
    }
    _write_utf8(json.dumps(task_context), init)
    _write_utf8(tempfile.gettempdir(), init)  # spark_files_dir
    write_int(0, init)  # num_python_includes
    init.write(b"\x00")  # needs_broadcast_decryption_server
    write_int(0, init)  # num_broadcast_variables
    write_int(eval_type, init)
    write_int(0, init)  # runner conf
    write_int(0, init)  # eval conf
    init.write(udf_info)

    buf = io.BytesIO()
    write_int(SpecialLengths.START_OF_INIT_MESSAGE, buf)
    write_with_length(init.getvalue(), buf)
    CPickleSerializer().dump_stream(iter(items), buf)
    write_int(SpecialLengths.END_OF_DATA_SECTION, buf)
    write_int(SpecialLengths.END_OF_STREAM, buf)
    return buf.getvalue()


def _framed(payload: bytes) -> bytes:
    buf = io.BytesIO()
    write_with_length(payload, buf)
    return buf.getvalue()


def _command(func: Any) -> bytes:
    """The udf_info of a classic RDD (NON_UDF) task running ``func``."""
    ser = CPickleSerializer()
    return _framed(CloudPickleSerializer().dumps((func, None, ser, ser)))


def _udf_info(func: Any, command: Optional[bytes] = None) -> bytes:
    """The udf_info of a task running one single-argument UDF ``func``, or the pickled
    ``command`` in its place."""
    if command is None:
        command = CloudPickleSerializer().dumps((func, LongType()))
    buf = io.BytesIO()
    write_int(1, buf)  # num_udfs
    write_int(1, buf)  # num_args
    write_int(0, buf)  # arg offset
    buf.write(b"\x00")  # is_kwarg
    write_int(1, buf)  # num_chained
    write_with_length(command, buf)
    write_long(0, buf)  # result_id
    return buf.getvalue()


def _udtf_info(udtf_cls: type, return_type: str) -> bytes:
    """The udf_info of a task running ``udtf_cls`` on one argument with ``return_type``."""
    buf = io.BytesIO()
    write_int(1, buf)  # num_args
    write_int(0, buf)  # arg offset
    buf.write(b"\x00")  # is_kwarg
    write_int(0, buf)  # num_partition_child_indexes
    buf.write(b"\x00")  # has_pickled_analyze_result
    write_with_length(CloudPickleSerializer().dumps(udtf_cls), buf)
    _write_utf8(return_type, buf)
    _write_utf8("test_udtf", buf)  # name
    return buf.getvalue()


class _TwoArgUDTF:
    def eval(self, a, b):
        yield (a,)


class _UnprintableError(Exception):
    def __str__(self):
        raise RuntimeError("str() failed")


def _raise_unprintable():
    raise _UnprintableError()


class _RaisesOnUnpickle:
    def __reduce__(self):
        return _raise_unprintable, ()


class _FailingHandler:
    def __init__(self, **kwargs):
        raise ImportError("pyarrow is not installed")


class WorkerInitResponseTests(unittest.TestCase):
    """Tests for the init response the worker sends before any output, run without a JVM."""

    def setUp(self):
        # The worker runs in this process and sets up process-wide state as a worker would.
        saved_files = (SparkFiles._root_directory, SparkFiles._is_running_on_worker)
        saved_path = list(sys.path)
        saved_accumulators = dict(_accumulatorRegistry)

        def restore():
            TaskContext._setTaskContext(None)
            SparkFiles._root_directory, SparkFiles._is_running_on_worker = saved_files
            sys.path[:] = saved_path
            _accumulatorRegistry.clear()
            _accumulatorRegistry.update(saved_accumulators)

        self.addCleanup(restore)

    def _run(self, worker_input: bytes, expect_exit: bool = False) -> io.BytesIO:
        outfile = io.BytesIO()
        if expect_exit:
            with self.assertRaises(SystemExit):
                worker_main(io.BytesIO(worker_input), outfile)
        else:
            worker_main(io.BytesIO(worker_input), outfile)
        outfile.seek(0)
        return outfile

    def _read_init_response(self, outfile: io.BytesIO) -> dict:
        self.assertEqual(read_int(outfile), SpecialLengths.START_OF_INIT_RESPONSE)
        return json.loads(outfile.read(read_int(outfile)).decode("utf-8"))

    def test_init_response_precedes_output(self):
        outfile = self._run(_worker_input(_command(lambda _, it: (x * 2 for x in it)), [1, 2]))
        self.assertEqual(self._read_init_response(outfile), {})
        ser = CPickleSerializer()
        self.assertEqual([ser._read_with_length(outfile) for _ in range(2)], [2, 4])
        self.assertEqual(read_int(outfile), SpecialLengths.METRICS_DATA)

    def test_payload_error_is_reported_in_init_response(self):
        outfile = self._run(_worker_input(_framed(b"not a pickle"), [1]), expect_exit=True)
        error = self._read_init_response(outfile)["error"]
        self.assertTrue(error["isPayloadError"])
        self.assertEqual(error["errorClass"], "UnpicklingError")
        self.assertIn("UnpicklingError", error["traceback"])
        self.assertTrue(error["message"])
        # No output follows an init error.
        self.assertEqual(outfile.read(), b"")

    def test_payload_error_with_unprintable_exception(self):
        payload = _framed(CloudPickleSerializer().dumps(_RaisesOnUnpickle()))
        outfile = self._run(_worker_input(payload, [1]), expect_exit=True)
        error = self._read_init_response(outfile)["error"]
        self.assertEqual(
            error,
            {
                "message": "<exception str() failed>",
                "traceback": error["traceback"],
                "errorClass": "_UnprintableError",
                "isPayloadError": True,
            },
        )
        self.assertIn("_UnprintableError", error["traceback"])
        self.assertEqual(outfile.read(), b"")

    def test_setup_error_is_reported_in_init_response(self):
        worker_input = _worker_input(_command(lambda _, it: it), [1], python_version="2.0")
        outfile = self._run(worker_input, expect_exit=True)
        error = self._read_init_response(outfile)["error"]
        self.assertFalse(error["isPayloadError"])
        self.assertEqual(error["errorClass"], "PySparkRuntimeError")
        self.assertIn("PYTHON_VERSION_MISMATCH", error["traceback"])
        self.assertEqual(outfile.read(), b"")

    def test_handler_construction_error_is_not_a_payload_error(self):
        worker_input = _worker_input(
            _udf_info(lambda x: x), [1], eval_type=PythonEvalType.SQL_BATCHED_UDF
        )
        with patch("pyspark.worker.get_eval_type_handler", return_value=_FailingHandler):
            outfile = self._run(worker_input, expect_exit=True)
        error = self._read_init_response(outfile)["error"]
        self.assertFalse(error["isPayloadError"])
        self.assertEqual(error["errorClass"], "ImportError")
        self.assertEqual(error["message"], "pyarrow is not installed")
        self.assertEqual(outfile.read(), b"")

    def test_payload_window_of_each_payload_kind(self):
        struct_type = StructType([StructField("x", LongType())]).json()
        cases = [
            # A UDF whose function does not unpickle.
            (
                PythonEvalType.SQL_BATCHED_UDF,
                _udf_info(None, command=b"not a pickle"),
                "UnpicklingError",
                True,
            ),
            # A UDTF that declares a non-struct return type.
            (
                PythonEvalType.SQL_TABLE_UDF,
                _udtf_info(_TwoArgUDTF, '"long"'),
                "must be a struct type",
                True,
            ),
            # A UDTF whose 'eval' signature does not match the call, checked by its handler.
            (
                PythonEvalType.SQL_TABLE_UDF,
                _udtf_info(_TwoArgUDTF, struct_type),
                "UDTF_EVAL_METHOD_ARGUMENTS_DO_NOT_MATCH_SIGNATURE",
                False,
            ),
        ]
        for eval_type, udf_info, expected_error, is_payload_error in cases:
            with self.subTest(expected_error=expected_error):
                outfile = self._run(
                    _worker_input(udf_info, [1], eval_type=eval_type), expect_exit=True
                )
                error = self._read_init_response(outfile)["error"]
                self.assertIn(expected_error, error["traceback"])
                self.assertEqual(error["isPayloadError"], is_payload_error)
                self.assertEqual(outfile.read(), b"")

    def test_data_error_is_reported_after_init_response(self):
        def fail(_, it):
            for _x in it:
                raise ValueError("data error")
            yield

        outfile = self._run(_worker_input(_command(fail), [1]), expect_exit=True)
        self.assertEqual(self._read_init_response(outfile), {})
        self.assertEqual(read_int(outfile), SpecialLengths.PYTHON_EXCEPTION_THROWN)
        self.assertIn("data error", outfile.read(read_int(outfile)).decode("utf-8"))


class WorkerTests(ReusedPySparkTestCase):
    def test_cancel_task(self):
        temp = tempfile.NamedTemporaryFile(delete=True)
        temp.close()
        path = temp.name

        def sleep(x):
            import os
            import time

            with open(path, "w") as f:
                f.write("%d %d" % (os.getppid(), os.getpid()))
            time.sleep(100)

        # start job in background thread
        def run():
            try:
                self.sc.parallelize(range(1), 1).foreach(sleep)
            except Exception:
                pass

        import threading

        t = threading.Thread(target=run)
        t.daemon = True
        t.start()

        daemon_pid, worker_pid = 0, 0
        cnt = 0
        while True:
            if os.path.exists(path):
                with open(path) as f:
                    data = f.read().split(" ")
                try:
                    daemon_pid, worker_pid = map(int, data)
                except ValueError:
                    # In case the value is not written yet.
                    cnt += 1
                    if cnt == 10:
                        raise
                else:
                    break
            time.sleep(1)

        # cancel jobs
        self.sc.cancelAllJobs()
        t.join()

        for i in range(50):
            try:
                os.kill(worker_pid, 0)
                time.sleep(0.1)
            except OSError:
                break  # worker was killed
        else:
            self.fail("worker has not been killed after 5 seconds")

        try:
            os.kill(daemon_pid, 0)
        except OSError:
            self.fail("daemon had been killed")

        # run a normal job
        rdd = self.sc.parallelize(range(100), 1)
        self.assertEqual(100, rdd.map(str).count())

    def test_after_exception(self):
        def raise_exception(_):
            raise RuntimeError()

        rdd = self.sc.parallelize(range(100), 1)
        with QuietTest(self.sc):
            self.assertRaises(Py4JJavaError, lambda: rdd.foreach(raise_exception))
        self.assertEqual(100, rdd.map(str).count())

    def test_after_non_exception_error(self):
        # SPARK-33339: Pyspark application will hang due to non Exception
        def raise_system_exit(_):
            raise SystemExit()

        rdd = self.sc.parallelize(range(100), 1)
        with QuietTest(self.sc):
            self.assertRaises(Exception, lambda: rdd.foreach(raise_system_exit))
        self.assertEqual(100, rdd.map(str).count())

    def test_after_jvm_exception(self):
        tempFile = tempfile.NamedTemporaryFile(delete=False)
        tempFile.write(b"Hello World!")
        tempFile.close()
        data = self.sc.textFile(tempFile.name, 1)
        filtered_data = data.filter(lambda x: True)
        self.assertEqual(1, filtered_data.count())
        os.unlink(tempFile.name)
        with QuietTest(self.sc):
            self.assertRaises(Exception, lambda: filtered_data.count())

        rdd = self.sc.parallelize(range(100), 1)
        self.assertEqual(100, rdd.map(str).count())

    def test_accumulator_when_reuse_worker(self):
        from pyspark.accumulators import INT_ACCUMULATOR_PARAM

        acc1 = self.sc.accumulator(0, INT_ACCUMULATOR_PARAM)
        self.sc.parallelize(range(100), 20).foreach(lambda x: acc1.add(x))
        self.assertEqual(sum(range(100)), acc1.value)

        acc2 = self.sc.accumulator(0, INT_ACCUMULATOR_PARAM)
        self.sc.parallelize(range(100), 20).foreach(lambda x: acc2.add(x))
        self.assertEqual(sum(range(100)), acc2.value)
        self.assertEqual(sum(range(100)), acc1.value)

    def test_worker_metrics_include_spills(self):
        accumulator = self.sc.accumulator(0)

        def increment(value):
            from pyspark import TaskContext, shuffle

            # Supply known spill totals to check their transport into JVM task metrics.
            shuffle.MemoryBytesSpilled += 7
            shuffle.DiskBytesSpilled += 9
            accumulator.add(1)
            return TaskContext.get().stageId(), value + 1

        result = self.sc.parallelize([1, 2], 1).map(increment).collect()
        self.assertEqual([value for _, value in result], [2, 3])
        self.assertEqual(accumulator.value, 2)
        self.sc._jsc.sc().listenerBus().waitUntilEmpty(10000)
        stage = self.sc._jsc.sc().statusStore().lastStageAttempt(result[0][0])
        self.assertEqual(stage.memoryBytesSpilled(), 14)
        self.assertEqual(stage.diskBytesSpilled(), 18)

    def test_reuse_worker_after_take(self):
        rdd = self.sc.parallelize(range(100000), 1)
        self.assertEqual(0, rdd.first())

        def count():
            try:
                rdd.count()
            except Exception:
                pass

        t = threading.Thread(target=count)
        t.daemon = True
        t.start()
        t.join(5)
        self.assertTrue(not t.is_alive())
        self.assertEqual(100000, rdd.count())

    def test_with_different_versions_of_python(self):
        rdd = self.sc.parallelize(range(10))
        rdd.count()
        version = self.sc.pythonVer
        self.sc.pythonVer = "2.0"
        try:
            with QuietTest(self.sc):
                self.assertRaises(Py4JJavaError, lambda: rdd.count())
        finally:
            self.sc.pythonVer = version

    def test_python_exception_non_hanging(self):
        # SPARK-21045: exceptions with no ascii encoding shall not hanging PySpark.
        try:

            def f():
                raise RuntimeError("exception with 中 and \xd6\xd0")

            self.sc.parallelize([1]).map(lambda x: f()).count()
        except Py4JJavaError as e:
            self.assertRegex(str(e), "exception with 中")


class WorkerReuseTest(ReusedPySparkTestCase):
    @classmethod
    def conf(cls):
        # Pin spark.python.use.daemon=true. Worker reuse (hence stable worker PIDs) only
        # happens on the daemon path; the simple-worker path spawns a fresh process per
        # task. Sibling suites here run with use.daemon=false and, because all suites in
        # this file share one gateway JVM, whichever launches it first leaks its conf into
        # the JVM system properties (SPARK-59885). Pinning it keeps this reuse assertion
        # independent of test ordering.
        conf = super().conf()
        conf.set("spark.python.use.daemon", "true")
        return conf

    @eventually(catch_assertions=True)
    def test_reuse_worker_of_parallelize_range(self):
        rdd = self.sc.parallelize(range(20), 8)
        previous_pids = rdd.map(lambda x: os.getpid()).collect()
        current_pids = rdd.map(lambda x: os.getpid()).collect()
        for pid in current_pids:
            self.assertTrue(pid in previous_pids)


@unittest.skipIf(
    not has_resource_module or sys.platform != "linux",
    "Memory limit feature in Python worker is dependent on "
    "Python's 'resource' module on Linux; however, not found or not on Linux.",
)
class WorkerMemoryTest(unittest.TestCase):
    def setUp(self):
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.executor.pyspark.memory", "2g")
        # Pin spark.task.cpus so a fractional value leaked from another suite via JVM system
        # properties (shared gateway JVM) cannot split the worker memory limit and flake this.
        conf = conf.set("spark.task.cpus", "1")
        self.sc = SparkContext("local[4]", class_name, conf=conf)

    def test_memory_limit(self):
        rdd = self.sc.parallelize(range(1), 1)

        def getrlimit():
            return resource.getrlimit(resource.RLIMIT_AS)

        actual = rdd.map(lambda _: getrlimit()).collect()
        self.assertEqual(len(actual), 1)
        self.assertEqual(len(actual[0]), 2)
        [(soft_limit, hard_limit)] = actual
        self.assertEqual(soft_limit, 2 * 1024 * 1024 * 1024)
        self.assertEqual(hard_limit, 2 * 1024 * 1024 * 1024)

    def tearDown(self):
        self.sc.stop()


@unittest.skipIf(
    not has_resource_module or sys.platform != "linux",
    "Memory limit feature in Python worker is dependent on "
    "Python's 'resource' module on Linux; however, not found or not on Linux.",
)
class WorkerMemoryFractionalTaskCpusTest(unittest.TestCase):
    def setUp(self):
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.executor.pyspark.memory", "2g")
        conf = conf.set("spark.task.cpus", "0.5")
        self.sc = SparkContext("local[4]", class_name, conf=conf)

    def test_memory_limit_split_across_fractional_task_slots(self):
        # SPARK-58192: with spark.task.cpus=0.5 each executor core fits two concurrent
        # tasks, so each Python worker gets half of the executor-wide pyspark memory.
        rdd = self.sc.parallelize(range(1), 1)

        def getrlimit():
            return resource.getrlimit(resource.RLIMIT_AS)

        [(soft_limit, hard_limit)] = rdd.map(lambda _: getrlimit()).collect()
        self.assertEqual(soft_limit, 1024 * 1024 * 1024)
        self.assertEqual(hard_limit, 1024 * 1024 * 1024)

    def tearDown(self):
        self.sc.stop()


class WorkerSegfaultTest(ReusedPySparkTestCase):
    @classmethod
    def conf(cls):
        _conf = super().conf()
        _conf.set("spark.python.worker.faulthandler.enabled", "true")
        return _conf

    @unittest.skipIf(sys.version_info > (3, 12), "SPARK-46130: Flaky with Python 3.12")
    def test_python_segfault(self):
        try:

            def f():
                import ctypes

                ctypes.string_at(0)

            self.sc.parallelize([1]).map(lambda x: f()).count()
        except Py4JJavaError as e:
            self.assertRegex(str(e), "Segmentation fault")


@unittest.skipIf(
    "COVERAGE_PROCESS_START" in os.environ,
    "Flaky with coverage enabled, skipping for now.",
)
class WorkerSegfaultNonDaemonTest(WorkerSegfaultTest):
    @classmethod
    def conf(cls):
        _conf = super().conf()
        _conf.set("spark.python.use.daemon", "false")
        return _conf


class WorkerPoolCrashTest(ReusedPySparkTestCase):
    @classmethod
    def conf(cls):
        # Pin daemon so worker reuse (stable PIDs) is independent of test ordering; see
        # WorkerReuseTest.conf for the shared-gateway leak rationale (SPARK-59885).
        conf = super().conf()
        conf.set("spark.python.use.daemon", "true")
        return conf

    def test_worker_crash(self):
        # SPARK-47565: Kill a worker that is currently idling
        rdd = self.sc.parallelize(range(20), 4)
        # first ensure that workers are reused
        worker_pids1 = set(rdd.map(lambda x: os.getpid()).collect())
        worker_pids2 = set(rdd.map(lambda x: os.getpid()).collect())
        self.assertEqual(worker_pids1, worker_pids2)
        for pid in list(worker_pids1)[1:]:  # kill all workers except for one
            os.kill(pid, signal.SIGTERM)
        # give things a moment to settle
        time.sleep(5)
        rdd.map(lambda x: os.getpid()).collect()


class SimpleWorkerTests(WorkerTests):
    """Run worker tests through the non-daemon (simple-worker) path.

    Windows always uses this path; Linux/macOS use it when
    spark.python.use.daemon=false.
    """

    @classmethod
    def conf(cls):
        conf = super().conf()
        conf.set("spark.python.use.daemon", "false")
        return conf


if __name__ == "__main__":
    from pyspark.testing import main

    main()
