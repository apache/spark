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
import sys
import tempfile
import unittest
from typing import Any, Optional
from unittest.mock import patch

from pyspark.serializers import (
    CloudPickleSerializer,
    CPickleSerializer,
    SpecialLengths,
    read_int,
    write_int,
    write_with_length,
)
from pyspark.taskcontext import TaskContext
from pyspark.util import PythonEvalType

with patch.dict(os.environ, {"SPARK_PYTHON_RUNTIME": "PYTHON_WORKER"}):
    from pyspark.worker import main as worker_main


def _write_utf8(s: str, buf: io.BytesIO) -> None:
    write_with_length(s.encode("utf-8"), buf)


def _worker_input(command: bytes, items: list, python_version: Optional[str] = None) -> bytes:
    """Build the socket input of a classic RDD (NON_UDF) task running ``command``."""
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
    write_int(PythonEvalType.NON_UDF, init)
    write_int(0, init)  # runner conf
    write_int(0, init)  # eval conf
    write_with_length(command, init)

    buf = io.BytesIO()
    write_int(SpecialLengths.START_OF_INIT_MESSAGE, buf)
    write_with_length(init.getvalue(), buf)
    CPickleSerializer().dump_stream(iter(items), buf)
    write_int(SpecialLengths.END_OF_DATA_SECTION, buf)
    write_int(SpecialLengths.END_OF_STREAM, buf)
    return buf.getvalue()


def _command(func: Any) -> bytes:
    ser = CPickleSerializer()
    return CloudPickleSerializer().dumps((func, None, ser, ser))


class WorkerInitResponseTests(unittest.TestCase):
    """Tests for the init response the worker sends before any output, run without a JVM."""

    def tearDown(self):
        TaskContext._setTaskContext(None)

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
        outfile = self._run(_worker_input(b"not a pickle", [1]), expect_exit=True)
        error = self._read_init_response(outfile)["error"]
        self.assertTrue(error["isPayloadError"])
        self.assertEqual(error["errorClass"], "UnpicklingError")
        self.assertIn("UnpicklingError", error["traceback"])
        self.assertTrue(error["message"])
        # No output follows an init error.
        self.assertEqual(outfile.read(), b"")

    def test_setup_error_is_reported_in_init_response(self):
        worker_input = _worker_input(_command(lambda _, it: it), [1], python_version="2.0")
        outfile = self._run(worker_input, expect_exit=True)
        error = self._read_init_response(outfile)["error"]
        self.assertFalse(error["isPayloadError"])
        self.assertEqual(error["errorClass"], "PySparkRuntimeError")
        self.assertIn("PYTHON_VERSION_MISMATCH", error["traceback"])
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


if __name__ == "__main__":
    from pyspark.testing import main

    main()
