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

from io import StringIO
import unittest
from typing import Iterable

from pyspark.testing.connectutils import (
    should_test_connect,
    connect_requirement_message,
    ReusedConnectTestCase,
)
from pyspark.testing.utils import PySparkErrorTestUtils

if should_test_connect:
    from pyspark.sql.connect.shell.progress import Progress, StageInfo


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class ProgressBarTest(unittest.TestCase, PySparkErrorTestUtils):
    def test_simple_progress(self):
        stages = [StageInfo(0, 100, 50, 999, False)]
        buffer = StringIO()
        p = Progress(output=buffer, enabled=True)
        p.update_ticks(stages, 10)
        val = buffer.getvalue()
        self.assertIn("50.00%", val, "Current progress is 50%")
        self.assertIn("****", val, "Should use the default char to print.")
        self.assertIn("Scanned 999.0 B", val, "Should contain the bytes scanned metric.")
        self.assertFalse(val.endswith("\r"), "Line should not be empty")
        p.finish()
        val = buffer.getvalue()
        self.assertTrue(val.endswith("\r"), "Line should be empty")

    def test_configure_char(self):
        stages = [StageInfo(0, 100, 50, 999, False)]
        buffer = StringIO()
        p = Progress(char="+", output=buffer, enabled=True)
        p.update_ticks(stages, 10)
        val = buffer.getvalue()
        self.assertIn("++++++", val, "Updating the char works.")

    def test_disabled_does_not_print(self):
        stages = [StageInfo(0, 100, 50, 999, False)]
        buffer = StringIO()
        p = Progress(char="+", output=buffer, enabled=False)
        p.update_ticks(stages, 10)
        stages = [StageInfo(0, 100, 51, 999, False)]
        p.update_ticks(stages, 10)
        val = buffer.getvalue()
        self.assertEqual(0, len(val), "If the printing is disabled, don't print.")

    def test_finish_progress(self):
        stages = [StageInfo(0, 100, 50, 999, False)]
        buffer = StringIO()
        p = Progress(char="+", output=buffer, enabled=True)
        p.update_ticks(stages, 10)
        p.finish()
        self.assertTrue(buffer.getvalue().endswith("\r"), "Last line should be empty")

    def test_progress_handler(self):
        stages = [StageInfo(0, 0, 0, 0, False)]

        handler_called = 0
        done_called = False

        def handler(
            stages: Iterable[StageInfo], inflight_tasks: int, operation_id: str, done: bool
        ):
            nonlocal handler_called, done_called
            handler_called = 1
            self.assertEqual(100, sum(map(lambda x: x.num_tasks, stages)))
            self.assertEqual(50, sum(map(lambda x: x.num_completed_tasks, stages)))
            self.assertEqual(999, sum(map(lambda x: x.num_bytes_read, stages)))
            self.assertEqual(10, inflight_tasks)
            self.assertEqual(operation_id, "operation_id")
            done_called = done

        buffer = StringIO()
        p = Progress(
            char="+", output=buffer, enabled=True, handlers=[handler], operation_id="operation_id"
        )
        p.update_ticks(stages, 1)
        stages = [StageInfo(0, 100, 50, 999, False)]
        p.update_ticks(stages, 10)
        self.assertIn("++++++", buffer.getvalue(), "Updating the char works.")
        self.assertEqual(1, handler_called, "Handler should be called.")
        self.assertFalse(done_called, "Before finish, done should be False")
        p.finish()
        self.assertTrue(buffer.getvalue().endswith("\r"), "Last line should be empty")
        self.assertTrue(done_called, "After finish, done should be True")


class SparkConnectProgressHandlerE2E(ReusedConnectTestCase):
    def test_custom_handler_works(self):
        called = False

        def handler(**kwargs):
            nonlocal called
            called = True
            self.assertIsNotNone(kwargs.get("stages"))
            self.assertIsNotNone(kwargs.get("operation_id"))
            self.assertIsNotNone(kwargs.get("inflight_tasks"))
            self.assertGreater(len(kwargs.get("stages")), 0)
            self.assertGreater(len(kwargs.get("operation_id")), 0)

        try:
            self.spark.registerProgressHandler(handler)
            self.spark.range(100).repartition(20).count()
            self.assertTrue(called, "Handler must have been called")
        finally:
            self.spark.clearProgressHandlers()

    def test_progress_properly_recorded(self):
        state = {"counter": 0}

        def handler(stages, inflight_tasks, operation_id, done):
            state["counter"] += 1

        try:
            self.spark.registerProgressHandler(handler)
            self.spark.range(10000).repartition(20).count()
            self.assertGreaterEqual(state["counter"], 1, "Handler should be called at least once.")
        finally:
            self.spark.clearProgressHandlers()


if __name__ == "__main__":
    from pyspark.sql.tests.connect.shell.test_progress import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
