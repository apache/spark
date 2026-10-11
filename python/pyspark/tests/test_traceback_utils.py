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

import unittest
from traceback import FrameSummary
from unittest import mock

from pyspark.traceback_utils import CallSite, SCCallSiteSync, first_spark_call


class FirstSparkCallTests(unittest.TestCase):
    # first_spark_call() treats the directory of the last frame (itself) as the Spark path, so the
    # frames below that live under "/x/pyspark" count as Spark frames and "/home/user" is user code.
    USER_FRAME = FrameSummary("/home/user/my_app.py", 10, "main")
    SPARK_FRAME = FrameSummary("/x/pyspark/core/rdd.py", 500, "collect")
    OTHER_SPARK_FRAME = FrameSummary("/x/pyspark/core/context.py", 200, "runJob")
    SELF_FRAME = FrameSummary("/x/pyspark/traceback_utils.py", 29, "first_spark_call")

    def test_empty_stack_returns_none(self):
        with mock.patch("pyspark.traceback_utils.traceback.extract_stack", return_value=[]):
            self.assertIsNone(first_spark_call())

    def test_returns_user_location_of_first_spark_call(self):
        stack = [self.USER_FRAME, self.SPARK_FRAME, self.SELF_FRAME]
        with mock.patch("pyspark.traceback_utils.traceback.extract_stack", return_value=stack):
            site = first_spark_call()
        self.assertEqual(site, CallSite("collect", "/home/user/my_app.py", 10))

    def test_uses_the_first_spark_frame_only(self):
        stack = [self.USER_FRAME, self.SPARK_FRAME, self.OTHER_SPARK_FRAME, self.SELF_FRAME]
        with mock.patch("pyspark.traceback_utils.traceback.extract_stack", return_value=stack):
            site = first_spark_call()
        self.assertEqual(site, CallSite("collect", "/home/user/my_app.py", 10))

    def test_spark_frame_at_the_bottom_of_the_stack(self):
        stack = [self.SPARK_FRAME, self.SELF_FRAME]
        with mock.patch("pyspark.traceback_utils.traceback.extract_stack", return_value=stack):
            site = first_spark_call()
        self.assertEqual(site, CallSite("collect", "/x/pyspark/core/rdd.py", 500))

    def test_real_stack_returns_a_call_site(self):
        self.assertIsInstance(first_spark_call(), CallSite)


class SCCallSiteSyncTests(unittest.TestCase):
    def setUp(self):
        # The nesting depth is class-level state shared by every instance, so isolate each test.
        original_depth = SCCallSiteSync._spark_stack_depth
        SCCallSiteSync._spark_stack_depth = 0
        self.addCleanup(setattr, SCCallSiteSync, "_spark_stack_depth", original_depth)
        self.sc = mock.MagicMock()

    def test_sets_and_clears_call_site(self):
        call_site = CallSite("collect", "/home/user/my_app.py", 10)
        with mock.patch("pyspark.traceback_utils.first_spark_call", return_value=call_site):
            with SCCallSiteSync(self.sc):
                self.sc._jsc.setCallSite.assert_called_once_with(
                    "collect at /home/user/my_app.py:10"
                )
        self.assertEqual(
            self.sc._jsc.setCallSite.call_args_list,
            [mock.call("collect at /home/user/my_app.py:10"), mock.call(None)],
        )

    def test_fallback_message_when_call_site_is_unknown(self):
        with mock.patch("pyspark.traceback_utils.first_spark_call", return_value=None):
            with SCCallSiteSync(self.sc):
                self.sc._jsc.setCallSite.assert_called_once_with(
                    "Error! Could not extract traceback info"
                )

    def test_nested_usage_sets_call_site_only_at_outermost_level(self):
        call_site = CallSite("collect", "/home/user/my_app.py", 10)
        with mock.patch("pyspark.traceback_utils.first_spark_call", return_value=call_site):
            with SCCallSiteSync(self.sc):
                with SCCallSiteSync(self.sc):
                    self.assertEqual(SCCallSiteSync._spark_stack_depth, 2)
                # Leaving the inner block must not reset the call site yet.
                self.assertEqual(self.sc._jsc.setCallSite.call_count, 1)
                self.assertEqual(SCCallSiteSync._spark_stack_depth, 1)
        self.assertEqual(
            self.sc._jsc.setCallSite.call_args_list,
            [mock.call("collect at /home/user/my_app.py:10"), mock.call(None)],
        )
        self.assertEqual(SCCallSiteSync._spark_stack_depth, 0)

    def test_call_site_is_cleared_when_the_block_raises(self):
        call_site = CallSite("collect", "/home/user/my_app.py", 10)
        with mock.patch("pyspark.traceback_utils.first_spark_call", return_value=call_site):
            with self.assertRaises(ValueError):
                with SCCallSiteSync(self.sc):
                    raise ValueError("boom")
        self.sc._jsc.setCallSite.assert_called_with(None)
        self.assertEqual(SCCallSiteSync._spark_stack_depth, 0)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
