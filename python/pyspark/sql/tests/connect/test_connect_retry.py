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
from collections import defaultdict

from pyspark.errors import RetriesExceeded
from pyspark.testing.connectutils import (
    should_test_connect,
    connect_requirement_message,
)

if should_test_connect:
    import grpc
    from pyspark.sql.connect.client.core import Retrying
    from pyspark.sql.connect.client.retries import RetryPolicy


if should_test_connect:

    class TestError(grpc.RpcError, Exception):
        def __init__(self, code: grpc.StatusCode):
            self._code = code

        def code(self):
            return self._code

    class TestPolicy(RetryPolicy):
        # Put a small value for initial backoff so that tests don't spend
        # Time waiting
        def __init__(self, initial_backoff=10, **kwargs):
            super().__init__(initial_backoff=initial_backoff, **kwargs)

        def can_retry(self, exception: BaseException):
            return isinstance(exception, TestError)

    class TestPolicySpecificError(TestPolicy):
        def __init__(self, specific_code: grpc.StatusCode, **kwargs):
            super().__init__(**kwargs)
            self.specific_code = specific_code

        def can_retry(self, exception: BaseException):
            return exception.code() == self.specific_code


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class RetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.call_wrap = defaultdict(int)

    def stub(self, retries, code):
        self.call_wrap["attempts"] += 1
        if self.call_wrap["attempts"] < retries:
            self.call_wrap["raised"] += 1
            raise TestError(code)

    def test_simple(self):
        # Check that max_retries 1 is only one retry so two attempts.
        for attempt in Retrying(TestPolicy(max_retries=1)):
            with attempt:
                self.stub(2, grpc.StatusCode.INTERNAL)

        self.assertEqual(2, self.call_wrap["attempts"])
        self.assertEqual(1, self.call_wrap["raised"])

    def test_below_limit(self):
        # Check that if we have less than 4 retries all is ok.
        for attempt in Retrying(TestPolicy(max_retries=4)):
            with attempt:
                self.stub(2, grpc.StatusCode.INTERNAL)

        self.assertLess(self.call_wrap["attempts"], 4)
        self.assertEqual(self.call_wrap["raised"], 1)

    def test_exceed_retries(self):
        # Exceed the retries.
        with self.assertRaises(RetriesExceeded):
            for attempt in Retrying(TestPolicy(max_retries=2)):
                with attempt:
                    self.stub(5, grpc.StatusCode.INTERNAL)

        self.assertLess(self.call_wrap["attempts"], 5)
        self.assertEqual(self.call_wrap["raised"], 3)

    def test_throw_not_retriable_error(self):
        with self.assertRaises(ValueError):
            for attempt in Retrying(TestPolicy(max_retries=2)):
                with attempt:
                    raise ValueError

    def test_specific_exception(self):
        # Check that only specific exceptions are retried.
        # Check that if we have less than 4 retries all is ok.
        policy = TestPolicySpecificError(max_retries=4, specific_code=grpc.StatusCode.UNAVAILABLE)

        for attempt in Retrying(policy):
            with attempt:
                self.stub(2, grpc.StatusCode.UNAVAILABLE)

        self.assertLess(self.call_wrap["attempts"], 4)
        self.assertEqual(self.call_wrap["raised"], 1)

    def test_specific_exception_exceed_retries(self):
        # Exceed the retries.
        policy = TestPolicySpecificError(max_retries=2, specific_code=grpc.StatusCode.UNAVAILABLE)
        with self.assertRaises(RetriesExceeded):
            for attempt in Retrying(policy):
                with attempt:
                    self.stub(5, grpc.StatusCode.UNAVAILABLE)

        self.assertLess(self.call_wrap["attempts"], 4)
        self.assertEqual(self.call_wrap["raised"], 3)

    def test_rejected_by_policy(self):
        # Test that another error is always thrown.
        policy = TestPolicySpecificError(max_retries=4, specific_code=grpc.StatusCode.UNAVAILABLE)

        with self.assertRaises(TestError):
            for attempt in Retrying(policy):
                with attempt:
                    self.stub(5, grpc.StatusCode.INTERNAL)

        self.assertEqual(self.call_wrap["attempts"], 1)
        self.assertEqual(self.call_wrap["raised"], 1)

    def test_multiple_policies(self):
        policy1 = TestPolicySpecificError(max_retries=2, specific_code=grpc.StatusCode.UNAVAILABLE)
        policy2 = TestPolicySpecificError(max_retries=4, specific_code=grpc.StatusCode.INTERNAL)

        # Tolerate 2 UNAVAILABLE errors and 4 INTERNAL errors

        error_suply = iter([grpc.StatusCode.UNAVAILABLE] * 2 + [grpc.StatusCode.INTERNAL] * 4)

        for attempt in Retrying([policy1, policy2]):
            with attempt:
                error = next(error_suply, None)
                if error:
                    raise TestError(error)

        self.assertEqual(next(error_suply, None), None)

    def test_multiple_policies_exceed(self):
        policy1 = TestPolicySpecificError(max_retries=2, specific_code=grpc.StatusCode.INTERNAL)
        policy2 = TestPolicySpecificError(max_retries=4, specific_code=grpc.StatusCode.INTERNAL)

        with self.assertRaises(RetriesExceeded):
            for attempt in Retrying([policy1, policy2]):
                with attempt:
                    self.stub(10, grpc.StatusCode.INTERNAL)

        self.assertEqual(self.call_wrap["attempts"], 7)
        self.assertEqual(self.call_wrap["raised"], 7)


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_retry import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
