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

from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

if should_test_connect:
    import grpc
    import google.protobuf.any_pb2 as any_pb2
    import google.protobuf.duration_pb2 as duration_pb2
    from google.rpc import status_pb2
    from google.rpc import error_details_pb2
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.client.retries import (
        Retrying,
        DefaultPolicy,
    )
    from pyspark.errors import RetriesExceeded
    from pyspark.sql.tests.connect.client.test_client import (
        TestPolicy,
        TestException,
    )

    class SleepTimeTracker:
        """Tracks sleep times in ms for testing purposes."""

        def __init__(self):
            self._times = []

        def sleep(self, t: float):
            self._times.append(int(1000 * t))

        @property
        def times(self):
            return list(self._times)

    def create_test_exception_with_details(
        msg: str,
        code: grpc.StatusCode = grpc.StatusCode.INTERNAL,
        retry_delay: int = 0,
    ) -> TestException:
        """Helper function for creating TestException with additional error details
        like retry_delay.
        """
        retry_delay_msg = duration_pb2.Duration()
        retry_delay_msg.FromMilliseconds(retry_delay)
        retry_info = error_details_pb2.RetryInfo()
        retry_info.retry_delay.CopyFrom(retry_delay_msg)

        # Pack RetryInfo into an Any type
        retry_info_any = any_pb2.Any()
        retry_info_any.Pack(retry_info)
        status = status_pb2.Status(
            code=code.value[0],
            message=msg,
            details=[retry_info_any],
        )
        return TestException(msg=msg, code=code, trailing_status=status)

    def get_client_policies_map(client: SparkConnectClient) -> dict:
        return {type(policy): policy for policy in client.get_retry_policies()}


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectClientRetriesTestCase(unittest.TestCase):
    def assertListsAlmostEqual(self, first, second, places=None, msg=None, delta=None):
        self.assertEqual(len(first), len(second), msg)
        for i in range(len(first)):
            self.assertAlmostEqual(first[i], second[i], places, msg, delta)

    def test_retry(self):
        client = SparkConnectClient("sc://foo/;token=bar")

        sleep_tracker = SleepTimeTracker()
        try:
            for attempt in Retrying(client._retry_policies, sleep=sleep_tracker.sleep):
                with attempt:
                    raise TestException("Retryable error", grpc.StatusCode.UNAVAILABLE)
        except RetriesExceeded:
            pass

        # tolerated at least 10 mins of fails
        self.assertGreaterEqual(sum(sleep_tracker.times), 600)

    def test_retry_client_unit(self):
        client = SparkConnectClient("sc://foo/;token=bar")

        policyA = TestPolicy()
        policyB = DefaultPolicy()

        client.set_retry_policies([policyA, policyB])

        self.assertEqual(client.get_retry_policies(), [policyA, policyB])

    def test_default_policy_retries_retry_info(self):
        client = SparkConnectClient("sc://foo/;token=bar")
        policy = get_client_policies_map(client).get(DefaultPolicy)
        self.assertIsNotNone(policy)

        # retry delay = 0, error code not matched by any policy.
        # Testing if errors with RetryInfo are being retried by the DefaultPolicy.
        retry_delay = 0
        sleep_tracker = SleepTimeTracker()
        try:
            for attempt in Retrying(client._retry_policies, sleep=sleep_tracker.sleep):
                with attempt:
                    raise create_test_exception_with_details(
                        msg="Some error message",
                        code=grpc.StatusCode.UNIMPLEMENTED,
                        retry_delay=retry_delay,
                    )
        except RetriesExceeded:
            pass
        expected_times = [
            min(policy.max_backoff, policy.initial_backoff * policy.backoff_multiplier**i)
            for i in range(policy.max_retries)
        ]
        self.assertListsAlmostEqual(sleep_tracker.times, expected_times, delta=policy.jitter)

    def test_retry_delay_overrides_max_backoff(self):
        client = SparkConnectClient("sc://foo/;token=bar")
        policy = get_client_policies_map(client).get(DefaultPolicy)
        self.assertIsNotNone(policy)

        # retry delay = 5 mins.
        # Testing if retry_delay overrides max_backoff.
        retry_delay = 5 * 60 * 1000
        sleep_tracker = SleepTimeTracker()
        # assert that retry_delay is greater than max_backoff to make sure the test is valid
        self.assertGreaterEqual(retry_delay, policy.max_backoff)
        try:
            for attempt in Retrying(client._retry_policies, sleep=sleep_tracker.sleep):
                with attempt:
                    raise create_test_exception_with_details(
                        "Some error message",
                        grpc.StatusCode.UNAVAILABLE,
                        retry_delay,
                    )
        except RetriesExceeded:
            pass
        expected_times = [retry_delay] * policy.max_retries
        self.assertListsAlmostEqual(sleep_tracker.times, expected_times, delta=policy.jitter)

    def test_max_server_retry_delay(self):
        client = SparkConnectClient("sc://foo/;token=bar")
        policy = get_client_policies_map(client).get(DefaultPolicy)
        self.assertIsNotNone(policy)

        # retry delay = 10 hours
        # Testing if max_server_retry_delay limit works.
        retry_delay = 10 * 60 * 60 * 1000
        sleep_tracker = SleepTimeTracker()
        try:
            for attempt in Retrying(client._retry_policies, sleep=sleep_tracker.sleep):
                with attempt:
                    raise create_test_exception_with_details(
                        "Some error message",
                        grpc.StatusCode.UNAVAILABLE,
                        retry_delay,
                    )
        except RetriesExceeded:
            pass

        expected_times = [policy.max_server_retry_delay] * policy.max_retries
        self.assertListsAlmostEqual(sleep_tracker.times, expected_times, delta=policy.jitter)

    def test_return_to_exponential_backoff(self):
        client = SparkConnectClient("sc://foo/;token=bar")
        policy = get_client_policies_map(client).get(DefaultPolicy)
        self.assertIsNotNone(policy)

        # Start with retry_delay = 5 mins, then set it to zero.
        # Test if backoff goes back to client's exponential strategy.
        initial_retry_delay = 5 * 60 * 1000
        sleep_tracker = SleepTimeTracker()
        try:
            for i, attempt in enumerate(
                Retrying(client._retry_policies, sleep=sleep_tracker.sleep)
            ):
                if i < 2:
                    retry_delay = initial_retry_delay
                elif i < 5:
                    retry_delay = 0
                else:
                    break
                with attempt:
                    raise create_test_exception_with_details(
                        "Some error message",
                        grpc.StatusCode.UNAVAILABLE,
                        retry_delay,
                    )
        except RetriesExceeded:
            pass

        expected_times = [initial_retry_delay] * 2 + [
            policy.initial_backoff * policy.backoff_multiplier**i for i in range(2, 5)
        ]
        self.assertListsAlmostEqual(sleep_tracker.times, expected_times, delta=policy.jitter)


if __name__ == "__main__":
    from pyspark.sql.tests.connect.client.test_client_retries import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
