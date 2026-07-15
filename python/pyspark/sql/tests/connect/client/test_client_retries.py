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
import warnings

from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

if should_test_connect:
    import grpc
    import google.protobuf.any_pb2 as any_pb2
    import google.protobuf.duration_pb2 as duration_pb2
    from google.rpc import status_pb2
    from google.rpc import error_details_pb2
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.client.core import RpcDeadlines
    from pyspark.sql.connect.client.retries import (
        Retrying,
        DefaultPolicy,
        RetryException,
        DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME,
    )
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

    class FakeClock:
        """Injectable monotonic clock for testing elapsed-time bounds without real waits."""

        def __init__(self):
            self._t = 0.0

        def advance(self, seconds: float):
            self._t += seconds

        def now(self) -> float:
            return self._t

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
        except TestException:
            pass

        # tolerated at least 10 mins of fails
        self.assertGreaterEqual(sum(sleep_tracker.times), 600)

    def test_retry_client_unit(self):
        client = SparkConnectClient("sc://foo/;token=bar")

        policyA = TestPolicy()
        policyB = DefaultPolicy()

        client.set_retry_policies([policyA, policyB])

        self.assertEqual(client.get_retry_policies(), [policyA, policyB])

    def test_warning_works(self):
        client = SparkConnectClient("sc://foo/;token=bar")
        policy = get_client_policies_map(client).get(DefaultPolicy)
        self.assertIsNotNone(policy)

        sleep_tracker = SleepTimeTracker()
        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            try:
                for attempt in Retrying(client._retry_policies, sleep=sleep_tracker.sleep):
                    with attempt:
                        raise TestException(
                            msg="Some error message", code=grpc.StatusCode.UNAVAILABLE
                        )
            except TestException:
                pass
            self.assertEqual(len(sleep_tracker.times), policy.max_retries)
            self.assertEqual(len(warning_list), 1)
            self.assertEqual(
                str(warning_list[0].message),
                "[RETRIES_EXCEEDED] The maximum number of retries has been exceeded.",
            )

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
        except TestException:
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
        except TestException:
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
        except TestException:
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
        except TestException:
            pass

        expected_times = [initial_retry_delay] * 2 + [
            policy.initial_backoff * policy.backoff_multiplier**i for i in range(2, 5)
        ]
        self.assertListsAlmostEqual(sleep_tracker.times, expected_times, delta=policy.jitter)

    def test_deadline_exceeded_not_retryable_by_default_policy(self):
        client = SparkConnectClient("sc://foo/;token=bar")
        policy = get_client_policies_map(client).get(DefaultPolicy)
        self.assertIsNotNone(policy)
        self.assertFalse(
            policy.can_retry(TestException("deadline", code=grpc.StatusCode.DEADLINE_EXCEEDED))
        )

    def test_deadline_exceeded_not_retried_by_retry_handler(self):
        client = SparkConnectClient("sc://foo/;token=bar")
        sleep_tracker = SleepTimeTracker()
        tries = 0
        with self.assertRaises(TestException):
            for attempt in Retrying(client._retry_policies, sleep=sleep_tracker.sleep):
                with attempt:
                    tries += 1
                    raise TestException("d", code=grpc.StatusCode.DEADLINE_EXCEEDED)
        self.assertEqual(tries, 1)
        self.assertEqual(len(sleep_tracker.times), 0)

    def test_retry_exception_bounded_by_elapsed_time(self):
        # Each attempt simulates a full 10-min reattach deadline elapsing before RetryException
        # is raised. The elapsed clock starts on the first RetryException, after that attempt
        # already advanced the clock, so with a 1-hour bound and a 10-min-per-attempt advance the
        # loop gives up after bound // increment + 1 = 7 attempts: attempts 1-6 leave elapsed
        # < 1h, and attempt 7 leaves elapsed = 6 * 10min = 1h >= bound, tripping the throw.
        client = SparkConnectClient("sc://foo/;token=bar")
        clock = FakeClock()
        cause = TestException("deadline exceeded", code=grpc.StatusCode.DEADLINE_EXCEEDED)
        tries = 0
        with self.assertRaises(TestException) as cm:
            for attempt in Retrying(
                client._retry_policies,
                sleep=lambda t: None,
                now=clock.now,
                max_retry_exception_elapsed_time=DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME,
            ):
                with attempt:
                    tries += 1
                    clock.advance(10 * 60)
                    raise RetryException() from cause
        self.assertIs(cm.exception, cause)
        self.assertEqual(tries, 7)

    def test_retry_exception_below_bound_keeps_retrying(self):
        # Non-regression: well under the elapsed-time budget, RetryException retries should
        # keep behaving as before (immediate retry, no policy consulted) until success.
        client = SparkConnectClient("sc://foo/;token=bar")
        clock = FakeClock()
        tries = 0
        for attempt in Retrying(
            client._retry_policies,
            sleep=lambda t: None,
            now=clock.now,
            max_retry_exception_elapsed_time=DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME,
        ):
            with attempt:
                tries += 1
                if tries <= 3:
                    clock.advance(10)
                    raise RetryException()
        self.assertEqual(tries, 4)

    def test_retry_exception_bare_no_cause_raises_self_when_bound_exceeded(self):
        # If a RetryException has no attached cause (e.g. the OPERATION_NOT_FOUND/
        # SESSION_NOT_FOUND path, which today raises a bare RetryException), falling back to
        # raising the bare RetryException itself once the bound is exceeded.
        client = SparkConnectClient("sc://foo/;token=bar")
        clock = FakeClock()
        with self.assertRaises(RetryException):
            for attempt in Retrying(
                client._retry_policies,
                sleep=lambda t: None,
                now=clock.now,
                max_retry_exception_elapsed_time=100,
            ):
                with attempt:
                    clock.advance(30)
                    raise RetryException()

    def test_retry_exception_elapsed_clock_starts_at_first_retry_exception(self):
        # The elapsed-time clock should start only when the first RetryException is observed,
        # not at Retrying construction, and should be unaffected by interleaved ordinary
        # (policy-governed) retries that happen first.
        client = SparkConnectClient("sc://foo/;token=bar")
        clock = FakeClock()
        cause = TestException("deadline exceeded", code=grpc.StatusCode.DEADLINE_EXCEEDED)
        sleep_tracker = SleepTimeTracker()
        tries = 0
        with self.assertRaises(TestException) as cm:
            for attempt in Retrying(
                client._retry_policies,
                sleep=sleep_tracker.sleep,
                now=clock.now,
                max_retry_exception_elapsed_time=DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME,
            ):
                with attempt:
                    tries += 1
                    if tries <= 2:
                        # Ordinary, policy-governed retries -- must not consume any of the
                        # RetryException elapsed-time budget.
                        raise TestException("Retryable error", grpc.StatusCode.UNAVAILABLE)
                    clock.advance(10 * 60)
                    raise RetryException() from cause
        self.assertIs(cm.exception, cause)
        # 2 ordinary retries + 7 RetryException retries (same count as the isolated test above).
        self.assertEqual(tries, 2 + 7)

    def test_default_max_retry_exception_elapsed_time(self):
        client = SparkConnectClient("sc://foo/;token=bar")
        self.assertEqual(
            client._max_retry_exception_elapsed_time, DEFAULT_MAX_RETRY_EXCEPTION_ELAPSED_TIME
        )

        client2 = SparkConnectClient("sc://foo/;token=bar", max_retry_exception_elapsed_time=42)
        self.assertEqual(client2._max_retry_exception_elapsed_time, 42)
        self.assertEqual(client2._retrying()._max_retry_exception_elapsed_time, 42)

    def test_deadline_exceeded_is_not_retried_for_non_retryable_codes(self):
        # Sanity check: ABORTED is not retried by DefaultPolicy (unless matching cluster message)
        policy = DefaultPolicy()
        self.assertFalse(
            policy.can_retry(TestException("some aborted error", code=grpc.StatusCode.ABORTED))
        )

    def test_deadline_exceeded_exception_message_contains_configuration_hint(self):
        """DEADLINE_EXCEEDED exceptions should carry a hint about how to adjust timeouts."""
        from pyspark.errors.exceptions.connect import SparkConnectGrpcException

        client = SparkConnectClient("sc://foo/;token=bar", retry_policy=dict(max_retries=0))
        err = TestException("deadline exceeded", code=grpc.StatusCode.DEADLINE_EXCEEDED)
        with self.assertRaises(SparkConnectGrpcException) as cm:
            client._handle_rpc_error(err)
        self.assertIn("RPC deadline exceeded", str(cm.exception))
        self.assertIn("RpcDeadlines.disabled()", str(cm.exception))
        self.assertEqual(cm.exception.getGrpcStatusCode(), grpc.StatusCode.DEADLINE_EXCEEDED)

    def test_rpc_deadlines_disabled_sets_all_fields_to_none(self):
        """RpcDeadlines.disabled() should create an instance with every field set to None."""
        d = RpcDeadlines.disabled()
        self.assertIsNone(d.reattachable_execute_plan)
        self.assertIsNone(d.reattach_execute)
        self.assertIsNone(d.analyze_plan)
        self.assertIsNone(d.add_artifacts)
        self.assertIsNone(d.config)
        self.assertIsNone(d.interrupt)
        self.assertIsNone(d.release_session)
        self.assertIsNone(d.artifact_status)
        self.assertIsNone(d.clone_session)
        self.assertIsNone(d.get_status)
        self.assertIsNone(d.fetch_error_details)

    def test_rpc_deadlines_defaults_are_set(self):
        """RpcDeadlines() default instance should have documented timeout values."""
        d = RpcDeadlines()
        self.assertEqual(d.reattachable_execute_plan, 10 * 60)
        self.assertEqual(d.reattach_execute, 10 * 60)
        self.assertEqual(d.analyze_plan, 60 * 60)
        self.assertEqual(d.add_artifacts, 60 * 60)
        self.assertEqual(d.config, 10 * 60)
        self.assertEqual(d.interrupt, 10 * 60)
        self.assertEqual(d.release_session, 10 * 60)
        self.assertEqual(d.artifact_status, 10 * 60)
        self.assertEqual(d.clone_session, 10 * 60)
        self.assertEqual(d.get_status, 10 * 60)
        self.assertEqual(d.fetch_error_details, 10 * 60)

    def test_rpc_deadlines_rejects_non_positive_values(self):
        """RpcDeadlines should raise ValueError for any non-None field that is <= 0."""
        from dataclasses import replace

        with self.assertRaises(ValueError):
            replace(RpcDeadlines(), analyze_plan=-1.0)
        with self.assertRaises(ValueError):
            replace(RpcDeadlines(), config=0)
        with self.assertRaises(ValueError):
            replace(RpcDeadlines(), reattachable_execute_plan=-0.001)
        with self.assertRaises(ValueError):
            replace(RpcDeadlines(), config=float("-inf"))
        # None (disabled) and positive values should be accepted without error.
        replace(RpcDeadlines(), analyze_plan=None)
        replace(RpcDeadlines(), analyze_plan=0.001)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
