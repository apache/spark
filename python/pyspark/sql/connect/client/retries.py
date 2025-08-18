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

import grpc
import random
import time
import typing
import warnings
from google.rpc import error_details_pb2
from grpc_status import rpc_status
from typing import Optional, Callable, Generator, List, Type, cast
from types import TracebackType
from pyspark.sql.connect.logging import logger
from pyspark.errors import PySparkRuntimeError

"""
This module contains retry system. The system is designed to be
significantly customizable.

A key aspect of retries is RetryPolicy class, describing a single policy.
There can be more than one policy defined at the same time. Each policy
determines which error types it can retry and how exactly.

For instance, networking errors should likely be retried differently that
remote resource being unavailable.

Given a sequence of policies, retry logic applies all of them in sequential
order, keeping track of different policies budgets.
"""


class RetryPolicy:
    """
    Describes key aspects of RetryPolicy.

    It's advised that different policies are implemented as different subclasses.

    Parameters
    ----------
    max_retries: int, optional
        Maximum number of retries.
    initial_backoff: int
        Start value of the exponential backoff.
    max_backoff: int, optional
        Maximal value of the exponential backoff.
    backoff_multiplier: float
        Multiplicative base of the exponential backoff.
    jitter: int
        Sample a random value uniformly from the range [0, jitter] and add it to the backoff.
    min_jitter_threshold: int
        Minimal value of the backoff to add random jitter.
    recognize_server_retry_delay: bool
        Per gRPC standard, the server can send error messages that contain `RetryInfo` message
        with `retry_delay` field indicating that the client should wait for at least `retry_delay`
        amount of time before retrying again, see:
        https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto#L91

        If this flag is set to true, RetryPolicy will use `RetryInfo.retry_delay` field
        in the backoff computation. Server's `retry_delay` can override client's `max_backoff`.

        This flag does not change which errors are retried, only how the backoff is computed.
        `DefaultPolicy` additionally has a rule for retrying any error that contains `RetryInfo`.
    max_server_retry_delay: int, optional
        Limit for the server-provided `retry_delay`.
    """

    def __init__(
        self,
        max_retries: Optional[int] = None,
        initial_backoff: int = 1000,
        max_backoff: Optional[int] = None,
        backoff_multiplier: float = 1.0,
        jitter: int = 0,
        min_jitter_threshold: int = 0,
        recognize_server_retry_delay: bool = False,
        max_server_retry_delay: Optional[int] = None,
    ):
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier
        self.jitter = jitter
        self.min_jitter_threshold = min_jitter_threshold
        self.recognize_server_retry_delay = recognize_server_retry_delay
        self.max_server_retry_delay = max_server_retry_delay
        self._name = self.__class__.__name__

    @property
    def name(self) -> str:
        return self._name

    def can_retry(self, exception: BaseException) -> bool:
        return False

    def to_state(self) -> "RetryPolicyState":
        return RetryPolicyState(self)


class RetryPolicyState:
    """
    This class represents stateful part of the specific policy.
    """

    def __init__(self, policy: RetryPolicy):
        self._policy = policy

        # Will allow attempts [0, self._policy.max_retries)
        self._attempt = 0
        self._next_wait: float = self._policy.initial_backoff

    @property
    def policy(self) -> RetryPolicy:
        return self._policy

    @property
    def name(self) -> str:
        return self.policy.name

    def can_retry(self, exception: BaseException) -> bool:
        return self.policy.can_retry(exception)

    def next_attempt(self, exception: Optional[BaseException] = None) -> Optional[int]:
        """
        Returns
        -------
            Randomized time (in milliseconds) to wait until this attempt
            or None if this policy doesn't allow more retries.
        """

        if self.policy.max_retries is not None and self._attempt >= self.policy.max_retries:
            # No more retries under this policy
            return None

        self._attempt += 1
        wait_time = self._next_wait

        # Calculate future backoff
        if self.policy.max_backoff is not None:
            self._next_wait = min(
                float(self.policy.max_backoff), wait_time * self.policy.backoff_multiplier
            )

        if exception is not None and self.policy.recognize_server_retry_delay:
            retry_delay = extract_retry_delay(exception)
            if retry_delay is not None:
                logger.debug(f"The server has sent a retry delay of {retry_delay} ms.")
                if self.policy.max_server_retry_delay is not None:
                    retry_delay = min(retry_delay, self.policy.max_server_retry_delay)
                wait_time = max(wait_time, retry_delay)

        # Jitter current backoff, after the future backoff was computed
        if wait_time >= self.policy.min_jitter_threshold:
            wait_time += random.uniform(0, self.policy.jitter)

        # Round to whole number of milliseconds
        return int(wait_time)


class AttemptManager:
    """
    Simple ContextManager that is used to capture the exception thrown inside the context.
    """

    def __init__(self, retrying: "Retrying") -> None:
        self._retrying = retrying

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exception: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if isinstance(exception, BaseException):
            # Swallow the exception.
            if self._retrying.accept_exception(exception):
                return True
            # Bubble up the exception.
            return False
        else:
            self._retrying.accept_succeeded()
            return None


class Retrying:
    """
    This class is a point of entry into the retry logic.
    The class accepts a list of retry policies and applies them in given order.
    The first policy accepting an exception will be used.
    If the error was matched by one policy, the other policies will be skipped.

    The usage of the class should be as follows:
    for attempt in Retrying(...):
        with attempt:
            Do something that can throw exception

    In case error is considered retriable, it would be retried based on policies, and
    it will be raised if the retries limit would exceed.

    Exceptions not considered retriable will be passed through transparently.
    """

    def __init__(
        self,
        policies: typing.Union[RetryPolicy, typing.Iterable[RetryPolicy]],
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if isinstance(policies, RetryPolicy):
            policies = [policies]
        self._policies: List[RetryPolicyState] = [policy.to_state() for policy in policies]
        self._sleep = sleep

        self._exception: Optional[BaseException] = None
        self._done = False

    def can_retry(self, exception: BaseException) -> bool:
        if isinstance(exception, RetryException):
            return True

        return any(policy.can_retry(exception) for policy in self._policies)

    def accept_exception(self, exception: BaseException) -> bool:
        if self.can_retry(exception):
            self._exception = exception
            return True
        return False

    def accept_succeeded(self) -> None:
        self._done = True

    def _last_exception(self) -> BaseException:
        if self._exception is None:
            raise PySparkRuntimeError(
                errorClass="NO_ACTIVE_EXCEPTION",
                messageParameters={},
            )
        return self._exception

    def _wait(self) -> None:
        exception = self._last_exception()

        if isinstance(exception, RetryException):
            # Considered immediately retriable
            logger.debug(f"Got error: {repr(exception)}. Retrying.")
            return

        # Attempt to find a policy to wait with
        matched_policy = None
        for policy in self._policies:
            if policy.can_retry(exception):
                matched_policy = policy
                break
        if matched_policy is not None:
            wait_time = matched_policy.next_attempt(exception)
            if wait_time is not None:
                logger.debug(
                    f"Got error: {repr(exception)}. "
                    + f"Will retry after {wait_time} ms (policy: {matched_policy.name})"
                )
                self._sleep(wait_time / 1000)
                return

        # Exceeded retries
        logger.debug(f"Given up on retrying. error: {repr(exception)}")
        warnings.warn("[RETRIES_EXCEEDED] The maximum number of retries has been exceeded.")
        raise exception

    def __iter__(self) -> Generator[AttemptManager, None, None]:
        """
        Generator function to wrap the exception producing code block.

        Returns
        -------
        A generator that yields the current attempt.
        """

        # First attempt is free, no need to do waiting.
        yield AttemptManager(self)

        while not self._done:
            self._wait()
            yield AttemptManager(self)


class RetryException(Exception):
    """
    An exception that can be thrown upstream when inside retry and which is always retryable
    even without policies
    """


class DefaultPolicy(RetryPolicy):
    # Please synchronize changes here with Scala side in
    # org.apache.spark.sql.connect.client.RetryPolicy
    #
    # Note: the number of retries is selected so that the maximum tolerated wait
    # is guaranteed to be at least 10 minutes

    def __init__(
        self,
        max_retries: Optional[int] = 15,
        backoff_multiplier: float = 4.0,
        initial_backoff: int = 50,
        max_backoff: Optional[int] = 60000,
        jitter: int = 500,
        min_jitter_threshold: int = 2000,
        recognize_server_retry_delay: bool = True,
        max_server_retry_delay: Optional[int] = 10 * 60 * 1000,  # 10 minutes
    ):
        super().__init__(
            max_retries=max_retries,
            backoff_multiplier=backoff_multiplier,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
            jitter=jitter,
            min_jitter_threshold=min_jitter_threshold,
            recognize_server_retry_delay=recognize_server_retry_delay,
            max_server_retry_delay=max_server_retry_delay,
        )

    def can_retry(self, e: BaseException) -> bool:
        """
        Helper function that is used to identify if an exception thrown by the server
        can be retried or not.

        Parameters
        ----------
        e : Exception
            The GRPC error as received from the server. Typed as Exception, because other exception
            thrown during client processing can be passed here as well.

        Returns
        -------
        True if the exception can be retried, False otherwise.

        """

        if not isinstance(e, grpc.RpcError):
            return False

        if e.code() in [grpc.StatusCode.INTERNAL]:
            msg = str(e)

            # This error happens if another RPC preempts this RPC.
            if "INVALID_CURSOR.DISCONNECTED" in msg:
                return True

        if e.code() == grpc.StatusCode.UNAVAILABLE:
            return True

        if extract_retry_info(e) is not None:
            # All errors messages containing `RetryInfo` should be retried.
            return True

        return False


def extract_retry_info(exception: BaseException) -> Optional[error_details_pb2.RetryInfo]:
    """Extract and return RetryInfo from the grpc.RpcError"""
    if isinstance(exception, grpc.RpcError):
        status = rpc_status.from_call(cast(grpc.Call, exception))
        if status:
            for d in status.details:
                if d.Is(error_details_pb2.RetryInfo.DESCRIPTOR):
                    info = error_details_pb2.RetryInfo()
                    d.Unpack(info)
                    return info
    return None


def extract_retry_delay(exception: BaseException) -> Optional[int]:
    """Extract and return RetryInfo.retry_delay in milliseconds from grpc.RpcError if present."""
    retry_info = extract_retry_info(exception)
    if retry_info is not None:
        return retry_info.retry_delay.ToMilliseconds()
    return None
