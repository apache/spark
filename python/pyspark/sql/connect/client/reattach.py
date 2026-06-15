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
from pyspark.sql.connect.client.retries import Retrying, RetryException

from threading import RLock
import uuid
from collections.abc import Generator
from typing import Optional, Any, Iterator, Iterable, Tuple, Callable, Union, cast, ClassVar
from concurrent.futures import Future, ThreadPoolExecutor
import os
import weakref

import grpc
from grpc_status import rpc_status

from pyspark.sql.connect.logging import logger
import pyspark.sql.connect.proto as pb2
import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib
from pyspark.errors import PySparkRuntimeError
from pyspark.util import disable_gc


class ExecutePlanResponseReattachableIterator(Generator):
    """
    Retryable iterator of ExecutePlanResponses to an ExecutePlan call.

    It can handle situations when:
      - the ExecutePlanResponse stream was broken by retryable network error (governed by
        retryPolicy)
      - the ExecutePlanResponse was gracefully ended by the server without a ResultComplete
        message; this tells the client that there is more, and it should reattach to continue.

    Initial iterator is the result of an ExecutePlan on the request, but it can be reattached with
    ReattachExecute request. ReattachExecute request is provided the responseId of last returned
    ExecutePlanResponse on the iterator to return a new iterator from server that continues after
    that. If the initial ExecutePlan did not even reach the server, and hence reattach fails with
    INVALID_HANDLE.OPERATION_NOT_FOUND, we attempt to retry ExecutePlan.

    In reattachable execute the server does buffer some responses in case the client needs to
    backtrack. To let server release this buffer sooner, this iterator asynchronously sends
    ReleaseExecute RPCs that instruct the server to release responses that it already processed.

    ``metadata`` may be a static iterable or a zero-arg callable; the callable is invoked
    before every RPC (so short-TTL credentials refresh across reattach) and must be thread-safe
    because ``ReleaseExecute`` runs in a background thread pool. A ``PERMISSION_DENIED`` mid-
    stream is retried once per iterator so the next ``ReattachExecute`` can use a refreshed
    credential; a second one propagates as a genuine auth failure.
    """

    _lock: ClassVar[RLock] = RLock()
    _release_thread_pool_instance: Optional[ThreadPoolExecutor] = None
    _instances: ClassVar[weakref.WeakSet["ExecutePlanResponseReattachableIterator"]] = (
        weakref.WeakSet()
    )

    def __new__(cls, *args: Any, **kwargs: Any) -> "ExecutePlanResponseReattachableIterator":
        instance = super().__new__(cls)
        cls._instances.add(instance)
        return instance

    def __init__(
        self,
        request: pb2.ExecutePlanRequest,
        stub: grpc_lib.SparkConnectServiceStub,
        retrying: Callable[[], Retrying],
        metadata: Union[
            Iterable[Tuple[str, str]],
            Callable[[], Iterable[Tuple[str, str]]],
        ],
        reattachable_execute_plan_timeout: Optional[float] = None,
        reattach_execute_timeout: Optional[float] = None,
    ):
        self._request = request
        self._retrying = retrying
        self._release_futures: weakref.WeakSet[Future] = weakref.WeakSet()
        if request.operation_id:
            self._operation_id = request.operation_id
        else:
            # Add operation id, if not present.
            # with operationId set by the client, the client can use it to try to reattach on error
            # even before getting the first response. If the operation in fact didn't even reach the
            # server, that will end with INVALID_HANDLE.OPERATION_NOT_FOUND error.
            self._operation_id = str(uuid.uuid4())

        self._stub = stub
        self._reattachable_execute_plan_timeout = reattachable_execute_plan_timeout
        self._reattach_execute_timeout = reattach_execute_timeout
        request.request_options.append(
            pb2.ExecutePlanRequest.RequestOption(
                reattach_options=pb2.ReattachOptions(reattachable=True)
            )
        )
        request.operation_id = self._operation_id
        self._initial_request = request

        # ResponseId of the last response returned by next()
        self._last_returned_response_id: Optional[str] = None

        # True after ResponseComplete message was seen in the stream.
        # Server will always send this message at the end of the stream, if the underlying iterator
        # finishes without producing one, another iterator needs to be reattached.
        self._result_complete = False

        # PERMISSION_DENIED budget: one reattach between successful responses.
        self._permission_denied_retried = False

        self._metadata_provider: Callable[[], Iterable[Tuple[str, str]]]
        if callable(metadata):
            self._metadata_provider = metadata
        else:
            # one-shot iterators would be exhausted after the first RPC.
            _cached_metadata = list(metadata)
            self._metadata_provider = lambda: _cached_metadata

        # ``_metadata_provider()`` runs eagerly here and may raise (e.g. credential refresh).
        with disable_gc():
            self._iterator: Optional[Iterator[pb2.ExecutePlanResponse]] = iter(
                self._stub.ExecutePlan(
                    self._initial_request,
                    metadata=self._metadata_provider(),
                    timeout=self._reattachable_execute_plan_timeout,
                )
            )

        # Current item from this iterator.
        self._current: Optional[pb2.ExecutePlanResponse] = None

    @property
    def release_futures(self) -> weakref.WeakSet[Future]:
        return self._release_futures

    @property
    def _release_thread_pool(self) -> ThreadPoolExecutor:
        if self._release_thread_pool_instance is not None:
            # It's impossible for the thread pool to shutdown when the iterator is still alive
            # We can safely return the thread pool instance here as long as it is not used
            # by external objects.
            return self._release_thread_pool_instance
        with self._lock:
            if self._release_thread_pool_instance is None:
                self._release_thread_pool_instance = ThreadPoolExecutor(
                    max_workers=os.cpu_count() or 8
                )
            return self._release_thread_pool_instance

    @classmethod
    def shutdown_threadpool_if_idle(cls) -> None:
        with cls._lock:
            if not cls._instances and cls._release_thread_pool_instance is not None:
                cls._release_thread_pool_instance.shutdown(wait=False)
                cls._release_thread_pool_instance = None

    def send(self, value: Any) -> pb2.ExecutePlanResponse:
        # will trigger reattach in case the stream completed without result_complete
        with disable_gc():
            if not self._has_next():
                raise StopIteration()

            ret = self._current
            assert ret is not None

            self._last_returned_response_id = ret.response_id
            if ret.HasField("result_complete"):
                self._release_all()
            else:
                self._release_until(self._last_returned_response_id)
            self._current = None
        return ret

    def _has_next(self) -> bool:
        if self._result_complete:
            # After response complete response
            return False
        else:
            try:
                for attempt in self._retrying():
                    with attempt:
                        if self._current is None:
                            try:
                                self._current = self._call_iter(
                                    lambda: next(self._iterator)  # type: ignore[arg-type]
                                )
                            except StopIteration:
                                pass

                        has_next = self._current is not None

                        # Graceful reattach:
                        # If iterator ended, but there was no ResponseComplete, it means that
                        # there is more, and we need to reattach. While ResponseComplete didn't
                        # arrive, we keep reattaching.
                        if not self._result_complete and not has_next:
                            while not has_next:
                                # unset iterator for new ReattachExecute to be called in _call_iter
                                self._iterator = None
                                # shouldn't change
                                assert not self._result_complete
                                try:
                                    self._current = self._call_iter(
                                        lambda: next(self._iterator)  # type: ignore[arg-type]
                                    )
                                except StopIteration:
                                    pass
                                has_next = self._current is not None
                        return has_next
            except Exception as e:
                self._release_all()
                raise e
            return False

    def _release_until(self, until_response_id: str) -> None:
        """
        Inform the server to release the buffered execution results until and including given
        result.

        This will send an asynchronous RPC which will not block this iterator, the iterator can
        continue to be consumed.
        """
        if self._result_complete:
            return

        request = self._create_release_execute_request(until_response_id)

        def target() -> None:
            try:
                metadata = self._metadata_provider()
            except Exception as e:
                # Don't let provider errors masquerade as server-side release failures.
                logger.warning(f"metadata provider failed before ReleaseExecute: {e}.")
                return
            try:
                for attempt in self._retrying():
                    with attempt:
                        self._stub.ReleaseExecute(request, metadata=metadata)
            except Exception as e:
                logger.warning(f"ReleaseExecute failed with exception: {e}.")

        self._release_futures.add(self._release_thread_pool.submit(target))

    def _release_all(self) -> None:
        """
        Inform the server to release the execution, either because all results were consumed,
        or the execution finished with error and the error was received.

        This will send an asynchronous RPC which will not block this. The client continues
        executing, and if the release fails, server is equipped to deal with abandoned executions.
        """
        if self._result_complete:
            return

        request = self._create_release_execute_request(None)

        def target() -> None:
            try:
                metadata = self._metadata_provider()
            except Exception as e:
                logger.warning(f"metadata provider failed before ReleaseExecute: {e}.")
                return
            try:
                for attempt in self._retrying():
                    with attempt:
                        self._stub.ReleaseExecute(request, metadata=metadata)
            except Exception as e:
                logger.warning(f"ReleaseExecute failed with exception: {e}.")

        self._release_futures.add(self._release_thread_pool.submit(target))
        self._result_complete = True

    def _call_iter(self, iter_fun: Callable) -> Any:
        """
        Call next() on the iterator. If this fails with this operationId not existing
        on the server, this means that the initial ExecutePlan request didn't even reach the
        server. In that case, attempt to start again with ExecutePlan.

        Called inside retry block, so retryable failure will get handled upstream.
        """
        if self._iterator is None:
            # we get a new iterator with ReattachExecute if it was unset.
            self._iterator = iter(
                self._stub.ReattachExecute(
                    self._create_reattach_execute_request(),
                    metadata=self._metadata_provider(),
                    timeout=self._reattach_execute_timeout,
                )
            )

        try:
            result = iter_fun()
        except grpc.RpcError as e:
            status = rpc_status.from_call(cast(grpc.Call, e))
            unexpected_error = next(
                (
                    error
                    for error in [
                        "INVALID_HANDLE.OPERATION_NOT_FOUND",
                        "INVALID_HANDLE.SESSION_NOT_FOUND",
                    ]
                    if status is not None and error in status.message
                ),
                None,
            )
            if unexpected_error is not None:
                if self._last_returned_response_id is not None:
                    raise PySparkRuntimeError(
                        errorClass="RESPONSE_ALREADY_RECEIVED",
                        messageParameters={"error_type": unexpected_error},
                    )
                # Try a new ExecutePlan, and throw upstream for retry.
                self._iterator = iter(
                    self._stub.ExecutePlan(
                        self._initial_request,
                        metadata=self._metadata_provider(),
                        timeout=self._reattachable_execute_plan_timeout,
                    )
                )
                self._permission_denied_retried = False
                raise RetryException()
            elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                # The per-RPC deadline fired. The server-side operation is still alive; clear
                # the iterator and raise RetryException so the retry loop opens a fresh
                # ReattachExecute stream with a new deadline countdown to resume results.
                logger.debug(
                    f"Deadline exceeded on stream for operation {self._operation_id}; "
                    f"will reattach. (last response: {self._last_returned_response_id})"
                )
                self._iterator = None
                raise RetryException() from e
            elif (
                e.code() == grpc.StatusCode.PERMISSION_DENIED
                and not self._permission_denied_retried
            ):
                # Treat the first mid-stream PERMISSION_DENIED as a token-expiry signal; reattach
                # once so the metadata provider can produce a fresh credential.
                logger.debug(
                    f"PERMISSION_DENIED on stream for operation {self._operation_id}; "
                    f"allowing one reattach with refreshed metadata."
                )
                self._permission_denied_retried = True
                self._iterator = None
                raise RetryException() from e
            else:
                # Remove the iterator, so that a new one will be created after retry.
                self._iterator = None
                raise e
        except Exception as e:
            # Remove the iterator, so that a new one will be created after retry.
            self._iterator = None
            raise e
        self._permission_denied_retried = False
        return result

    def _create_reattach_execute_request(self) -> pb2.ReattachExecuteRequest:
        server_side_session_id = (
            None
            if not self._initial_request.client_observed_server_side_session_id
            else self._initial_request.client_observed_server_side_session_id
        )
        reattach = pb2.ReattachExecuteRequest(
            session_id=self._initial_request.session_id,
            client_observed_server_side_session_id=server_side_session_id,
            user_context=self._initial_request.user_context,
            operation_id=self._initial_request.operation_id,
        )

        if self._initial_request.client_type:
            reattach.client_type = self._initial_request.client_type

        if self._last_returned_response_id:
            reattach.last_response_id = self._last_returned_response_id

        return reattach

    def _create_release_execute_request(
        self, until_response_id: Optional[str]
    ) -> pb2.ReleaseExecuteRequest:
        release = pb2.ReleaseExecuteRequest(
            session_id=self._initial_request.session_id,
            user_context=self._initial_request.user_context,
            operation_id=self._initial_request.operation_id,
        )

        if self._initial_request.client_type:
            release.client_type = self._initial_request.client_type

        if not until_response_id:
            release.release_all.CopyFrom(pb2.ReleaseExecuteRequest.ReleaseAll())
        else:
            release.release_until.response_id = until_response_id

        return release

    def throw(self, type: Any = None, value: Any = None, traceback: Any = None) -> Any:
        super().throw(type, value, traceback)

    def close(self) -> None:
        self._release_all()
        return super().close()
