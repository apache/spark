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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

import uuid
from collections.abc import Generator
from typing import Optional, Dict, Any

import pyspark.sql.connect.proto as pb2
import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib
from pyspark.sql.connect.client import SparkConnectClient
from pyspark.sql.connect.client.core import Retrying


class ExecutePlanResponseReattachableIterator(Generator):
    def __init__(
        self,
        request: pb2.ExecutePlanRequest,
        stub: grpc_lib.SparkConnectServiceStub,
        retry_policy: Dict[str, Any],
    ):
        self._request = request
        self._retry_policy = retry_policy
        self._operation_id = str(uuid.uuid4())
        self._stub = stub
        request.reattach_options.reattachable = True  # type: ignore[attr-defined]
        self._initial_request = request
        self._response_complete = False
        self._iterator = self._stub.ExecutePlan(self._initial_request)

        self._last_response_id: Optional[str] = None
        self._current: Optional[pb2.ExecutePlanResponse] = None

    def send(self, value: Any) -> pb2.ExecutePlanResponse:
        # will trigger reattach in case the stream completed without responseComplete
        self._has_next()

        # Get next response, possibly triggering reattach in case of stream error.
        first_try = True
        ret: Optional[pb2.ExecutePlanResponse] = None
        for attempt in Retrying(can_retry=SparkConnectClient.retry_exception, **self._retry_policy):
            with attempt:
                if first_try:
                    # on first try, we use the initial iterator.
                    first_try = False
                else:
                    # Error retry reattach: After an error, attempt to
                    self._iterator = self._stub.ReattachExecute(
                        self._create_reattach_execute_request()
                    )
                ret = self._current

        if ret is None:
            raise StopIteration()

        assert ret is not None
        self._last_response_id = ret.response_id
        if ret.response_complete:
            self._response_complete = True
            self._release_execute(None)  # release all
        else:
            self._release_execute(self._last_response_id)
        self._current = None
        return ret

    def _has_next(self) -> bool:
        if self._response_complete:
            # After response complete response
            return False
        else:
            for attempt in Retrying(
                can_retry=SparkConnectClient.retry_exception, **self._retry_policy
            ):
                with attempt:
                    if self._current is None:
                        try:
                            self._current = next(self._iterator)
                        except StopIteration:
                            pass
                    has_next = self._current is not None

                    # Graceful reattach:
                    # If iterator ended, but there was no ResponseComplete, it means that
                    # there is more, and we need to reattach. While ResponseComplete didn't
                    # arrive, we keep reattaching.
                    if not has_next and not self._response_complete:
                        while not has_next:
                            self._iterator = self._stub.ReattachExecute(
                                self._create_reattach_execute_request()
                            )
                            # shouldn't change
                            assert not self._response_complete
                            try:
                                self._current = next(self._iterator)
                            except StopIteration:
                                pass
                            has_next = self._current is not None
                    return has_next
            return False

    def _release_execute(self, until_response_id: Optional[str]) -> None:
        request = self._create_release_execute_request(until_response_id)
        for attempt in Retrying(can_retry=SparkConnectClient.retry_exception, **self._retry_policy):
            with attempt:
                self._stub.ReleaseExecute(request)

    def _create_reattach_execute_request(self) -> pb2.ReattachExecuteRequest:
        release = pb2.ReattachExecuteRequest(
            session_id=self._initial_request.session_id,
            user_context=self._initial_request.user_context,
            operation_id=self._initial_request.operation_id,
        )

        if self._initial_request.client_type:
            release.client_type = self._initial_request.client_type

        return release

    def _create_release_execute_request(
        self, until_response_id: Optional[str]
    ) -> pb2.ReattachExecuteRequest:
        release = pb2.ReattachExecuteRequest(
            session_id=self._initial_request.session_id,
            user_context=self._initial_request.user_context,
            operation_id=self._initial_request.operation_id,
        )

        if self._initial_request.client_type:
            release.client_type = self._initial_request.client_type

        if not until_response_id:
            release.release_type = (  # type: ignore[attr-defined]
                pb2.ReleaseExecuteRequest.ReleaseType.RELEASE_UNTIL_RESPONSE
            )
        else:
            release.release_type = (  # type: ignore[attr-defined]
                pb2.ReleaseExecuteRequest.ReleaseType.RELEASE_ALL
            )
            release.until_response_id = until_response_id  # type: ignore[attr-defined]

        return release

    def throw(self, type: Any = None, value: Any = None, traceback: Any = None) -> Any:
        super().throw(type, value, traceback)

    def close(self) -> None:
        return super().close()

    def __del__(self) -> None:
        return self.close()
