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

import json
import sys
from typing import TYPE_CHECKING, Any, cast, Dict, List, Optional

from pyspark.errors import StreamingQueryException, PySparkValueError
import pyspark.sql.connect.proto as pb2
from pyspark.serializers import CloudPickleSerializer
from pyspark.sql.connect import proto
from pyspark.sql.connect.utils import get_python_ver
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.query import (
    StreamingQuery as PySparkStreamingQuery,
    StreamingQueryManager as PySparkStreamingQueryManager,
)
from pyspark.errors.exceptions.connect import (
    StreamingQueryException as CapturedStreamingQueryException,
)

__all__ = ["StreamingQuery", "StreamingQueryManager"]

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession


class StreamingQuery:
    def __init__(
        self, session: "SparkSession", queryId: str, runId: str, name: Optional[str] = None
    ) -> None:
        self._session = session
        self._query_id = queryId
        self._run_id = runId
        self._name = name

    @property
    def id(self) -> str:
        return self._query_id

    id.__doc__ = PySparkStreamingQuery.id.__doc__

    @property
    def runId(self) -> str:
        return self._run_id

    runId.__doc__ = PySparkStreamingQuery.runId.__doc__

    @property
    def name(self) -> Optional[str]:
        return self._name

    name.__doc__ = PySparkStreamingQuery.name.__doc__

    @property
    def isActive(self) -> bool:
        return self._fetch_status().is_active

    isActive.__doc__ = PySparkStreamingQuery.isActive.__doc__

    def awaitTermination(self, timeout: Optional[int] = None) -> Optional[bool]:
        cmd = pb2.StreamingQueryCommand()
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout <= 0:
                raise PySparkValueError(
                    error_class="VALUE_NOT_POSITIVE",
                    message_parameters={"arg_name": "timeout", "arg_value": type(timeout).__name__},
                )
            cmd.await_termination.timeout_ms = int(timeout * 1000)
            terminated = self._execute_streaming_query_cmd(cmd).await_termination.terminated
            return terminated
        else:
            await_termination_cmd = pb2.StreamingQueryCommand.AwaitTerminationCommand()
            cmd.await_termination.CopyFrom(await_termination_cmd)
            self._execute_streaming_query_cmd(cmd)
            return None

    awaitTermination.__doc__ = PySparkStreamingQuery.awaitTermination.__doc__

    @property
    def status(self) -> Dict[str, Any]:
        proto = self._fetch_status()
        return {
            "message": proto.status_message,
            "isDataAvailable": proto.is_data_available,
            "isTriggerActive": proto.is_trigger_active,
        }

    status.__doc__ = PySparkStreamingQuery.status.__doc__

    @property
    def recentProgress(self) -> List[Dict[str, Any]]:
        cmd = pb2.StreamingQueryCommand()
        cmd.recent_progress = True
        progress = self._execute_streaming_query_cmd(cmd).recent_progress.recent_progress_json
        return [json.loads(p) for p in progress]

    recentProgress.__doc__ = PySparkStreamingQuery.recentProgress.__doc__

    @property
    def lastProgress(self) -> Optional[Dict[str, Any]]:
        cmd = pb2.StreamingQueryCommand()
        cmd.last_progress = True
        progress = self._execute_streaming_query_cmd(cmd).recent_progress.recent_progress_json
        if len(progress) > 0:
            return json.loads(progress[-1])
        else:
            return None

    lastProgress.__doc__ = PySparkStreamingQuery.lastProgress.__doc__

    def processAllAvailable(self) -> None:
        cmd = pb2.StreamingQueryCommand()
        cmd.process_all_available = True
        self._execute_streaming_query_cmd(cmd)

    processAllAvailable.__doc__ = PySparkStreamingQuery.processAllAvailable.__doc__

    def stop(self) -> None:
        cmd = pb2.StreamingQueryCommand()
        cmd.stop = True
        self._execute_streaming_query_cmd(cmd)

    stop.__doc__ = PySparkStreamingQuery.stop.__doc__

    def explain(self, extended: bool = False) -> None:
        cmd = pb2.StreamingQueryCommand()
        cmd.explain.extended = extended
        result = self._execute_streaming_query_cmd(cmd).explain.result
        print(result)

    explain.__doc__ = PySparkStreamingQuery.explain.__doc__

    def exception(self) -> Optional[StreamingQueryException]:
        cmd = pb2.StreamingQueryCommand()
        cmd.exception = True
        exception = self._execute_streaming_query_cmd(cmd).exception
        if not exception.HasField("exception_message"):
            return None
        else:
            # Drop the Java StreamingQueryException type info
            # exception_message maps to the return value of original
            # StreamingQueryException's toString method
            msg = exception.exception_message.split(": ", 1)[1]
            if exception.HasField("stack_trace"):
                msg += f"\n\nJVM stacktrace:\n{exception.stack_trace}"
            return CapturedStreamingQueryException(msg, reason=exception.error_class)

    exception.__doc__ = PySparkStreamingQuery.exception.__doc__

    def _fetch_status(self) -> pb2.StreamingQueryCommandResult.StatusResult:
        cmd = pb2.StreamingQueryCommand()
        cmd.status = True
        return self._execute_streaming_query_cmd(cmd).status

    def _execute_streaming_query_cmd(
        self, cmd: pb2.StreamingQueryCommand
    ) -> pb2.StreamingQueryCommandResult:
        cmd.query_id.id = self._query_id
        cmd.query_id.run_id = self._run_id
        exec_cmd = pb2.Command()
        exec_cmd.streaming_query_command.CopyFrom(cmd)
        (_, properties) = self._session.client.execute_command(exec_cmd)
        return cast(pb2.StreamingQueryCommandResult, properties["streaming_query_command_result"])


class StreamingQueryManager:
    def __init__(self, session: "SparkSession") -> None:
        self._session = session

    @property
    def active(self) -> List[StreamingQuery]:
        cmd = pb2.StreamingQueryManagerCommand()
        cmd.active = True
        queries = self._execute_streaming_query_manager_cmd(cmd).active.active_queries
        return [StreamingQuery(self._session, q.id.id, q.id.run_id, q.name) for q in queries]

    active.__doc__ = PySparkStreamingQueryManager.active.__doc__

    def get(self, id: str) -> Optional[StreamingQuery]:
        cmd = pb2.StreamingQueryManagerCommand()
        cmd.get_query = id
        response = self._execute_streaming_query_manager_cmd(cmd)
        if response.HasField("query"):
            query = response.query
            return StreamingQuery(self._session, query.id.id, query.id.run_id, query.name)
        else:
            return None

    get.__doc__ = PySparkStreamingQueryManager.get.__doc__

    def awaitAnyTermination(self, timeout: Optional[int] = None) -> Optional[bool]:
        cmd = pb2.StreamingQueryManagerCommand()
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout <= 0:
                raise PySparkValueError(
                    error_class="VALUE_NOT_POSITIVE",
                    message_parameters={"arg_name": "timeout", "arg_value": type(timeout).__name__},
                )
            cmd.await_any_termination.timeout_ms = int(timeout * 1000)
            terminated = self._execute_streaming_query_manager_cmd(
                cmd
            ).await_any_termination.terminated
            return terminated
        else:
            await_any_termination_cmd = (
                pb2.StreamingQueryManagerCommand.AwaitAnyTerminationCommand()
            )
            cmd.await_any_termination.CopyFrom(await_any_termination_cmd)
            self._execute_streaming_query_manager_cmd(cmd)
            return None

    awaitAnyTermination.__doc__ = PySparkStreamingQueryManager.awaitAnyTermination.__doc__

    def resetTerminated(self) -> None:
        cmd = pb2.StreamingQueryManagerCommand()
        cmd.reset_terminated = True
        self._execute_streaming_query_manager_cmd(cmd)

    resetTerminated.__doc__ = PySparkStreamingQueryManager.resetTerminated.__doc__

    def addListener(self, listener: StreamingQueryListener) -> None:
        listener._init_listener_id()
        cmd = pb2.StreamingQueryManagerCommand()
        expr = proto.PythonUDF()
        expr.command = CloudPickleSerializer().dumps(listener)
        expr.python_ver = get_python_ver()
        cmd.add_listener.python_listener_payload.CopyFrom(expr)
        cmd.add_listener.id = listener._id
        self._execute_streaming_query_manager_cmd(cmd)

    addListener.__doc__ = PySparkStreamingQueryManager.addListener.__doc__

    def removeListener(self, listener: StreamingQueryListener) -> None:
        cmd = pb2.StreamingQueryManagerCommand()
        cmd.remove_listener.id = listener._id
        self._execute_streaming_query_manager_cmd(cmd)

    removeListener.__doc__ = PySparkStreamingQueryManager.removeListener.__doc__

    def _execute_streaming_query_manager_cmd(
        self, cmd: pb2.StreamingQueryManagerCommand
    ) -> pb2.StreamingQueryManagerCommandResult:
        exec_cmd = pb2.Command()
        exec_cmd.streaming_query_manager_command.CopyFrom(cmd)
        (_, properties) = self._session.client.execute_command(exec_cmd)
        return cast(
            pb2.StreamingQueryManagerCommandResult,
            properties["streaming_query_manager_command_result"],
        )


def _test() -> None:
    import doctest
    import os
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.streaming.query

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.connect.streaming.query.__dict__.copy()

    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.streaming.query tests")
        .remote("local[4]")
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.streaming.query,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
