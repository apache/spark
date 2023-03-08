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

from pyspark.errors import StreamingQueryException
import pyspark.sql.connect.proto as pb2
from pyspark.sql.streaming.query import (
    StreamingQuery as PySparkStreamingQuery,
)

__all__ = [
    "StreamingQuery", # XXX "StreamingQueryManager"
]

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession

class StreamingQuery:

    def __init__(self, session: "SparkSession", queryId: str, runId: str, name: Optional[str] = None) -> None:
        self._session = session
        self._id = queryId
        self._runId = runId
        self._name = name

    @property
    def id(self) -> str:
        return self._id

    id.__doc__ = PySparkStreamingQuery.id.__doc__

    @property
    def runId(self) -> str:
        return self._runId

    runId.__doc__ = PySparkStreamingQuery.runId.__doc__

    @property
    def name(self) -> str:
        return self._name

    # TODO Add doc for all the methods.

    @property
    def isActive(self) -> bool:
        return self._fetch_status().is_active

    def awaitTermination(self, timeout: Optional[int] = None) -> Optional[bool]:
        raise NotImplementedError()

    @property
    def status(self) -> Dict[str, Any]:
        proto = self._fetch_status()
        return {
            "message": proto.status_message,
            "isDataAvailable": proto.is_data_available,
            "isTriggerActive": proto.is_trigger_active
        }

    @property
    def recentProgress(self) -> List[Dict[str, Any]]:
        progress = list(self._fetch_status(recent_progress_limit=-1).recent_progress_json)
        return [json.loads(p) for p in progress]

    @property
    def lastProgress(self) -> Optional[Dict[str, Any]]:
        progress = list(self._fetch_status(recent_progress_limit=1).recent_progress_json)
        if len(progress) > 0:
            return json.loads(progress[-1])
        else:
            return None

    def processAllAvailable(self) -> None:
        raise NotImplementedError()

    def stop(self) -> None:
        cmd = pb2.StreamingQueryCommand()
        cmd.stop.CopyFrom(pb2.StreamingQueryStopCommand())
        self._execute_streaming_query_cmd(cmd)

    def explain(self, extended: bool = False) -> None:
        raise NotImplementedError()

    def exception(self) -> Optional[StreamingQueryException]:
        raise NotImplementedError()

    def _fetch_status(self, recent_progress_limit=0) -> pb2.StreamingQueryStatusResult:
        cmd = pb2.StreamingQueryCommand()
        status = pb2.StreamingQueryStatusCommand()
        status.recent_progress_limit = recent_progress_limit
        cmd.status.CopyFrom(status)

        return self._execute_streaming_query_cmd(cmd).status_result

    def _execute_streaming_query_cmd(self, cmd: pb2.StreamingQueryCommand) -> pb2.StreamingQueryCommandResult:
        exec_cmd = pb2.Command()
        cmd.id = self._id
        exec_cmd.streaming_query_command.CopyFrom(cmd)
        (_, properties) = self._session.client.execute_command(exec_cmd)
        return cast(pb2.StreamingQueryCommandResult, properties["streaming_query_command_result"])


# TODO class StreamingQueryManager:


def _test() -> None:
    import doctest
    import os
    from pyspark.sql import SparkSession
    import pyspark.sql.streaming.query
    from py4j.protocol import Py4JError

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.streaming.query.__dict__.copy()
    try:
        spark = SparkSession._getActiveSessionOrCreate()
    except Py4JError:  # noqa: F821
        spark = SparkSession(sc)  # type: ignore[name-defined] # noqa: F821

    globs["spark"] = spark

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.streaming.query,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
