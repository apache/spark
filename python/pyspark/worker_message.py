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

import dataclasses
import json
import sys
from typing import Optional, Union, IO

from pyspark.errors import PySparkValueError
from pyspark.serializers import read_bool, read_int, read_long, SpecialLengths
from pyspark.taskcontext import BarrierTaskContext, ResourceInformation, TaskContext
from pyspark.util import PythonEvalType
from pyspark.worker_util import utf8_deserializer


@dataclasses.dataclass
class TaskContextInfo:
    @dataclasses.dataclass
    class ResourceInfo:
        name: str
        addresses: list[str]

    is_barrier: bool
    conn_info: Optional[Union[str, int]]
    secret: Optional[str]
    stage_id: int
    partition_id: int
    attempt_number: int
    task_attempt_id: int
    cpus: int
    resources: dict[str, ResourceInfo]
    local_properties: dict[str, str]

    @classmethod
    def from_stream(cls, stream: IO) -> "TaskContextInfo":
        task_context_json = json.loads(utf8_deserializer.loads(stream))
        return cls(
            is_barrier=task_context_json["isBarrier"],
            conn_info=task_context_json["connInfo"],
            secret=task_context_json["secret"],
            stage_id=task_context_json["stageId"],
            partition_id=task_context_json["partitionId"],
            attempt_number=task_context_json["attemptNumber"],
            task_attempt_id=task_context_json["taskAttemptId"],
            cpus=task_context_json["cpus"],
            resources={
                k: cls.ResourceInfo(name=v["name"], addresses=v["addresses"])
                for k, v in task_context_json["resources"].items()
            },
            local_properties=task_context_json["localProperties"],
        )

    def to_task_context(self) -> TaskContext:
        if self.is_barrier:
            return BarrierTaskContext(
                conn_info=self.conn_info,
                secret=self.secret,
                stageId=self.stage_id,
                partitionId=self.partition_id,
                attemptNumber=self.attempt_number,
                taskAttemptId=self.task_attempt_id,
                cpus=self.cpus,
                resources={
                    k: ResourceInformation(v.name, v.addresses) for k, v in self.resources.items()
                },
                localProperties=self.local_properties,
            )
        else:
            return TaskContext(
                stageId=self.stage_id,
                partitionId=self.partition_id,
                attemptNumber=self.attempt_number,
                taskAttemptId=self.task_attempt_id,
                cpus=self.cpus,
                resources={
                    k: ResourceInformation(v.name, v.addresses) for k, v in self.resources.items()
                },
                localProperties=self.local_properties,
            )


@dataclasses.dataclass
class BroadcastInfo:
    conn_info: Optional[Union[str, int]]
    auth_secret: Optional[str]
    variables: list[tuple[int, Optional[str]]]

    @classmethod
    def from_stream(cls, stream: IO) -> "BroadcastInfo":
        needs_broadcast_decryption_server = read_bool(stream)
        num_broadcast_variables = read_int(stream)
        conn_info = None
        auth_secret = None
        if needs_broadcast_decryption_server:
            conn_info = read_int(stream)
            if conn_info == -1:
                conn_info = utf8_deserializer.loads(stream)
            else:
                auth_secret = utf8_deserializer.loads(stream)

        variables = []
        for _ in range(num_broadcast_variables):
            bid = read_long(stream)
            path = None
            if bid >= 0 and not needs_broadcast_decryption_server:
                path = utf8_deserializer.loads(stream)
            variables.append((bid, path))

        return cls(conn_info=conn_info, auth_secret=auth_secret, variables=variables)


@dataclasses.dataclass
class UDFInfo:
    udfs: list[bytes]
    args: list[int]
    kwargs: dict[str, int]
    result_id: int

    @classmethod
    def from_stream(cls, stream: IO) -> "UDFInfo":
        num_args = read_int(stream)
        udfs = []
        args = []
        kwargs = {}

        for _ in range(num_args):
            offset = read_int(stream)
            if read_bool(stream):
                name = utf8_deserializer.loads(stream)
                kwargs[name] = offset
            else:
                args.append(offset)

        for i in range(read_int(stream)):
            length = read_int(stream)
            if length == SpecialLengths.END_OF_DATA_SECTION:
                raise EOFError
            elif length == SpecialLengths.NULL:
                raise PySparkValueError("Unexpected NULL value for UDF")
            else:
                data = stream.read(length)
                if len(data) < length:
                    raise EOFError
                udfs.append(data)

        result_id = read_long(stream)

        return cls(udfs=udfs, args=args, kwargs=kwargs, result_id=result_id)


@dataclasses.dataclass
class UDTFInfo:
    args: list[int]
    kwargs: dict[str, int]
    partition_child_indexes: list[int]
    pickled_analyze_result: Optional[bytes]
    handler: bytes
    return_type: str
    name: str

    @classmethod
    def from_stream(cls, stream: IO) -> "UDTFInfo":
        # See 'PythonUDTFRunner.PythonUDFWriterThread.writeCommand'
        args = []
        kwargs = {}
        for _ in range(read_int(stream)):
            offset = read_int(stream)
            if read_bool(stream):
                name = utf8_deserializer.loads(stream)
                kwargs[name] = offset
            else:
                args.append(offset)
        partition_child_indexes = [read_int(stream) for _ in range(read_int(stream))]
        if read_bool(stream):
            pickled_analyze_result = stream.read(read_int(stream))
        else:
            pickled_analyze_result = None
        handler = stream.read(read_int(stream))
        return_type = utf8_deserializer.loads(stream)
        name = utf8_deserializer.loads(stream)

        return cls(
            args=args,
            kwargs=kwargs,
            partition_child_indexes=partition_child_indexes,
            pickled_analyze_result=pickled_analyze_result,
            handler=handler,
            return_type=return_type,
            name=name,
        )


@dataclasses.dataclass
class WorkerInitInfo:
    split_index: int
    python_version: str
    spark_files_dir: str
    task_context: TaskContextInfo
    python_includes: list[str]
    broadcast: BroadcastInfo
    eval_type: int
    runner_conf: dict[str, str]
    eval_conf: dict[str, str]
    udf_info: Union[bytes, UDTFInfo, list[UDFInfo]]

    @classmethod
    def from_stream(cls, stream: IO) -> "WorkerInitInfo":
        split_index = read_int(stream)
        if split_index == -1:
            sys.exit(-1)
        python_version = utf8_deserializer.loads(stream)
        task_context = TaskContextInfo.from_stream(stream)

        spark_files_dir = utf8_deserializer.loads(stream)
        python_includes = []
        for _ in range(read_int(stream)):
            python_includes.append(utf8_deserializer.loads(stream))

        broadcast = BroadcastInfo.from_stream(stream)
        eval_type = read_int(stream)
        runner_conf = {}
        for _ in range(read_int(stream)):
            k = utf8_deserializer.loads(stream)
            v = utf8_deserializer.loads(stream)
            runner_conf[k] = v
        eval_conf = {}
        for _ in range(read_int(stream)):
            k = utf8_deserializer.loads(stream)
            v = utf8_deserializer.loads(stream)
            eval_conf[k] = v

        udf_info: Union[bytes, UDTFInfo, list[UDFInfo]]

        if eval_type == PythonEvalType.NON_UDF:
            udf_info = stream.read(read_int(stream))
        elif eval_type in (
            PythonEvalType.SQL_TABLE_UDF,
            PythonEvalType.SQL_ARROW_TABLE_UDF,
            PythonEvalType.SQL_ARROW_UDTF,
        ):
            udf_info = UDTFInfo.from_stream(stream)
        else:
            udf_info = []
            for _ in range(read_int(stream)):
                udf_info.append(UDFInfo.from_stream(stream))

        return cls(
            split_index=split_index,
            python_version=python_version,
            spark_files_dir=spark_files_dir,
            task_context=task_context,
            python_includes=python_includes,
            broadcast=broadcast,
            eval_type=eval_type,
            runner_conf=runner_conf,
            eval_conf=eval_conf,
            udf_info=udf_info,
        )
