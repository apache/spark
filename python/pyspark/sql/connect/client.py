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


import io
import logging
import typing
import uuid

import grpc  # type: ignore
import pandas
import pandas as pd
import pyarrow as pa

import pyspark.sql.connect.proto as pb2
import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib
import pyspark.sql.types
from pyspark import cloudpickle
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.readwriter import DataFrameReader
from pyspark.sql.connect.plan import SQL
from pyspark.sql.types import DataType, StructType, StructField, LongType, StringType

from typing import Optional, Any, Union

NumericType = typing.Union[int, float]

logging.basicConfig(level=logging.INFO)


class MetricValue:
    def __init__(self, name: str, value: NumericType, type: str):
        self._name = name
        self._type = type
        self._value = value

    def __repr__(self) -> str:
        return f"<{self._name}={self._value} ({self._type})>"

    @property
    def name(self) -> str:
        return self._name

    @property
    def value(self) -> NumericType:
        return self._value

    @property
    def metric_type(self) -> str:
        return self._type


class PlanMetrics:
    def __init__(self, name: str, id: int, parent: int, metrics: typing.List[MetricValue]):
        self._name = name
        self._id = id
        self._parent_id = parent
        self._metrics = metrics

    def __repr__(self) -> str:
        return f"Plan({self._name})={self._metrics}"

    @property
    def name(self) -> str:
        return self._name

    @property
    def plan_id(self) -> int:
        return self._id

    @property
    def parent_plan_id(self) -> int:
        return self._parent_id

    @property
    def metrics(self) -> typing.List[MetricValue]:
        return self._metrics


class AnalyzeResult:
    def __init__(self, schema: pb2.DataType, explain: str):
        self.schema = schema
        self.explain_string = explain

    @classmethod
    def fromProto(cls, pb: typing.Any) -> "AnalyzeResult":
        return AnalyzeResult(pb.schema, pb.explain_string)


class RemoteSparkSession(object):
    """Conceptually the remote spark session that communicates with the server"""

    def __init__(self, user_id: str, host: Optional[str] = None, port: int = 15002):
        self._host = "localhost" if host is None else host
        self._port = port
        self._user_id = user_id
        self._channel = grpc.insecure_channel(f"{self._host}:{self._port}")
        self._stub = grpc_lib.SparkConnectServiceStub(self._channel)

        # Create the reader
        self.read = DataFrameReader(self)

    def register_udf(
        self, function: Any, return_type: Union[str, pyspark.sql.types.DataType]
    ) -> str:
        """Create a temporary UDF in the session catalog on the other side. We generate a
        temporary name for it."""
        name = f"fun_{uuid.uuid4().hex}"
        fun = pb2.CreateScalarFunction()
        fun.parts.append(name)
        fun.serialized_function = cloudpickle.dumps((function, return_type))

        req = pb2.Request()
        req.user_context.user_id = self._user_id
        req.plan.command.create_function.CopyFrom(fun)

        self._execute_and_fetch(req)
        return name

    def _build_metrics(self, metrics: "pb2.Response.Metrics") -> typing.List[PlanMetrics]:
        return [
            PlanMetrics(
                x.name,
                x.plan_id,
                x.parent,
                [MetricValue(k, v.value, v.metric_type) for k, v in x.execution_metrics.items()],
            )
            for x in metrics.metrics
        ]

    def sql(self, sql_string: str) -> "DataFrame":
        return DataFrame.withPlan(SQL(sql_string), self)

    def _to_pandas(self, plan: pb2.Plan) -> Optional[pandas.DataFrame]:
        req = pb2.Request()
        req.user_context.user_id = self._user_id
        req.plan.CopyFrom(plan)
        return self._execute_and_fetch(req)

    def _proto_schema_to_pyspark_schema(self, schema: pb2.DataType) -> DataType:
        if schema.HasField("struct"):
            structFields = []
            for proto_field in schema.struct.fields:
                structFields.append(
                    StructField(
                        proto_field.name,
                        self._proto_schema_to_pyspark_schema(proto_field.type),
                        proto_field.nullable,
                    )
                )
            return StructType(structFields)
        elif schema.HasField("i64"):
            return LongType()
        elif schema.HasField("string"):
            return StringType()
        else:
            raise Exception("Only support long, string, struct conversion")

    def schema(self, plan: pb2.Plan) -> StructType:
        proto_schema = self._analyze(plan).schema
        # Server side should populate the struct field which is the schema.
        assert proto_schema.HasField("struct")
        structFields = []
        for proto_field in proto_schema.struct.fields:
            structFields.append(
                StructField(
                    proto_field.name,
                    self._proto_schema_to_pyspark_schema(proto_field.type),
                    proto_field.nullable,
                )
            )
        return StructType(structFields)

    def explain_string(self, plan: pb2.Plan) -> str:
        return self._analyze(plan).explain_string

    def _analyze(self, plan: pb2.Plan) -> AnalyzeResult:
        req = pb2.Request()
        req.user_context.user_id = self._user_id
        req.plan.CopyFrom(plan)

        resp = self._stub.AnalyzePlan(req)
        return AnalyzeResult.fromProto(resp)

    def _process_batch(self, b: pb2.Response) -> Optional[pandas.DataFrame]:
        if b.batch is not None and len(b.batch.data) > 0:
            with pa.ipc.open_stream(b.batch.data) as rd:
                return rd.read_pandas()
        elif b.json_batch is not None and len(b.json_batch.data) > 0:
            return pd.read_json(io.BytesIO(b.json_batch.data), lines=True)
        return None

    def _execute_and_fetch(self, req: pb2.Request) -> typing.Optional[pandas.DataFrame]:
        m: Optional[pb2.Response.Metrics] = None
        result_dfs = []

        for b in self._stub.ExecutePlan(req):
            if b.metrics is not None:
                m = b.metrics

            pb = self._process_batch(b)
            if pb is not None:
                result_dfs.append(pb)

        if len(result_dfs) > 0:
            df = pd.concat(result_dfs)
            # Attach the metrics to the DataFrame attributes.
            if m is not None:
                df.attrs["metrics"] = self._build_metrics(m)
            return df
        else:
            return None
