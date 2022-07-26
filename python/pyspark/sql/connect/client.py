import pyspark.sql.connect.proto as pb2

import grpc
import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib

try:
    import pyarrow as pa
except:
    pass

import pandas as pd
import io


import pyspark.sql.types
from pyspark.sql.connect.data_frame import DataFrame
from pyspark.sql.connect.plan import Read, Sql


from pyspark.cloudpickle import cloudpickle
import uuid


class Data:
    def __init__(self, schema, data):
        self.schema = schema
        self.data = data

    def append(self, data):
        self.data = self.data + data


class MetricValue:
    def __init__(self, name, value, type):
        self._name = name
        self._type = type
        self._value = value

    def __repr__(self):
        return f"<{self._name}={self._value} ({self._type})>"

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value

    @property
    def metric_type(self):
        return self._type


class PlanMetrics:
    def __init__(self, name, id, parent, metrics):
        self._name = name
        self._id = id
        self._parent_id = parent
        self._metrics = metrics

    def __repr__(self):
        return f"Plan({self._name})={self._metrics}"

    @property
    def name(self):
        return self._name

    @property
    def plan_id(self):
        return self._id

    @property
    def parent_plan_id(self):
        return self._parent_id

    @property
    def metrics(self):
        return self._metrics


class RemoteSparkSession(object):
    """Conceptually the remote spark session that communicates with the server"""

    def __init__(
        self, host=None, port=15001, user_id="Martin"
    ):
        self._host = "localhost" if host is None else host
        self._port = port
        self._user_id = user_id
        self._channel = grpc.insecure_channel(f"{self._host}:{self._port}")
        self._stub = grpc_lib.SparkConnectServiceStub(self._channel)

    def readTable(self, tableName: str) -> "DataFrame":
        df = DataFrame.withPlan(Read(tableName), self)
        return df

    def register_udf(self, function, return_type):
        """Create a temporary UDF in the session catalog on the other side. We generate a
        temporary name for it."""
        name = f"fun_{uuid.uuid4().hex}"
        fun = pb2.CreateScalarFunction()
        fun.parts.append(name)
        fun.serialized_function = cloudpickle.dumps((function, return_type))

        req = pb2.Request()
        req.user_context.user_id = self._user_id
        req.plan.command.create_function.CopyFrom(fun)

        result = self._execute_and_fetch(req)
        return name

    def _build_metrics(self, metrics: pb2.Response.Metrics):
        return [
            PlanMetrics(
                x.name,
                x.plan_id,
                x.parent,
                [
                    MetricValue(k, v.value, v.metric_type)
                    for k, v in x.execution_metrics.items()
                ],
            )
            for x in metrics.metrics
        ]

    def sql(self, sql_string) -> "DataFrame":
        return DataFrame.withPlan(Sql(sql_string), self)

    def collect(self, plan: pb2.Plan):
        req = pb2.Request()
        req.user_context.user_id = self._user_id
        req.plan.CopyFrom(plan)
        return self._execute_and_fetch(req)

    def _process_batch(self, b):
        if b.batch is not None and len(b.batch.data) > 0:
            with pa.ipc.open_stream(b.data) as rd:
                return rd.read_pandas()
        elif b.csv_batch is not None and len(b.csv_batch.data) > 0:
            return pd.read_csv(io.StringIO(b.csv_batch.data), delimiter="|")

    def _execute_and_fetch(self, req: pb2.Request):
        m = None
        result_dfs = []

        for b in self._stub.ExecutePlan(req):
            if b.metrics is not None:
                m = b.metrics
            result_dfs.append(self._process_batch(b))

        if len(result_dfs) > 0:
            df = pd.concat(result_dfs)
            # Attach the metrics to the DataFrame attributes.
            df.attrs["metrics"] = self._build_metrics(m)
            return df
        else:
            return None
