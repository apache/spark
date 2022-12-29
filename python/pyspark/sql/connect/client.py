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

import logging
import os
import urllib.parse
import uuid
from typing import Iterable, Optional, Any, Union, List, Tuple, Dict, NoReturn, cast

import google.protobuf.message
from grpc_status import rpc_status
import grpc
import pandas
from google.protobuf import text_format
import pyarrow as pa
from google.rpc import error_details_pb2

import pyspark.sql.connect.proto as pb2
import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib
import pyspark.sql.connect.types as types
import pyspark.sql.types
from pyspark import cloudpickle
from pyspark.sql.types import (
    DataType,
    StructType,
    StructField,
)


def _configure_logging() -> logging.Logger:
    """Configure logging for the Spark Connect clients."""
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(fmt="%(asctime)s %(process)d %(levelname)s %(funcName)s %(message)s")
    )
    logger.addHandler(handler)

    # Check the environment variables for log levels:
    if "SPARK_CONNECT_LOG_LEVEL" in os.environ:
        logger.setLevel(os.getenv("SPARK_CONNECT_LOG_LEVEL", "error").upper())
    else:
        logger.disabled = True
    return logger


# Instantiate the logger based on the environment configuration.
logger = _configure_logging()


class SparkConnectException(Exception):
    def __init__(self, message: str, reason: Optional[str] = None) -> None:
        super(SparkConnectException, self).__init__(message)
        self._reason = reason
        self._message = message

    def __str__(self) -> str:
        if self._reason is not None:
            return f"({self._reason}) {self._message}"
        else:
            return self._message


class SparkConnectAnalysisException(SparkConnectException):
    def __init__(self, reason: str, message: str, plan: str) -> None:
        self._reason = reason
        self._message = message
        self._plan = plan

    def __str__(self) -> str:
        return f"{self._message}\nPlan: {self._plan}"


class ChannelBuilder:
    """
    This is a helper class that is used to create a GRPC channel based on the given
    connection string per the documentation of Spark Connect.

    .. versionadded:: 3.4.0

    Examples
    --------
    >>> cb =  ChannelBuilder("sc://localhost")
    ... cb.endpoint
    "localhost:15002"

    >>> cb = ChannelBuilder("sc://localhost/;use_ssl=true;token=aaa")
    ... cb.secure
    True
    """

    PARAM_USE_SSL = "use_ssl"
    PARAM_TOKEN = "token"
    PARAM_USER_ID = "user_id"

    DEFAULT_PORT = 15002

    def __init__(self, url: str, channelOptions: Optional[List[Tuple[str, Any]]] = None) -> None:
        """
        Constructs a new channel builder. This is used to create the proper GRPC channel from
        the connection string.

        Parameters
        ----------
        url : str
            Spark Connect connection string
        channelOptions: list of tuple, optional
            Additional options that can be passed to the GRPC channel construction.
        """
        # Explicitly check the scheme of the URL.
        if url[:5] != "sc://":
            raise AttributeError("URL scheme must be set to `sc`.")
        # Rewrite the URL to use http as the scheme so that we can leverage
        # Python's built-in parser.
        tmp_url = "http" + url[2:]
        self.url = urllib.parse.urlparse(tmp_url)
        self.params: Dict[str, str] = {}
        if len(self.url.path) > 0 and self.url.path != "/":
            raise AttributeError(
                f"Path component for connection URI must be empty: {self.url.path}"
            )
        self._extract_attributes()
        self._channel_options = channelOptions

    def _extract_attributes(self) -> None:
        if len(self.url.params) > 0:
            parts = self.url.params.split(";")
            for p in parts:
                kv = p.split("=")
                if len(kv) != 2:
                    raise AttributeError(f"Parameter '{p}' is not a valid parameter key-value pair")
                self.params[kv[0]] = urllib.parse.unquote(kv[1])

        netloc = self.url.netloc.split(":")
        if len(netloc) == 1:
            self.host = netloc[0]
            self.port = ChannelBuilder.DEFAULT_PORT
        elif len(netloc) == 2:
            self.host = netloc[0]
            self.port = int(netloc[1])
        else:
            raise AttributeError(
                f"Target destination {self.url.netloc} does not match '<host>:<port>' pattern"
            )

    def metadata(self) -> Iterable[Tuple[str, str]]:
        """
        Builds the GRPC specific metadata list to be injected into the request. All
        parameters will be converted to metadata except ones that are explicitly used
        by the channel.

        Returns
        -------
        A list of tuples (key, value)
        """
        return [
            (k, self.params[k])
            for k in self.params
            if k
            not in [
                ChannelBuilder.PARAM_TOKEN,
                ChannelBuilder.PARAM_USE_SSL,
                ChannelBuilder.PARAM_USER_ID,
            ]
        ]

    @property
    def secure(self) -> bool:
        if self._token is not None:
            return True

        value = self.params.get(ChannelBuilder.PARAM_USE_SSL, "")
        return value.lower() == "true"

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def _token(self) -> Optional[str]:
        return self.params.get(ChannelBuilder.PARAM_TOKEN, None)

    @property
    def userId(self) -> Optional[str]:
        """
        Returns
        -------
        The user_id extracted from the parameters of the connection string or `None` if not
        specified.
        """
        return self.params.get(ChannelBuilder.PARAM_USER_ID, None)

    def get(self, key: str) -> Any:
        """
        Parameters
        ----------
        key : str
            Parameter key name.

        Returns
        -------
        The parameter value if present, raises exception otherwise.
        """
        return self.params[key]

    def toChannel(self) -> grpc.Channel:
        """
        Applies the parameters of the connection string and creates a new
        GRPC channel according to the configuration. Passes optional channel options to
        construct the channel.

        Returns
        -------
        GRPC Channel instance.
        """
        destination = f"{self.host}:{self.port}"

        # Setting a token implicitly sets the `use_ssl` to True.
        if not self.secure and self._token is not None:
            use_secure = True
        elif self.secure:
            use_secure = True
        else:
            use_secure = False

        if not use_secure:
            return grpc.insecure_channel(destination, options=self._channel_options)
        else:
            # Default SSL Credentials.
            opt_token = self.params.get(ChannelBuilder.PARAM_TOKEN, None)
            # When a token is present, pass the token to the channel.
            if opt_token is not None:
                ssl_creds = grpc.ssl_channel_credentials()
                composite_creds = grpc.composite_channel_credentials(
                    ssl_creds, grpc.access_token_call_credentials(opt_token)
                )
                return grpc.secure_channel(
                    destination, credentials=composite_creds, options=self._channel_options
                )
            else:
                return grpc.secure_channel(
                    destination,
                    credentials=grpc.ssl_channel_credentials(),
                    options=self._channel_options,
                )


class MetricValue:
    def __init__(self, name: str, value: Union[int, float], type: str):
        self._name = name
        self._type = type
        self._value = value

    def __repr__(self) -> str:
        return f"<{self._name}={self._value} ({self._type})>"

    @property
    def name(self) -> str:
        return self._name

    @property
    def value(self) -> Union[int, float]:
        return self._value

    @property
    def metric_type(self) -> str:
        return self._type


class PlanMetrics:
    def __init__(self, name: str, id: int, parent: int, metrics: List[MetricValue]):
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
    def metrics(self) -> List[MetricValue]:
        return self._metrics


class AnalyzeResult:
    def __init__(
        self,
        schema: pb2.DataType,
        explain: str,
        tree_string: str,
        is_local: bool,
        is_streaming: bool,
        input_files: List[str],
    ):
        self.schema = schema
        self.explain_string = explain
        self.tree_string = tree_string
        self.is_local = is_local
        self.is_streaming = is_streaming
        self.input_files = input_files

    @classmethod
    def fromProto(cls, pb: Any) -> "AnalyzeResult":
        return AnalyzeResult(
            pb.schema,
            pb.explain_string,
            pb.tree_string,
            pb.is_local,
            pb.is_streaming,
            pb.input_files,
        )


class SparkConnectClient(object):
    """Conceptually the remote spark session that communicates with the server"""

    def __init__(
        self,
        connectionString: str,
        userId: Optional[str] = None,
        channelOptions: Optional[List[Tuple[str, Any]]] = None,
    ):
        """
        Creates a new SparkSession for the Spark Connect interface.

        Parameters
        ----------
        connectionString: Optional[str]
            Connection string that is used to extract the connection parameters and configure
            the GRPC connection. Defaults to `sc://localhost`.
        userId : Optional[str]
            Optional unique user ID that is used to differentiate multiple users and
            isolate their Spark Sessions. If the `user_id` is not set, will default to
            the $USER environment. Defining the user ID as part of the connection string
            takes precedence.
        """
        # Parse the connection string.
        self._builder = ChannelBuilder(connectionString, channelOptions)
        self._user_id = None
        # Generate a unique session ID for this client. This UUID must be unique to allow
        # concurrent Spark sessions of the same user. If the channel is closed, creating
        # a new client will create a new session ID.
        self._session_id = str(uuid.uuid4())
        if self._builder.userId is not None:
            self._user_id = self._builder.userId
        elif userId is not None:
            self._user_id = userId
        else:
            self._user_id = os.getenv("USER", None)

        self._channel = self._builder.toChannel()
        self._stub = grpc_lib.SparkConnectServiceStub(self._channel)
        # Configure logging for the SparkConnect client.

    def register_udf(
        self, function: Any, return_type: Union[str, pyspark.sql.types.DataType]
    ) -> str:
        """Create a temporary UDF in the session catalog on the other side. We generate a
        temporary name for it."""
        name = f"fun_{uuid.uuid4().hex}"
        fun = pb2.CreateScalarFunction()
        fun.parts.append(name)
        logger.info(f"Registering UDF: {self._proto_to_string(fun)}")
        fun.serialized_function = cloudpickle.dumps((function, return_type))

        req = self._execute_plan_request_with_metadata()
        req.plan.command.create_function.CopyFrom(fun)

        self._execute(req)
        return name

    def _build_metrics(self, metrics: "pb2.ExecutePlanResponse.Metrics") -> List[PlanMetrics]:
        return [
            PlanMetrics(
                x.name,
                x.plan_id,
                x.parent,
                [MetricValue(k, v.value, v.metric_type) for k, v in x.execution_metrics.items()],
            )
            for x in metrics.metrics
        ]

    def to_pandas(self, plan: pb2.Plan) -> "pandas.DataFrame":
        logger.info(f"Executing plan {self._proto_to_string(plan)}")
        req = self._execute_plan_request_with_metadata()
        req.plan.CopyFrom(plan)
        return self._execute_and_fetch(req)

    def _proto_schema_to_pyspark_schema(self, schema: pb2.DataType) -> DataType:
        return types.proto_schema_to_pyspark_data_type(schema)

    def _proto_to_string(self, p: google.protobuf.message.Message) -> str:
        """
        Helper method to generate a one line string representation of the plan.
        Parameters
        ----------
        p : google.protobuf.message.Message
            Generic Message type

        Returns
        -------
        Single line string of the serialized proto message.
        """
        return text_format.MessageToString(p, as_one_line=True)

    def schema(self, plan: pb2.Plan) -> StructType:
        logger.info(f"Schema for plan: {self._proto_to_string(plan)}")
        proto_schema = self._analyze(plan).schema
        # Server side should populate the struct field which is the schema.
        assert proto_schema.HasField("struct")

        fields = [
            StructField(
                f.name,
                self._proto_schema_to_pyspark_schema(f.data_type),
                f.nullable,
            )
            for f in proto_schema.struct.fields
        ]
        return StructType(fields)

    def explain_string(self, plan: pb2.Plan, explain_mode: str = "extended") -> str:
        logger.info(f"Explain (mode={explain_mode}) for plan {self._proto_to_string(plan)}")
        result = self._analyze(plan, explain_mode)
        return result.explain_string

    def execute_command(self, command: pb2.Command) -> None:
        logger.info(f"Execute command for command {self._proto_to_string(command)}")
        req = self._execute_plan_request_with_metadata()
        if self._user_id:
            req.user_context.user_id = self._user_id
        req.plan.command.CopyFrom(command)
        self._execute(req)
        return

    def close(self) -> None:
        self._channel.close()

    def _execute_plan_request_with_metadata(self) -> pb2.ExecutePlanRequest:
        req = pb2.ExecutePlanRequest()
        req.client_id = self._session_id
        req.client_type = "_SPARK_CONNECT_PYTHON"
        if self._user_id:
            req.user_context.user_id = self._user_id
        return req

    def _analyze_plan_request_with_metadata(self) -> pb2.AnalyzePlanRequest:
        req = pb2.AnalyzePlanRequest()
        req.client_id = self._session_id
        req.client_type = "_SPARK_CONNECT_PYTHON"
        if self._user_id:
            req.user_context.user_id = self._user_id
        return req

    def _analyze(self, plan: pb2.Plan, explain_mode: str = "extended") -> AnalyzeResult:
        """
        Call the analyze RPC of Spark Connect.

        Parameters
        ----------
        plan : :class:`pyspark.sql.connect.proto.Plan`
           Proto representation of the plan.
        explain_mode : str
           Explain mode

        Returns
        -------
        The result of the analyze call.
        """
        req = self._analyze_plan_request_with_metadata()
        req.plan.CopyFrom(plan)
        if explain_mode not in ["simple", "extended", "codegen", "cost", "formatted"]:
            raise ValueError(
                f"""
                Unknown explain mode: {explain_mode}. Accepted "
                "explain modes are 'simple', 'extended', 'codegen', 'cost', 'formatted'."
                """
            )
        if explain_mode == "simple":
            req.explain.explain_mode = pb2.Explain.ExplainMode.SIMPLE
        elif explain_mode == "extended":
            req.explain.explain_mode = pb2.Explain.ExplainMode.EXTENDED
        elif explain_mode == "cost":
            req.explain.explain_mode = pb2.Explain.ExplainMode.COST
        elif explain_mode == "codegen":
            req.explain.explain_mode = pb2.Explain.ExplainMode.CODEGEN
        else:  # formatted
            req.explain.explain_mode = pb2.Explain.ExplainMode.FORMATTED

        try:
            resp = self._stub.AnalyzePlan(req, metadata=self._builder.metadata())
            if resp.client_id != self._session_id:
                raise SparkConnectException("Received incorrect session identifier for request.")
            return AnalyzeResult.fromProto(resp)
        except grpc.RpcError as rpc_error:
            self._handle_error(rpc_error)

    def _process_batch(self, arrow_batch: pb2.ExecutePlanResponse.ArrowBatch) -> "pandas.DataFrame":
        with pa.ipc.open_stream(arrow_batch.data) as rd:
            return rd.read_pandas()

    def _execute(self, req: pb2.ExecutePlanRequest) -> None:
        """
        Execute the passed request `req` and drop all results.

        Parameters
        ----------
        req : pb2.ExecutePlanRequest
            Proto representation of the plan.

        """
        logger.info("Execute")
        try:
            for b in self._stub.ExecutePlan(req, metadata=self._builder.metadata()):
                if b.client_id != self._session_id:
                    raise SparkConnectException(
                        "Received incorrect session identifier for request."
                    )
                continue
        except grpc.RpcError as rpc_error:
            self._handle_error(rpc_error)

    def _execute_and_fetch(self, req: pb2.ExecutePlanRequest) -> "pandas.DataFrame":
        logger.info("ExecuteAndFetch")
        import pandas as pd

        m: Optional[pb2.ExecutePlanResponse.Metrics] = None
        result_dfs = []

        try:
            for b in self._stub.ExecutePlan(req, metadata=self._builder.metadata()):
                if b.client_id != self._session_id:
                    raise SparkConnectException(
                        "Received incorrect session identifier for request."
                    )
                if b.metrics is not None:
                    logger.debug("Received metric batch.")
                    m = b.metrics
                if b.HasField("arrow_batch"):
                    logger.debug(
                        f"Received arrow batch rows={b.arrow_batch.row_count} "
                        f"size={len(b.arrow_batch.data)}"
                    )
                    pb = self._process_batch(b.arrow_batch)
                    result_dfs.append(pb)
        except grpc.RpcError as rpc_error:
            self._handle_error(rpc_error)

        assert len(result_dfs) > 0

        df = pd.concat(result_dfs)

        # pd.concat generates non-consecutive index like:
        #   Int64Index([0, 1, 0, 1, 2, 0, 1, 0, 1, 2], dtype='int64')
        # set it to RangeIndex to be consistent with pyspark
        n = len(df)
        df.set_index(pd.RangeIndex(start=0, stop=n, step=1), inplace=True)

        # Attach the metrics to the DataFrame attributes.
        if m is not None:
            df.attrs["metrics"] = self._build_metrics(m)
        return df

    def _handle_error(self, rpc_error: grpc.RpcError) -> NoReturn:
        """
        Error handling helper for dealing with GRPC Errors. On the server side, certain
        exceptions are enriched with additional RPC Status information. These are
        unpacked in this function and put into the exception.

        To avoid overloading the user with GRPC errors, this message explicitly
        swallows the error context from the call. This GRPC Error is logged however,
        and can be enabled.

        Parameters
        ----------
        rpc_error : grpc.RpcError
           RPC Error containing the details of the exception.

        Returns
        -------
        Throws the appropriate internal Python exception.
        """
        logger.exception("GRPC Error received")
        # We have to cast the value here because, a RpcError is a Call as well.
        # https://grpc.github.io/grpc/python/grpc.html#grpc.UnaryUnaryMultiCallable.__call__
        status = rpc_status.from_call(cast(grpc.Call, rpc_error))
        if status:
            for d in status.details:
                if d.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
                    info = error_details_pb2.ErrorInfo()
                    d.Unpack(info)
                    if info.reason == "org.apache.spark.sql.AnalysisException":
                        raise SparkConnectAnalysisException(
                            info.reason, info.metadata["message"], info.metadata["plan"]
                        ) from None
                    else:
                        raise SparkConnectException(status.message, info.reason) from None

            raise SparkConnectException(status.message) from None
        else:
            raise SparkConnectException(str(rpc_error)) from None


__all__ = [
    "ChannelBuilder",
    "SparkConnectClient",
    "SparkConnectException",
    "SparkConnectAnalysisException",
]
