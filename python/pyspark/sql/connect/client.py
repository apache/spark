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
__all__ = [
    "ChannelBuilder",
    "SparkConnectClient",
]

import string

from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

import logging
import os
import random
import time
import urllib.parse
import uuid
import sys
from types import TracebackType
from typing import (
    Iterable,
    Iterator,
    Optional,
    Any,
    Union,
    List,
    Tuple,
    Dict,
    NoReturn,
    cast,
    Callable,
    Generator,
    Type,
    TYPE_CHECKING,
)

import pandas as pd
import pyarrow as pa

import google.protobuf.message
from grpc_status import rpc_status
import grpc
from google.protobuf import text_format
from google.rpc import error_details_pb2

import pyspark.sql.connect.proto as pb2
import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib
import pyspark.sql.connect.types as types
from pyspark.errors.exceptions.connect import (
    convert_exception,
    SparkConnectException,
    SparkConnectGrpcException,
)
from pyspark.sql.connect.expressions import (
    PythonUDF,
    CommonInlineUserDefinedFunction,
    JavaUDF,
)
from pyspark.sql.pandas.types import _check_series_localize_timestamps, _convert_map_items_to_dict
from pyspark.sql.types import DataType, MapType, StructType, TimestampType
from pyspark.rdd import PythonEvalType
from pyspark.storagelevel import StorageLevel


if TYPE_CHECKING:
    from pyspark.sql.connect._typing import DataTypeOrString


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
    PARAM_USER_AGENT = "user_agent"
    MAX_MESSAGE_LENGTH = 128 * 1024 * 1024

    @staticmethod
    def default_port() -> int:
        if "SPARK_TESTING" in os.environ:
            from pyspark.sql.session import SparkSession as PySparkSession

            # In the case when Spark Connect uses the local mode, it starts the regular Spark
            # session that starts Spark Connect server that sets `SparkSession._instantiatedSession`
            # via SparkSession.__init__.
            #
            # We are getting the actual server port from the Spark session via Py4J to address
            # the case when the server port is set to 0 (in which allocates an ephemeral port).
            #
            # This is only used in the test/development mode.
            session = PySparkSession._instantiatedSession

            # 'spark.local.connect' is set when we use the local mode in Spark Connect.
            if session is not None and session.conf.get("spark.local.connect", "0") == "1":

                jvm = PySparkSession._instantiatedSession._jvm  # type: ignore[union-attr]
                return getattr(
                    getattr(
                        jvm.org.apache.spark.sql.connect.service,  # type: ignore[union-attr]
                        "SparkConnectService$",
                    ),
                    "MODULE$",
                ).localPort()
        return 15002

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

        GRPC_DEFAULT_OPTIONS = [
            ("grpc.max_send_message_length", ChannelBuilder.MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", ChannelBuilder.MAX_MESSAGE_LENGTH),
        ]

        if channelOptions is None:
            self._channel_options = GRPC_DEFAULT_OPTIONS
        else:
            self._channel_options = GRPC_DEFAULT_OPTIONS + channelOptions

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
            self.port = ChannelBuilder.default_port()
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
                ChannelBuilder.PARAM_USER_AGENT,
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

    @property
    def userAgent(self) -> str:
        """
        Returns
        -------
        user_agent : str
            The user_agent parameter specified in the connection string,
            or "_SPARK_CONNECT_PYTHON" when not specified.
        """
        user_agent = self.params.get(ChannelBuilder.PARAM_USER_AGENT, "_SPARK_CONNECT_PYTHON")
        allowed_chars = string.ascii_letters + string.punctuation
        if len(user_agent) > 200:
            raise SparkConnectException(
                "'user_agent' parameter cannot exceed 200 characters in length"
            )
        if set(user_agent).difference(allowed_chars):
            raise SparkConnectException(
                "Only alphanumeric and common punctuations are allowed for 'user_agent'"
            )
        return user_agent

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


class PlanObservedMetrics:
    def __init__(self, name: str, metrics: List[pb2.Expression.Literal]):
        self._name = name
        self._metrics = metrics

    def __repr__(self) -> str:
        return f"Plan observed({self._name}={self._metrics})"

    @property
    def name(self) -> str:
        return self._name

    @property
    def metrics(self) -> List[pb2.Expression.Literal]:
        return self._metrics


class AnalyzeResult:
    def __init__(
        self,
        schema: Optional[DataType],
        explain_string: Optional[str],
        tree_string: Optional[str],
        is_local: Optional[bool],
        is_streaming: Optional[bool],
        input_files: Optional[List[str]],
        spark_version: Optional[str],
        parsed: Optional[DataType],
        is_same_semantics: Optional[bool],
        semantic_hash: Optional[int],
        storage_level: Optional[StorageLevel],
    ):
        self.schema = schema
        self.explain_string = explain_string
        self.tree_string = tree_string
        self.is_local = is_local
        self.is_streaming = is_streaming
        self.input_files = input_files
        self.spark_version = spark_version
        self.parsed = parsed
        self.is_same_semantics = is_same_semantics
        self.semantic_hash = semantic_hash
        self.storage_level = storage_level

    @classmethod
    def fromProto(cls, pb: Any) -> "AnalyzeResult":
        schema: Optional[DataType] = None
        explain_string: Optional[str] = None
        tree_string: Optional[str] = None
        is_local: Optional[bool] = None
        is_streaming: Optional[bool] = None
        input_files: Optional[List[str]] = None
        spark_version: Optional[str] = None
        parsed: Optional[DataType] = None
        is_same_semantics: Optional[bool] = None
        semantic_hash: Optional[int] = None
        storage_level: Optional[StorageLevel] = None

        if pb.HasField("schema"):
            schema = types.proto_schema_to_pyspark_data_type(pb.schema.schema)
        elif pb.HasField("explain"):
            explain_string = pb.explain.explain_string
        elif pb.HasField("tree_string"):
            tree_string = pb.tree_string.tree_string
        elif pb.HasField("is_local"):
            is_local = pb.is_local.is_local
        elif pb.HasField("is_streaming"):
            is_streaming = pb.is_streaming.is_streaming
        elif pb.HasField("input_files"):
            input_files = pb.input_files.files
        elif pb.HasField("spark_version"):
            spark_version = pb.spark_version.version
        elif pb.HasField("ddl_parse"):
            parsed = types.proto_schema_to_pyspark_data_type(pb.ddl_parse.parsed)
        elif pb.HasField("same_semantics"):
            is_same_semantics = pb.same_semantics.result
        elif pb.HasField("semantic_hash"):
            semantic_hash = pb.semantic_hash.result
        elif pb.HasField("persist"):
            pass
        elif pb.HasField("unpersist"):
            pass
        elif pb.HasField("get_storage_level"):
            storage_level = StorageLevel(
                useDisk=pb.get_storage_level.storage_level.use_disk,
                useMemory=pb.get_storage_level.storage_level.use_memory,
                useOffHeap=pb.get_storage_level.storage_level.use_off_heap,
                deserialized=pb.get_storage_level.storage_level.deserialized,
                replication=pb.get_storage_level.storage_level.replication,
            )
        else:
            raise SparkConnectException("No analyze result found!")

        return AnalyzeResult(
            schema,
            explain_string,
            tree_string,
            is_local,
            is_streaming,
            input_files,
            spark_version,
            parsed,
            is_same_semantics,
            semantic_hash,
            storage_level,
        )


class ConfigResult:
    def __init__(self, pairs: List[Tuple[str, Optional[str]]], warnings: List[str]):
        self.pairs = pairs
        self.warnings = warnings

    @classmethod
    def fromProto(cls, pb: pb2.ConfigResponse) -> "ConfigResult":
        return ConfigResult(
            pairs=[(pair.key, pair.value if pair.HasField("value") else None) for pair in pb.pairs],
            warnings=list(pb.warnings),
        )


class SparkConnectClient(object):
    """
    Conceptually the remote spark session that communicates with the server

    .. versionadded:: 3.4.0
    """

    @classmethod
    def retry_exception(cls, e: Exception) -> bool:
        if isinstance(e, grpc.RpcError):
            return e.code() == grpc.StatusCode.UNAVAILABLE
        else:
            return False

    def __init__(
        self,
        connectionString: str,
        userId: Optional[str] = None,
        channelOptions: Optional[List[Tuple[str, Any]]] = None,
        retryPolicy: Optional[Dict[str, Any]] = None,
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
        self._retry_policy = {
            "max_retries": 15,
            "backoff_multiplier": 4,
            "initial_backoff": 50,
            "max_backoff": 60000,
        }
        if retryPolicy:
            self._retry_policy.update(retryPolicy)

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
        self,
        function: Any,
        return_type: "DataTypeOrString",
        name: Optional[str] = None,
        eval_type: int = PythonEvalType.SQL_BATCHED_UDF,
        deterministic: bool = True,
    ) -> str:
        """
        Create a temporary UDF in the session catalog on the other side. We generate a
        temporary name for it.
        """

        if name is None:
            name = f"fun_{uuid.uuid4().hex}"

        # construct a PythonUDF
        py_udf = PythonUDF(
            output_type=return_type,
            eval_type=eval_type,
            func=function,
            python_ver="%d.%d" % sys.version_info[:2],
        )

        # construct a CommonInlineUserDefinedFunction
        fun = CommonInlineUserDefinedFunction(
            function_name=name,
            arguments=[],
            function=py_udf,
            deterministic=deterministic,
        ).to_plan_udf(self)

        # construct the request
        req = self._execute_plan_request_with_metadata()
        req.plan.command.register_function.CopyFrom(fun)

        self._execute(req)
        return name

    def register_java(
        self,
        name: str,
        javaClassName: str,
        return_type: Optional["DataTypeOrString"] = None,
        aggregate: bool = False,
    ) -> None:
        # construct a JavaUDF
        if return_type is None:
            java_udf = JavaUDF(class_name=javaClassName, aggregate=aggregate)
        else:
            java_udf = JavaUDF(class_name=javaClassName, output_type=return_type)
        fun = CommonInlineUserDefinedFunction(
            function_name=name,
            function=java_udf,
        ).to_plan_judf(self)
        # construct the request
        req = self._execute_plan_request_with_metadata()
        req.plan.command.register_function.CopyFrom(fun)

        self._execute(req)

    def _build_metrics(self, metrics: "pb2.ExecutePlanResponse.Metrics") -> Iterator[PlanMetrics]:
        return (
            PlanMetrics(
                x.name,
                x.plan_id,
                x.parent,
                [MetricValue(k, v.value, v.metric_type) for k, v in x.execution_metrics.items()],
            )
            for x in metrics.metrics
        )

    def _build_observed_metrics(
        self, metrics: List["pb2.ExecutePlanResponse.ObservedMetrics"]
    ) -> Iterator[PlanObservedMetrics]:
        return (PlanObservedMetrics(x.name, [v for v in x.values]) for x in metrics)

    def to_table_as_iterator(self, plan: pb2.Plan) -> Iterator[Union[StructType, "pa.Table"]]:
        """
        Return given plan as a PyArrow Table iterator.
        """
        logger.info(f"Executing plan {self._proto_to_string(plan)}")
        req = self._execute_plan_request_with_metadata()
        req.plan.CopyFrom(plan)
        for response in self._execute_and_fetch_as_iterator(req):
            if isinstance(response, StructType):
                yield response
            elif isinstance(response, pa.RecordBatch):
                yield pa.Table.from_batches([response])

    def to_table(self, plan: pb2.Plan) -> Tuple["pa.Table", Optional[StructType]]:
        """
        Return given plan as a PyArrow Table.
        """
        logger.info(f"Executing plan {self._proto_to_string(plan)}")
        req = self._execute_plan_request_with_metadata()
        req.plan.CopyFrom(plan)
        table, schema, _, _, _ = self._execute_and_fetch(req)
        assert table is not None
        return table, schema

    def to_pandas(self, plan: pb2.Plan) -> "pd.DataFrame":
        """
        Return given plan as a pandas DataFrame.
        """
        logger.info(f"Executing plan {self._proto_to_string(plan)}")
        req = self._execute_plan_request_with_metadata()
        req.plan.CopyFrom(plan)
        table, schema, metrics, observed_metrics, _ = self._execute_and_fetch(req)
        assert table is not None
        pdf = table.rename_columns([f"col_{i}" for i in range(len(table.column_names))]).to_pandas()
        pdf.columns = table.column_names

        schema = schema or types.from_arrow_schema(table.schema)
        assert schema is not None and isinstance(schema, StructType)

        for field, pa_field in zip(schema, table.schema):
            if isinstance(field.dataType, TimestampType):
                assert pa_field.type.tz is not None
                pdf[field.name] = _check_series_localize_timestamps(
                    pdf[field.name], pa_field.type.tz
                )
            elif isinstance(field.dataType, MapType):
                pdf[field.name] = _convert_map_items_to_dict(pdf[field.name])

        if len(metrics) > 0:
            pdf.attrs["metrics"] = metrics
        if len(observed_metrics) > 0:
            pdf.attrs["observed_metrics"] = observed_metrics
        return pdf

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
        """
        Return schema for given plan.
        """
        logger.info(f"Schema for plan: {self._proto_to_string(plan)}")
        schema = self._analyze(method="schema", plan=plan).schema
        assert schema is not None
        # Server side should populate the struct field which is the schema.
        assert isinstance(schema, StructType)
        return schema

    def explain_string(self, plan: pb2.Plan, explain_mode: str = "extended") -> str:
        """
        Return explain string for given plan.
        """
        logger.info(f"Explain (mode={explain_mode}) for plan {self._proto_to_string(plan)}")
        result = self._analyze(
            method="explain", plan=plan, explain_mode=explain_mode
        ).explain_string
        assert result is not None
        return result

    def execute_command(
        self, command: pb2.Command
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        """
        Execute given command.
        """
        logger.info(f"Execute command for command {self._proto_to_string(command)}")
        req = self._execute_plan_request_with_metadata()
        if self._user_id:
            req.user_context.user_id = self._user_id
        req.plan.command.CopyFrom(command)
        data, _, _, _, properties = self._execute_and_fetch(req)
        if data is not None:
            return (data.to_pandas(), properties)
        else:
            return (None, properties)

    def same_semantics(self, plan: pb2.Plan, other: pb2.Plan) -> bool:
        """
        return if two plans have the same semantics.
        """
        result = self._analyze(method="same_semantics", plan=plan, other=other).is_same_semantics
        assert result is not None
        return result

    def semantic_hash(self, plan: pb2.Plan) -> int:
        """
        returns a `hashCode` of the logical query plan.
        """
        result = self._analyze(method="semantic_hash", plan=plan).semantic_hash
        assert result is not None
        return result

    def close(self) -> None:
        """
        Close the channel.
        """
        self._channel.close()

    def _execute_plan_request_with_metadata(self) -> pb2.ExecutePlanRequest:
        req = pb2.ExecutePlanRequest()
        req.session_id = self._session_id
        req.client_type = self._builder.userAgent
        if self._user_id:
            req.user_context.user_id = self._user_id
        return req

    def _analyze_plan_request_with_metadata(self) -> pb2.AnalyzePlanRequest:
        req = pb2.AnalyzePlanRequest()
        req.session_id = self._session_id
        req.client_type = self._builder.userAgent
        if self._user_id:
            req.user_context.user_id = self._user_id
        return req

    def _analyze(self, method: str, **kwargs: Any) -> AnalyzeResult:
        """
        Call the analyze RPC of Spark Connect.

        Returns
        -------
        The result of the analyze call.
        """
        req = self._analyze_plan_request_with_metadata()
        if method == "schema":
            req.schema.plan.CopyFrom(cast(pb2.Plan, kwargs.get("plan")))
        elif method == "explain":
            req.explain.plan.CopyFrom(cast(pb2.Plan, kwargs.get("plan")))
            explain_mode = kwargs.get("explain_mode")
            if explain_mode not in ["simple", "extended", "codegen", "cost", "formatted"]:
                raise ValueError(
                    f"""
                    Unknown explain mode: {explain_mode}. Accepted "
                    "explain modes are 'simple', 'extended', 'codegen', 'cost', 'formatted'."
                    """
                )
            if explain_mode == "simple":
                req.explain.explain_mode = (
                    pb2.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE
                )
            elif explain_mode == "extended":
                req.explain.explain_mode = (
                    pb2.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED
                )
            elif explain_mode == "cost":
                req.explain.explain_mode = (
                    pb2.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_COST
                )
            elif explain_mode == "codegen":
                req.explain.explain_mode = (
                    pb2.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_CODEGEN
                )
            else:  # formatted
                req.explain.explain_mode = (
                    pb2.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_FORMATTED
                )
        elif method == "tree_string":
            req.tree_string.plan.CopyFrom(cast(pb2.Plan, kwargs.get("plan")))
        elif method == "is_local":
            req.is_local.plan.CopyFrom(cast(pb2.Plan, kwargs.get("plan")))
        elif method == "is_streaming":
            req.is_streaming.plan.CopyFrom(cast(pb2.Plan, kwargs.get("plan")))
        elif method == "input_files":
            req.input_files.plan.CopyFrom(cast(pb2.Plan, kwargs.get("plan")))
        elif method == "spark_version":
            req.spark_version.SetInParent()
        elif method == "ddl_parse":
            req.ddl_parse.ddl_string = cast(str, kwargs.get("ddl_string"))
        elif method == "same_semantics":
            req.same_semantics.target_plan.CopyFrom(cast(pb2.Plan, kwargs.get("plan")))
            req.same_semantics.other_plan.CopyFrom(cast(pb2.Plan, kwargs.get("other")))
        elif method == "semantic_hash":
            req.semantic_hash.plan.CopyFrom(cast(pb2.Plan, kwargs.get("plan")))
        elif method == "persist":
            req.persist.relation.CopyFrom(cast(pb2.Relation, kwargs.get("relation")))
            if kwargs.get("storage_level", None) is not None:
                storage_level = cast(StorageLevel, kwargs.get("storage_level"))
                req.persist.storage_level.CopyFrom(
                    pb2.StorageLevel(
                        use_disk=storage_level.useDisk,
                        use_memory=storage_level.useMemory,
                        use_off_heap=storage_level.useOffHeap,
                        deserialized=storage_level.deserialized,
                        replication=storage_level.replication,
                    )
                )
        elif method == "unpersist":
            req.unpersist.relation.CopyFrom(cast(pb2.Relation, kwargs.get("relation")))
            if kwargs.get("blocking", None) is not None:
                req.unpersist.blocking = cast(bool, kwargs.get("blocking"))
        elif method == "get_storage_level":
            req.get_storage_level.relation.CopyFrom(cast(pb2.Relation, kwargs.get("relation")))
        else:
            raise ValueError(f"Unknown Analyze method: {method}")

        try:
            for attempt in Retrying(
                can_retry=SparkConnectClient.retry_exception, **self._retry_policy
            ):
                with attempt:
                    resp = self._stub.AnalyzePlan(req, metadata=self._builder.metadata())
                    if resp.session_id != self._session_id:
                        raise SparkConnectException(
                            "Received incorrect session identifier for request:"
                            f"{resp.session_id} != {self._session_id}"
                        )
                    return AnalyzeResult.fromProto(resp)
            raise SparkConnectException("Invalid state during retry exception handling.")
        except Exception as error:
            self._handle_error(error)

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
            for attempt in Retrying(
                can_retry=SparkConnectClient.retry_exception, **self._retry_policy
            ):
                with attempt:
                    for b in self._stub.ExecutePlan(req, metadata=self._builder.metadata()):
                        if b.session_id != self._session_id:
                            raise SparkConnectException(
                                "Received incorrect session identifier for request: "
                                f"{b.session_id} != {self._session_id}"
                            )
        except Exception as error:
            self._handle_error(error)

    def _execute_and_fetch_as_iterator(
        self, req: pb2.ExecutePlanRequest
    ) -> Iterator[
        Union[
            "pa.RecordBatch",
            StructType,
            PlanMetrics,
            PlanObservedMetrics,
            Dict[str, Any],
        ]
    ]:
        logger.info("ExecuteAndFetchAsIterator")

        try:
            for attempt in Retrying(
                can_retry=SparkConnectClient.retry_exception, **self._retry_policy
            ):
                with attempt:
                    for b in self._stub.ExecutePlan(req, metadata=self._builder.metadata()):
                        if b.session_id != self._session_id:
                            raise SparkConnectException(
                                "Received incorrect session identifier for request: "
                                f"{b.session_id} != {self._session_id}"
                            )
                        if b.HasField("metrics"):
                            logger.debug("Received metric batch.")
                            yield from self._build_metrics(b.metrics)
                        if b.observed_metrics:
                            logger.debug("Received observed metric batch.")
                            yield from self._build_observed_metrics(b.observed_metrics)
                        if b.HasField("schema"):
                            logger.debug("Received the schema.")
                            dt = types.proto_schema_to_pyspark_data_type(b.schema)
                            assert isinstance(dt, StructType)
                            yield dt
                        if b.HasField("sql_command_result"):
                            logger.debug("Received the SQL command result.")
                            yield {"sql_command_result": b.sql_command_result.relation}
                        if b.HasField("arrow_batch"):
                            logger.debug(
                                f"Received arrow batch rows={b.arrow_batch.row_count} "
                                f"size={len(b.arrow_batch.data)}"
                            )

                            with pa.ipc.open_stream(b.arrow_batch.data) as reader:
                                for batch in reader:
                                    assert isinstance(batch, pa.RecordBatch)
                                    yield batch
        except Exception as error:
            self._handle_error(error)

    def _execute_and_fetch(
        self, req: pb2.ExecutePlanRequest
    ) -> Tuple[
        Optional["pa.Table"],
        Optional[StructType],
        List[PlanMetrics],
        List[PlanObservedMetrics],
        Dict[str, Any],
    ]:
        logger.info("ExecuteAndFetch")

        observed_metrics: List[PlanObservedMetrics] = []
        metrics: List[PlanMetrics] = []
        batches: List[pa.RecordBatch] = []
        schema: Optional[StructType] = None
        properties: Dict[str, Any] = {}

        for response in self._execute_and_fetch_as_iterator(req):
            if isinstance(response, StructType):
                schema = response
            elif isinstance(response, pa.RecordBatch):
                batches.append(response)
            elif isinstance(response, PlanMetrics):
                metrics.append(response)
            elif isinstance(response, PlanObservedMetrics):
                observed_metrics.append(response)
            elif isinstance(response, dict):
                properties.update(**response)
            else:
                raise ValueError(f"Unknown response: {response}")

        if len(batches) > 0:
            table = pa.Table.from_batches(batches=batches)
            return table, schema, metrics, observed_metrics, properties
        else:
            return None, schema, metrics, observed_metrics, properties

    def _config_request_with_metadata(self) -> pb2.ConfigRequest:
        req = pb2.ConfigRequest()
        req.session_id = self._session_id
        req.client_type = self._builder.userAgent
        if self._user_id:
            req.user_context.user_id = self._user_id
        return req

    def config(self, operation: pb2.ConfigRequest.Operation) -> ConfigResult:
        """
        Call the config RPC of Spark Connect.

        Parameters
        ----------
        operation : str
           Operation kind

        Returns
        -------
        The result of the config call.
        """
        req = self._config_request_with_metadata()
        req.operation.CopyFrom(operation)
        try:
            for attempt in Retrying(
                can_retry=SparkConnectClient.retry_exception, **self._retry_policy
            ):
                with attempt:
                    resp = self._stub.Config(req, metadata=self._builder.metadata())
                    if resp.session_id != self._session_id:
                        raise SparkConnectException(
                            "Received incorrect session identifier for request:"
                            f"{resp.session_id} != {self._session_id}"
                        )
                    return ConfigResult.fromProto(resp)
            raise SparkConnectException("Invalid state during retry exception handling.")
        except Exception as error:
            self._handle_error(error)

    def _handle_error(self, error: Exception) -> NoReturn:
        """
        Handle errors that occur during RPC calls.

        Parameters
        ----------
        error : Exception
            An exception thrown during RPC calls.

        Returns
        -------
        Throws the appropriate internal Python exception.
        """
        if isinstance(error, grpc.RpcError):
            self._handle_rpc_error(error)
        elif isinstance(error, ValueError):
            if "Cannot invoke RPC" in str(error) and "closed" in str(error):
                raise SparkConnectException(
                    error_class="NO_ACTIVE_SESSION", message_parameters=dict()
                ) from None
        raise error

    def _handle_rpc_error(self, rpc_error: grpc.RpcError) -> NoReturn:
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
                    raise convert_exception(info, status.message) from None

            raise SparkConnectGrpcException(status.message) from None
        else:
            raise SparkConnectGrpcException(str(rpc_error)) from None


class RetryState:
    """
    Simple state helper that captures the state between retries of the exceptions. It
    keeps track of the last exception thrown and how many in total. When the task
    finishes successfully done() returns True.
    """

    def __init__(self) -> None:
        self._exception: Optional[BaseException] = None
        self._done = False
        self._count = 0

    def set_exception(self, exc: Optional[BaseException]) -> None:
        self._exception = exc
        self._count += 1

    def exception(self) -> Optional[BaseException]:
        return self._exception

    def set_done(self) -> None:
        self._done = True

    def count(self) -> int:
        return self._count

    def done(self) -> bool:
        return self._done


class AttemptManager:
    """
    Simple ContextManager that is used to capture the exception thrown inside the context.
    """

    def __init__(self, check: Callable[..., bool], retry_state: RetryState) -> None:
        self._retry_state = retry_state
        self._can_retry = check

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if isinstance(exc_val, BaseException):
            # Swallow the exception.
            if self._can_retry(exc_val):
                self._retry_state.set_exception(exc_val)
                return True
            # Bubble up the exception.
            return False
        else:
            self._retry_state.set_done()
            return None


class Retrying:
    """
    This helper class is used as a generator together with a context manager to
    allow retrying exceptions in particular code blocks. The Retrying can be configured
    with a lambda function that is can be filtered what kind of exceptions should be
    retried.

    In addition, there are several parameters that are used to configure the exponential
    backoff behavior.

    An example to use this class looks like this:

    .. code-block:: python

        for attempt in Retrying(can_retry=lambda x: isinstance(x, TransientError)):
            with attempt:
                # do the work.

    """

    def __init__(
        self,
        max_retries: int,
        initial_backoff: int,
        max_backoff: int,
        backoff_multiplier: float,
        can_retry: Callable[..., bool] = lambda x: True,
    ) -> None:
        self._can_retry = can_retry
        self._max_retries = max_retries
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._backoff_multiplier = backoff_multiplier

    def __iter__(self) -> Generator[AttemptManager, None, None]:
        """
        Generator function to wrap the exception producing code block.

        Returns
        -------
        A generator that yields the current attempt.
        """
        retry_state = RetryState()
        while True:
            # Check if the operation was completed successfully.
            if retry_state.done():
                break

            # If the number of retries have exceeded the maximum allowed retries.
            if retry_state.count() > self._max_retries:
                e = retry_state.exception()
                if e is not None:
                    raise e
                else:
                    raise ValueError("Retries exceeded but no exception caught.")

            # Do backoff
            if retry_state.count() > 0:
                backoff = random.randrange(
                    0,
                    int(
                        min(
                            self._initial_backoff * self._backoff_multiplier ** retry_state.count(),
                            self._max_backoff,
                        )
                    ),
                )
                logger.debug(f"Retrying call after {backoff} ms sleep")
                # Pythons sleep takes seconds as arguments.
                time.sleep(backoff / 1000.0)

            yield AttemptManager(self._can_retry, retry_state)
