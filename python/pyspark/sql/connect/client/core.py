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
    "DefaultChannelBuilder",
    "SparkConnectClient",
]

from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

import logging
import threading
import os
import copy
import platform
import urllib.parse
import uuid
import sys
from typing import (
    Iterable,
    Iterator,
    Optional,
    Any,
    Union,
    List,
    Tuple,
    Dict,
    Set,
    NoReturn,
    cast,
    TYPE_CHECKING,
    Type,
    Sequence,
)

import pandas as pd
import pyarrow as pa

import google.protobuf.message
from grpc_status import rpc_status
import grpc
from google.protobuf import text_format, any_pb2
from google.rpc import error_details_pb2

from pyspark.util import is_remote_only
from pyspark.accumulators import SpecialAccumulatorIds
from pyspark.loose_version import LooseVersion
from pyspark.version import __version__
from pyspark.resource.information import ResourceInformation
from pyspark.sql.metrics import MetricValue, PlanMetrics, ExecutionInfo, ObservedMetrics
from pyspark.sql.connect.client.artifact import ArtifactManager
from pyspark.sql.connect.client.logging import logger
from pyspark.sql.connect.profiler import ConnectProfilerCollector
from pyspark.sql.connect.client.reattach import ExecutePlanResponseReattachableIterator
from pyspark.sql.connect.client.retries import RetryPolicy, Retrying, DefaultPolicy
from pyspark.sql.connect.conversion import (
    storage_level_to_proto,
    proto_to_storage_level,
    proto_to_remote_cached_dataframe,
)
import pyspark.sql.connect.proto as pb2
import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib
import pyspark.sql.connect.types as types
from pyspark.errors.exceptions.connect import (
    convert_exception,
    SparkConnectException,
    SparkConnectGrpcException,
)
from pyspark.sql.connect.expressions import (
    LiteralExpression,
    PythonUDF,
    CommonInlineUserDefinedFunction,
    JavaUDF,
)
from pyspark.sql.connect.plan import (
    CommonInlineUserDefinedTableFunction,
    CommonInlineUserDefinedDataSource,
    PythonUDTF,
    PythonDataSource,
)
from pyspark.sql.connect.observation import Observation
from pyspark.sql.connect.utils import get_python_ver
from pyspark.sql.pandas.types import _create_converter_to_pandas, from_arrow_schema
from pyspark.sql.types import DataType, StructType, TimestampType, _has_type
from pyspark.util import PythonEvalType
from pyspark.storagelevel import StorageLevel
from pyspark.errors import PySparkValueError, PySparkAssertionError, PySparkNotImplementedError
from pyspark.sql.connect.shell.progress import Progress, ProgressHandler, from_proto

if TYPE_CHECKING:
    from google.rpc.error_details_pb2 import ErrorInfo
    from pyspark.sql.connect._typing import DataTypeOrString
    from pyspark.sql.datasource import DataSource


class ChannelBuilder:
    """
    This is a helper class that is used to create a GRPC channel based on the given
    connection string per the documentation of Spark Connect.

    The standard implementation is in :class:`DefaultChannelBuilder`.
    """

    PARAM_USE_SSL = "use_ssl"
    PARAM_TOKEN = "token"
    PARAM_USER_ID = "user_id"
    PARAM_USER_AGENT = "user_agent"
    PARAM_SESSION_ID = "session_id"

    GRPC_MAX_MESSAGE_LENGTH_DEFAULT = 128 * 1024 * 1024

    GRPC_DEFAULT_OPTIONS = [
        ("grpc.max_send_message_length", GRPC_MAX_MESSAGE_LENGTH_DEFAULT),
        ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_LENGTH_DEFAULT),
    ]

    def __init__(
        self,
        channelOptions: Optional[List[Tuple[str, Any]]] = None,
        params: Optional[Dict[str, str]] = None,
    ):
        self._interceptors: List[grpc.UnaryStreamClientInterceptor] = []
        self._params: Dict[str, str] = params or dict()
        self._channel_options: List[Tuple[str, Any]] = ChannelBuilder.GRPC_DEFAULT_OPTIONS.copy()

        if channelOptions is not None:
            for key, value in channelOptions:
                self.setChannelOption(key, value)

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
        return self._params[key]

    def getDefault(self, key: str, default: Any) -> Any:
        return self._params.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._params[key] = value

    def setChannelOption(self, key: str, value: Any) -> None:
        # overwrite option if it exists already else append it
        for i, option in enumerate(self._channel_options):
            if option[0] == key:
                self._channel_options[i] = (key, value)
                return
        self._channel_options.append((key, value))

    def add_interceptor(self, interceptor: grpc.UnaryStreamClientInterceptor) -> None:
        self._interceptors.append(interceptor)

    def toChannel(self) -> grpc.Channel:
        """
        The actual channel builder implementations should implement this function
        to return grpc Channel.
        This function should generally use self._insecure_channel or
        self._secure_channel so that configuration options are applied
        appropriately.
        """
        raise PySparkNotImplementedError

    @property
    def host(self) -> str:
        """
        The hostname where this client intends to connect.
        This is used for end-user display purpose in REPL
        """
        raise PySparkNotImplementedError

    def _insecure_channel(self, target: Any, **kwargs: Any) -> grpc.Channel:
        channel = grpc.insecure_channel(target, options=self._channel_options, **kwargs)

        if len(self._interceptors) > 0:
            logger.debug(f"Applying interceptors ({self._interceptors})")
            channel = grpc.intercept_channel(channel, *self._interceptors)
        return channel

    def _secure_channel(self, target: Any, credentials: Any, **kwargs: Any) -> grpc.Channel:
        channel = grpc.secure_channel(target, credentials, options=self._channel_options, **kwargs)

        if len(self._interceptors) > 0:
            logger.debug(f"Applying interceptors ({self._interceptors})")
            channel = grpc.intercept_channel(channel, *self._interceptors)
        return channel

    @property
    def userId(self) -> Optional[str]:
        """
        Returns
        -------
        The user_id (extracted from connection string or configured by other means).
        """
        return self._params.get(ChannelBuilder.PARAM_USER_ID, None)

    @property
    def token(self) -> Optional[str]:
        return self._params.get(ChannelBuilder.PARAM_TOKEN, None)

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
            (k, self._params[k])
            for k in self._params
            if k
            not in [
                ChannelBuilder.PARAM_TOKEN,
                ChannelBuilder.PARAM_USE_SSL,
                ChannelBuilder.PARAM_USER_ID,
                ChannelBuilder.PARAM_USER_AGENT,
                ChannelBuilder.PARAM_SESSION_ID,
            ]
        ]

    @property
    def session_id(self) -> Optional[str]:
        """
        Returns
        -------
        The session_id extracted from the parameters of the connection string or `None` if not
        specified.
        """
        session_id = self._params.get(ChannelBuilder.PARAM_SESSION_ID, None)
        if session_id is not None:
            try:
                uuid.UUID(session_id, version=4)
            except ValueError as ve:
                raise PySparkValueError(
                    errorClass="INVALID_SESSION_UUID_ID",
                    messageParameters={"arg_name": "session_id", "origin": str(ve)},
                )
        return session_id

    @property
    def userAgent(self) -> str:
        """
        Returns
        -------
        user_agent : str
            The user_agent parameter specified in the connection string,
            or "_SPARK_CONNECT_PYTHON" when not specified.
            The returned value will be percent encoded.
        """
        user_agent = self._params.get(
            ChannelBuilder.PARAM_USER_AGENT,
            os.getenv("SPARK_CONNECT_USER_AGENT", "_SPARK_CONNECT_PYTHON"),
        )

        ua_len = len(urllib.parse.quote(user_agent))
        if ua_len > 2048:
            raise SparkConnectException(
                f"'user_agent' parameter should not exceed 2048 characters, found {len} characters."
            )
        return " ".join(
            [
                user_agent,
                f"spark/{__version__}",
                f"os/{platform.uname().system.lower()}",
                f"python/{platform.python_version()}",
            ]
        )


class DefaultChannelBuilder(ChannelBuilder):
    """
    This is a helper class that is used to create a GRPC channel based on the given
    connection string per the documentation of Spark Connect.

    .. versionadded:: 3.4.0

    Examples
    --------
    >>> cb =  DefaultChannelBuilder("sc://localhost")
    ... cb.endpoint
    "localhost:15002"

    >>> cb = DefaultChannelBuilder("sc://localhost/;use_ssl=true;token=aaa")
    ... cb.secure
    True
    """

    @staticmethod
    def default_port() -> int:
        if "SPARK_TESTING" in os.environ and not is_remote_only():
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

        super().__init__(channelOptions=channelOptions)

        # Explicitly check the scheme of the URL.
        if url[:5] != "sc://":
            raise PySparkValueError(
                errorClass="INVALID_CONNECT_URL",
                messageParameters={
                    "detail": "The URL must start with 'sc://'. Please update the URL to "
                    "follow the correct format, e.g., 'sc://hostname:port'.",
                },
            )
        # Rewrite the URL to use http as the scheme so that we can leverage
        # Python's built-in parser.
        tmp_url = "http" + url[2:]
        self.url = urllib.parse.urlparse(tmp_url)
        if len(self.url.path) > 0 and self.url.path != "/":
            raise PySparkValueError(
                errorClass="INVALID_CONNECT_URL",
                messageParameters={
                    "detail": f"The path component '{self.url.path}' must be empty. Please update "
                    f"the URL to follow the correct format, e.g., 'sc://hostname:port'.",
                },
            )
        self._extract_attributes()

    def _extract_attributes(self) -> None:
        if len(self.url.params) > 0:
            parts = self.url.params.split(";")
            for p in parts:
                kv = p.split("=")
                if len(kv) != 2:
                    raise PySparkValueError(
                        errorClass="INVALID_CONNECT_URL",
                        messageParameters={
                            "detail": f"Parameter '{p}' should be provided as a "
                            f"key-value pair separated by an equal sign (=). Please update "
                            f"the parameter to follow the correct format, e.g., 'key=value'.",
                        },
                    )
                self.set(kv[0], urllib.parse.unquote(kv[1]))

        netloc = self.url.netloc.split(":")
        if len(netloc) == 1:
            self._host = netloc[0]
            self._port = DefaultChannelBuilder.default_port()
        elif len(netloc) == 2:
            self._host = netloc[0]
            self._port = int(netloc[1])
        else:
            raise PySparkValueError(
                errorClass="INVALID_CONNECT_URL",
                messageParameters={
                    "detail": f"Target destination '{self.url.netloc}' should match the "
                    f"'<host>:<port>' pattern. Please update the destination to follow "
                    f"the correct format, e.g., 'hostname:port'.",
                },
            )

    @property
    def secure(self) -> bool:
        return (
            self.getDefault(ChannelBuilder.PARAM_USE_SSL, "").lower() == "true"
            or self.token is not None
        )

    @property
    def host(self) -> str:
        """
        The hostname where this client intends to connect.
        """
        return self._host

    @property
    def endpoint(self) -> str:
        return f"{self._host}:{self._port}"

    def toChannel(self) -> grpc.Channel:
        """
        Applies the parameters of the connection string and creates a new
        GRPC channel according to the configuration. Passes optional channel options to
        construct the channel.

        Returns
        -------
        GRPC Channel instance.
        """

        if not self.secure:
            return self._insecure_channel(self.endpoint)
        else:
            ssl_creds = grpc.ssl_channel_credentials()

            if self.token is None:
                creds = ssl_creds
            else:
                creds = grpc.composite_channel_credentials(
                    ssl_creds, grpc.access_token_call_credentials(self.token)
                )

            return self._secure_channel(self.endpoint, creds)


class PlanObservedMetrics(ObservedMetrics):
    def __init__(self, name: str, metrics: List[pb2.Expression.Literal], keys: List[str]):
        self._name = name
        self._metrics = metrics
        self._keys = keys if keys else [f"observed_metric_{i}" for i in range(len(self.metrics))]

    def __repr__(self) -> str:
        return f"Plan observed({self._name}={self._metrics})"

    @property
    def name(self) -> str:
        return self._name

    @property
    def metrics(self) -> List[pb2.Expression.Literal]:
        return self._metrics

    @property
    def pairs(self) -> dict[str, Any]:
        result = {}
        for x in range(len(self._metrics)):
            result[self.keys[x]] = LiteralExpression._to_value(self.metrics[x])
        return result

    @property
    def keys(self) -> List[str]:
        return self._keys


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
        size_in_bytes: Optional[bytes],
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
        self.size_in_bytes = size_in_bytes

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
        size_in_bytes: Optional[bytes] = None

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
            storage_level = proto_to_storage_level(pb.get_storage_level.storage_level)
        elif pb.HasField("size_in_bytes"):
            size_in_bytes = pb.size_in_bytes.result
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
            size_in_bytes,
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
    """

    def __init__(
        self,
        connection: Union[str, ChannelBuilder],
        user_id: Optional[str] = None,
        channel_options: Optional[List[Tuple[str, Any]]] = None,
        retry_policy: Optional[Dict[str, Any]] = None,
        use_reattachable_execute: bool = True,
    ):
        """
        Creates a new SparkSession for the Spark Connect interface.

        Parameters
        ----------
        connection : str or :class:`ChannelBuilder`
            Connection string that is used to extract the connection parameters and configure
            the GRPC connection. Or instance of ChannelBuilder that creates GRPC connection.
            Defaults to `sc://localhost`.
        user_id : str, optional
            Optional unique user ID that is used to differentiate multiple users and
            isolate their Spark Sessions. If the `user_id` is not set, will default to
            the $USER environment. Defining the user ID as part of the connection string
            takes precedence.
        channel_options: list of tuple, optional
            Additional options that can be passed to the GRPC channel construction.
        retry_policy: dict of str and any, optional
            Additional configuration for retrying. There are four configurations as below
                * ``max_retries``
                    Maximum number of tries default 15
                * ``backoff_multiplier``
                    Backoff multiplier for the policy. Default: 4(ms)
                * ``initial_backoff``
                    Backoff to wait before the first retry. Default: 50(ms)
                * ``max_backoff``
                    Maximum backoff controls the maximum amount of time to wait before retrying
                    a failed request. Default: 60000(ms).
        use_reattachable_execute: bool
            Enable reattachable execution.
        """
        self.thread_local = threading.local()

        # Parse the connection string.
        self._builder = (
            connection
            if isinstance(connection, ChannelBuilder)
            else DefaultChannelBuilder(connection, channel_options)
        )
        self._user_id = None
        self._retry_policies: List[RetryPolicy] = []

        retry_policy_args = retry_policy or dict()
        default_policy = DefaultPolicy(**retry_policy_args)
        self.set_retry_policies([default_policy])

        if self._builder.session_id is None:
            # Generate a unique session ID for this client. This UUID must be unique to allow
            # concurrent Spark sessions of the same user. If the channel is closed, creating
            # a new client will create a new session ID.
            self._session_id = str(uuid.uuid4())
        else:
            # Use the pre-defined session ID.
            self._session_id = str(self._builder.session_id)

        if self._builder.userId is not None:
            self._user_id = self._builder.userId
        elif user_id is not None:
            self._user_id = user_id
        else:
            self._user_id = os.getenv("USER", None)

        self._channel = self._builder.toChannel()
        self._closed = False
        self._stub = grpc_lib.SparkConnectServiceStub(self._channel)
        self._artifact_manager = ArtifactManager(
            self._user_id, self._session_id, self._channel, self._builder.metadata()
        )
        self._use_reattachable_execute = use_reattachable_execute
        # Configure logging for the SparkConnect client.

        # Capture the server-side session ID and set it to None initially. It will
        # be updated on the first response received.
        self._server_session_id: Optional[str] = None

        self._profiler_collector = ConnectProfilerCollector()

        self._progress_handlers: List[ProgressHandler] = []

    def register_progress_handler(self, handler: ProgressHandler) -> None:
        """
        Register a progress handler to be called when a progress message is received.

        Parameters
        ----------
        handler : ProgressHandler
          The callable that will be called with the progress information.

        """
        if handler in self._progress_handlers:
            return
        self._progress_handlers.append(handler)

    def clear_progress_handlers(self) -> None:
        self._progress_handlers.clear()

    def remove_progress_handler(self, handler: ProgressHandler) -> None:
        """
        Remove a progress handler from the list of registered handlers.

        Parameters
        ----------
        handler : ProgressHandler
          The callable to remove from the list of progress handlers.

        """
        self._progress_handlers.remove(handler)

    def _retrying(self) -> "Retrying":
        return Retrying(self._retry_policies)

    def disable_reattachable_execute(self) -> "SparkConnectClient":
        self._use_reattachable_execute = False
        return self

    def enable_reattachable_execute(self) -> "SparkConnectClient":
        self._use_reattachable_execute = True
        return self

    def set_retry_policies(self, policies: Iterable[RetryPolicy]) -> None:
        """
        Sets list of policies to be used for retries.
        I.e. set_retry_policies([DefaultPolicy(), CustomPolicy()]).

        """
        self._retry_policies = list(policies)

    def get_retry_policies(self) -> List[RetryPolicy]:
        """
        Return list of currently used policies
        """
        return list(self._retry_policies)

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

    def register_udtf(
        self,
        function: Any,
        return_type: Optional["DataTypeOrString"],
        name: str,
        eval_type: int = PythonEvalType.SQL_TABLE_UDF,
        deterministic: bool = True,
    ) -> str:
        """
        Register a user-defined table function (UDTF) in the session catalog
        as a temporary function. The return type, if specified, must be a
        struct type and it's validated when building the proto message
        for the PythonUDTF.
        """
        udtf = PythonUDTF(
            func=function,
            return_type=return_type,
            eval_type=eval_type,
            python_ver=get_python_ver(),
        )

        func = CommonInlineUserDefinedTableFunction(
            function_name=name,
            function=udtf,
            deterministic=deterministic,
            arguments=[],
        ).udtf_plan(self)

        req = self._execute_plan_request_with_metadata()
        req.plan.command.register_table_function.CopyFrom(func)

        self._execute(req)
        return name

    def register_data_source(self, dataSource: Type["DataSource"]) -> None:
        """
        Register a data source in the session catalog.
        """
        data_source = PythonDataSource(
            data_source=dataSource,
            python_ver=get_python_ver(),
        )
        proto = CommonInlineUserDefinedDataSource(
            name=dataSource.name(),
            data_source=data_source,
        ).to_data_source_proto(self)

        req = self._execute_plan_request_with_metadata()
        req.plan.command.register_data_source.CopyFrom(proto)
        self._execute(req)

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

    def _resources(self) -> Dict[str, ResourceInformation]:
        logger.debug("Fetching the resources")
        cmd = pb2.Command()
        cmd.get_resources_command.SetInParent()
        (_, properties, _) = self.execute_command(cmd)
        resources = properties["get_resources_command_result"]
        return resources

    def _build_observed_metrics(
        self, metrics: Sequence["pb2.ExecutePlanResponse.ObservedMetrics"]
    ) -> Iterator[PlanObservedMetrics]:
        return (PlanObservedMetrics(x.name, [v for v in x.values], list(x.keys)) for x in metrics)

    def to_table_as_iterator(
        self, plan: pb2.Plan, observations: Dict[str, Observation]
    ) -> Iterator[Union[StructType, "pa.Table"]]:
        """
        Return given plan as a PyArrow Table iterator.
        """
        if logger.isEnabledFor(logging.DEBUG):
            # inside an if statement to not incur a performance cost converting proto to string
            # when not at debug log level.
            logger.debug(f"Executing plan {self._proto_to_string(plan, True)}")
        req = self._execute_plan_request_with_metadata()
        req.plan.CopyFrom(plan)
        with Progress(handlers=self._progress_handlers, operation_id=req.operation_id) as progress:
            for response in self._execute_and_fetch_as_iterator(req, observations, progress):
                if isinstance(response, StructType):
                    yield response
                elif isinstance(response, pa.RecordBatch):
                    yield pa.Table.from_batches([response])

    def to_table(
        self, plan: pb2.Plan, observations: Dict[str, Observation]
    ) -> Tuple["pa.Table", Optional[StructType], ExecutionInfo]:
        """
        Return given plan as a PyArrow Table.
        """
        if logger.isEnabledFor(logging.DEBUG):
            # inside an if statement to not incur a performance cost converting proto to string
            # when not at debug log level.
            logger.debug(f"Executing plan {self._proto_to_string(plan, True)}")
        req = self._execute_plan_request_with_metadata()
        req.plan.CopyFrom(plan)
        table, schema, metrics, observed_metrics, _ = self._execute_and_fetch(req, observations)

        # Create a query execution object.
        ei = ExecutionInfo(metrics, observed_metrics)
        assert table is not None
        return table, schema, ei

    def to_pandas(
        self, plan: pb2.Plan, observations: Dict[str, Observation]
    ) -> Tuple["pd.DataFrame", "ExecutionInfo"]:
        """
        Return given plan as a pandas DataFrame.
        """
        if logger.isEnabledFor(logging.DEBUG):
            # inside an if statement to not incur a performance cost converting proto to string
            # when not at debug log level.
            logger.debug(f"Executing plan {self._proto_to_string(plan, True)}")
        req = self._execute_plan_request_with_metadata()
        req.plan.CopyFrom(plan)
        (self_destruct_conf,) = self.get_config_with_defaults(
            ("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "false"),
        )
        self_destruct = cast(str, self_destruct_conf).lower() == "true"
        table, schema, metrics, observed_metrics, _ = self._execute_and_fetch(
            req, observations, self_destruct=self_destruct
        )
        assert table is not None
        ei = ExecutionInfo(metrics, observed_metrics)

        schema = schema or from_arrow_schema(table.schema, prefer_timestamp_ntz=True)
        assert schema is not None and isinstance(schema, StructType)

        # Rename columns to avoid duplicated column names.
        renamed_table = table.rename_columns([f"col_{i}" for i in range(table.num_columns)])

        pandas_options = {}
        if self_destruct:
            # Configure PyArrow to use as little memory as possible:
            # self_destruct - free columns as they are converted
            # split_blocks - create a separate Pandas block for each column
            # use_threads - convert one column at a time
            pandas_options.update(
                {
                    "self_destruct": True,
                    "split_blocks": True,
                    "use_threads": False,
                }
            )
        if LooseVersion(pa.__version__) >= LooseVersion("13.0.0"):
            # A legacy option to coerce date32, date64, duration, and timestamp
            # time units to nanoseconds when converting to pandas.
            # This option can only be added since 13.0.0.
            pandas_options.update(
                {
                    "coerce_temporal_nanoseconds": True,
                }
            )
        pdf = renamed_table.to_pandas(**pandas_options)
        pdf.columns = schema.names

        if len(pdf.columns) > 0:
            timezone: Optional[str] = None
            if any(_has_type(f.dataType, TimestampType) for f in schema.fields):
                (timezone,) = self.get_configs("spark.sql.session.timeZone")

            struct_in_pandas: Optional[str] = None
            error_on_duplicated_field_names: bool = False
            if any(_has_type(f.dataType, StructType) for f in schema.fields):
                (struct_in_pandas,) = self.get_config_with_defaults(
                    ("spark.sql.execution.pandas.structHandlingMode", "legacy"),
                )

                if struct_in_pandas == "legacy":
                    error_on_duplicated_field_names = True
                    struct_in_pandas = "dict"

            pdf = pd.concat(
                [
                    _create_converter_to_pandas(
                        field.dataType,
                        field.nullable,
                        timezone=timezone,
                        struct_in_pandas=struct_in_pandas,
                        error_on_duplicated_field_names=error_on_duplicated_field_names,
                    )(pser)
                    for (_, pser), field, pa_field in zip(pdf.items(), schema.fields, table.schema)
                ],
                axis="columns",
            )

        if len(metrics) > 0:
            pdf.attrs["metrics"] = metrics
        if len(observed_metrics) > 0:
            pdf.attrs["observed_metrics"] = observed_metrics
        return pdf, ei

    def _proto_to_string(self, p: google.protobuf.message.Message, truncate: bool = False) -> str:
        """
        Helper method to generate a one line string representation of the plan.

        Parameters
        ----------
        p : google.protobuf.message.Message
            Generic Message type
        truncate: bool
            Indicates whether to truncate the message

        Returns
        -------
        Single line string of the serialized proto message.
        """
        try:
            max_level = 8 if truncate else sys.maxsize
            p2 = self._truncate(p, max_level) if truncate else p
            return text_format.MessageToString(p2, as_one_line=True)
        except RecursionError:
            return "<Truncated message due to recursion error>"
        except Exception:
            return "<Truncated message due to truncation error>"

    def _truncate(
        self, p: google.protobuf.message.Message, allowed_recursion_depth: int
    ) -> google.protobuf.message.Message:
        """
        Helper method to truncate the protobuf message.
        Refer to 'org.apache.spark.sql.connect.common.Abbreviator' in the server side.
        """

        def truncate_str(s: str) -> str:
            if len(s) > 1024:
                return s[:1024] + "[truncated]"
            return s

        def truncate_bytes(b: bytes) -> bytes:
            if len(b) > 8:
                return b[:8] + b"[truncated]"
            return b

        p2 = copy.deepcopy(p)

        for descriptor, value in p.ListFields():
            if value is not None:
                field_name = descriptor.name

                if descriptor.type == descriptor.TYPE_MESSAGE:
                    if allowed_recursion_depth == 0:
                        p2.ClearField(field_name)
                    elif descriptor.label == descriptor.LABEL_REPEATED:
                        p2.ClearField(field_name)
                        getattr(p2, field_name).extend(
                            [self._truncate(v, allowed_recursion_depth - 1) for v in value]
                        )
                    else:
                        getattr(p2, field_name).CopyFrom(
                            self._truncate(value, allowed_recursion_depth - 1)
                        )

                elif descriptor.type == descriptor.TYPE_STRING:
                    if descriptor.label == descriptor.LABEL_REPEATED:
                        p2.ClearField(field_name)
                        getattr(p2, field_name).extend([truncate_str(v) for v in value])
                    else:
                        setattr(p2, field_name, truncate_str(value))

                elif descriptor.type == descriptor.TYPE_BYTES:
                    if descriptor.label == descriptor.LABEL_REPEATED:
                        p2.ClearField(field_name)
                        getattr(p2, field_name).extend([truncate_bytes(v) for v in value])
                    else:
                        setattr(p2, field_name, truncate_bytes(value))

        return p2

    def schema(self, plan: pb2.Plan) -> StructType:
        """
        Return schema for given plan.
        """
        if logger.isEnabledFor(logging.DEBUG):
            # inside an if statement to not incur a performance cost converting proto to string
            # when not at debug log level.
            logger.debug(f"Schema for plan: {self._proto_to_string(plan, True)}")
        schema = self._analyze(method="schema", plan=plan).schema
        assert schema is not None
        # Server side should populate the struct field which is the schema.
        assert isinstance(schema, StructType)
        return schema

    def explain_string(self, plan: pb2.Plan, explain_mode: str = "extended") -> str:
        """
        Return explain string for given plan.
        """
        if logger.isEnabledFor(logging.DEBUG):
            # inside an if statement to not incur a performance cost converting proto to string
            # when not at debug log level.
            logger.debug(
                f"Explain (mode={explain_mode}) for plan {self._proto_to_string(plan, True)}"
            )
        result = self._analyze(
            method="explain", plan=plan, explain_mode=explain_mode
        ).explain_string
        assert result is not None
        return result

    def execute_command(
        self, command: pb2.Command, observations: Optional[Dict[str, Observation]] = None
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any], ExecutionInfo]:
        """
        Execute given command.
        """
        if logger.isEnabledFor(logging.DEBUG):
            # inside an if statement to not incur a performance cost converting proto to string
            # when not at debug log level.
            logger.debug(f"Execute command for command {self._proto_to_string(command, True)}")
        req = self._execute_plan_request_with_metadata()
        if self._user_id:
            req.user_context.user_id = self._user_id
        req.plan.command.CopyFrom(command)
        data, _, metrics, observed_metrics, properties = self._execute_and_fetch(
            req, observations or {}
        )
        # Create a query execution object.
        ei = ExecutionInfo(metrics, observed_metrics)
        if data is not None:
            return (data.to_pandas(), properties, ei)
        else:
            return (None, properties, ei)

    def execute_command_as_iterator(
        self, command: pb2.Command, observations: Optional[Dict[str, Observation]] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Execute given command. Similar to execute_command, but the value is returned using yield.
        """
        if logger.isEnabledFor(logging.DEBUG):
            # inside an if statement to not incur a performance cost converting proto to string
            # when not at debug log level.
            logger.debug(
                f"Execute command as iterator for command {self._proto_to_string(command, True)}"
            )
        req = self._execute_plan_request_with_metadata()
        if self._user_id:
            req.user_context.user_id = self._user_id
        req.plan.command.CopyFrom(command)
        for response in self._execute_and_fetch_as_iterator(req, observations or {}):
            if isinstance(response, dict):
                yield response
            else:
                raise PySparkValueError(
                    errorClass="UNKNOWN_RESPONSE",
                    messageParameters={
                        "response": str(response),
                    },
                )

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
        ExecutePlanResponseReattachableIterator.shutdown()
        self._channel.close()
        self._closed = True

    @property
    def is_closed(self) -> bool:
        """
        Returns if the channel was closed previously using close() method
        """
        return self._closed

    @property
    def host(self) -> str:
        """
        The hostname where this client intends to connect.
        """
        return self._builder.host

    @property
    def token(self) -> Optional[str]:
        """
        The authentication bearer token during connection.
        If authentication is not using a bearer token, None will be returned.
        """
        return self._builder.token

    def _execute_plan_request_with_metadata(self) -> pb2.ExecutePlanRequest:
        req = pb2.ExecutePlanRequest(
            session_id=self._session_id,
            client_type=self._builder.userAgent,
            tags=list(self.get_tags()),
        )
        if self._server_session_id is not None:
            req.client_observed_server_side_session_id = self._server_session_id
        if self._user_id:
            req.user_context.user_id = self._user_id
        return req

    def _analyze_plan_request_with_metadata(self) -> pb2.AnalyzePlanRequest:
        req = pb2.AnalyzePlanRequest()
        req.session_id = self._session_id
        if self._server_session_id is not None:
            req.client_observed_server_side_session_id = self._server_session_id
        req.client_type = self._builder.userAgent
        if self._user_id:
            req.user_context.user_id = self._user_id
        return req

    def _size_in_bytes(self, relation: pb2.Relation) -> AnalyzeResult:
        req = self._analyze_plan_request_with_metadata()
        req.size_in_bytes.relation.CopyFrom(relation)

        try:
            for attempt in self._retrying():
                with attempt:
                    resp = self._stub.AnalyzePlan(req, metadata=self._builder.metadata())
                    self._verify_response_integrity(resp)
                    return AnalyzeResult.fromProto(resp)
            raise SparkConnectException("Invalid state during retry exception handling.")
        except Exception as error:
            self._handle_error(error)

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
                raise PySparkValueError(
                    errorClass="UNKNOWN_EXPLAIN_MODE",
                    messageParameters={
                        "explain_mode": str(explain_mode),
                    },
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
            level = kwargs.get("level")
            if level and isinstance(level, int):
                req.tree_string.level = level
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
                req.persist.storage_level.CopyFrom(storage_level_to_proto(storage_level))
        elif method == "unpersist":
            req.unpersist.relation.CopyFrom(cast(pb2.Relation, kwargs.get("relation")))
            if kwargs.get("blocking", None) is not None:
                req.unpersist.blocking = cast(bool, kwargs.get("blocking"))
        elif method == "get_storage_level":
            req.get_storage_level.relation.CopyFrom(cast(pb2.Relation, kwargs.get("relation")))
        else:
            raise PySparkValueError(
                errorClass="UNSUPPORTED_OPERATION",
                messageParameters={
                    "operation": method,
                },
            )

        try:
            for attempt in self._retrying():
                with attempt:
                    resp = self._stub.AnalyzePlan(req, metadata=self._builder.metadata())
                    self._verify_response_integrity(resp)
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
        logger.debug("Execute")

        def handle_response(b: pb2.ExecutePlanResponse) -> None:
            self._verify_response_integrity(b)

        try:
            if self._use_reattachable_execute:
                # Don't use retryHandler - own retry handling is inside.
                generator = ExecutePlanResponseReattachableIterator(
                    req, self._stub, self._retrying, self._builder.metadata()
                )
                for b in generator:
                    handle_response(b)
            else:
                for attempt in self._retrying():
                    with attempt:
                        for b in self._stub.ExecutePlan(req, metadata=self._builder.metadata()):
                            handle_response(b)
        except Exception as error:
            self._handle_error(error)

    def _execute_and_fetch_as_iterator(
        self,
        req: pb2.ExecutePlanRequest,
        observations: Dict[str, Observation],
        progress: Optional["Progress"] = None,
    ) -> Iterator[
        Union[
            "pa.RecordBatch",
            StructType,
            PlanMetrics,
            PlanObservedMetrics,
            Dict[str, Any],
        ]
    ]:
        if logger.isEnabledFor(logging.DEBUG):
            # inside an if statement to not incur a performance cost converting proto to string
            # when not at debug log level.
            logger.debug(f"ExecuteAndFetchAsIterator. Request: {self._proto_to_string(req)}")

        num_records = 0

        def handle_response(
            b: pb2.ExecutePlanResponse,
        ) -> Iterator[
            Union[
                "pa.RecordBatch",
                StructType,
                PlanMetrics,
                PlanObservedMetrics,
                Dict[str, Any],
                any_pb2.Any,
            ]
        ]:
            nonlocal num_records
            # The session ID is the local session ID and should match what we expect.
            self._verify_response_integrity(b)
            if logger.isEnabledFor(logging.DEBUG):
                # inside an if statement to not incur a performance cost converting proto to string
                # when not at debug log level.
                logger.debug(
                    f"ExecuteAndFetchAsIterator. Response received: {self._proto_to_string(b)}"
                )

            if b.HasField("metrics"):
                logger.debug("Received metric batch.")
                yield from self._build_metrics(b.metrics)
            if b.observed_metrics:
                logger.debug("Received observed metric batch.")
                for observed_metrics in self._build_observed_metrics(b.observed_metrics):
                    if observed_metrics.name == "__python_accumulator__":
                        from pyspark.worker_util import pickleSer

                        for metric in observed_metrics.metrics:
                            (aid, update) = pickleSer.loads(LiteralExpression._to_value(metric))
                            if aid == SpecialAccumulatorIds.SQL_UDF_PROFIER:
                                self._profiler_collector._update(update)
                    elif observed_metrics.name in observations:
                        observation_result = observations[observed_metrics.name]._result
                        assert observation_result is not None
                        observation_result.update(
                            {
                                key: LiteralExpression._to_value(metric)
                                for key, metric in zip(
                                    observed_metrics.keys, observed_metrics.metrics
                                )
                            }
                        )
                    yield observed_metrics
            if b.HasField("schema"):
                logger.debug("Received the schema.")
                dt = types.proto_schema_to_pyspark_data_type(b.schema)
                assert isinstance(dt, StructType)
                yield dt
            if b.HasField("sql_command_result"):
                logger.debug("Received the SQL command result.")
                yield {"sql_command_result": b.sql_command_result.relation}
            if b.HasField("write_stream_operation_start_result"):
                field = "write_stream_operation_start_result"
                yield {field: b.write_stream_operation_start_result}
            if b.HasField("streaming_query_command_result"):
                yield {"streaming_query_command_result": b.streaming_query_command_result}
            if b.HasField("streaming_query_manager_command_result"):
                cmd_result = b.streaming_query_manager_command_result
                yield {"streaming_query_manager_command_result": cmd_result}
            if b.HasField("streaming_query_listener_events_result"):
                event_result = b.streaming_query_listener_events_result
                yield {"streaming_query_listener_events_result": event_result}
            if b.HasField("get_resources_command_result"):
                resources = {}
                for key, resource in b.get_resources_command_result.resources.items():
                    name = resource.name
                    addresses = [address for address in resource.addresses]
                    resources[key] = ResourceInformation(name, addresses)
                yield {"get_resources_command_result": resources}
            if b.HasField("extension"):
                yield b.extension
            if b.HasField("execution_progress"):
                if progress:
                    p = from_proto(b.execution_progress)
                    progress.update_ticks(*p, operation_id=b.operation_id)
            if b.HasField("arrow_batch"):
                logger.debug(
                    f"Received arrow batch rows={b.arrow_batch.row_count} "
                    f"size={len(b.arrow_batch.data)}"
                )

                if (
                    b.arrow_batch.HasField("start_offset")
                    and num_records != b.arrow_batch.start_offset
                ):
                    raise SparkConnectException(
                        f"Expected arrow batch to start at row offset {num_records} in results, "
                        + "but received arrow batch starting at offset "
                        + f"{b.arrow_batch.start_offset}."
                    )

                num_records_in_batch = 0
                with pa.ipc.open_stream(b.arrow_batch.data) as reader:
                    for batch in reader:
                        assert isinstance(batch, pa.RecordBatch)
                        num_records_in_batch += batch.num_rows
                        yield batch

                if num_records_in_batch != b.arrow_batch.row_count:
                    raise SparkConnectException(
                        f"Expected {b.arrow_batch.row_count} rows in arrow batch but got "
                        + f"{num_records_in_batch}."
                    )
                num_records += num_records_in_batch
            if b.HasField("create_resource_profile_command_result"):
                profile_id = b.create_resource_profile_command_result.profile_id
                yield {"create_resource_profile_command_result": profile_id}
            if b.HasField("checkpoint_command_result"):
                yield {
                    "checkpoint_command_result": proto_to_remote_cached_dataframe(
                        b.checkpoint_command_result.relation
                    )
                }

        try:
            if self._use_reattachable_execute:
                # Don't use retryHandler - own retry handling is inside.
                generator = ExecutePlanResponseReattachableIterator(
                    req, self._stub, self._retrying, self._builder.metadata()
                )
                for b in generator:
                    yield from handle_response(b)
            else:
                for attempt in self._retrying():
                    with attempt:
                        for b in self._stub.ExecutePlan(req, metadata=self._builder.metadata()):
                            yield from handle_response(b)
        except KeyboardInterrupt as kb:
            logger.debug(f"Interrupt request received for operation={req.operation_id}")
            if progress is not None:
                progress.finish()
            self.interrupt_operation(req.operation_id)
            raise kb
        except Exception as error:
            self._handle_error(error)

    def _execute_and_fetch(
        self,
        req: pb2.ExecutePlanRequest,
        observations: Dict[str, Observation],
        self_destruct: bool = False,
    ) -> Tuple[
        Optional["pa.Table"],
        Optional[StructType],
        List[PlanMetrics],
        List[PlanObservedMetrics],
        Dict[str, Any],
    ]:
        logger.debug("ExecuteAndFetch")

        observed_metrics: List[PlanObservedMetrics] = []
        metrics: List[PlanMetrics] = []
        batches: List[pa.RecordBatch] = []
        schema: Optional[StructType] = None
        properties: Dict[str, Any] = {}

        with Progress(handlers=self._progress_handlers, operation_id=req.operation_id) as progress:
            for response in self._execute_and_fetch_as_iterator(
                req, observations, progress=progress
            ):
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
                    raise PySparkValueError(
                        errorClass="UNKNOWN_RESPONSE",
                        messageParameters={
                            "response": response,
                        },
                    )

        if len(batches) > 0:
            if self_destruct:
                results = []
                for batch in batches:
                    # self_destruct frees memory column-wise, but Arrow record batches are
                    # oriented row-wise, so copies each column into its own allocation
                    batch = pa.RecordBatch.from_arrays(
                        [
                            # This call actually reallocates the array
                            pa.concat_arrays([array])
                            for array in batch
                        ],
                        schema=batch.schema,
                    )
                    results.append(batch)
                table = pa.Table.from_batches(batches=results)
                # Ensure only the table has a reference to the batches, so that
                # self_destruct (if enabled) is effective
                del results
                del batches
            else:
                table = pa.Table.from_batches(batches=batches)
            return table, schema, metrics, observed_metrics, properties
        else:
            return None, schema, metrics, observed_metrics, properties

    def _config_request_with_metadata(self) -> pb2.ConfigRequest:
        req = pb2.ConfigRequest()
        req.session_id = self._session_id
        if self._server_session_id is not None:
            req.client_observed_server_side_session_id = self._server_session_id
        req.client_type = self._builder.userAgent
        if self._user_id:
            req.user_context.user_id = self._user_id
        return req

    def get_configs(self, *keys: str) -> Tuple[Optional[str], ...]:
        op = pb2.ConfigRequest.Operation(get=pb2.ConfigRequest.Get(keys=keys))
        configs = dict(self.config(op).pairs)
        return tuple(configs.get(key) for key in keys)

    def get_config_with_defaults(
        self, *pairs: Tuple[str, Optional[str]]
    ) -> Tuple[Optional[str], ...]:
        op = pb2.ConfigRequest.Operation(
            get_with_default=pb2.ConfigRequest.GetWithDefault(
                pairs=[pb2.KeyValue(key=key, value=default) for key, default in pairs]
            )
        )
        configs = dict(self.config(op).pairs)
        return tuple(configs.get(key) for key, _ in pairs)

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
        if self._server_session_id is not None:
            req.client_observed_server_side_session_id = self._server_session_id
        req.operation.CopyFrom(operation)
        try:
            for attempt in self._retrying():
                with attempt:
                    resp = self._stub.Config(req, metadata=self._builder.metadata())
                    self._verify_response_integrity(resp)
                    return ConfigResult.fromProto(resp)
            raise SparkConnectException("Invalid state during retry exception handling.")
        except Exception as error:
            self._handle_error(error)

    def _interrupt_request(
        self, interrupt_type: str, id_or_tag: Optional[str] = None
    ) -> pb2.InterruptRequest:
        req = pb2.InterruptRequest()
        req.session_id = self._session_id
        if self._server_session_id is not None:
            req.client_observed_server_side_session_id = self._server_session_id
        req.client_type = self._builder.userAgent
        if interrupt_type == "all":
            req.interrupt_type = pb2.InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL
        elif interrupt_type == "tag":
            assert id_or_tag is not None
            req.interrupt_type = pb2.InterruptRequest.InterruptType.INTERRUPT_TYPE_TAG
            req.operation_tag = id_or_tag
        elif interrupt_type == "operation":
            assert id_or_tag is not None
            req.interrupt_type = pb2.InterruptRequest.InterruptType.INTERRUPT_TYPE_OPERATION_ID
            req.operation_id = id_or_tag
        else:
            raise PySparkValueError(
                errorClass="UNKNOWN_INTERRUPT_TYPE",
                messageParameters={
                    "interrupt_type": str(interrupt_type),
                },
            )
        if self._user_id:
            req.user_context.user_id = self._user_id
        return req

    def interrupt_all(self) -> Optional[List[str]]:
        req = self._interrupt_request("all")
        try:
            for attempt in self._retrying():
                with attempt:
                    resp = self._stub.Interrupt(req, metadata=self._builder.metadata())
                    self._verify_response_integrity(resp)
                    return list(resp.interrupted_ids)
            raise SparkConnectException("Invalid state during retry exception handling.")
        except Exception as error:
            self._handle_error(error)

    def interrupt_tag(self, tag: str) -> Optional[List[str]]:
        req = self._interrupt_request("tag", tag)
        try:
            for attempt in self._retrying():
                with attempt:
                    resp = self._stub.Interrupt(req, metadata=self._builder.metadata())
                    self._verify_response_integrity(resp)
                    return list(resp.interrupted_ids)
            raise SparkConnectException("Invalid state during retry exception handling.")
        except Exception as error:
            self._handle_error(error)

    def interrupt_operation(self, op_id: str) -> Optional[List[str]]:
        req = self._interrupt_request("operation", op_id)
        try:
            for attempt in self._retrying():
                with attempt:
                    resp = self._stub.Interrupt(req, metadata=self._builder.metadata())
                    self._verify_response_integrity(resp)
                    return list(resp.interrupted_ids)
            raise SparkConnectException("Invalid state during retry exception handling.")
        except Exception as error:
            self._handle_error(error)

    def release_session(self) -> None:
        req = pb2.ReleaseSessionRequest()
        req.session_id = self._session_id
        req.client_type = self._builder.userAgent
        if self._user_id:
            req.user_context.user_id = self._user_id
        try:
            for attempt in self._retrying():
                with attempt:
                    resp = self._stub.ReleaseSession(req, metadata=self._builder.metadata())
                    self._verify_response_integrity(resp)
                    return
            raise SparkConnectException("Invalid state during retry exception handling.")
        except Exception as error:
            self._handle_error(error)

    def add_tag(self, tag: str) -> None:
        self._throw_if_invalid_tag(tag)
        if not hasattr(self.thread_local, "tags"):
            self.thread_local.tags = set()
        self.thread_local.tags.add(tag)

    def remove_tag(self, tag: str) -> None:
        self._throw_if_invalid_tag(tag)
        if not hasattr(self.thread_local, "tags"):
            self.thread_local.tags = set()
        self.thread_local.tags.remove(tag)

    def get_tags(self) -> Set[str]:
        if not hasattr(self.thread_local, "tags"):
            self.thread_local.tags = set()
        return self.thread_local.tags

    def clear_tags(self) -> None:
        self.thread_local.tags = set()

    def _throw_if_invalid_tag(self, tag: str) -> None:
        """
        Validate if a tag for ExecutePlanRequest.tags is valid. Throw ``ValueError`` if
        not.
        """
        spark_job_tags_sep = ","
        if tag is None:
            raise PySparkValueError(
                errorClass="CANNOT_BE_NONE", message_paramters={"arg_name": "Spark Connect tag"}
            )
        if spark_job_tags_sep in tag:
            raise PySparkValueError(
                errorClass="VALUE_ALLOWED",
                messageParameters={
                    "arg_name": "Spark Connect tag",
                    "disallowed_value": spark_job_tags_sep,
                },
            )
        if len(tag) == 0:
            raise PySparkValueError(
                errorClass="VALUE_NOT_NON_EMPTY_STR",
                messageParameters={"arg_name": "Spark Connect tag", "arg_value": tag},
            )

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

        if getattr(self.thread_local, "inside_error_handling", False):
            # We are already inside error handling routine,
            # avoid recursive error processing (with potentially infinite recursion)
            raise error

        try:
            self.thread_local.inside_error_handling = True
            if isinstance(error, grpc.RpcError):
                self._handle_rpc_error(error)
            elif isinstance(error, ValueError):
                if "Cannot invoke RPC" in str(error) and "closed" in str(error):
                    raise SparkConnectException(
                        errorClass="NO_ACTIVE_SESSION", messageParameters=dict()
                    ) from None
            raise error
        finally:
            self.thread_local.inside_error_handling = False

    def _fetch_enriched_error(self, info: "ErrorInfo") -> Optional[pb2.FetchErrorDetailsResponse]:
        if "errorId" not in info.metadata:
            return None

        req = pb2.FetchErrorDetailsRequest(
            session_id=self._session_id,
            client_type=self._builder.userAgent,
            error_id=info.metadata["errorId"],
        )
        if self._server_session_id is not None:
            req.client_observed_server_side_session_id = self._server_session_id
        if self._user_id:
            req.user_context.user_id = self._user_id

        try:
            return self._stub.FetchErrorDetails(req)
        except grpc.RpcError:
            return None

    def _display_server_stack_trace(self) -> bool:
        from pyspark.sql.connect.conf import RuntimeConf

        conf = RuntimeConf(self)
        try:
            if conf.get("spark.sql.connect.serverStacktrace.enabled") == "true":
                return True
            return conf.get("spark.sql.pyspark.jvmStacktrace.enabled") == "true"
        except Exception as e:  # noqa: F841
            # Falls back to true if an exception occurs during reading the config.
            # Otherwise, it will recursively try to get the conf when it consistently
            # fails, ending up with `RecursionError`.
            return True

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
                    logger.debug(f"Received ErrorInfo: {info}")

                    if info.metadata["errorClass"] == "INVALID_HANDLE.SESSION_CHANGED":
                        self._closed = True

                    raise convert_exception(
                        info,
                        status.message,
                        self._fetch_enriched_error(info),
                        self._display_server_stack_trace(),
                    ) from None

            raise SparkConnectGrpcException(status.message) from None
        else:
            raise SparkConnectGrpcException(str(rpc_error)) from None

    def add_artifacts(self, *paths: str, pyfile: bool, archive: bool, file: bool) -> None:
        try:
            for path in paths:
                for attempt in self._retrying():
                    with attempt:
                        self._artifact_manager.add_artifacts(
                            path, pyfile=pyfile, archive=archive, file=file
                        )
        except Exception as error:
            self._handle_error(error)

    def copy_from_local_to_fs(self, local_path: str, dest_path: str) -> None:
        for attempt in self._retrying():
            with attempt:
                self._artifact_manager._add_forward_to_fs_artifacts(local_path, dest_path)

    def cache_artifact(self, blob: bytes) -> str:
        for attempt in self._retrying():
            with attempt:
                return self._artifact_manager.cache_artifact(blob)
        raise SparkConnectException("Invalid state during retry exception handling.")

    def _verify_response_integrity(
        self,
        response: Union[
            pb2.ConfigResponse,
            pb2.ExecutePlanResponse,
            pb2.InterruptResponse,
            pb2.ReleaseExecuteResponse,
            pb2.AddArtifactsResponse,
            pb2.AnalyzePlanResponse,
            pb2.FetchErrorDetailsResponse,
            pb2.ReleaseSessionResponse,
        ],
    ) -> None:
        """
        Verifies the integrity of the response. This method checks if the session ID and the
        server-side session ID match. If not, it throws an exception.
        Parameters
        ----------
        response - One of the different response types handled by the Spark Connect service
        """
        if self._session_id != response.session_id:
            raise PySparkAssertionError(
                "Received incorrect session identifier for request:"
                f"{response.session_id} != {self._session_id}"
            )
        if self._server_session_id is not None:
            if (
                response.server_side_session_id
                and response.server_side_session_id != self._server_session_id
            ):
                self._closed = True
                raise PySparkAssertionError(
                    "Received incorrect server side session identifier for request. "
                    "Please create a new Spark Session to reconnect. ("
                    f"{response.server_side_session_id} != {self._server_session_id})"
                )
        else:
            # Update the server side session ID.
            self._server_session_id = response.server_side_session_id

    def _create_profile(self, profile: pb2.ResourceProfile) -> int:
        """Create the ResourceProfile on the server side and return the profile ID"""
        logger.debug("Creating the ResourceProfile")
        cmd = pb2.Command()
        cmd.create_resource_profile_command.profile.CopyFrom(profile)
        (_, properties, _) = self.execute_command(cmd)
        profile_id = properties["create_resource_profile_command_result"]
        return profile_id
