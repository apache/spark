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

import os
import json
import sys
import random
import warnings
from collections.abc import Iterable
from functools import reduce
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
    overload,
    TYPE_CHECKING,
)

from pyspark import _NoValue
from pyspark.resource import ResourceProfile
from pyspark._globals import _NoValueType
from pyspark.errors import (
    PySparkTypeError,
    PySparkValueError,
    PySparkIndexError,
    PySparkAttributeError,
)
from pyspark.util import (
    _load_from_socket,
    _local_iterator_from_socket,
)
from pyspark.serializers import BatchedSerializer, CPickleSerializer, UTF8Deserializer
from pyspark.storagelevel import StorageLevel
from pyspark.traceback_utils import SCCallSiteSync
from pyspark.sql.column import Column
from pyspark.sql.classic.column import _to_seq, _to_list, _to_java_column
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.merge import MergeIntoWriter
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import (
    StructType,
    Row,
    _parse_datatype_json_string,
)
from pyspark.sql.dataframe import (
    DataFrame as ParentDataFrame,
    DataFrameNaFunctions as ParentDataFrameNaFunctions,
    DataFrameStatFunctions as ParentDataFrameStatFunctions,
)
from pyspark.sql.utils import get_active_spark_context, to_java_array, to_scala_map
from pyspark.sql.pandas.conversion import PandasConversionMixin
from pyspark.sql.pandas.map_ops import PandasMapOpsMixin

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    import pyarrow as pa
    from pyspark.core.rdd import RDD
    from pyspark.core.context import SparkContext
    from pyspark._typing import PrimitiveType
    from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
    from pyspark.sql._typing import (
        ColumnOrName,
        ColumnOrNameOrOrdinal,
        LiteralType,
        OptionalPrimitiveType,
    )
    from pyspark.sql.pandas._typing import (
        PandasMapIterFunction,
        ArrowMapIterFunction,
        DataFrameLike as PandasDataFrameLike,
    )
    from pyspark.sql.context import SQLContext
    from pyspark.sql.session import SparkSession
    from pyspark.sql.group import GroupedData
    from pyspark.sql.observation import Observation
    from pyspark.sql.metrics import ExecutionInfo


class DataFrame(ParentDataFrame, PandasMapOpsMixin, PandasConversionMixin):
    def __new__(
        cls,
        jdf: "JavaObject",
        sql_ctx: Union["SQLContext", "SparkSession"],
    ) -> "DataFrame":
        self = object.__new__(cls)
        self.__init__(jdf, sql_ctx)  # type: ignore[misc]
        return self

    def __init__(
        self,
        jdf: "JavaObject",
        sql_ctx: Union["SQLContext", "SparkSession"],
    ):
        from pyspark.sql.context import SQLContext

        self._sql_ctx: Optional["SQLContext"] = None

        if isinstance(sql_ctx, SQLContext):
            assert not os.environ.get("SPARK_TESTING")  # Sanity check for our internal usage.
            assert isinstance(sql_ctx, SQLContext)
            # We should remove this if-else branch in the future release, and rename
            # sql_ctx to session in the constructor. This is an internal code path but
            # was kept with a warning because it's used intensively by third-party libraries.
            warnings.warn("DataFrame constructor is internal. Do not directly use it.")
            self._sql_ctx = sql_ctx
            session = sql_ctx.sparkSession
        else:
            session = sql_ctx
        self._session: "SparkSession" = session

        self._sc: "SparkContext" = sql_ctx._sc
        self._jdf: "JavaObject" = jdf
        self.is_cached = False
        # initialized lazily
        self._schema: Optional[StructType] = None
        self._lazy_rdd: Optional["RDD[Row]"] = None
        # Check whether _repr_html is supported or not, we use it to avoid calling _jdf twice
        # by __repr__ and _repr_html_ while eager evaluation opens.
        self._support_repr_html = False

    @property
    def sql_ctx(self) -> "SQLContext":
        from pyspark.sql.context import SQLContext

        warnings.warn(
            "DataFrame.sql_ctx is an internal property, and will be removed "
            "in future releases. Use DataFrame.sparkSession instead."
        )
        if self._sql_ctx is None:
            self._sql_ctx = SQLContext._get_or_create(self._sc)
        return self._sql_ctx

    @property
    def sparkSession(self) -> "SparkSession":
        return self._session

    @property
    def rdd(self) -> "RDD[Row]":
        from pyspark.core.rdd import RDD

        if self._lazy_rdd is None:
            jrdd = self._jdf.javaToPython()
            self._lazy_rdd = RDD(
                jrdd, self.sparkSession._sc, BatchedSerializer(CPickleSerializer())
            )
        return self._lazy_rdd

    @property
    def na(self) -> ParentDataFrameNaFunctions:
        return DataFrameNaFunctions(self)

    @property
    def stat(self) -> ParentDataFrameStatFunctions:
        return DataFrameStatFunctions(self)

    def toJSON(self, use_unicode: bool = True) -> "RDD[str]":
        from pyspark.core.rdd import RDD

        rdd = self._jdf.toJSON()
        return RDD(rdd.toJavaRDD(), self._sc, UTF8Deserializer(use_unicode))

    def registerTempTable(self, name: str) -> None:
        warnings.warn("Deprecated in 2.0, use createOrReplaceTempView instead.", FutureWarning)
        self._jdf.createOrReplaceTempView(name)

    def createTempView(self, name: str) -> None:
        self._jdf.createTempView(name)

    def createOrReplaceTempView(self, name: str) -> None:
        self._jdf.createOrReplaceTempView(name)

    def createGlobalTempView(self, name: str) -> None:
        self._jdf.createGlobalTempView(name)

    def createOrReplaceGlobalTempView(self, name: str) -> None:
        self._jdf.createOrReplaceGlobalTempView(name)

    @property
    def write(self) -> DataFrameWriter:
        return DataFrameWriter(self)

    @property
    def writeStream(self) -> DataStreamWriter:
        return DataStreamWriter(self)

    @property
    def schema(self) -> StructType:
        if self._schema is None:
            try:
                self._schema = cast(
                    StructType, _parse_datatype_json_string(self._jdf.schema().json())
                )
            except Exception as e:
                raise PySparkValueError(
                    error_class="CANNOT_PARSE_DATATYPE",
                    message_parameters={"error": str(e)},
                )
        return self._schema

    def printSchema(self, level: Optional[int] = None) -> None:
        if level:
            print(self._jdf.schema().treeString(level))
        else:
            print(self._jdf.schema().treeString())

    def explain(
        self, extended: Optional[Union[bool, str]] = None, mode: Optional[str] = None
    ) -> None:
        if extended is not None and mode is not None:
            raise PySparkValueError(
                error_class="CANNOT_SET_TOGETHER",
                message_parameters={"arg_list": "extended and mode"},
            )

        # For the no argument case: df.explain()
        is_no_argument = extended is None and mode is None

        # For the cases below:
        #   explain(True)
        #   explain(extended=False)
        is_extended_case = isinstance(extended, bool) and mode is None

        # For the case when extended is mode:
        #   df.explain("formatted")
        is_extended_as_mode = isinstance(extended, str) and mode is None

        # For the mode specified:
        #   df.explain(mode="formatted")
        is_mode_case = extended is None and isinstance(mode, str)

        if not (is_no_argument or is_extended_case or is_extended_as_mode or is_mode_case):
            if (extended is not None) and (not isinstance(extended, (bool, str))):
                raise PySparkTypeError(
                    error_class="NOT_BOOL_OR_STR",
                    message_parameters={
                        "arg_name": "extended",
                        "arg_type": type(extended).__name__,
                    },
                )
            if (mode is not None) and (not isinstance(mode, str)):
                raise PySparkTypeError(
                    error_class="NOT_STR",
                    message_parameters={"arg_name": "mode", "arg_type": type(mode).__name__},
                )

        # Sets an explain mode depending on a given argument
        if is_no_argument:
            explain_mode = "simple"
        elif is_extended_case:
            explain_mode = "extended" if extended else "simple"
        elif is_mode_case:
            explain_mode = cast(str, mode)
        elif is_extended_as_mode:
            explain_mode = cast(str, extended)
        assert self._sc._jvm is not None
        print(self._sc._jvm.PythonSQLUtils.explainString(self._jdf.queryExecution(), explain_mode))

    def exceptAll(self, other: ParentDataFrame) -> ParentDataFrame:
        return DataFrame(self._jdf.exceptAll(other._jdf), self.sparkSession)

    def isLocal(self) -> bool:
        return self._jdf.isLocal()

    @property
    def isStreaming(self) -> bool:
        return self._jdf.isStreaming()

    def isEmpty(self) -> bool:
        return self._jdf.isEmpty()

    def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:
        print(self._show_string(n, truncate, vertical))

    def _show_string(
        self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False
    ) -> str:
        if not isinstance(n, int) or isinstance(n, bool):
            raise PySparkTypeError(
                error_class="NOT_INT",
                message_parameters={"arg_name": "n", "arg_type": type(n).__name__},
            )

        if not isinstance(vertical, bool):
            raise PySparkTypeError(
                error_class="NOT_BOOL",
                message_parameters={"arg_name": "vertical", "arg_type": type(vertical).__name__},
            )

        if isinstance(truncate, bool) and truncate:
            return self._jdf.showString(n, 20, vertical)
        else:
            try:
                int_truncate = int(truncate)
            except ValueError:
                raise PySparkTypeError(
                    error_class="NOT_BOOL",
                    message_parameters={
                        "arg_name": "truncate",
                        "arg_type": type(truncate).__name__,
                    },
                )

            return self._jdf.showString(n, int_truncate, vertical)

    def __repr__(self) -> str:
        if not self._support_repr_html and self.sparkSession._jconf.isReplEagerEvalEnabled():
            vertical = False
            return self._jdf.showString(
                self.sparkSession._jconf.replEagerEvalMaxNumRows(),
                self.sparkSession._jconf.replEagerEvalTruncate(),
                vertical,
            )
        else:
            return "DataFrame[%s]" % (", ".join("%s: %s" % c for c in self.dtypes))

    def _repr_html_(self) -> Optional[str]:
        """Returns a :class:`DataFrame` with html code when you enabled eager evaluation
        by 'spark.sql.repl.eagerEval.enabled', this only called by REPL you are
        using support eager evaluation with HTML.
        """
        if not self._support_repr_html:
            self._support_repr_html = True
        if self.sparkSession._jconf.isReplEagerEvalEnabled():
            return self._jdf.htmlString(
                self.sparkSession._jconf.replEagerEvalMaxNumRows(),
                self.sparkSession._jconf.replEagerEvalTruncate(),
            )
        else:
            return None

    def checkpoint(self, eager: bool = True) -> ParentDataFrame:
        jdf = self._jdf.checkpoint(eager)
        return DataFrame(jdf, self.sparkSession)

    def localCheckpoint(self, eager: bool = True) -> ParentDataFrame:
        jdf = self._jdf.localCheckpoint(eager)
        return DataFrame(jdf, self.sparkSession)

    def withWatermark(self, eventTime: str, delayThreshold: str) -> ParentDataFrame:
        if not eventTime or type(eventTime) is not str:
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "eventTime", "arg_type": type(eventTime).__name__},
            )
        if not delayThreshold or type(delayThreshold) is not str:
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={
                    "arg_name": "delayThreshold",
                    "arg_type": type(delayThreshold).__name__,
                },
            )
        jdf = self._jdf.withWatermark(eventTime, delayThreshold)
        return DataFrame(jdf, self.sparkSession)

    def hint(
        self, name: str, *parameters: Union["PrimitiveType", "Column", List["PrimitiveType"]]
    ) -> ParentDataFrame:
        if len(parameters) == 1 and isinstance(parameters[0], list):
            parameters = parameters[0]  # type: ignore[assignment]

        if not isinstance(name, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "name", "arg_type": type(name).__name__},
            )

        allowed_types = (str, float, int, Column, list)
        allowed_primitive_types = (str, float, int)
        allowed_types_repr = ", ".join(
            [t.__name__ for t in allowed_types[:-1]]
            + ["list[" + t.__name__ + "]" for t in allowed_primitive_types]
        )
        for p in parameters:
            if not isinstance(p, allowed_types):
                raise PySparkTypeError(
                    error_class="DISALLOWED_TYPE_FOR_CONTAINER",
                    message_parameters={
                        "arg_name": "parameters",
                        "arg_type": type(parameters).__name__,
                        "allowed_types": allowed_types_repr,
                        "item_type": type(p).__name__,
                    },
                )
            if isinstance(p, list):
                if not all(isinstance(e, allowed_primitive_types) for e in p):
                    raise PySparkTypeError(
                        error_class="DISALLOWED_TYPE_FOR_CONTAINER",
                        message_parameters={
                            "arg_name": "parameters",
                            "arg_type": type(parameters).__name__,
                            "allowed_types": allowed_types_repr,
                            "item_type": type(p).__name__ + "[" + type(p[0]).__name__ + "]",
                        },
                    )

        def _converter(parameter: Union[str, list, float, int, Column]) -> Any:
            if isinstance(parameter, Column):
                return _to_java_column(parameter)
            elif isinstance(parameter, list):
                # for list input, we are assuming only one element type exist in the list.
                # for empty list, we are converting it into an empty long[] in the JVM side.
                gateway = self._sc._gateway
                assert gateway is not None
                jclass = gateway.jvm.long
                if len(parameter) >= 1:
                    mapping = {
                        str: gateway.jvm.java.lang.String,
                        float: gateway.jvm.double,
                        int: gateway.jvm.long,
                    }
                    jclass = mapping[type(parameter[0])]
                return to_java_array(gateway, jclass, parameter)
            else:
                return parameter

        jdf = self._jdf.hint(name, self._jseq(parameters, _converter))
        return DataFrame(jdf, self.sparkSession)

    def count(self) -> int:
        return int(self._jdf.count())

    def collect(self) -> List[Row]:
        with SCCallSiteSync(self._sc):
            sock_info = self._jdf.collectToPython()
        return list(_load_from_socket(sock_info, BatchedSerializer(CPickleSerializer())))

    def toLocalIterator(self, prefetchPartitions: bool = False) -> Iterator[Row]:
        with SCCallSiteSync(self._sc):
            sock_info = self._jdf.toPythonIterator(prefetchPartitions)
        return _local_iterator_from_socket(sock_info, BatchedSerializer(CPickleSerializer()))

    def limit(self, num: int) -> ParentDataFrame:
        jdf = self._jdf.limit(num)
        return DataFrame(jdf, self.sparkSession)

    def offset(self, num: int) -> ParentDataFrame:
        jdf = self._jdf.offset(num)
        return DataFrame(jdf, self.sparkSession)

    def take(self, num: int) -> List[Row]:
        return self.limit(num).collect()

    def tail(self, num: int) -> List[Row]:
        with SCCallSiteSync(self._sc):
            sock_info = self._jdf.tailToPython(num)
        return list(_load_from_socket(sock_info, BatchedSerializer(CPickleSerializer())))

    def foreach(self, f: Callable[[Row], None]) -> None:
        self.rdd.foreach(f)

    def foreachPartition(self, f: Callable[[Iterator[Row]], None]) -> None:
        self.rdd.foreachPartition(f)  # type: ignore[arg-type]

    def cache(self) -> ParentDataFrame:
        self.is_cached = True
        self._jdf.cache()
        return self

    def persist(
        self,
        storageLevel: StorageLevel = (StorageLevel.MEMORY_AND_DISK_DESER),
    ) -> ParentDataFrame:
        self.is_cached = True
        javaStorageLevel = self._sc._getJavaStorageLevel(storageLevel)
        self._jdf.persist(javaStorageLevel)
        return self

    @property
    def storageLevel(self) -> StorageLevel:
        java_storage_level = self._jdf.storageLevel()
        storage_level = StorageLevel(
            java_storage_level.useDisk(),
            java_storage_level.useMemory(),
            java_storage_level.useOffHeap(),
            java_storage_level.deserialized(),
            java_storage_level.replication(),
        )
        return storage_level

    def unpersist(self, blocking: bool = False) -> ParentDataFrame:
        self.is_cached = False
        self._jdf.unpersist(blocking)
        return self

    def coalesce(self, numPartitions: int) -> ParentDataFrame:
        return DataFrame(self._jdf.coalesce(numPartitions), self.sparkSession)

    @overload
    def repartition(self, numPartitions: int, *cols: "ColumnOrName") -> ParentDataFrame:
        ...

    @overload
    def repartition(self, *cols: "ColumnOrName") -> ParentDataFrame:
        ...

    def repartition(  # type: ignore[misc]
        self, numPartitions: Union[int, "ColumnOrName"], *cols: "ColumnOrName"
    ) -> ParentDataFrame:
        if isinstance(numPartitions, int):
            if len(cols) == 0:
                return DataFrame(self._jdf.repartition(numPartitions), self.sparkSession)
            else:
                return DataFrame(
                    self._jdf.repartition(numPartitions, self._jcols(*cols)),
                    self.sparkSession,
                )
        elif isinstance(numPartitions, (str, Column)):
            cols = (numPartitions,) + cols
            return DataFrame(self._jdf.repartition(self._jcols(*cols)), self.sparkSession)
        else:
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_STR",
                message_parameters={
                    "arg_name": "numPartitions",
                    "arg_type": type(numPartitions).__name__,
                },
            )

    @overload
    def repartitionByRange(self, numPartitions: int, *cols: "ColumnOrName") -> ParentDataFrame:
        ...

    @overload
    def repartitionByRange(self, *cols: "ColumnOrName") -> ParentDataFrame:
        ...

    def repartitionByRange(  # type: ignore[misc]
        self, numPartitions: Union[int, "ColumnOrName"], *cols: "ColumnOrName"
    ) -> ParentDataFrame:
        if isinstance(numPartitions, int):
            if len(cols) == 0:
                raise PySparkValueError(
                    error_class="CANNOT_BE_EMPTY",
                    message_parameters={"item": "partition-by expression"},
                )
            else:
                return DataFrame(
                    self._jdf.repartitionByRange(numPartitions, self._jcols(*cols)),
                    self.sparkSession,
                )
        elif isinstance(numPartitions, (str, Column)):
            cols = (numPartitions,) + cols
            return DataFrame(self._jdf.repartitionByRange(self._jcols(*cols)), self.sparkSession)
        else:
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_INT_OR_STR",
                message_parameters={
                    "arg_name": "numPartitions",
                    "arg_type": type(numPartitions).__name__,
                },
            )

    def distinct(self) -> ParentDataFrame:
        return DataFrame(self._jdf.distinct(), self.sparkSession)

    @overload
    def sample(self, fraction: float, seed: Optional[int] = ...) -> ParentDataFrame:
        ...

    @overload
    def sample(
        self,
        withReplacement: Optional[bool],
        fraction: float,
        seed: Optional[int] = ...,
    ) -> ParentDataFrame:
        ...

    def sample(  # type: ignore[misc]
        self,
        withReplacement: Optional[Union[float, bool]] = None,
        fraction: Optional[Union[int, float]] = None,
        seed: Optional[int] = None,
    ) -> ParentDataFrame:
        # For the cases below:
        #   sample(True, 0.5 [, seed])
        #   sample(True, fraction=0.5 [, seed])
        #   sample(withReplacement=False, fraction=0.5 [, seed])
        is_withReplacement_set = type(withReplacement) == bool and isinstance(fraction, float)

        # For the case below:
        #   sample(faction=0.5 [, seed])
        is_withReplacement_omitted_kwargs = withReplacement is None and isinstance(fraction, float)

        # For the case below:
        #   sample(0.5 [, seed])
        is_withReplacement_omitted_args = isinstance(withReplacement, float)

        if not (
            is_withReplacement_set
            or is_withReplacement_omitted_kwargs
            or is_withReplacement_omitted_args
        ):
            argtypes = [type(arg).__name__ for arg in [withReplacement, fraction, seed]]
            raise PySparkTypeError(
                error_class="NOT_BOOL_OR_FLOAT_OR_INT",
                message_parameters={
                    "arg_name": "withReplacement (optional), "
                    + "fraction (required) and seed (optional)",
                    "arg_type": ", ".join(argtypes),
                },
            )

        if is_withReplacement_omitted_args:
            if fraction is not None:
                seed = cast(int, fraction)
            fraction = withReplacement
            withReplacement = None

        seed = int(seed) if seed is not None else None
        args = [arg for arg in [withReplacement, fraction, seed] if arg is not None]
        jdf = self._jdf.sample(*args)
        return DataFrame(jdf, self.sparkSession)

    def sampleBy(
        self, col: "ColumnOrName", fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> ParentDataFrame:
        if isinstance(col, str):
            col = Column(col)
        elif not isinstance(col, Column):
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_STR",
                message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
            )
        if not isinstance(fractions, dict):
            raise PySparkTypeError(
                error_class="NOT_DICT",
                message_parameters={"arg_name": "fractions", "arg_type": type(fractions).__name__},
            )
        for k, v in fractions.items():
            if not isinstance(k, (float, int, str)):
                raise PySparkTypeError(
                    error_class="DISALLOWED_TYPE_FOR_CONTAINER",
                    message_parameters={
                        "arg_name": "fractions",
                        "arg_type": type(fractions).__name__,
                        "allowed_types": "float, int, str",
                        "item_type": type(k).__name__,
                    },
                )
            fractions[k] = float(v)
        col = col._jc
        seed = seed if seed is not None else random.randint(0, sys.maxsize)
        return DataFrame(
            self._jdf.stat().sampleBy(col, self._jmap(fractions), seed), self.sparkSession
        )

    def randomSplit(
        self, weights: List[float], seed: Optional[int] = None
    ) -> List[ParentDataFrame]:
        for w in weights:
            if w < 0.0:
                raise PySparkValueError(
                    error_class="VALUE_NOT_POSITIVE",
                    message_parameters={"arg_name": "weights", "arg_value": str(w)},
                )
        seed = seed if seed is not None else random.randint(0, sys.maxsize)
        df_array = self._jdf.randomSplit(
            _to_list(self.sparkSession._sc, cast(List["ColumnOrName"], weights)), int(seed)
        )
        return [DataFrame(df, self.sparkSession) for df in df_array]

    @property
    def dtypes(self) -> List[Tuple[str, str]]:
        return [(str(f.name), f.dataType.simpleString()) for f in self.schema.fields]

    @property
    def columns(self) -> List[str]:
        return [f.name for f in self.schema.fields]

    def colRegex(self, colName: str) -> Column:
        if not isinstance(colName, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "colName", "arg_type": type(colName).__name__},
            )
        jc = self._jdf.colRegex(colName)
        return Column(jc)

    def to(self, schema: StructType) -> ParentDataFrame:
        assert schema is not None
        jschema = self._jdf.sparkSession().parseDataType(schema.json())
        return DataFrame(self._jdf.to(jschema), self.sparkSession)

    def alias(self, alias: str) -> ParentDataFrame:
        assert isinstance(alias, str), "alias should be a string"
        return DataFrame(getattr(self._jdf, "as")(alias), self.sparkSession)

    def crossJoin(self, other: ParentDataFrame) -> ParentDataFrame:
        jdf = self._jdf.crossJoin(other._jdf)
        return DataFrame(jdf, self.sparkSession)

    def join(
        self,
        other: ParentDataFrame,
        on: Optional[Union[str, List[str], Column, List[Column]]] = None,
        how: Optional[str] = None,
    ) -> ParentDataFrame:
        if on is not None and not isinstance(on, list):
            on = [on]  # type: ignore[assignment]

        if on is not None:
            if isinstance(on[0], str):
                on = self._jseq(cast(List[str], on))
            else:
                assert isinstance(on[0], Column), "on should be Column or list of Column"
                on = reduce(lambda x, y: x.__and__(y), cast(List[Column], on))
                on = on._jc

        if on is None and how is None:
            jdf = self._jdf.join(other._jdf)
        else:
            if how is None:
                how = "inner"
            if on is None:
                on = self._jseq([])
            assert isinstance(how, str), "how should be a string"
            jdf = self._jdf.join(other._jdf, on, how)
        return DataFrame(jdf, self.sparkSession)

    # TODO(SPARK-22947): Fix the DataFrame API.
    def _joinAsOf(
        self,
        other: ParentDataFrame,
        leftAsOfColumn: Union[str, Column],
        rightAsOfColumn: Union[str, Column],
        on: Optional[Union[str, List[str], Column, List[Column]]] = None,
        how: Optional[str] = None,
        *,
        tolerance: Optional[Column] = None,
        allowExactMatches: bool = True,
        direction: str = "backward",
    ) -> ParentDataFrame:
        """
        Perform an as-of join.

        This is similar to a left-join except that we match on the nearest
        key rather than equal keys.

        .. versionchanged:: 4.0.0
            Supports Spark Connect.

        Parameters
        ----------
        other : :class:`DataFrame`
            Right side of the join
        leftAsOfColumn : str or :class:`Column`
            a string for the as-of join column name, or a Column
        rightAsOfColumn : str or :class:`Column`
            a string for the as-of join column name, or a Column
        on : str, list or :class:`Column`, optional
            a string for the join column name, a list of column names,
            a join expression (Column), or a list of Columns.
            If `on` is a string or a list of strings indicating the name of the join column(s),
            the column(s) must exist on both sides, and this performs an equi-join.
        how : str, optional
            default ``inner``. Must be one of: ``inner`` and ``left``.
        tolerance : :class:`Column`, optional
            an asof tolerance within this range; must be compatible
            with the merge index.
        allowExactMatches : bool, optional
            default ``True``.
        direction : str, optional
            default ``backward``. Must be one of: ``backward``, ``forward``, and ``nearest``.

        Examples
        --------
        The following performs an as-of join between ``left`` and ``right``.

        >>> left = spark.createDataFrame([(1, "a"), (5, "b"), (10,  "c")], ["a", "left_val"])
        >>> right = spark.createDataFrame([(1, 1), (2, 2), (3, 3), (6, 6), (7, 7)],
        ...                               ["a", "right_val"])
        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a"
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=1, left_val='a', right_val=1),
         Row(a=5, left_val='b', right_val=3),
         Row(a=10, left_val='c', right_val=7)]

        >>> from pyspark.sql import functions as sf
        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a", tolerance=sf.lit(1)
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=1, left_val='a', right_val=1)]

        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a", how="left", tolerance=sf.lit(1)
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=1, left_val='a', right_val=1),
         Row(a=5, left_val='b', right_val=None),
         Row(a=10, left_val='c', right_val=None)]

        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a", allowExactMatches=False
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=5, left_val='b', right_val=3),
         Row(a=10, left_val='c', right_val=7)]

        >>> left._joinAsOf(
        ...     right, leftAsOfColumn="a", rightAsOfColumn="a", direction="forward"
        ... ).select(left.a, 'left_val', 'right_val').sort("a").collect()
        [Row(a=1, left_val='a', right_val=1),
         Row(a=5, left_val='b', right_val=6)]
        """
        if isinstance(leftAsOfColumn, str):
            leftAsOfColumn = self[leftAsOfColumn]
        left_as_of_jcol = leftAsOfColumn._jc
        if isinstance(rightAsOfColumn, str):
            rightAsOfColumn = other[rightAsOfColumn]
        right_as_of_jcol = rightAsOfColumn._jc

        if on is not None and not isinstance(on, list):
            on = [on]  # type: ignore[assignment]

        if on is not None:
            if isinstance(on[0], str):
                on = self._jseq(cast(List[str], on))
            else:
                assert isinstance(on[0], Column), "on should be Column or list of Column"
                on = reduce(lambda x, y: x.__and__(y), cast(List[Column], on))
                on = on._jc

        if how is None:
            how = "inner"
        assert isinstance(how, str), "how should be a string"

        if tolerance is not None:
            assert isinstance(tolerance, Column), "tolerance should be Column"
            tolerance = tolerance._jc

        jdf = self._jdf.joinAsOf(
            other._jdf,
            left_as_of_jcol,
            right_as_of_jcol,
            on,
            how,
            tolerance,
            allowExactMatches,
            direction,
        )
        return DataFrame(jdf, self.sparkSession)

    def sortWithinPartitions(
        self,
        *cols: Union[int, str, Column, List[Union[int, str, Column]]],
        **kwargs: Any,
    ) -> ParentDataFrame:
        jdf = self._jdf.sortWithinPartitions(self._sort_cols(cols, kwargs))
        return DataFrame(jdf, self.sparkSession)

    def sort(
        self,
        *cols: Union[int, str, Column, List[Union[int, str, Column]]],
        **kwargs: Any,
    ) -> ParentDataFrame:
        jdf = self._jdf.sort(self._sort_cols(cols, kwargs))
        return DataFrame(jdf, self.sparkSession)

    orderBy = sort

    def _jseq(
        self,
        cols: Sequence,
        converter: Optional[Callable[..., Union["PrimitiveType", "JavaObject"]]] = None,
    ) -> "JavaObject":
        """Return a JVM Seq of Columns from a list of Column or names"""
        return _to_seq(self.sparkSession._sc, cols, converter)

    def _jmap(self, jm: Dict) -> "JavaObject":
        """Return a JVM Scala Map from a dict"""
        return to_scala_map(self.sparkSession._sc._jvm, jm)

    def _jcols(self, *cols: "ColumnOrName") -> "JavaObject":
        """Return a JVM Seq of Columns from a list of Column or column names

        If `cols` has only one list in it, cols[0] will be used as the list.
        """
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]
        return self._jseq(cols, _to_java_column)

    def _jcols_ordinal(self, *cols: "ColumnOrNameOrOrdinal") -> "JavaObject":
        """Return a JVM Seq of Columns from a list of Column or column names or column ordinals.

        If `cols` has only one list in it, cols[0] will be used as the list.
        """
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]

        _cols = []
        for c in cols:
            if isinstance(c, int) and not isinstance(c, bool):
                if c < 1:
                    raise PySparkIndexError(
                        error_class="INDEX_NOT_POSITIVE", message_parameters={"index": str(c)}
                    )
                # ordinal is 1-based
                _cols.append(self[c - 1])
            else:
                _cols.append(c)  # type: ignore[arg-type]
        return self._jseq(_cols, _to_java_column)

    def _sort_cols(
        self,
        cols: Sequence[Union[int, str, Column, List[Union[int, str, Column]]]],
        kwargs: Dict[str, Any],
    ) -> "JavaObject":
        """Return a JVM Seq of Columns that describes the sort order"""
        if not cols:
            raise PySparkValueError(
                error_class="CANNOT_BE_EMPTY",
                message_parameters={"item": "column"},
            )
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]

        jcols = []
        for c in cols:
            if isinstance(c, int) and not isinstance(c, bool):
                # ordinal is 1-based
                if c > 0:
                    _c = self[c - 1]
                # negative ordinal means sort by desc
                elif c < 0:
                    _c = self[-c - 1].desc()
                else:
                    raise PySparkIndexError(
                        error_class="ZERO_INDEX",
                        message_parameters={},
                    )
            else:
                _c = c  # type: ignore[assignment]
            jcols.append(_to_java_column(cast("ColumnOrName", _c)))

        ascending = kwargs.get("ascending", True)
        if isinstance(ascending, (bool, int)):
            if not ascending:
                jcols = [jc.desc() for jc in jcols]
        elif isinstance(ascending, list):
            jcols = [jc if asc else jc.desc() for asc, jc in zip(ascending, jcols)]
        else:
            raise PySparkTypeError(
                error_class="NOT_BOOL_OR_LIST",
                message_parameters={"arg_name": "ascending", "arg_type": type(ascending).__name__},
            )
        return self._jseq(jcols)

    def describe(self, *cols: Union[str, List[str]]) -> ParentDataFrame:
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = cols[0]  # type: ignore[assignment]
        jdf = self._jdf.describe(self._jseq(cols))
        return DataFrame(jdf, self.sparkSession)

    def summary(self, *statistics: str) -> ParentDataFrame:
        if len(statistics) == 1 and isinstance(statistics[0], list):
            statistics = statistics[0]
        jdf = self._jdf.summary(self._jseq(statistics))
        return DataFrame(jdf, self.sparkSession)

    @overload
    def head(self) -> Optional[Row]:
        ...

    @overload
    def head(self, n: int) -> List[Row]:
        ...

    def head(self, n: Optional[int] = None) -> Union[Optional[Row], List[Row]]:
        if n is None:
            rs = self.head(1)
            return rs[0] if rs else None
        return self.take(n)

    def first(self) -> Optional[Row]:
        return self.head()

    @overload
    def __getitem__(self, item: Union[int, str]) -> Column:
        ...

    @overload
    def __getitem__(self, item: Union[Column, List, Tuple]) -> ParentDataFrame:
        ...

    def __getitem__(
        self, item: Union[int, str, Column, List, Tuple]
    ) -> Union[Column, ParentDataFrame]:
        if isinstance(item, str):
            jc = self._jdf.apply(item)
            return Column(jc)
        elif isinstance(item, Column):
            return self.filter(item)
        elif isinstance(item, (list, tuple)):
            return self.select(*item)
        elif isinstance(item, int):
            jc = self._jdf.apply(self.columns[item])
            return Column(jc)
        else:
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_FLOAT_OR_INT_OR_LIST_OR_STR",
                message_parameters={"arg_name": "item", "arg_type": type(item).__name__},
            )

    def __getattr__(self, name: str) -> Column:
        if name not in self.columns:
            raise PySparkAttributeError(
                error_class="ATTRIBUTE_NOT_SUPPORTED", message_parameters={"attr_name": name}
            )
        jc = self._jdf.apply(name)
        return Column(jc)

    def __dir__(self) -> List[str]:
        attrs = set(dir(DataFrame))
        attrs.update(filter(lambda s: s.isidentifier(), self.columns))
        return sorted(attrs)

    @overload
    def select(self, *cols: "ColumnOrName") -> ParentDataFrame:
        ...

    @overload
    def select(self, __cols: Union[List[Column], List[str]]) -> ParentDataFrame:
        ...

    def select(self, *cols: "ColumnOrName") -> ParentDataFrame:  # type: ignore[misc]
        jdf = self._jdf.select(self._jcols(*cols))
        return DataFrame(jdf, self.sparkSession)

    @overload
    def selectExpr(self, *expr: str) -> ParentDataFrame:
        ...

    @overload
    def selectExpr(self, *expr: List[str]) -> ParentDataFrame:
        ...

    def selectExpr(self, *expr: Union[str, List[str]]) -> ParentDataFrame:
        if len(expr) == 1 and isinstance(expr[0], list):
            expr = expr[0]  # type: ignore[assignment]
        jdf = self._jdf.selectExpr(self._jseq(expr))
        return DataFrame(jdf, self.sparkSession)

    def filter(self, condition: "ColumnOrName") -> ParentDataFrame:
        if isinstance(condition, str):
            jdf = self._jdf.filter(condition)
        elif isinstance(condition, Column):
            jdf = self._jdf.filter(condition._jc)
        else:
            raise PySparkTypeError(
                error_class="NOT_COLUMN_OR_STR",
                message_parameters={"arg_name": "condition", "arg_type": type(condition).__name__},
            )
        return DataFrame(jdf, self.sparkSession)

    @overload
    def groupBy(self, *cols: "ColumnOrNameOrOrdinal") -> "GroupedData":
        ...

    @overload
    def groupBy(self, __cols: Union[List[Column], List[str], List[int]]) -> "GroupedData":
        ...

    def groupBy(self, *cols: "ColumnOrNameOrOrdinal") -> "GroupedData":  # type: ignore[misc]
        jgd = self._jdf.groupBy(self._jcols_ordinal(*cols))
        from pyspark.sql.group import GroupedData

        return GroupedData(jgd, self)

    @overload
    def rollup(self, *cols: "ColumnOrName") -> "GroupedData":
        ...

    @overload
    def rollup(self, __cols: Union[List[Column], List[str]]) -> "GroupedData":
        ...

    def rollup(self, *cols: "ColumnOrNameOrOrdinal") -> "GroupedData":  # type: ignore[misc]
        jgd = self._jdf.rollup(self._jcols_ordinal(*cols))
        from pyspark.sql.group import GroupedData

        return GroupedData(jgd, self)

    @overload
    def cube(self, *cols: "ColumnOrName") -> "GroupedData":
        ...

    @overload
    def cube(self, __cols: Union[List[Column], List[str]]) -> "GroupedData":
        ...

    def cube(self, *cols: "ColumnOrName") -> "GroupedData":  # type: ignore[misc]
        jgd = self._jdf.cube(self._jcols_ordinal(*cols))
        from pyspark.sql.group import GroupedData

        return GroupedData(jgd, self)

    def groupingSets(
        self, groupingSets: Sequence[Sequence["ColumnOrName"]], *cols: "ColumnOrName"
    ) -> "GroupedData":
        from pyspark.sql.group import GroupedData

        jgrouping_sets = _to_seq(self._sc, [self._jcols(*inner) for inner in groupingSets])

        jgd = self._jdf.groupingSets(jgrouping_sets, self._jcols(*cols))
        return GroupedData(jgd, self)

    def unpivot(
        self,
        ids: Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]],
        values: Optional[Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]],
        variableColumnName: str,
        valueColumnName: str,
    ) -> ParentDataFrame:
        assert ids is not None, "ids must not be None"

        def to_jcols(
            cols: Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]
        ) -> "JavaObject":
            if isinstance(cols, list):
                return self._jcols(*cols)
            if isinstance(cols, tuple):
                return self._jcols(*list(cols))
            return self._jcols(cols)

        jids = to_jcols(ids)
        if values is None:
            jdf = self._jdf.unpivotWithSeq(jids, variableColumnName, valueColumnName)
        else:
            jvals = to_jcols(values)
            jdf = self._jdf.unpivotWithSeq(jids, jvals, variableColumnName, valueColumnName)

        return DataFrame(jdf, self.sparkSession)

    def melt(
        self,
        ids: Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]],
        values: Optional[Union["ColumnOrName", List["ColumnOrName"], Tuple["ColumnOrName", ...]]],
        variableColumnName: str,
        valueColumnName: str,
    ) -> ParentDataFrame:
        return self.unpivot(ids, values, variableColumnName, valueColumnName)

    def agg(self, *exprs: Union[Column, Dict[str, str]]) -> ParentDataFrame:
        return self.groupBy().agg(*exprs)  # type: ignore[arg-type]

    def observe(
        self,
        observation: Union["Observation", str],
        *exprs: Column,
    ) -> ParentDataFrame:
        from pyspark.sql import Observation

        if len(exprs) == 0:
            raise PySparkValueError(
                error_class="CANNOT_BE_EMPTY",
                message_parameters={"item": "exprs"},
            )
        if not all(isinstance(c, Column) for c in exprs):
            raise PySparkTypeError(
                error_class="NOT_LIST_OF_COLUMN",
                message_parameters={"arg_name": "exprs"},
            )

        if isinstance(observation, Observation):
            return observation._on(self, *exprs)
        elif isinstance(observation, str):
            return DataFrame(
                self._jdf.observe(
                    observation, exprs[0]._jc, _to_seq(self._sc, [c._jc for c in exprs[1:]])
                ),
                self.sparkSession,
            )
        else:
            raise PySparkTypeError(
                error_class="NOT_LIST_OF_COLUMN",
                message_parameters={
                    "arg_name": "observation",
                    "arg_type": type(observation).__name__,
                },
            )

    def union(self, other: ParentDataFrame) -> ParentDataFrame:
        return DataFrame(self._jdf.union(other._jdf), self.sparkSession)

    def unionAll(self, other: ParentDataFrame) -> ParentDataFrame:
        return self.union(other)

    def unionByName(
        self, other: ParentDataFrame, allowMissingColumns: bool = False
    ) -> ParentDataFrame:
        return DataFrame(self._jdf.unionByName(other._jdf, allowMissingColumns), self.sparkSession)

    def intersect(self, other: ParentDataFrame) -> ParentDataFrame:
        return DataFrame(self._jdf.intersect(other._jdf), self.sparkSession)

    def intersectAll(self, other: ParentDataFrame) -> ParentDataFrame:
        return DataFrame(self._jdf.intersectAll(other._jdf), self.sparkSession)

    def subtract(self, other: ParentDataFrame) -> ParentDataFrame:
        return DataFrame(getattr(self._jdf, "except")(other._jdf), self.sparkSession)

    def dropDuplicates(self, *subset: Union[str, List[str]]) -> ParentDataFrame:
        # Acceptable args should be str, ... or a single List[str]
        # So if subset length is 1, it can be either single str, or a list of str
        # if subset length is greater than 1, it must be a sequence of str
        if len(subset) > 1:
            assert all(isinstance(c, str) for c in subset)

        if not subset:
            jdf = self._jdf.dropDuplicates()
        elif len(subset) == 1 and isinstance(subset[0], list):
            jdf = self._jdf.dropDuplicates(self._jseq(subset[0]))
        else:
            jdf = self._jdf.dropDuplicates(self._jseq(subset))
        return DataFrame(jdf, self.sparkSession)

    drop_duplicates = dropDuplicates

    def dropDuplicatesWithinWatermark(self, *subset: Union[str, List[str]]) -> ParentDataFrame:
        # Acceptable args should be str, ... or a single List[str]
        # So if subset length is 1, it can be either single str, or a list of str
        # if subset length is greater than 1, it must be a sequence of str
        if len(subset) > 1:
            assert all(isinstance(c, str) for c in subset)

        if not subset:
            jdf = self._jdf.dropDuplicatesWithinWatermark()
        elif len(subset) == 1 and isinstance(subset[0], list):
            jdf = self._jdf.dropDuplicatesWithinWatermark(self._jseq(subset[0]))
        else:
            jdf = self._jdf.dropDuplicatesWithinWatermark(self._jseq(subset))
        return DataFrame(jdf, self.sparkSession)

    def dropna(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> ParentDataFrame:
        if how is not None and how not in ["any", "all"]:
            raise PySparkValueError(
                error_class="VALUE_NOT_ANY_OR_ALL",
                message_parameters={"arg_name": "how", "arg_type": how},
            )

        if subset is None:
            subset = self.columns
        elif isinstance(subset, str):
            subset = [subset]
        elif not isinstance(subset, (list, tuple)):
            raise PySparkTypeError(
                error_class="NOT_LIST_OR_STR_OR_TUPLE",
                message_parameters={"arg_name": "subset", "arg_type": type(subset).__name__},
            )

        if thresh is None:
            thresh = len(subset) if how == "any" else 1

        return DataFrame(self._jdf.na().drop(thresh, self._jseq(subset)), self.sparkSession)

    @overload
    def fillna(
        self,
        value: "LiteralType",
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = ...,
    ) -> ParentDataFrame:
        ...

    @overload
    def fillna(self, value: Dict[str, "LiteralType"]) -> ParentDataFrame:
        ...

    def fillna(
        self,
        value: Union["LiteralType", Dict[str, "LiteralType"]],
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> ParentDataFrame:
        if not isinstance(value, (float, int, str, bool, dict)):
            raise PySparkTypeError(
                error_class="NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR",
                message_parameters={"arg_name": "value", "arg_type": type(value).__name__},
            )

        # Note that bool validates isinstance(int), but we don't want to
        # convert bools to floats

        if not isinstance(value, bool) and isinstance(value, int):
            value = float(value)

        if isinstance(value, dict):
            return DataFrame(self._jdf.na().fill(value), self.sparkSession)
        elif subset is None:
            return DataFrame(self._jdf.na().fill(value), self.sparkSession)
        else:
            if isinstance(subset, str):
                subset = [subset]
            elif not isinstance(subset, (list, tuple)):
                raise PySparkTypeError(
                    error_class="NOT_LIST_OR_TUPLE",
                    message_parameters={"arg_name": "subset", "arg_type": type(subset).__name__},
                )

            return DataFrame(self._jdf.na().fill(value, self._jseq(subset)), self.sparkSession)

    @overload
    def replace(
        self,
        to_replace: "LiteralType",
        value: "OptionalPrimitiveType",
        subset: Optional[List[str]] = ...,
    ) -> ParentDataFrame:
        ...

    @overload
    def replace(
        self,
        to_replace: List["LiteralType"],
        value: List["OptionalPrimitiveType"],
        subset: Optional[List[str]] = ...,
    ) -> ParentDataFrame:
        ...

    @overload
    def replace(
        self,
        to_replace: Dict["LiteralType", "OptionalPrimitiveType"],
        subset: Optional[List[str]] = ...,
    ) -> ParentDataFrame:
        ...

    @overload
    def replace(
        self,
        to_replace: List["LiteralType"],
        value: "OptionalPrimitiveType",
        subset: Optional[List[str]] = ...,
    ) -> ParentDataFrame:
        ...

    def replace(  # type: ignore[misc]
        self,
        to_replace: Union[
            "LiteralType", List["LiteralType"], Dict["LiteralType", "OptionalPrimitiveType"]
        ],
        value: Optional[
            Union["OptionalPrimitiveType", List["OptionalPrimitiveType"], _NoValueType]
        ] = _NoValue,
        subset: Optional[List[str]] = None,
    ) -> ParentDataFrame:
        if value is _NoValue:
            if isinstance(to_replace, dict):
                value = None
            else:
                raise PySparkTypeError(
                    error_class="ARGUMENT_REQUIRED",
                    message_parameters={"arg_name": "value", "condition": "`to_replace` is dict"},
                )

        # Helper functions
        def all_of(types: Union[Type, Tuple[Type, ...]]) -> Callable[[Iterable], bool]:
            """Given a type or tuple of types and a sequence of xs
            check if each x is instance of type(s)

            >>> all_of(bool)([True, False])
            True
            >>> all_of(str)(["a", 1])
            False
            """

            def all_of_(xs: Iterable) -> bool:
                return all(isinstance(x, types) for x in xs)

            return all_of_

        all_of_bool = all_of(bool)
        all_of_str = all_of(str)
        all_of_numeric = all_of((float, int))

        # Validate input types
        valid_types = (bool, float, int, str, list, tuple)
        if not isinstance(to_replace, valid_types + (dict,)):
            raise PySparkTypeError(
                error_class="NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE",
                message_parameters={
                    "arg_name": "to_replace",
                    "arg_type": type(to_replace).__name__,
                },
            )

        if (
            not isinstance(value, valid_types)
            and value is not None
            and not isinstance(to_replace, dict)
        ):
            raise PySparkTypeError(
                error_class="NOT_BOOL_OR_FLOAT_OR_INT_OR_LIST_OR_NONE_OR_STR_OR_TUPLE",
                message_parameters={
                    "arg_name": "value",
                    "arg_type": type(value).__name__,
                },
            )

        if isinstance(to_replace, (list, tuple)) and isinstance(value, (list, tuple)):
            if len(to_replace) != len(value):
                raise PySparkValueError(
                    error_class="LENGTH_SHOULD_BE_THE_SAME",
                    message_parameters={
                        "arg1": "to_replace",
                        "arg2": "value",
                        "arg1_length": str(len(to_replace)),
                        "arg2_length": str(len(value)),
                    },
                )

        if not (subset is None or isinstance(subset, (list, tuple, str))):
            raise PySparkTypeError(
                error_class="NOT_LIST_OR_STR_OR_TUPLE",
                message_parameters={"arg_name": "subset", "arg_type": type(subset).__name__},
            )

        # Reshape input arguments if necessary
        if isinstance(to_replace, (float, int, str)):
            to_replace = [to_replace]

        if isinstance(to_replace, dict):
            rep_dict = to_replace
            if value is not None:
                warnings.warn("to_replace is a dict and value is not None. value will be ignored.")
        else:
            if isinstance(value, (float, int, str)) or value is None:
                value = [value for _ in range(len(to_replace))]
            rep_dict = dict(zip(to_replace, cast("Iterable[Optional[Union[float, str]]]", value)))

        if isinstance(subset, str):
            subset = [subset]

        # Verify we were not passed in mixed type generics.
        if not any(
            all_of_type(rep_dict.keys())
            and all_of_type(x for x in rep_dict.values() if x is not None)
            for all_of_type in [all_of_bool, all_of_str, all_of_numeric]
        ):
            raise PySparkValueError(
                error_class="MIXED_TYPE_REPLACEMENT",
                message_parameters={},
            )

        if subset is None:
            return DataFrame(self._jdf.na().replace("*", rep_dict), self.sparkSession)
        else:
            return DataFrame(
                self._jdf.na().replace(self._jseq(subset), self._jmap(rep_dict)),
                self.sparkSession,
            )

    @overload
    def approxQuantile(
        self,
        col: str,
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> List[float]:
        ...

    @overload
    def approxQuantile(
        self,
        col: Union[List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> List[List[float]]:
        ...

    def approxQuantile(
        self,
        col: Union[str, List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        if not isinstance(col, (str, list, tuple)):
            raise PySparkTypeError(
                error_class="NOT_LIST_OR_STR_OR_TUPLE",
                message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
            )

        isStr = isinstance(col, str)

        if isinstance(col, tuple):
            col = list(col)
        elif isStr:
            col = [cast(str, col)]

        for c in col:
            if not isinstance(c, str):
                raise PySparkTypeError(
                    error_class="DISALLOWED_TYPE_FOR_CONTAINER",
                    message_parameters={
                        "arg_name": "col",
                        "arg_type": type(col).__name__,
                        "allowed_types": "str",
                        "item_type": type(c).__name__,
                    },
                )
        col = _to_list(self._sc, cast(List["ColumnOrName"], col))

        if not isinstance(probabilities, (list, tuple)):
            raise PySparkTypeError(
                error_class="NOT_LIST_OR_TUPLE",
                message_parameters={
                    "arg_name": "probabilities",
                    "arg_type": type(probabilities).__name__,
                },
            )
        if isinstance(probabilities, tuple):
            probabilities = list(probabilities)
        for p in probabilities:
            if not isinstance(p, (float, int)) or p < 0 or p > 1:
                raise PySparkTypeError(
                    error_class="NOT_LIST_OF_FLOAT_OR_INT",
                    message_parameters={
                        "arg_name": "probabilities",
                        "arg_type": type(p).__name__,
                    },
                )
        probabilities = _to_list(self._sc, cast(List["ColumnOrName"], probabilities))

        if not isinstance(relativeError, (float, int)):
            raise PySparkTypeError(
                error_class="NOT_FLOAT_OR_INT",
                message_parameters={
                    "arg_name": "relativeError",
                    "arg_type": type(relativeError).__name__,
                },
            )
        if relativeError < 0:
            raise PySparkValueError(
                error_class="NEGATIVE_VALUE",
                message_parameters={
                    "arg_name": "relativeError",
                    "arg_value": str(relativeError),
                },
            )
        relativeError = float(relativeError)

        jaq = self._jdf.stat().approxQuantile(col, probabilities, relativeError)
        jaq_list = [list(j) for j in jaq]
        return jaq_list[0] if isStr else jaq_list

    def corr(self, col1: str, col2: str, method: Optional[str] = None) -> float:
        if not isinstance(col1, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "col1", "arg_type": type(col1).__name__},
            )
        if not isinstance(col2, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "col2", "arg_type": type(col2).__name__},
            )
        if not method:
            method = "pearson"
        if not method == "pearson":
            raise PySparkValueError(
                error_class="VALUE_NOT_PEARSON",
                message_parameters={"arg_name": "method", "arg_value": method},
            )
        return self._jdf.stat().corr(col1, col2, method)

    def cov(self, col1: str, col2: str) -> float:
        if not isinstance(col1, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "col1", "arg_type": type(col1).__name__},
            )
        if not isinstance(col2, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "col2", "arg_type": type(col2).__name__},
            )
        return self._jdf.stat().cov(col1, col2)

    def crosstab(self, col1: str, col2: str) -> ParentDataFrame:
        if not isinstance(col1, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "col1", "arg_type": type(col1).__name__},
            )
        if not isinstance(col2, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "col2", "arg_type": type(col2).__name__},
            )
        return DataFrame(self._jdf.stat().crosstab(col1, col2), self.sparkSession)

    def freqItems(
        self, cols: Union[List[str], Tuple[str]], support: Optional[float] = None
    ) -> ParentDataFrame:
        if isinstance(cols, tuple):
            cols = list(cols)
        if not isinstance(cols, list):
            raise PySparkTypeError(
                error_class="NOT_LIST_OR_TUPLE",
                message_parameters={"arg_name": "cols", "arg_type": type(cols).__name__},
            )
        if not support:
            support = 0.01
        return DataFrame(
            self._jdf.stat().freqItems(_to_seq(self._sc, cols), support), self.sparkSession
        )

    def _ipython_key_completions_(self) -> List[str]:
        """Returns the names of columns in this :class:`DataFrame`.

        Examples
        --------
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age", "name"])
        >>> df._ipython_key_completions_()
        ['age', 'name']

        Would return illegal identifiers.
        >>> df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], ["age 1", "name?1"])
        >>> df._ipython_key_completions_()
        ['age 1', 'name?1']
        """
        return self.columns

    def withColumns(self, *colsMap: Dict[str, Column]) -> ParentDataFrame:
        # Below code is to help enable kwargs in future.
        assert len(colsMap) == 1
        colsMap = colsMap[0]  # type: ignore[assignment]

        if not isinstance(colsMap, dict):
            raise PySparkTypeError(
                error_class="NOT_DICT",
                message_parameters={"arg_name": "colsMap", "arg_type": type(colsMap).__name__},
            )

        col_names = list(colsMap.keys())
        cols = list(colsMap.values())

        return DataFrame(
            self._jdf.withColumns(_to_seq(self._sc, col_names), self._jcols(*cols)),
            self.sparkSession,
        )

    def withColumn(self, colName: str, col: Column) -> ParentDataFrame:
        if not isinstance(col, Column):
            raise PySparkTypeError(
                error_class="NOT_COLUMN",
                message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
            )
        return DataFrame(self._jdf.withColumn(colName, col._jc), self.sparkSession)

    def withColumnRenamed(self, existing: str, new: str) -> ParentDataFrame:
        return DataFrame(self._jdf.withColumnRenamed(existing, new), self.sparkSession)

    def withColumnsRenamed(self, colsMap: Dict[str, str]) -> ParentDataFrame:
        if not isinstance(colsMap, dict):
            raise PySparkTypeError(
                error_class="NOT_DICT",
                message_parameters={"arg_name": "colsMap", "arg_type": type(colsMap).__name__},
            )

        col_names: List[str] = []
        new_col_names: List[str] = []
        for k, v in colsMap.items():
            col_names.append(k)
            new_col_names.append(v)

        return DataFrame(
            self._jdf.withColumnsRenamed(
                _to_seq(self._sc, col_names), _to_seq(self._sc, new_col_names)
            ),
            self.sparkSession,
        )

    def withMetadata(self, columnName: str, metadata: Dict[str, Any]) -> ParentDataFrame:
        from py4j.java_gateway import JVMView

        if not isinstance(metadata, dict):
            raise PySparkTypeError(
                error_class="NOT_DICT",
                message_parameters={"arg_name": "metadata", "arg_type": type(metadata).__name__},
            )
        sc = get_active_spark_context()
        jmeta = cast(JVMView, sc._jvm).org.apache.spark.sql.types.Metadata.fromJson(
            json.dumps(metadata)
        )
        return DataFrame(self._jdf.withMetadata(columnName, jmeta), self.sparkSession)

    @overload
    def drop(self, cols: "ColumnOrName") -> ParentDataFrame:
        ...

    @overload
    def drop(self, *cols: str) -> ParentDataFrame:
        ...

    def drop(self, *cols: "ColumnOrName") -> ParentDataFrame:  # type: ignore[misc]
        column_names: List[str] = []
        java_columns: List["JavaObject"] = []

        for c in cols:
            if isinstance(c, str):
                column_names.append(c)
            elif isinstance(c, Column):
                java_columns.append(c._jc)
            else:
                raise PySparkTypeError(
                    error_class="NOT_COLUMN_OR_STR",
                    message_parameters={"arg_name": "col", "arg_type": type(c).__name__},
                )

        jdf = self._jdf
        if len(java_columns) > 0:
            first_column, *remaining_columns = java_columns
            jdf = jdf.drop(first_column, self._jseq(remaining_columns))
        if len(column_names) > 0:
            jdf = jdf.drop(self._jseq(column_names))

        return DataFrame(jdf, self.sparkSession)

    def toDF(self, *cols: str) -> ParentDataFrame:
        for col in cols:
            if not isinstance(col, str):
                raise PySparkTypeError(
                    error_class="NOT_LIST_OF_STR",
                    message_parameters={"arg_name": "cols", "arg_type": type(col).__name__},
                )
        jdf = self._jdf.toDF(self._jseq(cols))
        return DataFrame(jdf, self.sparkSession)

    def transform(
        self, func: Callable[..., ParentDataFrame], *args: Any, **kwargs: Any
    ) -> ParentDataFrame:
        result = func(self, *args, **kwargs)
        assert isinstance(
            result, DataFrame
        ), "Func returned an instance of type [%s], " "should have been DataFrame." % type(result)
        return result

    def sameSemantics(self, other: ParentDataFrame) -> bool:
        if not isinstance(other, DataFrame):
            raise PySparkTypeError(
                error_class="NOT_DATAFRAME",
                message_parameters={"arg_name": "other", "arg_type": type(other).__name__},
            )
        return self._jdf.sameSemantics(other._jdf)

    def semanticHash(self) -> int:
        return self._jdf.semanticHash()

    def inputFiles(self) -> List[str]:
        return list(self._jdf.inputFiles())

    def where(self, condition: "ColumnOrName") -> ParentDataFrame:
        return self.filter(condition)

    # Two aliases below were added for pandas compatibility many years ago.
    # There are too many differences compared to pandas and we cannot just
    # make it "compatible" by adding aliases. Therefore, we stop adding such
    # aliases as of Spark 3.0. Two methods below remain just
    # for legacy users currently.
    @overload
    def groupby(self, *cols: "ColumnOrNameOrOrdinal") -> "GroupedData":
        ...

    @overload
    def groupby(self, __cols: Union[List[Column], List[str], List[int]]) -> "GroupedData":
        ...

    def groupby(self, *cols: "ColumnOrNameOrOrdinal") -> "GroupedData":  # type: ignore[misc]
        return self.groupBy(*cols)

    def writeTo(self, table: str) -> DataFrameWriterV2:
        return DataFrameWriterV2(self, table)

    def mergeInto(self, table: str, condition: Column) -> MergeIntoWriter:
        return MergeIntoWriter(self, table, condition)

    def pandas_api(
        self, index_col: Optional[Union[str, List[str]]] = None
    ) -> "PandasOnSparkDataFrame":
        from pyspark.pandas.namespace import _get_index_map
        from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
        from pyspark.pandas.internal import InternalFrame

        index_spark_columns, index_names = _get_index_map(self, index_col)
        internal = InternalFrame(
            spark_frame=self,
            index_spark_columns=index_spark_columns,
            index_names=index_names,  # type: ignore[arg-type]
        )
        return PandasOnSparkDataFrame(internal)

    def mapInPandas(
        self,
        func: "PandasMapIterFunction",
        schema: Union[StructType, str],
        barrier: bool = False,
        profile: Optional[ResourceProfile] = None,
    ) -> ParentDataFrame:
        return PandasMapOpsMixin.mapInPandas(self, func, schema, barrier, profile)

    def mapInArrow(
        self,
        func: "ArrowMapIterFunction",
        schema: Union[StructType, str],
        barrier: bool = False,
        profile: Optional[ResourceProfile] = None,
    ) -> ParentDataFrame:
        return PandasMapOpsMixin.mapInArrow(self, func, schema, barrier, profile)

    def toArrow(self) -> "pa.Table":
        return PandasConversionMixin.toArrow(self)

    def toPandas(self) -> "PandasDataFrameLike":
        return PandasConversionMixin.toPandas(self)

    @property
    def executionInfo(self) -> Optional["ExecutionInfo"]:
        raise PySparkValueError(
            error_class="CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF",
            message_parameters={"member": "queryExecution"},
        )


class DataFrameNaFunctions(ParentDataFrameNaFunctions):
    def __init__(self, df: ParentDataFrame):
        self.df = df

    def drop(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Tuple[str, ...], List[str]]] = None,
    ) -> ParentDataFrame:
        return self.df.dropna(how=how, thresh=thresh, subset=subset)

    @overload
    def fill(self, value: "LiteralType", subset: Optional[List[str]] = ...) -> ParentDataFrame:
        ...

    @overload
    def fill(self, value: Dict[str, "LiteralType"]) -> ParentDataFrame:
        ...

    def fill(
        self,
        value: Union["LiteralType", Dict[str, "LiteralType"]],
        subset: Optional[List[str]] = None,
    ) -> ParentDataFrame:
        return self.df.fillna(value=value, subset=subset)  # type: ignore[arg-type]

    @overload
    def replace(
        self,
        to_replace: List["LiteralType"],
        value: List["OptionalPrimitiveType"],
        subset: Optional[List[str]] = ...,
    ) -> ParentDataFrame:
        ...

    @overload
    def replace(
        self,
        to_replace: Dict["LiteralType", "OptionalPrimitiveType"],
        subset: Optional[List[str]] = ...,
    ) -> ParentDataFrame:
        ...

    @overload
    def replace(
        self,
        to_replace: List["LiteralType"],
        value: "OptionalPrimitiveType",
        subset: Optional[List[str]] = ...,
    ) -> ParentDataFrame:
        ...

    def replace(  # type: ignore[misc]
        self,
        to_replace: Union[List["LiteralType"], Dict["LiteralType", "OptionalPrimitiveType"]],
        value: Optional[
            Union["OptionalPrimitiveType", List["OptionalPrimitiveType"], _NoValueType]
        ] = _NoValue,
        subset: Optional[List[str]] = None,
    ) -> ParentDataFrame:
        return self.df.replace(to_replace, value, subset)  # type: ignore[arg-type]


class DataFrameStatFunctions(ParentDataFrameStatFunctions):
    def __init__(self, df: ParentDataFrame):
        self.df = df

    @overload
    def approxQuantile(
        self,
        col: str,
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> List[float]:
        ...

    @overload
    def approxQuantile(
        self,
        col: Union[List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> List[List[float]]:
        ...

    def approxQuantile(
        self,
        col: Union[str, List[str], Tuple[str]],
        probabilities: Union[List[float], Tuple[float]],
        relativeError: float,
    ) -> Union[List[float], List[List[float]]]:
        return self.df.approxQuantile(col, probabilities, relativeError)

    def corr(self, col1: str, col2: str, method: Optional[str] = None) -> float:
        return self.df.corr(col1, col2, method)

    def cov(self, col1: str, col2: str) -> float:
        return self.df.cov(col1, col2)

    def crosstab(self, col1: str, col2: str) -> ParentDataFrame:
        return self.df.crosstab(col1, col2)

    def freqItems(self, cols: List[str], support: Optional[float] = None) -> ParentDataFrame:
        return self.df.freqItems(cols, support)

    def sampleBy(
        self, col: str, fractions: Dict[Any, float], seed: Optional[int] = None
    ) -> ParentDataFrame:
        return self.df.sampleBy(col, fractions, seed)


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.dataframe

    # It inherits docstrings but doctests cannot detect them so we run
    # the parent classe's doctests here directly.
    globs = pyspark.sql.dataframe.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[4]").appName("sql.classic.dataframe tests").getOrCreate()
    )
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.dataframe,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
