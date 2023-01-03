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
from threading import RLock
from collections.abc import Sized
from functools import reduce

import numpy as np
import pandas as pd
import pyarrow as pa

from pyspark import SparkContext, SparkConf
from pyspark.sql.session import classproperty, SparkSession as PySparkSession
from pyspark.sql.types import (
    _infer_schema,
    _has_nulltype,
    _merge_type,
    Row,
    DataType,
    StructType,
)
from pyspark.sql.utils import to_str

from pyspark.sql.connect.client import SparkConnectClient
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import SQL, Range, LocalRelation
from pyspark.sql.connect.readwriter import DataFrameReader

from typing import (
    Optional,
    Any,
    Union,
    Dict,
    List,
    Tuple,
    cast,
    overload,
    Iterable,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import OptionalPrimitiveType
    from pyspark.sql.connect.catalog import Catalog


class SparkSession:
    class Builder:
        """Builder for :class:`SparkSession`."""

        _lock = RLock()

        def __init__(self) -> None:
            self._options: Dict[str, Any] = {}

        @overload
        def config(self, key: str, value: Any) -> "SparkSession.Builder":
            ...

        @overload
        def config(self, *, map: Dict[str, "OptionalPrimitiveType"]) -> "SparkSession.Builder":
            ...

        def config(
            self,
            key: Optional[str] = None,
            value: Optional[Any] = None,
            *,
            map: Optional[Dict[str, "OptionalPrimitiveType"]] = None,
        ) -> "SparkSession.Builder":
            with self._lock:
                if map is not None:
                    for k, v in map.items():
                        self._options[k] = to_str(v)
                else:
                    self._options[cast(str, key)] = to_str(value)
                return self

        def master(self, master: str) -> "SparkSession.Builder":
            return self

        def appName(self, name: str) -> "SparkSession.Builder":
            return self.config("spark.app.name", name)

        def remote(self, location: str = "sc://localhost") -> "SparkSession.Builder":
            return self.config("spark.remote", location)

        def enableHiveSupport(self) -> "SparkSession.Builder":
            raise NotImplementedError("enableHiveSupport not implemented for Spark Connect")

        def getOrCreate(self) -> "SparkSession":
            return SparkSession(connectionString=self._options["spark.remote"])

    _client: SparkConnectClient

    @classproperty
    def builder(cls) -> Builder:
        """Creates a :class:`Builder` for constructing a :class:`SparkSession`."""
        return cls.Builder()

    def __init__(self, connectionString: str, userId: Optional[str] = None):
        """
        Creates a new SparkSession for the Spark Connect interface.

        Parameters
        ----------
        connectionString: str, optional
            Connection string that is used to extract the connection parameters and configure
            the GRPC connection. Defaults to `sc://localhost`.
        userId : str, optional
            Optional unique user ID that is used to differentiate multiple users and
            isolate their Spark Sessions. If the `user_id` is not set, will default to
            the $USER environment. Defining the user ID as part of the connection string
            takes precedence.
        """
        # Parse the connection string.
        self._client = SparkConnectClient(connectionString)

    def table(self, tableName: str) -> DataFrame:
        return self.read.table(tableName)

    table.__doc__ = PySparkSession.table.__doc__

    @property
    def read(self) -> "DataFrameReader":
        return DataFrameReader(self)

    read.__doc__ = PySparkSession.read.__doc__

    def _inferSchemaFromList(
        self, data: Iterable[Any], names: Optional[List[str]] = None
    ) -> StructType:
        """
        Infer schema from list of Row, dict, or tuple.

        Refer to 'pyspark.sql.session._inferSchemaFromList' with default configurations:

          - 'infer_dict_as_struct' : False
          - 'infer_array_from_first_element' : False
          - 'prefer_timestamp_ntz' : False
        """
        if not data:
            raise ValueError("can not infer schema from empty dataset")
        infer_dict_as_struct = False
        infer_array_from_first_element = False
        prefer_timestamp_ntz = False
        schema = reduce(
            _merge_type,
            (
                _infer_schema(
                    row,
                    names,
                    infer_dict_as_struct=infer_dict_as_struct,
                    infer_array_from_first_element=infer_array_from_first_element,
                    prefer_timestamp_ntz=prefer_timestamp_ntz,
                )
                for row in data
            ),
        )
        if _has_nulltype(schema):
            raise ValueError("Some of types cannot be determined after inferring")
        return schema

    def createDataFrame(
        self,
        data: Union["pd.DataFrame", "np.ndarray", Iterable[Any]],
        schema: Optional[Union[StructType, str, List[str], Tuple[str, ...]]] = None,
    ) -> "DataFrame":
        assert data is not None
        if isinstance(data, DataFrame):
            raise TypeError("data is already a DataFrame")
        if isinstance(data, Sized) and len(data) == 0:
            raise ValueError("Input data cannot be empty")

        table: Optional[pa.Table] = None
        _schema: Optional[StructType] = None
        _schema_str: Optional[str] = None
        _cols: Optional[List[str]] = None

        if isinstance(schema, StructType):
            _schema = schema

        elif isinstance(schema, str):
            _schema_str = schema

        elif isinstance(schema, (list, tuple)):
            # Must re-encode any unicode strings to be consistent with StructField names
            _cols = [x.encode("utf-8") if not isinstance(x, str) else x for x in schema]

        if isinstance(data, pd.DataFrame):
            table = pa.Table.from_pandas(data)

        elif isinstance(data, np.ndarray):
            if data.ndim not in [1, 2]:
                raise ValueError("NumPy array input should be of 1 or 2 dimensions.")

            if _cols is None:
                if data.ndim == 1 or data.shape[1] == 1:
                    _cols = ["value"]
                else:
                    _cols = ["_%s" % i for i in range(1, data.shape[1] + 1)]

            if data.ndim == 1:
                if 1 != len(_cols):
                    raise ValueError(
                        f"Length mismatch: Expected axis has 1 element, "
                        f"new values have {len(_cols)} elements"
                    )

                table = pa.Table.from_arrays([pa.array(data)], _cols)
            else:
                if data.shape[1] != len(_cols):
                    raise ValueError(
                        f"Length mismatch: Expected axis has {data.shape[1]} elements, "
                        f"new values have {len(_cols)} elements"
                    )

                table = pa.Table.from_arrays(
                    [pa.array(data[::, i]) for i in range(0, data.shape[1])], _cols
                )

        else:
            _data = list(data)

            if _schema is None and isinstance(_data[0], (Row, dict)):
                if isinstance(_data[0], dict):
                    # Sort the data to respect inferred schema.
                    # For dictionaries, we sort the schema in alphabetical order.
                    _data = [dict(sorted(d.items())) for d in _data]

                _schema = self._inferSchemaFromList(_data, _cols)
                if _cols is not None:
                    for i, name in enumerate(_cols):
                        _schema.fields[i].name = name
                        _schema.names[i] = name

            if _cols is None:
                if _schema is None:
                    if isinstance(_data[0], (list, tuple)):
                        _cols = ["_%s" % i for i in range(1, len(_data[0]) + 1)]
                    else:
                        _cols = ["_1"]
                else:
                    _cols = _schema.names

            if isinstance(_data[0], Row):
                table = pa.Table.from_pylist([row.asDict(recursive=True) for row in _data])
            elif isinstance(_data[0], dict):
                table = pa.Table.from_pylist(_data)
            elif isinstance(_data[0], (list, tuple)):
                table = pa.Table.from_pylist([dict(zip(_cols, list(item))) for item in _data])
            else:
                # input data can be [1, 2, 3]
                table = pa.Table.from_pylist([dict(zip(_cols, [item])) for item in _data])

        # Validate number of columns
        num_cols = table.shape[1]
        if _schema is not None and len(_schema.fields) != num_cols:
            raise ValueError(
                f"Length mismatch: Expected axis has {num_cols} elements, "
                f"new values have {len(_schema.fields)} elements"
            )
        elif _cols is not None and len(_cols) != num_cols:
            raise ValueError(
                f"Length mismatch: Expected axis has {num_cols} elements, "
                f"new values have {len(_cols)} elements"
            )

        if _schema is not None:
            return DataFrame.withPlan(LocalRelation(table, schema=_schema), self)
        elif _schema_str is not None:
            return DataFrame.withPlan(LocalRelation(table, schema=_schema_str), self)
        elif _cols is not None and len(_cols) > 0:
            return DataFrame.withPlan(LocalRelation(table), self).toDF(*_cols)
        else:
            return DataFrame.withPlan(LocalRelation(table), self)

    createDataFrame.__doc__ = PySparkSession.createDataFrame.__doc__

    def sql(self, sqlQuery: str) -> "DataFrame":
        return DataFrame.withPlan(SQL(sqlQuery), self)

    sql.__doc__ = PySparkSession.sql.__doc__

    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numPartitions: Optional[int] = None,
    ) -> DataFrame:
        if end is None:
            actual_end = start
            start = 0
        else:
            actual_end = end

        return DataFrame.withPlan(
            Range(start=start, end=actual_end, step=step, num_partitions=numPartitions), self
        )

    range.__doc__ = PySparkSession.range.__doc__

    @property
    def catalog(self) -> "Catalog":
        from pyspark.sql.connect.catalog import Catalog

        if not hasattr(self, "_catalog"):
            self._catalog = Catalog(self)
        return self._catalog

    catalog.__doc__ = PySparkSession.catalog.__doc__

    def stop(self) -> None:
        self.client.close()

    stop.__doc__ = PySparkSession.stop.__doc__

    # SparkConnect-specific API
    @property
    def client(self) -> "SparkConnectClient":
        """
        Gives access to the Spark Connect client. In normal cases this is not necessary to be used
        and only relevant for testing.
        Returns
        -------
        :class:`SparkConnectClient`
        """
        return self._client

    def register_udf(self, function: Any, return_type: Union[str, DataType]) -> str:
        return self._client.register_udf(function, return_type)


SparkSession.__doc__ = PySparkSession.__doc__


def _test() -> None:
    import os
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

    os.chdir(os.environ["SPARK_HOME"])

    if should_test_connect:
        import pyspark.sql.connect.session

        globs = pyspark.sql.connect.session.__dict__.copy()
        # Works around to create a regular Spark session
        sc = SparkContext("local[4]", "sql.connect.session tests", conf=SparkConf())
        globs["_spark"] = PySparkSession(
            sc, options={"spark.app.name": "sql.connect.session tests"}
        )

        # Creates a remote Spark session.
        os.environ["SPARK_REMOTE"] = "sc://localhost"
        globs["spark"] = PySparkSession.builder.remote("sc://localhost").getOrCreate()

        # Uses PySpark session to test builder.
        globs["SparkSession"] = PySparkSession
        # Spark Connect does not support to set master together.
        pyspark.sql.connect.session.SparkSession.__doc__ = None
        del pyspark.sql.connect.session.SparkSession.Builder.master.__doc__
        # RDD API is not supported in Spark Connect.
        del pyspark.sql.connect.session.SparkSession.createDataFrame.__doc__

        # TODO(SPARK-41811): Implement SparkSession.sql's string formatter
        del pyspark.sql.connect.session.SparkSession.sql.__doc__

        (failure_count, test_count) = doctest.testmod(
            pyspark.sql.connect.session,
            globs=globs,
            optionflags=doctest.ELLIPSIS
            | doctest.NORMALIZE_WHITESPACE
            | doctest.IGNORE_EXCEPTION_DETAIL,
        )

        globs["spark"].stop()
        globs["_spark"].stop()
        if failure_count:
            sys.exit(-1)
    else:
        print(
            f"Skipping pyspark.sql.connect.session doctests: {connect_requirement_message}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    _test()
