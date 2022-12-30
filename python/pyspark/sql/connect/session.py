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

import numpy as np
import pandas as pd
import pyarrow as pa

from pyspark.sql.session import classproperty, SparkSession as PySparkSession
from pyspark.sql.types import DataType, StructType
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


class SparkSession(object):
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

        # Create the Pandas DataFrame
        if isinstance(data, pd.DataFrame):
            pdf = data

        elif isinstance(data, np.ndarray):
            # `data` of numpy.ndarray type will be converted to a pandas DataFrame,
            if data.ndim not in [1, 2]:
                raise ValueError("NumPy array input should be of 1 or 2 dimensions.")

            pdf = pd.DataFrame(data)

            if _cols is None:
                if data.ndim == 1 or data.shape[1] == 1:
                    _cols = ["value"]
                else:
                    _cols = ["_%s" % i for i in range(1, data.shape[1] + 1)]

        else:
            pdf = pd.DataFrame(list(data))

            if _cols is None:
                _cols = ["_%s" % i for i in range(1, pdf.shape[1] + 1)]

        # Validate number of columns
        num_cols = pdf.shape[1]
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

        table = pa.Table.from_pandas(pdf)

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
