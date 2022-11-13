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


from typing import Dict, Optional

from pyspark.sql.connect.column import PrimitiveType
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import Read, DataSource
from pyspark.sql.utils import to_str


OptionalPrimitiveType = Optional[PrimitiveType]

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.connect.client import RemoteSparkSession


class DataFrameReader:
    """
    TODO(SPARK-40539) Achieve parity with PySpark.
    """

    def __init__(self, client: "RemoteSparkSession"):
        self._client = client
        self._format = ""
        self._schema = ""
        self._options: Dict[str, str] = {}

    def format(self, source: str) -> "DataFrameReader":
        """
        Specifies the input data source format.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        source : str
        string, name of the data source, e.g. 'json', 'parquet'.

        """
        self._format = source
        return self

    # TODO(SPARK-40539): support StructType in python client and support schema as StructType.
    def schema(self, schema: str) -> "DataFrameReader":
        """
        Specifies the input schema.

        Some data sources (e.g. JSON) can infer the input schema automatically from data.
        By specifying the schema here, the underlying data source can skip the schema
        inference step, and thus speed up data loading.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        schema : str
        a DDL-formatted string
        (For example ``col0 INT, col1 DOUBLE``).

        """
        self._schema = schema
        return self

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataFrameReader":
        """
        Adds an input option for the underlying data source.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        key : str
            The key for the option to set. key string is case-insensitive.
        value
            The value for the option to set.

        """
        self._options[key] = str(value)
        return self

    def options(self, **options: "OptionalPrimitiveType") -> "DataFrameReader":
        """
        Adds input options for the underlying data source.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        **options : dict
            The dictionary of string keys and prmitive-type values.
        """
        for k in options:
            self.option(k, to_str(options[k]))
        return self

    def load(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        schema: Optional[str] = None,
        **options: "OptionalPrimitiveType",
    ) -> "DataFrame":
        """
        Loads data from a data source and returns it as a :class:`DataFrame`.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        path : str or list, optional
            optional string or a list of string for file-system backed data sources.
        format : str, optional
            optional string for format of the data source.
        schema : str, optional
            optional DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        **options : dict
            all other string options
        """
        if format is not None:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        self.options(**options)
        if path is not None:
            self.option("path", path)

        plan = DataSource(format=self._format, schema=self._schema, options=self._options)
        df = DataFrame.withPlan(plan, self._client)
        return df

    def table(self, tableName: str) -> "DataFrame":
        df = DataFrame.withPlan(Read(tableName), self._client)
        return df
