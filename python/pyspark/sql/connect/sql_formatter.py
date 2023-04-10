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

import string
import typing
from typing import Any, Optional, List, Tuple, Sequence, Mapping
import uuid

from pyspark.sql.connect.proto import Expression

if typing.TYPE_CHECKING:
    from pyspark.sql.connect import SparkSession, DataFrame
from pyspark.sql.connect.functions import lit


class SQLStringFormatter(string.Formatter):
    """
    A standard ``string.Formatter`` in Python that can understand PySpark instances
    with basic Python objects. This object has to be clear after the use for single SQL
    query; cannot be reused across multiple SQL queries without cleaning.
    """

    def __init__(self, session: "SparkSession") -> None:
        self._session: "SparkSession" = session
        self._temp_views: List[Tuple[DataFrame, str]] = []
        self._unresolvedColumns: typing.Dict[str, Expression] = {}

    def get_field(self, field_name: str, args: Sequence[Any], kwargs: Mapping[str, Any]) -> Any:
        obj, first = super(SQLStringFormatter, self).get_field(field_name, args, kwargs)
        return self._convert_value(obj, field_name), first

    def _convert_value(self, val: Any, field_name: str) -> Optional[str]:
        """
        Converts the given value into a SQL string.
        """
        from pyspark.sql.connect import Column, DataFrame

        if isinstance(val, DataFrame):
            for df, n in self._temp_views:
                if df is val:
                    return n
            df_name = "_pyspark_%s" % str(uuid.uuid4()).replace("-", "")
            self._temp_views.append((val, df_name))
            val.createOrReplaceTempView(df_name)
            return df_name
        if isinstance(val, Column):
            for f, _ in self._unresolvedColumns.items():
                if f is field_name:
                    break
            self._unresolvedColumns[field_name] = val.expr.to_plan(self._session.client)
            return wrap(field_name)
        elif isinstance(val, str):
            self._unresolvedColumns[field_name] = lit(val).expr.to_plan(self._session.client)
            return wrap(field_name)
        return val

    def clear(self) -> None:
        for _, n in self._temp_views:
            self._session.catalog.dropTempView(n)
        self._temp_views = []

    @property
    def unresolvedColumns(self):
        return self._unresolvedColumns


def wrap(field_name: str) -> str:
    return "{%s}" % field_name
