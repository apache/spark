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
import sys
from typing import Union, TYPE_CHECKING, Optional

from pyspark.resource.requests import ExecutorResourceRequests, TaskResourceRequests
from pyspark.resource import ResourceProfile
from pyspark.util import PythonEvalType
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.pandas._typing import PandasMapIterFunction, ArrowMapIterFunction


class PandasMapOpsMixin:
    """
    Mix-in for pandas map operations. Currently, only :class:`DataFrame`
    can use this class.
    """

    def mapInPandas(
        self,
        func: "PandasMapIterFunction",
        schema: Union[StructType, str],
        barrier: bool = False,
        profile: Optional[ResourceProfile] = None,
    ) -> "DataFrame":
        from pyspark.sql import DataFrame
        from pyspark.sql.pandas.functions import pandas_udf

        assert isinstance(self, DataFrame)

        # Seamless observed-accumulator support: if func captures an accumulator, emit a hidden
        # delta column and observe it (see observed_accumulator.maybe_wrap_operator).
        from pyspark.sql.observed_accumulator import maybe_wrap_operator

        finalize = None
        hooked = maybe_wrap_operator(self, func, schema, PythonEvalType.SQL_MAP_PANDAS_ITER_UDF)
        if hooked is not None:
            func, schema, finalize = hooked

        # The usage of the pandas_udf is internal so type checking is disabled.
        udf = pandas_udf(
            func, returnType=schema, functionType=PythonEvalType.SQL_MAP_PANDAS_ITER_UDF
        )  # type: ignore[call-overload]
        udf_column = udf(*[self[col] for col in self.columns])

        jrp = self._build_java_profile(profile)
        jdf = self._jdf.mapInPandas(udf_column._jc, barrier, jrp)
        result = DataFrame(jdf, self.sparkSession)
        return finalize(result) if finalize is not None else result

    def mapInArrow(
        self,
        func: "ArrowMapIterFunction",
        schema: Union[StructType, str],
        barrier: bool = False,
        profile: Optional[ResourceProfile] = None,
    ) -> "DataFrame":
        from pyspark.sql import DataFrame
        from pyspark.sql.pandas.functions import pandas_udf

        assert isinstance(self, DataFrame)

        from pyspark.sql.observed_accumulator import maybe_wrap_operator

        finalize = None
        hooked = maybe_wrap_operator(self, func, schema, PythonEvalType.SQL_MAP_ARROW_ITER_UDF)
        if hooked is not None:
            func, schema, finalize = hooked

        # The usage of the pandas_udf is internal so type checking is disabled.
        udf = pandas_udf(
            func, returnType=schema, functionType=PythonEvalType.SQL_MAP_ARROW_ITER_UDF
        )  # type: ignore[call-overload]
        udf_column = udf(*[self[col] for col in self.columns])

        jrp = self._build_java_profile(profile)
        jdf = self._jdf.mapInArrow(udf_column._jc, barrier, jrp)
        result = DataFrame(jdf, self.sparkSession)
        return finalize(result) if finalize is not None else result

    def _build_java_profile(
        self, profile: Optional[ResourceProfile] = None
    ) -> Optional["JavaObject"]:
        """Build the java ResourceProfile based on PySpark ResourceProfile"""
        from pyspark.sql import DataFrame

        assert isinstance(self, DataFrame)

        jrp = None
        if profile is not None:
            if profile._java_resource_profile is not None:
                jrp = profile._java_resource_profile
            else:
                jvm = self.sparkSession.sparkContext._jvm
                assert jvm is not None

                builder = getattr(jvm, "org.apache.spark.resource.ResourceProfileBuilder")()
                ereqs = ExecutorResourceRequests(jvm, profile._executor_resource_requests)
                treqs = TaskResourceRequests(jvm, profile._task_resource_requests)
                builder.require(ereqs._java_executor_resource_requests)
                builder.require(treqs._java_task_resource_requests)
                jrp = builder.build()
        return jrp


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.pandas.map_ops

    globs = pyspark.sql.pandas.map_ops.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[4]").appName("sql.pandas.map_ops tests").getOrCreate()
    )
    globs["spark"] = spark
    failure_count, test_count = doctest.testmod(
        pyspark.sql.pandas.map_ops,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
