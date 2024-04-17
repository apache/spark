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
    Min-in for pandas map operations. Currently, only :class:`DataFrame`
    can use this class.
    """

    def mapInPandas(
        self,
        func: "PandasMapIterFunction",
        schema: Union[StructType, str],
        barrier: bool = False,
        profile: Optional[ResourceProfile] = None,
    ) -> "DataFrame":
        """
        Maps an iterator of batches in the current :class:`DataFrame` using a Python native
        function that takes and outputs a pandas DataFrame, and returns the result as a
        :class:`DataFrame`.

        The function should take an iterator of `pandas.DataFrame`\\s and return
        another iterator of `pandas.DataFrame`\\s. All columns are passed
        together as an iterator of `pandas.DataFrame`\\s to the function and the
        returned iterator of `pandas.DataFrame`\\s are combined as a :class:`DataFrame`.
        Each `pandas.DataFrame` size can be controlled by
        `spark.sql.execution.arrow.maxRecordsPerBatch`. The size of the function's input and
        output can be different.

        .. versionadded:: 3.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        func : function
            a Python native function that takes an iterator of `pandas.DataFrame`\\s, and
            outputs an iterator of `pandas.DataFrame`\\s.
        schema : :class:`pyspark.sql.types.DataType` or str
            the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        barrier : bool, optional, default False
            Use barrier mode execution.

            .. versionadded: 3.5.0

        profile : :class:`pyspark.resource.ResourceProfile`. The optional ResourceProfile
            to be used for mapInPandas.

            .. versionadded: 4.0.0


        Examples
        --------
        >>> df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

        Filter rows with id equal to 1:

        >>> def filter_func(iterator):
        ...     for pdf in iterator:
        ...         yield pdf[pdf.id == 1]
        ...
        >>> df.mapInPandas(filter_func, df.schema).show()  # doctest: +SKIP
        +---+---+
        | id|age|
        +---+---+
        |  1| 21|
        +---+---+

        Compute the mean age for each id:

        >>> def mean_age(iterator):
        ...     for pdf in iterator:
        ...         yield pdf.groupby("id").mean().reset_index()
        ...
        >>> df.mapInPandas(mean_age, "id: bigint, age: double").show()  # doctest: +SKIP
        +---+----+
        | id| age|
        +---+----+
        |  1|21.0|
        |  2|30.0|
        +---+----+

        Add a new column with the double of the age:

        >>> def double_age(iterator):
        ...     for pdf in iterator:
        ...         pdf["double_age"] = pdf["age"] * 2
        ...         yield pdf
        ...
        >>> df.mapInPandas(
        ...     double_age, "id: bigint, age: bigint, double_age: bigint").show()  # doctest: +SKIP
        +---+---+----------+
        | id|age|double_age|
        +---+---+----------+
        |  1| 21|        42|
        |  2| 30|        60|
        +---+---+----------+

        Set ``barrier`` to ``True`` to force the ``mapInPandas`` stage running in the
        barrier mode, it ensures all Python workers in the stage will be
        launched concurrently.

        >>> df.mapInPandas(filter_func, df.schema, barrier=True).show()  # doctest: +SKIP
        +---+---+
        | id|age|
        +---+---+
        |  1| 21|
        +---+---+

        Notes
        -----
        This API is experimental

        See Also
        --------
        pyspark.sql.functions.pandas_udf
        """
        from pyspark.sql import DataFrame
        from pyspark.sql.pandas.functions import pandas_udf

        assert isinstance(self, DataFrame)

        # The usage of the pandas_udf is internal so type checking is disabled.
        udf = pandas_udf(
            func, returnType=schema, functionType=PythonEvalType.SQL_MAP_PANDAS_ITER_UDF
        )  # type: ignore[call-overload]
        udf_column = udf(*[self[col] for col in self.columns])

        jrp = self._build_java_profile(profile)
        jdf = self._jdf.mapInPandas(udf_column._jc.expr(), barrier, jrp)
        return DataFrame(jdf, self.sparkSession)

    def mapInArrow(
        self,
        func: "ArrowMapIterFunction",
        schema: Union[StructType, str],
        barrier: bool = False,
        profile: Optional[ResourceProfile] = None,
    ) -> "DataFrame":
        """
        Maps an iterator of batches in the current :class:`DataFrame` using a Python native
        function that is performed on `pyarrow.RecordBatch`\\s both as input and output,
        and returns the result as a :class:`DataFrame`.

        This method applies the specified Python function to an iterator of
        `pyarrow.RecordBatch`\\s, each representing a batch of rows from the original DataFrame.
        The returned iterator of `pyarrow.RecordBatch`\\s are combined as a :class:`DataFrame`.
        The size of the function's input and output can be different. Each `pyarrow.RecordBatch`
        size can be controlled by `spark.sql.execution.arrow.maxRecordsPerBatch`.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        func : function
            a Python native function that takes an iterator of `pyarrow.RecordBatch`\\s, and
            outputs an iterator of `pyarrow.RecordBatch`\\s.
        schema : :class:`pyspark.sql.types.DataType` or str
            the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        barrier : bool, optional, default False
            Use barrier mode execution, ensuring that all Python workers in the stage will be
            launched concurrently.

            .. versionadded: 3.5.0

        profile : :class:`pyspark.resource.ResourceProfile`. The optional ResourceProfile
            to be used for mapInArrow.

            .. versionadded: 4.0.0

        Examples
        --------
        >>> import pyarrow  # doctest: +SKIP
        >>> df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))
        >>> def filter_func(iterator):
        ...     for batch in iterator:
        ...         pdf = batch.to_pandas()
        ...         yield pyarrow.RecordBatch.from_pandas(pdf[pdf.id == 1])
        >>> df.mapInArrow(filter_func, df.schema).show()  # doctest: +SKIP
        +---+---+
        | id|age|
        +---+---+
        |  1| 21|
        +---+---+

        Set ``barrier`` to ``True`` to force the ``mapInArrow`` stage running in the
        barrier mode, it ensures all Python workers in the stage will be
        launched concurrently.

        >>> df.mapInArrow(filter_func, df.schema, barrier=True).show()  # doctest: +SKIP
        +---+---+
        | id|age|
        +---+---+
        |  1| 21|
        +---+---+

        Notes
        -----
        This API is unstable, and for developers.

        See Also
        --------
        pyspark.sql.functions.pandas_udf
        pyspark.sql.DataFrame.mapInPandas
        """
        from pyspark.sql import DataFrame
        from pyspark.sql.pandas.functions import pandas_udf

        assert isinstance(self, DataFrame)

        # The usage of the pandas_udf is internal so type checking is disabled.
        udf = pandas_udf(
            func, returnType=schema, functionType=PythonEvalType.SQL_MAP_ARROW_ITER_UDF
        )  # type: ignore[call-overload]
        udf_column = udf(*[self[col] for col in self.columns])

        jrp = self._build_java_profile(profile)
        jdf = self._jdf.mapInArrow(udf_column._jc.expr(), barrier, jrp)
        return DataFrame(jdf, self.sparkSession)

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

                builder = jvm.org.apache.spark.resource.ResourceProfileBuilder()
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
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.pandas.map_ops,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
