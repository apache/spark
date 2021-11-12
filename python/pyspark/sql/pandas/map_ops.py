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
from typing import Union, TYPE_CHECKING

from pyspark.rdd import PythonEvalType
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.pandas._typing import PandasMapIterFunction, ArrowMapIterFunction


class PandasMapOpsMixin(object):
    """
    Min-in for pandas map operations. Currently, only :class:`DataFrame`
    can use this class.
    """

    def mapInPandas(
        self, func: "PandasMapIterFunction", schema: Union[StructType, str]
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
        `spark.sql.execution.arrow.maxRecordsPerBatch`.

        .. versionadded:: 3.0.0

        Parameters
        ----------
        func : function
            a Python native function that takes an iterator of `pandas.DataFrame`\\s, and
            outputs an iterator of `pandas.DataFrame`\\s.
        schema : :class:`pyspark.sql.types.DataType` or str
            the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.

        Examples
        --------
        >>> from pyspark.sql.functions import pandas_udf
        >>> df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))
        >>> def filter_func(iterator):
        ...     for pdf in iterator:
        ...         yield pdf[pdf.id == 1]
        >>> df.mapInPandas(filter_func, df.schema).show()  # doctest: +SKIP
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
        jdf = self._jdf.mapInPandas(udf_column._jc.expr())  # type: ignore[operator]
        return DataFrame(jdf, self.sql_ctx)

    def mapInArrow(
        self, func: "ArrowMapIterFunction", schema: Union[StructType, str]
    ) -> "DataFrame":
        """
        Maps an iterator of batches in the current :class:`DataFrame` using a Python native
        function that takes and outputs a PyArrow's `RecordBatch`, and returns the result as a
        :class:`DataFrame`.

        The function should take an iterator of `pyarrow.RecordBatch`\\s and return
        another iterator of `pyarrow.RecordBatch`\\s. All columns are passed
        together as an iterator of `pyarrow.RecordBatch`\\s to the function and the
        returned iterator of `pyarrow.RecordBatch`\\s are combined as a :class:`DataFrame`.
        Each `pyarrow.RecordBatch` size can be controlled by
        `spark.sql.execution.arrow.maxRecordsPerBatch`.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        func : function
            a Python native function that takes an iterator of `pyarrow.RecordBatch`\\s, and
            outputs an iterator of `pyarrow.RecordBatch`\\s.
        schema : :class:`pyspark.sql.types.DataType` or str
            the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.

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
        jdf = self._jdf.pythonMapInArrow(udf_column._jc.expr())
        return DataFrame(jdf, self.sql_ctx)


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
