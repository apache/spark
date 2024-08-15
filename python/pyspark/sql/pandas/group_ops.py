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
from typing import Any, Iterator, List, Union, TYPE_CHECKING, cast
import warnings

from pyspark.errors import PySparkTypeError
from pyspark.util import PythonEvalType
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.streaming.stateful_processor_api_client import (
    StatefulProcessorApiClient,
    StatefulProcessorHandleState,
)
from pyspark.sql.streaming.stateful_processor import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType, _parse_datatype_string

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import (
        GroupedMapPandasUserDefinedFunction,
        PandasGroupedMapFunction,
        PandasGroupedMapFunctionWithState,
        PandasCogroupedMapFunction,
        ArrowGroupedMapFunction,
        ArrowCogroupedMapFunction,
        DataFrameLike as PandasDataFrameLike,
    )
    from pyspark.sql.group import GroupedData


class PandasGroupedOpsMixin:
    """
    Min-in for pandas grouped operations. Currently, only :class:`GroupedData`
    can use this class.
    """

    def apply(self, udf: "GroupedMapPandasUserDefinedFunction") -> DataFrame:
        """
        It is an alias of :meth:`pyspark.sql.GroupedData.applyInPandas`; however, it takes a
        :meth:`pyspark.sql.functions.pandas_udf` whereas
        :meth:`pyspark.sql.GroupedData.applyInPandas` takes a Python native function.

        .. versionadded:: 2.3.0

        .. versionchanged:: 3.4.0
            Support Spark Connect.

        Parameters
        ----------
        udf : :func:`pyspark.sql.functions.pandas_udf`
            a grouped map user-defined function returned by
            :func:`pyspark.sql.functions.pandas_udf`.

        Notes
        -----
        It is preferred to use :meth:`pyspark.sql.GroupedData.applyInPandas` over this
        API. This API will be deprecated in the future releases.

        Examples
        --------
        >>> from pyspark.sql.functions import pandas_udf, PandasUDFType
        >>> df = spark.createDataFrame(
        ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ...     ("id", "v"))
        >>> @pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)  # doctest: +SKIP
        ... def normalize(pdf):
        ...     v = pdf.v
        ...     return pdf.assign(v=(v - v.mean()) / v.std())
        ...
        >>> df.groupby("id").apply(normalize).show()  # doctest: +SKIP
        +---+-------------------+
        | id|                  v|
        +---+-------------------+
        |  1|-0.7071067811865475|
        |  1| 0.7071067811865475|
        |  2|-0.8320502943378437|
        |  2|-0.2773500981126146|
        |  2| 1.1094003924504583|
        +---+-------------------+

        See Also
        --------
        pyspark.sql.functions.pandas_udf
        """
        # Columns are special because hasattr always return True
        if (
            isinstance(udf, Column)
            or not hasattr(udf, "func")
            or (
                udf.evalType  # type: ignore[attr-defined]
                != PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF
            )
        ):
            raise PySparkTypeError(
                errorClass="INVALID_UDF_EVAL_TYPE",
                messageParameters={"eval_type": "SQL_GROUPED_MAP_PANDAS_UDF"},
            )

        warnings.warn(
            "It is preferred to use 'applyInPandas' over this "
            "API. This API will be deprecated in the future releases. See SPARK-28264 for "
            "more details.",
            UserWarning,
        )

        return self.applyInPandas(udf.func, schema=udf.returnType)  # type: ignore[attr-defined]

    def applyInPandas(
        self, func: "PandasGroupedMapFunction", schema: Union[StructType, str]
    ) -> DataFrame:
        """
        Maps each group of the current :class:`DataFrame` using a pandas udf and returns the result
        as a `DataFrame`.

        The function should take a `pandas.DataFrame` and return another
        `pandas.DataFrame`. Alternatively, the user can pass a function that takes
        a tuple of the grouping key(s) and a `pandas.DataFrame`.
        For each group, all columns are passed together as a `pandas.DataFrame`
        to the user-function and the returned `pandas.DataFrame` are combined as a
        :class:`DataFrame`.

        The `schema` should be a :class:`StructType` describing the schema of the returned
        `pandas.DataFrame`. The column labels of the returned `pandas.DataFrame` must either match
        the field names in the defined schema if specified as strings, or match the
        field data types by position if not strings, e.g. integer indices.
        The length of the returned `pandas.DataFrame` can be arbitrary.

        .. versionadded:: 3.0.0

        .. versionchanged:: 3.4.0
            Support Spark Connect.

        Parameters
        ----------
        func : function
            a Python native function that takes a `pandas.DataFrame` and outputs a
            `pandas.DataFrame`, or that takes one tuple (grouping keys) and a
            `pandas.DataFrame` and outputs a `pandas.DataFrame`.
        schema : :class:`pyspark.sql.types.DataType` or str
            the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.

        Examples
        --------
        >>> import pandas as pd  # doctest: +SKIP
        >>> from pyspark.sql.functions import ceil
        >>> df = spark.createDataFrame(
        ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ...     ("id", "v"))  # doctest: +SKIP
        >>> def normalize(pdf):
        ...     v = pdf.v
        ...     return pdf.assign(v=(v - v.mean()) / v.std())
        ...
        >>> df.groupby("id").applyInPandas(
        ...     normalize, schema="id long, v double").show()  # doctest: +SKIP
        +---+-------------------+
        | id|                  v|
        +---+-------------------+
        |  1|-0.7071067811865475|
        |  1| 0.7071067811865475|
        |  2|-0.8320502943378437|
        |  2|-0.2773500981126146|
        |  2| 1.1094003924504583|
        +---+-------------------+

        Alternatively, the user can pass a function that takes two arguments.
        In this case, the grouping key(s) will be passed as the first argument and the data will
        be passed as the second argument. The grouping key(s) will be passed as a tuple of numpy
        data types, e.g., `numpy.int32` and `numpy.float64`. The data will still be passed in
        as a `pandas.DataFrame` containing all columns from the original Spark DataFrame.
        This is useful when the user does not want to hardcode grouping key(s) in the function.

        >>> df = spark.createDataFrame(
        ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ...     ("id", "v"))  # doctest: +SKIP
        >>> def mean_func(key, pdf):
        ...     # key is a tuple of one numpy.int64, which is the value
        ...     # of 'id' for the current group
        ...     return pd.DataFrame([key + (pdf.v.mean(),)])
        ...
        >>> df.groupby('id').applyInPandas(
        ...     mean_func, schema="id long, v double").show()  # doctest: +SKIP
        +---+---+
        | id|  v|
        +---+---+
        |  1|1.5|
        |  2|6.0|
        +---+---+

        >>> def sum_func(key, pdf):
        ...     # key is a tuple of two numpy.int64s, which is the values
        ...     # of 'id' and 'ceil(df.v / 2)' for the current group
        ...     return pd.DataFrame([key + (pdf.v.sum(),)])
        ...
        >>> df.groupby(df.id, ceil(df.v / 2)).applyInPandas(
        ...     sum_func, schema="id long, `ceil(v / 2)` long, v double").show()  # doctest: +SKIP
        +---+-----------+----+
        | id|ceil(v / 2)|   v|
        +---+-----------+----+
        |  2|          5|10.0|
        |  1|          1| 3.0|
        |  2|          3| 5.0|
        |  2|          2| 3.0|
        +---+-----------+----+

        Notes
        -----
        This function requires a full shuffle. All the data of a group will be loaded
        into memory, so the user should be aware of the potential OOM risk if data is skewed
        and certain groups are too large to fit in memory.

        This API is experimental.

        See Also
        --------
        pyspark.sql.functions.pandas_udf
        """
        from pyspark.sql import GroupedData
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        assert isinstance(self, GroupedData)

        udf = pandas_udf(func, returnType=schema, functionType=PandasUDFType.GROUPED_MAP)
        df = self._df
        udf_column = udf(*[df[col] for col in df.columns])
        jdf = self._jgd.flatMapGroupsInPandas(udf_column._jc.expr())
        return DataFrame(jdf, self.session)

    def applyInPandasWithState(
        self,
        func: "PandasGroupedMapFunctionWithState",
        outputStructType: Union[StructType, str],
        stateStructType: Union[StructType, str],
        outputMode: str,
        timeoutConf: str,
    ) -> DataFrame:
        """
        Applies the given function to each group of data, while maintaining a user-defined
        per-group state. The result Dataset will represent the flattened record returned by the
        function.

        For a streaming :class:`DataFrame`, the function will be invoked first for all input groups
        and then for all timed out states where the input data is set to be empty. Updates to each
        group's state will be saved across invocations.

        The function should take parameters (key, Iterator[`pandas.DataFrame`], state) and
        return another Iterator[`pandas.DataFrame`]. The grouping key(s) will be passed as a tuple
        of numpy data types, e.g., `numpy.int32` and `numpy.float64`. The state will be passed as
        :class:`pyspark.sql.streaming.state.GroupState`.

        For each group, all columns are passed together as `pandas.DataFrame` to the user-function,
        and the returned `pandas.DataFrame` across all invocations are combined as a
        :class:`DataFrame`. Note that the user function should not make a guess of the number of
        elements in the iterator. To process all data, the user function needs to iterate all
        elements and process them. On the other hand, the user function is not strictly required to
        iterate through all elements in the iterator if it intends to read a part of data.

        The `outputStructType` should be a :class:`StructType` describing the schema of all
        elements in the returned value, `pandas.DataFrame`. The column labels of all elements in
        returned `pandas.DataFrame` must either match the field names in the defined schema if
        specified as strings, or match the field data types by position if not strings,
        e.g. integer indices.

        The `stateStructType` should be :class:`StructType` describing the schema of the
        user-defined state. The value of the state will be presented as a tuple, as well as the
        update should be performed with the tuple. The corresponding Python types for
        :class:DataType are supported. Please refer to the page
        https://spark.apache.org/docs/latest/sql-ref-datatypes.html (Python tab).

        The size of each `pandas.DataFrame` in both the input and output can be arbitrary. The
        number of `pandas.DataFrame` in both the input and output can also be arbitrary.

        .. versionadded:: 3.4.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        func : function
            a Python native function to be called on every group. It should take parameters
            (key, Iterator[`pandas.DataFrame`], state) and return Iterator[`pandas.DataFrame`].
            Note that the type of the key is tuple and the type of the state is
            :class:`pyspark.sql.streaming.state.GroupState`.
        outputStructType : :class:`pyspark.sql.types.DataType` or str
            the type of the output records. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        stateStructType : :class:`pyspark.sql.types.DataType` or str
            the type of the user-defined state. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        outputMode : str
            the output mode of the function.
        timeoutConf : str
            timeout configuration for groups that do not receive data for a while. valid values
            are defined in :class:`pyspark.sql.streaming.state.GroupStateTimeout`.

        Examples
        --------
        >>> import pandas as pd  # doctest: +SKIP
        >>> from pyspark.sql.streaming.state import GroupStateTimeout
        >>> def count_fn(key, pdf_iter, state):
        ...     assert isinstance(state, GroupStateImpl)
        ...     total_len = 0
        ...     for pdf in pdf_iter:
        ...         total_len += len(pdf)
        ...     state.update((total_len,))
        ...     yield pd.DataFrame({"id": [key[0]], "countAsString": [str(total_len)]})
        ...
        >>> df.groupby("id").applyInPandasWithState(
        ...     count_fn, outputStructType="id long, countAsString string",
        ...     stateStructType="len long", outputMode="Update",
        ...     timeoutConf=GroupStateTimeout.NoTimeout) # doctest: +SKIP

        Notes
        -----
        This function requires a full shuffle.

        This API is experimental.
        """

        from pyspark.sql import GroupedData
        from pyspark.sql.functions import pandas_udf

        assert isinstance(self, GroupedData)
        assert timeoutConf in [
            GroupStateTimeout.NoTimeout,
            GroupStateTimeout.ProcessingTimeTimeout,
            GroupStateTimeout.EventTimeTimeout,
        ]

        if isinstance(outputStructType, str):
            outputStructType = cast(StructType, _parse_datatype_string(outputStructType))
        if isinstance(stateStructType, str):
            stateStructType = cast(StructType, _parse_datatype_string(stateStructType))

        udf = pandas_udf(
            func,  # type: ignore[call-overload]
            returnType=outputStructType,
            functionType=PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
        )
        df = self._df
        udf_column = udf(*[df[col] for col in df.columns])
        jdf = self._jgd.applyInPandasWithState(
            udf_column._jc.expr(),
            self.session._jsparkSession.parseDataType(outputStructType.json()),
            self.session._jsparkSession.parseDataType(stateStructType.json()),
            outputMode,
            timeoutConf,
        )
        return DataFrame(jdf, self.session)

    def transformWithStateInPandas(
        self,
        statefulProcessor: StatefulProcessor,
        outputStructType: Union[StructType, str],
        outputMode: str,
        timeMode: str,
    ) -> DataFrame:
        """
        Invokes methods defined in the stateful processor used in arbitrary state API v2. It
        requires protobuf, pandas and pyarrow as dependencies to process input/state data. We
        allow the user to act on per-group set of input rows along with keyed state and the user
        can choose to output/return 0 or more rows.

        For a streaming dataframe, we will repeatedly invoke the interface methods for new rows
        in each trigger and the user's state/state variables will be stored persistently across
        invocations.

        The `statefulProcessor` should be a Python class that implements the interface defined in
        :class:`StatefulProcessor`.

        The `outputStructType` should be a :class:`StructType` describing the schema of all
        elements in the returned value, `pandas.DataFrame`. The column labels of all elements in
        returned `pandas.DataFrame` must either match the field names in the defined schema if
        specified as strings, or match the field data types by position if not strings,
        e.g. integer indices.

        The size of each `pandas.DataFrame` in both the input and output can be arbitrary. The
        number of `pandas.DataFrame` in both the input and output can also be arbitrary.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        statefulProcessor : :class:`pyspark.sql.streaming.stateful_processor.StatefulProcessor`
            Instance of StatefulProcessor whose functions will be invoked by the operator.
        outputStructType : :class:`pyspark.sql.types.DataType` or str
            The type of the output records. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
        outputMode : str
            The output mode of the stateful processor.
        timeMode : str
            The time mode semantics of the stateful processor for timers and TTL.

        Examples
        --------
        >>> from typing import Iterator
        ...
        >>> import pandas as pd # doctest: +SKIP
        ...
        >>> from pyspark.sql import Row
        >>> from pyspark.sql.functions import col, split
        >>> from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
        >>> from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType
        ...
        >>> spark.conf.set("spark.sql.streaming.stateStore.providerClass",
        ...     "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        ... # Below is a simple example to find erroneous sensors from temperature sensor data. The
        ... # processor returns a count of total readings, while keeping erroneous reading counts
        ... # in streaming state. A violation is defined when the temperature is above 100.
        ... # The input data is a DataFrame with the following schema:
        ... #    `id: string, temperature: long`.
        ... # The output schema and state schema are defined as below.
        >>> output_schema = StructType([
        ...     StructField("id", StringType(), True),
        ...     StructField("count", IntegerType(), True)
        ... ])
        >>> state_schema = StructType([
        ...     StructField("value", IntegerType(), True)
        ... ])
        >>> class SimpleStatefulProcessor(StatefulProcessor):
        ...     def init(self, handle: StatefulProcessorHandle):
        ...         self.num_violations_state = handle.getValueState("numViolations", state_schema)
        ...
        ...     def handleInputRows(self, key, rows):
        ...         new_violations = 0
        ...         count = 0
        ...         exists = self.num_violations_state.exists()
        ...         if exists:
        ...             existing_violations_row = self.num_violations_state.get()
        ...             existing_violations = existing_violations_row[0]
        ...         else:
        ...             existing_violations = 0
        ...         for pdf in rows:
        ...             pdf_count = pdf.count()
        ...             count += pdf_count.get('temperature')
        ...             violations_pdf = pdf.loc[pdf['temperature'] > 100]
        ...             new_violations += violations_pdf.count().get('temperature')
        ...         updated_violations = new_violations + existing_violations
        ...         self.num_violations_state.update((updated_violations,))
        ...         yield pd.DataFrame({'id': key, 'count': count})
        ...
        ...     def close(self) -> None:
        ...         pass

        Input DataFrame:
        +---+-----------+
        | id|temperature|
        +---+-----------+
        |  0|        123|
        |  0|         23|
        |  1|         33|
        |  1|        188|
        |  1|         88|
        +---+-----------+

        >>> df.groupBy("value").transformWithStateInPandas(statefulProcessor =
        ...     SimpleStatefulProcessor(), outputStructType=output_schema, outputMode="Update",
        ...     timeMode="None") # doctest: +SKIP

        Output DataFrame:
        +---+-----+
        | id|count|
        +---+-----+
        |  0|    2|
        |  1|    3|
        +---+-----+

        Notes
        -----
        This function requires a full shuffle.

        This API is experimental.
        """

        from pyspark.sql import GroupedData
        from pyspark.sql.functions import pandas_udf

        assert isinstance(self, GroupedData)

        def transformWithStateUDF(
            statefulProcessorApiClient: StatefulProcessorApiClient,
            key: Any,
            inputRows: Iterator["PandasDataFrameLike"],
        ) -> Iterator["PandasDataFrameLike"]:
            handle = StatefulProcessorHandle(statefulProcessorApiClient)

            if statefulProcessorApiClient.handle_state == StatefulProcessorHandleState.CREATED:
                statefulProcessor.init(handle)
                statefulProcessorApiClient.set_handle_state(
                    StatefulProcessorHandleState.INITIALIZED
                )

            statefulProcessorApiClient.set_implicit_key(key)
            result = statefulProcessor.handleInputRows(key, inputRows)

            return result

        if isinstance(outputStructType, str):
            outputStructType = cast(StructType, _parse_datatype_string(outputStructType))

        udf = pandas_udf(
            transformWithStateUDF,  # type: ignore
            returnType=outputStructType,
            functionType=PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_UDF,
        )
        df = self._df
        udf_column = udf(*[df[col] for col in df.columns])

        jdf = self._jgd.transformWithStateInPandas(
            udf_column._jc.expr(),
            self.session._jsparkSession.parseDataType(outputStructType.json()),
            outputMode,
            timeMode,
        )
        return DataFrame(jdf, self.session)

    def applyInArrow(
        self, func: "ArrowGroupedMapFunction", schema: Union[StructType, str]
    ) -> "DataFrame":
        """
        Maps each group of the current :class:`DataFrame` using an Arrow udf and returns the result
        as a `DataFrame`.

        The function should take a `pyarrow.Table` and return another
        `pyarrow.Table`. Alternatively, the user can pass a function that takes
        a tuple of `pyarrow.Scalar` grouping key(s) and a `pyarrow.Table`.
        For each group, all columns are passed together as a `pyarrow.Table`
        to the user-function and the returned `pyarrow.Table` are combined as a
        :class:`DataFrame`.

        The `schema` should be a :class:`StructType` describing the schema of the returned
        `pyarrow.Table`. The column labels of the returned `pyarrow.Table` must either match
        the field names in the defined schema if specified as strings, or match the
        field data types by position if not strings, e.g. integer indices.
        The length of the returned `pyarrow.Table` can be arbitrary.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        func : function
            a Python native function that takes a `pyarrow.Table` and outputs a
            `pyarrow.Table`, or that takes one tuple (grouping keys) and a
            `pyarrow.Table` and outputs a `pyarrow.Table`.
        schema : :class:`pyspark.sql.types.DataType` or str
            the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.

        Examples
        --------
        >>> from pyspark.sql.functions import ceil
        >>> import pyarrow  # doctest: +SKIP
        >>> import pyarrow.compute as pc  # doctest: +SKIP
        >>> df = spark.createDataFrame(
        ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ...     ("id", "v"))  # doctest: +SKIP
        >>> def normalize(table):
        ...     v = table.column("v")
        ...     norm = pc.divide(pc.subtract(v, pc.mean(v)), pc.stddev(v, ddof=1))
        ...     return table.set_column(1, "v", norm)
        >>> df.groupby("id").applyInArrow(
        ...     normalize, schema="id long, v double").show()  # doctest: +SKIP
        +---+-------------------+
        | id|                  v|
        +---+-------------------+
        |  1|-0.7071067811865475|
        |  1| 0.7071067811865475|
        |  2|-0.8320502943378437|
        |  2|-0.2773500981126146|
        |  2| 1.1094003924504583|
        +---+-------------------+

        Alternatively, the user can pass a function that takes two arguments.
        In this case, the grouping key(s) will be passed as the first argument and the data will
        be passed as the second argument. The grouping key(s) will be passed as a tuple of Arrow
        scalars types, e.g., `pyarrow.Int32Scalar` and `pyarrow.FloatScalar`. The data will still
        be passed in as a `pyarrow.Table` containing all columns from the original Spark DataFrame.
        This is useful when the user does not want to hardcode grouping key(s) in the function.

        >>> df = spark.createDataFrame(
        ...     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ...     ("id", "v"))  # doctest: +SKIP
        >>> def mean_func(key, table):
        ...     # key is a tuple of one pyarrow.Int64Scalar, which is the value
        ...     # of 'id' for the current group
        ...     mean = pc.mean(table.column("v"))
        ...     return pyarrow.Table.from_pydict({"id": [key[0].as_py()], "v": [mean.as_py()]})
        >>> df.groupby('id').applyInArrow(
        ...     mean_func, schema="id long, v double")  # doctest: +SKIP
        +---+---+
        | id|  v|
        +---+---+
        |  1|1.5|
        |  2|6.0|
        +---+---+

        >>> def sum_func(key, table):
        ...     # key is a tuple of two pyarrow.Int64Scalars, which is the values
        ...     # of 'id' and 'ceil(df.v / 2)' for the current group
        ...     sum = pc.sum(table.column("v"))
        ...     return pyarrow.Table.from_pydict({
        ...         "id": [key[0].as_py()],
        ...         "ceil(v / 2)": [key[1].as_py()],
        ...         "v": [sum.as_py()]
        ...     })
        >>> df.groupby(df.id, ceil(df.v / 2)).applyInArrow(
        ...     sum_func, schema="id long, `ceil(v / 2)` long, v double").show()  # doctest: +SKIP
        +---+-----------+----+
        | id|ceil(v / 2)|   v|
        +---+-----------+----+
        |  2|          5|10.0|
        |  1|          1| 3.0|
        |  2|          3| 5.0|
        |  2|          2| 3.0|
        +---+-----------+----+

        Notes
        -----
        This function requires a full shuffle. All the data of a group will be loaded
        into memory, so the user should be aware of the potential OOM risk if data is skewed
        and certain groups are too large to fit in memory.

        This API is unstable, and for developers.

        See Also
        --------
        pyspark.sql.functions.pandas_udf
        """
        from pyspark.sql import GroupedData
        from pyspark.sql.functions import pandas_udf

        assert isinstance(self, GroupedData)

        # The usage of the pandas_udf is internal so type checking is disabled.
        udf = pandas_udf(
            func, returnType=schema, functionType=PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF
        )  # type: ignore[call-overload]
        df = self._df
        udf_column = udf(*[df[col] for col in df.columns])
        jdf = self._jgd.flatMapGroupsInArrow(udf_column._jc.expr())
        return DataFrame(jdf, self.session)

    def cogroup(self, other: "GroupedData") -> "PandasCogroupedOps":
        """
        Cogroups this group with another group so that we can run cogrouped operations.

        .. versionadded:: 3.0.0

        .. versionchanged:: 3.4.0
            Support Spark Connect.

        See :class:`PandasCogroupedOps` for the operations that can be run.
        """
        from pyspark.sql import GroupedData

        assert isinstance(self, GroupedData)

        return PandasCogroupedOps(self, other)


class PandasCogroupedOps:
    """
    A logical grouping of two :class:`GroupedData`,
    created by :func:`GroupedData.cogroup`.

    .. versionadded:: 3.0.0

    .. versionchanged:: 3.4.0
        Support Spark Connect.

    Notes
    -----
    This API is experimental.
    """

    def __init__(self, gd1: "GroupedData", gd2: "GroupedData"):
        self._gd1 = gd1
        self._gd2 = gd2

    def applyInPandas(
        self, func: "PandasCogroupedMapFunction", schema: Union[StructType, str]
    ) -> DataFrame:
        """
        Applies a function to each cogroup using pandas and returns the result
        as a `DataFrame`.

        The function should take two `pandas.DataFrame`\\s and return another
        `pandas.DataFrame`. Alternatively, the user can pass a function that takes
        a tuple of the grouping key(s) and the two `pandas.DataFrame`\\s.
        For each side of the cogroup, all columns are passed together as a
        `pandas.DataFrame` to the user-function and the returned `pandas.DataFrame` are combined as
        a :class:`DataFrame`.

        The `schema` should be a :class:`StructType` describing the schema of the returned
        `pandas.DataFrame`. The column labels of the returned `pandas.DataFrame` must either match
        the field names in the defined schema if specified as strings, or match the
        field data types by position if not strings, e.g. integer indices.
        The length of the returned `pandas.DataFrame` can be arbitrary.

        .. versionadded:: 3.0.0

        .. versionchanged:: 3.4.0
            Support Spark Connect.

        Parameters
        ----------
        func : function
            a Python native function that takes two `pandas.DataFrame`\\s, and
            outputs a `pandas.DataFrame`, or that takes one tuple (grouping keys) and two
            ``pandas.DataFrame``\\s, and outputs a ``pandas.DataFrame``.
        schema : :class:`pyspark.sql.types.DataType` or str
            the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.

        Examples
        --------
        >>> df1 = spark.createDataFrame(
        ...     [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
        ...     ("time", "id", "v1"))
        >>> df2 = spark.createDataFrame(
        ...     [(20000101, 1, "x"), (20000101, 2, "y")],
        ...     ("time", "id", "v2"))
        >>> def asof_join(l, r):
        ...     return pd.merge_asof(l, r, on="time", by="id")
        ...
        >>> df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
        ...     asof_join, schema="time int, id int, v1 double, v2 string"
        ... ).show()  # doctest: +SKIP
        +--------+---+---+---+
        |    time| id| v1| v2|
        +--------+---+---+---+
        |20000101|  1|1.0|  x|
        |20000102|  1|3.0|  x|
        |20000101|  2|2.0|  y|
        |20000102|  2|4.0|  y|
        +--------+---+---+---+

        Alternatively, the user can define a function that takes three arguments.  In this case,
        the grouping key(s) will be passed as the first argument and the data will be passed as the
        second and third arguments.  The grouping key(s) will be passed as a tuple of numpy data
        types, e.g., `numpy.int32` and `numpy.float64`. The data will still be passed in as two
        `pandas.DataFrame` containing all columns from the original Spark DataFrames.

        >>> def asof_join(k, l, r):
        ...     if k == (1,):
        ...         return pd.merge_asof(l, r, on="time", by="id")
        ...     else:
        ...         return pd.DataFrame(columns=['time', 'id', 'v1', 'v2'])
        ...
        >>> df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
        ...     asof_join, "time int, id int, v1 double, v2 string").show()  # doctest: +SKIP
        +--------+---+---+---+
        |    time| id| v1| v2|
        +--------+---+---+---+
        |20000101|  1|1.0|  x|
        |20000102|  1|3.0|  x|
        +--------+---+---+---+

        Notes
        -----
        This function requires a full shuffle. All the data of a cogroup will be loaded
        into memory, so the user should be aware of the potential OOM risk if data is skewed
        and certain groups are too large to fit in memory.

        This API is experimental.

        See Also
        --------
        pyspark.sql.functions.pandas_udf
        """
        from pyspark.sql.pandas.functions import pandas_udf

        # The usage of the pandas_udf is internal so type checking is disabled.
        udf = pandas_udf(
            func, returnType=schema, functionType=PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF
        )  # type: ignore[call-overload]

        all_cols = self._extract_cols(self._gd1) + self._extract_cols(self._gd2)
        udf_column = udf(*all_cols)
        jdf = self._gd1._jgd.flatMapCoGroupsInPandas(self._gd2._jgd, udf_column._jc.expr())
        return DataFrame(jdf, self._gd1.session)

    def applyInArrow(
        self, func: "ArrowCogroupedMapFunction", schema: Union[StructType, str]
    ) -> "DataFrame":
        """
        Applies a function to each cogroup using Arrow and returns the result
        as a `DataFrame`.

        The function should take two `pyarrow.Table`\\s and return another
        `pyarrow.Table`. Alternatively, the user can pass a function that takes
        a tuple of `pyarrow.Scalar` grouping key(s) and the two `pyarrow.Table`\\s.
        For each side of the cogroup, all columns are passed together as a
        `pyarrow.Table` to the user-function and the returned `pyarrow.Table` are combined as
        a :class:`DataFrame`.

        The `schema` should be a :class:`StructType` describing the schema of the returned
        `pyarrow.Table`. The column labels of the returned `pyarrow.Table` must either match
        the field names in the defined schema if specified as strings, or match the
        field data types by position if not strings, e.g. integer indices.
        The length of the returned `pyarrow.Table` can be arbitrary.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        func : function
            a Python native function that takes two `pyarrow.Table`\\s, and
            outputs a `pyarrow.Table`, or that takes one tuple (grouping keys) and two
            ``pyarrow.Table``\\s, and outputs a ``pyarrow.Table``.
        schema : :class:`pyspark.sql.types.DataType` or str
            the return type of the `func` in PySpark. The value can be either a
            :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.

        Examples
        --------
        >>> import pyarrow  # doctest: +SKIP
        >>> df1 = spark.createDataFrame([(1, 1.0), (2, 2.0), (1, 3.0), (2, 4.0)], ("id", "v1"))
        >>> df2 = spark.createDataFrame([(1, "x"), (2, "y")], ("id", "v2"))
        >>> def summarize(l, r):
        ...     return pyarrow.Table.from_pydict({
        ...         "left": [l.num_rows],
        ...         "right": [r.num_rows]
        ...     })
        >>> df1.groupby("id").cogroup(df2.groupby("id")).applyInArrow(
        ...     summarize, schema="left long, right long"
        ... ).show()  # doctest: +SKIP
        +----+-----+
        |left|right|
        +----+-----+
        |   2|    1|
        |   2|    1|
        +----+-----+

        Alternatively, the user can define a function that takes three arguments.  In this case,
        the grouping key(s) will be passed as the first argument and the data will be passed as the
        second and third arguments.  The grouping key(s) will be passed as a tuple of Arrow scalars
        types, e.g., `pyarrow.Int32Scalar` and `pyarrow.FloatScalar`. The data will still be passed
        in as two `pyarrow.Table`\\s containing all columns from the original Spark DataFrames.

        >>> def summarize(key, l, r):
        ...     return pyarrow.Table.from_pydict({
        ...         "key": [key[0].as_py()],
        ...         "left": [l.num_rows],
        ...         "right": [r.num_rows]
        ...     })
        >>> df1.groupby("id").cogroup(df2.groupby("id")).applyInArrow(
        ...     summarize, schema="key long, left long, right long"
        ... ).show()  # doctest: +SKIP
        +---+----+-----+
        |key|left|right|
        +---+----+-----+
        |  1|   2|    1|
        |  2|   2|    1|
        +---+----+-----+

        Notes
        -----
        This function requires a full shuffle. All the data of a cogroup will be loaded
        into memory, so the user should be aware of the potential OOM risk if data is skewed
        and certain groups are too large to fit in memory.

        This API is unstable, and for developers.

        See Also
        --------
        pyspark.sql.functions.pandas_udf
        """
        from pyspark.sql.pandas.functions import pandas_udf

        # The usage of the pandas_udf is internal so type checking is disabled.
        udf = pandas_udf(
            func, returnType=schema, functionType=PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF
        )  # type: ignore[call-overload]

        all_cols = self._extract_cols(self._gd1) + self._extract_cols(self._gd2)
        udf_column = udf(*all_cols)
        jdf = self._gd1._jgd.flatMapCoGroupsInArrow(self._gd2._jgd, udf_column._jc.expr())
        return DataFrame(jdf, self._gd1.session)

    @staticmethod
    def _extract_cols(gd: "GroupedData") -> List[Column]:
        df = gd._df
        return [df[col] for col in df.columns]


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.pandas.group_ops

    globs = pyspark.sql.pandas.group_ops.__dict__.copy()
    spark = SparkSession.builder.master("local[4]").appName("sql.pandas.group tests").getOrCreate()
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.pandas.group_ops,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
