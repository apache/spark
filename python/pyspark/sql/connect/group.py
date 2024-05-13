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

from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

import warnings
from typing import (
    Dict,
    List,
    Sequence,
    Union,
    TYPE_CHECKING,
    Optional,
    overload,
    cast,
)

from pyspark.util import PythonEvalType
from pyspark.sql.group import GroupedData as PySparkGroupedData
from pyspark.sql.pandas.group_ops import PandasCogroupedOps as PySparkPandasCogroupedOps
from pyspark.sql.pandas.functions import _validate_pandas_udf  # type: ignore[attr-defined]
from pyspark.sql.types import NumericType
from pyspark.sql.types import StructType

import pyspark.sql.connect.plan as plan
from pyspark.sql.column import Column
from pyspark.sql.connect.functions import builtin as F
from pyspark.errors import PySparkNotImplementedError, PySparkTypeError

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import (
        LiteralType,
        PandasGroupedMapFunction,
        GroupedMapPandasUserDefinedFunction,
        PandasCogroupedMapFunction,
        ArrowCogroupedMapFunction,
        ArrowGroupedMapFunction,
        PandasGroupedMapFunctionWithState,
    )
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.types import StructType


class GroupedData:
    def __init__(
        self,
        df: "DataFrame",
        group_type: str,
        grouping_cols: Sequence[Column],
        pivot_col: Optional[Column] = None,
        pivot_values: Optional[Sequence["LiteralType"]] = None,
        grouping_sets: Optional[Sequence[Sequence[Column]]] = None,
    ) -> None:
        from pyspark.sql.connect.dataframe import DataFrame

        assert isinstance(df, DataFrame)
        self._df = df

        assert isinstance(group_type, str) and group_type in [
            "groupby",
            "rollup",
            "cube",
            "pivot",
            "grouping_sets",
        ]
        self._group_type = group_type

        assert isinstance(grouping_cols, list) and all(isinstance(g, Column) for g in grouping_cols)
        self._grouping_cols: List[Column] = grouping_cols

        self._pivot_col: Optional["Column"] = None
        self._pivot_values: Optional[List["Column"]] = None
        if group_type == "pivot":
            assert pivot_col is not None and isinstance(pivot_col, Column)
            self._pivot_col = pivot_col

            if pivot_values is not None:
                assert isinstance(pivot_values, list)
                self._pivot_values = [F.lit(v) for v in pivot_values]

        self._grouping_sets: Optional[Sequence[Sequence["Column"]]] = None
        if group_type == "grouping_sets":
            assert grouping_sets is None or isinstance(grouping_sets, list)
            self._grouping_sets = grouping_sets

    def __repr__(self) -> str:
        # the expressions are not resolved here,
        # so the string representation can be different from vanilla PySpark.
        grouping_str = ", ".join(str(e._expr) for e in self._grouping_cols)
        grouping_str = f"grouping expressions: [{grouping_str}]"

        value_str = ", ".join("%s: %s" % c for c in self._df.dtypes)

        if self._group_type == "groupby":
            type_str = "GroupBy"
        elif self._group_type == "rollup":
            type_str = "RollUp"
        elif self._group_type == "cube":
            type_str = "Cube"
        elif self._group_type == "grouping_sets":
            type_str = "GroupingSets"
        else:
            type_str = "Pivot"

        return f"GroupedData[{grouping_str}, value: [{value_str}], type: {type_str}]"

    @overload
    def agg(self, *exprs: Column) -> "DataFrame":
        ...

    @overload
    def agg(self, __exprs: Dict[str, str]) -> "DataFrame":
        ...

    def agg(self, *exprs: Union[Column, Dict[str, str]]) -> "DataFrame":
        from pyspark.sql.connect.dataframe import DataFrame

        assert exprs, "exprs should not be empty"
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            # Convert the dict into key value pairs
            aggregate_cols = [F._invoke_function(exprs[0][k], F.col(k)) for k in exprs[0]]
        else:
            # Columns
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
            aggregate_cols = cast(List[Column], list(exprs))

        return DataFrame(
            plan.Aggregate(
                child=self._df._plan,
                group_type=self._group_type,
                grouping_cols=self._grouping_cols,
                aggregate_cols=aggregate_cols,
                pivot_col=self._pivot_col,
                pivot_values=self._pivot_values,
                grouping_sets=self._grouping_sets,
            ),
            session=self._df._session,
        )

    agg.__doc__ = PySparkGroupedData.agg.__doc__

    def _numeric_agg(self, function: str, cols: Sequence[str]) -> "DataFrame":
        from pyspark.sql.connect.dataframe import DataFrame

        assert isinstance(function, str) and function in ["min", "max", "avg", "sum"]

        assert isinstance(cols, list) and all(isinstance(c, str) for c in cols)

        schema = self._df.schema

        numerical_cols: List[str] = [
            field.name for field in schema.fields if isinstance(field.dataType, NumericType)
        ]

        if len(cols) > 0:
            invalid_cols = [c for c in cols if c not in numerical_cols]
            if len(invalid_cols) > 0:
                raise PySparkTypeError(
                    error_class="NOT_NUMERIC_COLUMNS",
                    message_parameters={"invalid_columns": str(invalid_cols)},
                )
            agg_cols = cols
        else:
            # if no column is provided, then all numerical columns are selected
            agg_cols = numerical_cols

        return DataFrame(
            plan.Aggregate(
                child=self._df._plan,
                group_type=self._group_type,
                grouping_cols=self._grouping_cols,
                aggregate_cols=[F._invoke_function(function, F.col(c)) for c in agg_cols],
                pivot_col=self._pivot_col,
                pivot_values=self._pivot_values,
                grouping_sets=self._grouping_sets,
            ),
            session=self._df._session,
        )

    def min(self, *cols: str) -> "DataFrame":
        return self._numeric_agg("min", list(cols))

    min.__doc__ = PySparkGroupedData.min.__doc__

    def max(self, *cols: str) -> "DataFrame":
        return self._numeric_agg("max", list(cols))

    max.__doc__ = PySparkGroupedData.max.__doc__

    def sum(self, *cols: str) -> "DataFrame":
        return self._numeric_agg("sum", list(cols))

    sum.__doc__ = PySparkGroupedData.sum.__doc__

    def avg(self, *cols: str) -> "DataFrame":
        return self._numeric_agg("avg", list(cols))

    avg.__doc__ = PySparkGroupedData.avg.__doc__

    mean = avg

    def count(self) -> "DataFrame":
        return self.agg(F._invoke_function("count", F.lit(1)).alias("count"))

    count.__doc__ = PySparkGroupedData.count.__doc__

    def pivot(self, pivot_col: str, values: Optional[List["LiteralType"]] = None) -> "GroupedData":
        if self._group_type != "groupby":
            if self._group_type == "pivot":
                raise PySparkNotImplementedError(
                    error_class="UNSUPPORTED_OPERATION",
                    message_parameters={"operation": "Repeated PIVOT operation"},
                )
            else:
                raise PySparkNotImplementedError(
                    error_class="UNSUPPORTED_OPERATION",
                    message_parameters={"operation": f"PIVOT after {self._group_type.upper()}"},
                )

        if not isinstance(pivot_col, str):
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "pivot_col", "arg_type": type(pivot_col).__name__},
            )

        if values is not None:
            if not isinstance(values, list):
                raise PySparkTypeError(
                    error_class="NOT_LIST",
                    message_parameters={"arg_name": "values", "arg_type": type(values).__name__},
                )
            for v in values:
                if not isinstance(v, (bool, float, int, str)):
                    raise PySparkTypeError(
                        error_class="NOT_BOOL_OR_FLOAT_OR_INT_OR_STR",
                        message_parameters={"arg_name": "value", "arg_type": type(v).__name__},
                    )

        return GroupedData(
            df=self._df,
            group_type="pivot",
            grouping_cols=self._grouping_cols,
            pivot_col=self._df[pivot_col],
            pivot_values=values,
        )

    pivot.__doc__ = PySparkGroupedData.pivot.__doc__

    def apply(self, udf: "GroupedMapPandasUserDefinedFunction") -> "DataFrame":
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
                error_class="INVALID_UDF_EVAL_TYPE",
                message_parameters={"eval_type": "SQL_GROUPED_MAP_PANDAS_UDF"},
            )

        warnings.warn(
            "It is preferred to use 'applyInPandas' over this "
            "API. This API will be deprecated in the future releases. See SPARK-28264 for "
            "more details.",
            UserWarning,
        )

        return self.applyInPandas(udf.func, schema=udf.returnType)  # type: ignore[attr-defined]

    apply.__doc__ = PySparkGroupedData.apply.__doc__

    def applyInPandas(
        self, func: "PandasGroupedMapFunction", schema: Union["StructType", str]
    ) -> "DataFrame":
        from pyspark.sql.connect.udf import UserDefinedFunction
        from pyspark.sql.connect.dataframe import DataFrame

        _validate_pandas_udf(func, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)
        udf_obj = UserDefinedFunction(
            func,
            returnType=schema,
            evalType=PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
        )

        return DataFrame(
            plan.GroupMap(
                child=self._df._plan,
                grouping_cols=self._grouping_cols,
                function=udf_obj,
                cols=self._df.columns,
            ),
            session=self._df._session,
        )

    applyInPandas.__doc__ = PySparkGroupedData.applyInPandas.__doc__

    def applyInPandasWithState(
        self,
        func: "PandasGroupedMapFunctionWithState",
        outputStructType: Union[StructType, str],
        stateStructType: Union[StructType, str],
        outputMode: str,
        timeoutConf: str,
    ) -> "DataFrame":
        from pyspark.sql.connect.udf import UserDefinedFunction
        from pyspark.sql.connect.dataframe import DataFrame

        _validate_pandas_udf(func, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE)
        udf_obj = UserDefinedFunction(
            func,
            returnType=outputStructType,
            evalType=PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
        )

        output_schema: str = (
            outputStructType.json()
            if isinstance(outputStructType, StructType)
            else outputStructType
        )

        state_schema: str = (
            stateStructType.json() if isinstance(stateStructType, StructType) else stateStructType
        )

        return DataFrame(
            plan.ApplyInPandasWithState(
                child=self._df._plan,
                grouping_cols=self._grouping_cols,
                function=udf_obj,
                output_schema=output_schema,
                state_schema=state_schema,
                output_mode=outputMode,
                timeout_conf=timeoutConf,
                cols=self._df.columns,
            ),
            session=self._df._session,
        )

    applyInPandasWithState.__doc__ = PySparkGroupedData.applyInPandasWithState.__doc__

    def applyInArrow(
        self, func: "ArrowGroupedMapFunction", schema: Union[StructType, str]
    ) -> "DataFrame":
        from pyspark.sql.connect.udf import UserDefinedFunction
        from pyspark.sql.connect.dataframe import DataFrame

        _validate_pandas_udf(func, PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF)
        udf_obj = UserDefinedFunction(
            func,
            returnType=schema,
            evalType=PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF,
        )

        return DataFrame(
            plan.GroupMap(
                child=self._df._plan,
                grouping_cols=self._grouping_cols,
                function=udf_obj,
                cols=self._df.columns,
            ),
            session=self._df._session,
        )

    applyInArrow.__doc__ = PySparkGroupedData.applyInArrow.__doc__

    def cogroup(self, other: "GroupedData") -> "PandasCogroupedOps":
        return PandasCogroupedOps(self, other)

    cogroup.__doc__ = PySparkGroupedData.cogroup.__doc__


GroupedData.__doc__ = PySparkGroupedData.__doc__


class PandasCogroupedOps:
    def __init__(self, gd1: "GroupedData", gd2: "GroupedData"):
        gd1._df._check_same_session(gd2._df)
        self._gd1 = gd1
        self._gd2 = gd2

    def applyInPandas(
        self, func: "PandasCogroupedMapFunction", schema: Union["StructType", str]
    ) -> "DataFrame":
        from pyspark.sql.connect.udf import UserDefinedFunction
        from pyspark.sql.connect.dataframe import DataFrame

        _validate_pandas_udf(func, PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF)
        udf_obj = UserDefinedFunction(
            func,
            returnType=schema,
            evalType=PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
        )

        return DataFrame(
            plan.CoGroupMap(
                input=self._gd1._df._plan,
                input_grouping_cols=self._gd1._grouping_cols,
                other=self._gd2._df._plan,
                other_grouping_cols=self._gd2._grouping_cols,
                function=udf_obj,
            ),
            session=self._gd1._df._session,
        )

    applyInPandas.__doc__ = PySparkPandasCogroupedOps.applyInPandas.__doc__

    def applyInArrow(
        self, func: "ArrowCogroupedMapFunction", schema: Union[StructType, str]
    ) -> "DataFrame":
        from pyspark.sql.connect.udf import UserDefinedFunction
        from pyspark.sql.connect.dataframe import DataFrame

        _validate_pandas_udf(func, PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF)
        udf_obj = UserDefinedFunction(
            func,
            returnType=schema,
            evalType=PythonEvalType.SQL_COGROUPED_MAP_ARROW_UDF,
        )

        return DataFrame(
            plan.CoGroupMap(
                input=self._gd1._df._plan,
                input_grouping_cols=self._gd1._grouping_cols,
                other=self._gd2._df._plan,
                other_grouping_cols=self._gd2._grouping_cols,
                function=udf_obj,
            ),
            session=self._gd1._df._session,
        )

    applyInArrow.__doc__ = PySparkPandasCogroupedOps.applyInArrow.__doc__


PandasCogroupedOps.__doc__ = PySparkPandasCogroupedOps.__doc__


def _test() -> None:
    import os
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.group

    globs = pyspark.sql.connect.group.__dict__.copy()

    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.group tests")
        .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]"))
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.group,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )

    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
