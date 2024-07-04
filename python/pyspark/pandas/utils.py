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
"""
Commonly used utils in pandas-on-Spark.
"""

import functools
from contextlib import contextmanager
import os
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    TYPE_CHECKING,
    cast,
    no_type_check,
    overload,
)
import warnings

import pandas as pd
from pandas.api.types import is_list_like  # type: ignore[attr-defined]

from pyspark.sql import functions as F, Column, DataFrame as PySparkDataFrame, SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.utils import is_remote
from pyspark.errors import PySparkTypeError
from pyspark import pandas as ps  # noqa: F401
from pyspark.pandas._typing import (
    Axis,
    Label,
    Name,
    DataFrameOrSeries,
)
from pyspark.pandas.typedef.typehints import as_spark_type


if TYPE_CHECKING:
    from pyspark.pandas.indexes.base import Index
    from pyspark.pandas.base import IndexOpsMixin
    from pyspark.pandas.frame import DataFrame
    from pyspark.pandas.internal import InternalFrame
    from pyspark.pandas.series import Series


ERROR_MESSAGE_CANNOT_COMBINE = (
    "Cannot combine the series or dataframe because it comes from a different dataframe. "
    "In order to allow this operation, enable 'compute.ops_on_diff_frames' option."
)


SPARK_CONF_ARROW_ENABLED = "spark.sql.execution.arrow.pyspark.enabled"


class PandasAPIOnSparkAdviceWarning(Warning):
    pass


def same_anchor(
    this: Union["DataFrame", "IndexOpsMixin", "InternalFrame"],
    that: Union["DataFrame", "IndexOpsMixin", "InternalFrame"],
) -> bool:
    """
    Check if the anchors of the given DataFrame or Series are the same or not.
    """
    from pyspark.pandas.base import IndexOpsMixin
    from pyspark.pandas.frame import DataFrame
    from pyspark.pandas.internal import InternalFrame

    if isinstance(this, InternalFrame):
        this_internal = this
    else:
        assert isinstance(this, (DataFrame, IndexOpsMixin)), type(this)
        this_internal = this._internal

    if isinstance(that, InternalFrame):
        that_internal = that
    else:
        assert isinstance(that, (DataFrame, IndexOpsMixin)), type(that)
        that_internal = that._internal

    return (
        this_internal.spark_frame is that_internal.spark_frame
        and this_internal.index_level == that_internal.index_level
        and all(
            spark_column_equals(this_scol, that_scol)
            for this_scol, that_scol in zip(
                this_internal.index_spark_columns, that_internal.index_spark_columns
            )
        )
    )


def combine_frames(
    this: "DataFrame",
    *args: DataFrameOrSeries,
    how: str = "full",
    preserve_order_column: bool = False,
) -> "DataFrame":
    """
    This method combines `this` DataFrame with a different `that` DataFrame or
    Series from a different DataFrame.

    It returns a DataFrame that has prefix `this_` and `that_` to distinct
    the columns names from both DataFrames

    It internally performs a join operation which can be expensive in general.
    So, if `compute.ops_on_diff_frames` option is False,
    this method throws an exception.
    """
    from pyspark.pandas.config import get_option
    from pyspark.pandas.frame import DataFrame
    from pyspark.pandas.internal import (
        InternalField,
        InternalFrame,
        HIDDEN_COLUMNS,
        NATURAL_ORDER_COLUMN_NAME,
        SPARK_INDEX_NAME_FORMAT,
    )
    from pyspark.pandas.series import Series

    if all(isinstance(arg, Series) for arg in args):
        assert all(
            same_anchor(arg, args[0]) for arg in args
        ), "Currently only one different DataFrame (from given Series) is supported"
        assert not same_anchor(this, args[0]), "We don't need to combine. All series is in this."
        that = args[0]._psdf[list(args)]
    elif len(args) == 1 and isinstance(args[0], DataFrame):
        assert isinstance(args[0], DataFrame)
        assert not same_anchor(
            this, args[0]
        ), "We don't need to combine. `this` and `that` are same."
        that = args[0]
    else:
        raise AssertionError("args should be single DataFrame or " "single/multiple Series")

    if get_option("compute.ops_on_diff_frames"):

        def resolve(internal: InternalFrame, side: str) -> InternalFrame:
            def rename(col: str) -> str:
                return "__{}_{}".format(side, col)

            internal = internal.resolved_copy
            sdf = internal.spark_frame
            sdf = internal.spark_frame.select(
                *[
                    scol_for(sdf, col).alias(rename(col))
                    for col in sdf.columns
                    if col not in HIDDEN_COLUMNS
                ],
                *HIDDEN_COLUMNS,
            )
            return internal.copy(
                spark_frame=sdf,
                index_spark_columns=[
                    scol_for(sdf, rename(col)) for col in internal.index_spark_column_names
                ],
                index_fields=[
                    field.copy(name=rename(field.name)) for field in internal.index_fields
                ],
                data_spark_columns=[
                    scol_for(sdf, rename(col)) for col in internal.data_spark_column_names
                ],
                data_fields=[field.copy(name=rename(field.name)) for field in internal.data_fields],
            )

        this_internal = resolve(this._internal, "this")
        that_internal = resolve(that._internal, "that")

        this_index_map = list(
            zip(
                this_internal.index_spark_column_names,
                this_internal.index_names,
                this_internal.index_fields,
            )
        )
        that_index_map = list(
            zip(
                that_internal.index_spark_column_names,
                that_internal.index_names,
                that_internal.index_fields,
            )
        )
        assert len(this_index_map) == len(that_index_map)

        join_scols = []
        merged_index_scols = []

        # Note that the order of each element in index_map is guaranteed according to the index
        # level.
        this_and_that_index_map = list(zip(this_index_map, that_index_map))

        this_sdf = this_internal.spark_frame.alias("this")
        that_sdf = that_internal.spark_frame.alias("that")

        # If the same named index is found, that's used.
        index_column_names = []
        index_use_extension_dtypes = []
        for (
            i,
            ((this_column, this_name, this_field), (that_column, that_name, that_field)),
        ) in enumerate(this_and_that_index_map):
            if this_name == that_name:
                # We should merge the Spark columns into one
                # to mimic pandas' behavior.
                this_scol = scol_for(this_sdf, this_column)
                that_scol = scol_for(that_sdf, that_column)
                join_scol = this_scol == that_scol
                join_scols.append(join_scol)

                column_name = SPARK_INDEX_NAME_FORMAT(i)
                index_column_names.append(column_name)
                index_use_extension_dtypes.append(
                    any(field.is_extension_dtype for field in [this_field, that_field])
                )
                merged_index_scols.append(
                    F.when(this_scol.isNotNull(), this_scol).otherwise(that_scol).alias(column_name)
                )
            else:
                raise ValueError("Index names must be exactly matched currently.")

        assert len(join_scols) > 0, "cannot join with no overlapping index names"

        joined_df = this_sdf.join(that_sdf, on=join_scols, how=how)

        if preserve_order_column:
            order_column = [scol_for(this_sdf, NATURAL_ORDER_COLUMN_NAME)]
        else:
            order_column = []

        joined_df = joined_df.select(
            *merged_index_scols,
            *(
                scol_for(this_sdf, this_internal.spark_column_name_for(label))
                for label in this_internal.column_labels
            ),
            *(
                scol_for(that_sdf, that_internal.spark_column_name_for(label))
                for label in that_internal.column_labels
            ),
            *order_column,
        )

        index_spark_columns = [scol_for(joined_df, col) for col in index_column_names]

        index_columns = set(index_column_names)
        new_data_columns = [
            col
            for col in joined_df.columns
            if col not in index_columns and col != NATURAL_ORDER_COLUMN_NAME
        ]

        schema = joined_df.select(*index_spark_columns, *new_data_columns).schema

        index_fields = [
            InternalField.from_struct_field(struct_field, use_extension_dtypes=use_extension_dtypes)
            for struct_field, use_extension_dtypes in zip(
                schema.fields[: len(index_spark_columns)], index_use_extension_dtypes
            )
        ]
        data_fields = [
            InternalField.from_struct_field(
                struct_field, use_extension_dtypes=field.is_extension_dtype
            )
            for struct_field, field in zip(
                schema.fields[len(index_spark_columns) :],
                this_internal.data_fields + that_internal.data_fields,
            )
        ]

        level = max(this_internal.column_labels_level, that_internal.column_labels_level)

        def fill_label(label: Optional[Label]) -> List:
            if label is None:
                return ([""] * (level - 1)) + [None]
            else:
                return ([""] * (level - len(label))) + list(label)

        column_labels = [
            tuple(["this"] + fill_label(label)) for label in this_internal.column_labels
        ] + [tuple(["that"] + fill_label(label)) for label in that_internal.column_labels]
        column_label_names = (
            cast(List[Optional[Label]], [None]) * (1 + level - this_internal.column_labels_level)
        ) + this_internal.column_label_names
        return DataFrame(
            InternalFrame(
                spark_frame=joined_df,
                index_spark_columns=index_spark_columns,
                index_names=this_internal.index_names,
                index_fields=index_fields,
                column_labels=column_labels,
                data_spark_columns=[scol_for(joined_df, col) for col in new_data_columns],
                data_fields=data_fields,
                column_label_names=column_label_names,
            )
        )
    else:
        raise ValueError(ERROR_MESSAGE_CANNOT_COMBINE)


def align_diff_frames(
    resolve_func: Callable[
        ["DataFrame", List[Label], List[Label]], Iterator[Tuple["Series", Label]]
    ],
    this: "DataFrame",
    that: "DataFrame",
    fillna: bool = True,
    how: str = "full",
    preserve_order_column: bool = False,
) -> "DataFrame":
    """
    This method aligns two different DataFrames with a given `func`. Columns are resolved and
    handled within the given `func`.
    To use this, `compute.ops_on_diff_frames` should be True, for now.

    :param resolve_func: Takes aligned (joined) DataFrame, the column of the current DataFrame, and
        the column of another DataFrame. It returns an iterable that produces Series.

        >>> from pyspark.pandas.config import set_option, reset_option
        >>>
        >>> set_option("compute.ops_on_diff_frames", True)
        >>>
        >>> psdf1 = ps.DataFrame({'a': [9, 8, 7, 6, 5, 4, 3, 2, 1]})
        >>> psdf2 = ps.DataFrame({'a': [9, 8, 7, 6, 5, 4, 3, 2, 1]})
        >>>
        >>> def func(psdf, this_column_labels, that_column_labels):
        ...    psdf  # conceptually this is A + B.
        ...
        ...    # Within this function, Series from A or B can be performed against `psdf`.
        ...    this_label = this_column_labels[0]  # this is ('a',) from psdf1.
        ...    that_label = that_column_labels[0]  # this is ('a',) from psdf2.
        ...    new_series = (psdf[this_label] - psdf[that_label]).rename(str(this_label))
        ...
        ...    # This new series will be placed in new DataFrame.
        ...    yield (new_series, this_label)
        >>>
        >>>
        >>> align_diff_frames(func, psdf1, psdf2).sort_index()
           a
        0  0
        1  0
        2  0
        3  0
        4  0
        5  0
        6  0
        7  0
        8  0
        >>> reset_option("compute.ops_on_diff_frames")

    :param this: a DataFrame to align
    :param that: another DataFrame to align
    :param fillna: If True, it fills missing values in non-common columns in both `this` and `that`.
        Otherwise, it returns as are.
    :param how: join way. In addition, it affects how `resolve_func` resolves the column conflict.
        - full: `resolve_func` should resolve only common columns from 'this' and 'that' DataFrames.
            For instance, if 'this' has columns A, B, C and that has B, C, D, `this_columns` and
            'that_columns' in this function are B, C and B, C.
        - left: `resolve_func` should resolve columns including `that` column.
            For instance, if 'this' has columns A, B, C and that has B, C, D, `this_columns` is
            B, C but `that_columns` are B, C, D.
        - inner: Same as 'full' mode; however, internally performs inner join instead.
    :return: Aligned DataFrame
    """
    from pyspark.pandas.frame import DataFrame

    assert how == "full" or how == "left" or how == "inner"

    this_column_labels = this._internal.column_labels
    that_column_labels = that._internal.column_labels
    common_column_labels = set(this_column_labels).intersection(that_column_labels)

    # 1. Perform the join given two dataframes.
    combined = combine_frames(this, that, how=how, preserve_order_column=preserve_order_column)

    # 2. Apply the given function to transform the columns in a batch and keep the new columns.
    combined_column_labels = combined._internal.column_labels

    that_columns_to_apply: List[Label] = []
    this_columns_to_apply: List[Label] = []
    additional_that_columns: List[Label] = []
    if is_remote():
        from pyspark.sql.connect.column import Column as ConnectColumn

        Column = ConnectColumn
    columns_to_keep: List[Union[Series, Column]] = []  # type: ignore[valid-type]
    column_labels_to_keep: List[Label] = []

    for combined_label in combined_column_labels:
        for common_label in common_column_labels:
            if combined_label == tuple(["this", *common_label]):
                this_columns_to_apply.append(combined_label)
                break
            elif combined_label == tuple(["that", *common_label]):
                that_columns_to_apply.append(combined_label)
                break
        else:
            if how == "left" and combined_label in [
                tuple(["that", *label]) for label in that_column_labels
            ]:
                # In this case, we will drop `that_columns` in `columns_to_keep` but passes
                # it later to `func`. `func` should resolve it.
                # Note that adding this into a separate list (`additional_that_columns`)
                # is intentional so that `this_columns` and `that_columns` can be paired.
                additional_that_columns.append(combined_label)
            elif fillna:
                columns_to_keep.append(F.lit(None).cast(DoubleType()).alias(str(combined_label)))
                column_labels_to_keep.append(combined_label)
            else:
                columns_to_keep.append(combined._psser_for(combined_label))
                column_labels_to_keep.append(combined_label)

    that_columns_to_apply += additional_that_columns

    # Should extract columns to apply and do it in a batch in case
    # it adds new columns for example.
    columns_applied: List[Union[Series, Column]]  # type: ignore[valid-type]
    column_labels_applied: List[Label]
    if len(this_columns_to_apply) > 0 or len(that_columns_to_apply) > 0:
        psser_set, column_labels_set = zip(
            *resolve_func(combined, this_columns_to_apply, that_columns_to_apply)
        )
        columns_applied = list(psser_set)
        column_labels_applied = list(column_labels_set)
    else:
        columns_applied = []
        column_labels_applied = []

    applied: DataFrame = DataFrame(
        combined._internal.with_new_columns(
            columns_applied + columns_to_keep,
            column_labels=column_labels_applied + column_labels_to_keep,
        )
    )

    # 3. Restore the names back and deduplicate columns.
    this_labels: Dict[Label, Label] = {}
    # Add columns in an order of its original frame.
    for this_label in this_column_labels:
        for new_label in applied._internal.column_labels:
            if new_label[1:] not in this_labels and this_label == new_label[1:]:
                this_labels[new_label[1:]] = new_label

    # After that, we will add the rest columns.
    other_labels: Dict[Label, Label] = {}
    for new_label in applied._internal.column_labels:
        if new_label[1:] not in this_labels:
            other_labels[new_label[1:]] = new_label

    psdf = applied[list(this_labels.values()) + list(other_labels.values())]
    psdf.columns = psdf.columns.droplevel()
    return psdf


def is_testing() -> bool:
    """Indicates whether Spark is currently running tests."""
    return "SPARK_TESTING" in os.environ


def default_session() -> SparkSession:
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("pandas-on-Spark").getOrCreate()

    # Turn ANSI off when testing the pandas API on Spark since
    # the behavior of pandas API on Spark follows pandas, not SQL.
    if is_testing():
        spark.conf.set("spark.sql.ansi.enabled", False)
    if spark.conf.get("spark.sql.ansi.enabled") == "true":
        log_advice(
            "The config 'spark.sql.ansi.enabled' is set to True. "
            "This can cause unexpected behavior "
            "from pandas API on Spark since pandas API on Spark follows "
            "the behavior of pandas, not SQL."
        )

    return spark


@contextmanager
def sql_conf(pairs: Dict[str, Any], *, spark: Optional[SparkSession] = None) -> Iterator[None]:
    """
    A convenient context manager to set `value` to the Spark SQL configuration `key` and
    then restores it back when it exits.
    """
    assert isinstance(pairs, dict), "pairs should be a dictionary."

    if spark is None:
        spark = default_session()

    keys = pairs.keys()
    new_values = pairs.values()
    old_values = [spark.conf.get(key, None) for key in keys]
    for key, new_value in zip(keys, new_values):
        spark.conf.set(key, new_value)
    try:
        yield
    finally:
        for key, old_value in zip(keys, old_values):
            if old_value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, old_value)


def validate_arguments_and_invoke_function(
    pobj: Union[pd.DataFrame, pd.Series],
    pandas_on_spark_func: Callable,
    pandas_func: Callable,
    input_args: Dict,
) -> Any:
    """
    Invokes a pandas function.

    This is created because different versions of pandas support different parameters, and as a
    result when we code against the latest version, our users might get a confusing
    "got an unexpected keyword argument" error if they are using an older version of pandas.

    This function validates all the arguments, removes the ones that are not supported if they
    are simply the default value (i.e. most likely the user didn't explicitly specify it). It
    throws a TypeError if the user explicitly specifies an argument that is not supported by the
    pandas version available.

    For example usage, look at DataFrame.to_html().

    :param pobj: the pandas DataFrame or Series to operate on
    :param pandas_on_spark_func: pandas-on-Spark function, used to get default parameter values
    :param pandas_func: pandas function, used to check whether pandas supports all the arguments
    :param input_args: arguments to pass to the pandas function, often created by using locals().
                       Make sure locals() call is at the top of the function so it captures only
                       input parameters, rather than local variables.
    :return: whatever pandas_func returns
    """
    import inspect

    # Makes a copy since whatever passed in is likely created by locals(), and we can't delete
    # 'self' key from that.
    args = input_args.copy()
    del args["self"]

    if "kwargs" in args:
        # explode kwargs
        kwargs = args["kwargs"]
        del args["kwargs"]
        args = {**args, **kwargs}

    pandas_on_spark_params = inspect.signature(pandas_on_spark_func).parameters
    pandas_params = inspect.signature(pandas_func).parameters

    for param in pandas_on_spark_params.values():
        if param.name not in pandas_params:
            if args[param.name] == param.default:
                del args[param.name]
            else:
                raise TypeError(
                    (
                        "The pandas version [%s] available does not support parameter '%s' "
                        + "for function '%s'."
                    )
                    % (pd.__version__, param.name, pandas_func.__name__)
                )

    args["self"] = pobj
    return pandas_func(**args)


@no_type_check
def lazy_property(fn: Callable[[Any], Any]) -> property:
    """
    Decorator that makes a property lazy-evaluated.

    Copied from https://stevenloria.com/lazy-properties/
    """
    attr_name = "_lazy_" + fn.__name__

    @property
    @functools.wraps(fn)
    def wrapped_lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    def deleter(self):
        if hasattr(self, attr_name):
            delattr(self, attr_name)

    return wrapped_lazy_property.deleter(deleter)


def scol_for(sdf: PySparkDataFrame, column_name: str) -> Column:
    """Return Spark Column for the given column name."""
    if is_remote():
        return sdf._col("`{}`".format(column_name))  # type: ignore[operator]
    else:
        return sdf["`{}`".format(column_name)]


def column_labels_level(column_labels: List[Label]) -> int:
    """Return the level of the column index."""
    if len(column_labels) == 0:
        return 1
    else:
        levels = set(1 if label is None else len(label) for label in column_labels)
        assert len(levels) == 1, levels
        return list(levels)[0]


def name_like_string(name: Optional[Name]) -> str:
    """
    Return the name-like strings from str or tuple of str

    Examples
    --------
    >>> name = 'abc'
    >>> name_like_string(name)
    'abc'

    >>> name = ('abc',)
    >>> name_like_string(name)
    'abc'

    >>> name = ('a', 'b', 'c')
    >>> name_like_string(name)
    '(a, b, c)'
    """
    label: Label
    if name is None:
        label = ("__none__",)
    elif is_list_like(name):
        label = tuple([str(n) for n in name])
    else:
        label = (str(name),)
    return ("(%s)" % ", ".join(label)) if len(label) > 1 else label[0]


def is_name_like_tuple(value: Any, allow_none: bool = True, check_type: bool = False) -> bool:
    """
    Check the given tuple is to be able to be used as a name.

    Examples
    --------
    >>> is_name_like_tuple(('abc',))
    True
    >>> is_name_like_tuple((1,))
    True
    >>> is_name_like_tuple(('abc', 1, None))
    True
    >>> is_name_like_tuple(('abc', 1, None), check_type=True)
    True
    >>> is_name_like_tuple((1.0j,))
    True
    >>> is_name_like_tuple(tuple())
    False
    >>> is_name_like_tuple((list('abc'),))
    False
    >>> is_name_like_tuple(('abc', 1, None), allow_none=False)
    False
    >>> is_name_like_tuple((1.0j,), check_type=True)
    False
    """
    if value is None:
        return allow_none
    elif not isinstance(value, tuple):
        return False
    elif len(value) == 0:
        return False
    elif not allow_none and any(v is None for v in value):
        return False
    elif any(is_list_like(v) or isinstance(v, slice) for v in value):
        return False
    elif check_type:
        return all(
            v is None or as_spark_type(type(v), raise_error=False) is not None for v in value
        )
    else:
        return True


def is_name_like_value(
    value: Any, allow_none: bool = True, allow_tuple: bool = True, check_type: bool = False
) -> bool:
    """
    Check the given value is like a name.

    Examples
    --------
    >>> is_name_like_value('abc')
    True
    >>> is_name_like_value(1)
    True
    >>> is_name_like_value(None)
    True
    >>> is_name_like_value(('abc',))
    True
    >>> is_name_like_value(1.0j)
    True
    >>> is_name_like_value(list('abc'))
    False
    >>> is_name_like_value(None, allow_none=False)
    False
    >>> is_name_like_value(('abc',), allow_tuple=False)
    False
    >>> is_name_like_value(1.0j, check_type=True)
    False
    """
    if value is None:
        return allow_none
    elif isinstance(value, tuple):
        return allow_tuple and is_name_like_tuple(
            value, allow_none=allow_none, check_type=check_type
        )
    elif is_list_like(value) or isinstance(value, slice):
        return False
    elif check_type:
        return as_spark_type(type(value), raise_error=False) is not None
    else:
        return True


def validate_axis(axis: Optional[Axis] = 0, none_axis: int = 0) -> int:
    """Check the given axis is valid."""
    # convert to numeric axis
    axis = cast(Dict[Optional[Axis], int], {None: none_axis, "index": 0, "columns": 1}).get(
        axis, axis
    )
    if axis in (none_axis, 0, 1):
        return cast(int, axis)
    else:
        raise ValueError("No axis named {0}".format(axis))


def validate_bool_kwarg(value: Any, arg_name: str) -> Optional[bool]:
    """Ensures that argument passed in arg_name is of type bool."""
    if not (isinstance(value, bool) or value is None):
        raise TypeError(
            'For argument "{}" expected type bool, received '
            "type {}.".format(arg_name, type(value).__name__)
        )
    return value


def validate_how(how: str) -> str:
    """Check the given how for join is valid."""
    if how == "full":
        warnings.warn(
            "Warning: While pandas-on-Spark will accept 'full', you should use 'outer' "
            + "instead to be compatible with the pandas merge API",
            UserWarning,
        )
    if how == "outer":
        # 'outer' in pandas equals 'full' in Spark
        how = "full"
    if how not in ("inner", "left", "right", "full"):
        raise ValueError(
            "The 'how' parameter has to be amongst the following values: ",
            "['inner', 'left', 'right', 'outer']",
        )
    return how


def validate_mode(mode: str) -> str:
    """Check the given mode for writing is valid."""
    if mode in ("w", "w+"):
        # 'w' in pandas equals 'overwrite' in Spark
        # '+' is meaningless for writing methods, but pandas just pass it as 'w'.
        mode = "overwrite"
    if mode in ("a", "a+"):
        # 'a' in pandas equals 'append' in Spark
        # '+' is meaningless for writing methods, but pandas just pass it as 'a'.
        mode = "append"
    if mode not in (
        "w",
        "a",
        "w+",
        "a+",
        "overwrite",
        "append",
        "ignore",
        "error",
        "errorifexists",
    ):
        raise ValueError(
            "The 'mode' parameter has to be amongst the following values: ",
            "['w', 'a', 'w+', 'a+', 'overwrite', 'append', 'ignore', 'error', 'errorifexists']",
        )
    return mode


@overload
def verify_temp_column_name(df: PySparkDataFrame, column_name_or_label: str) -> str:
    ...


@overload
def verify_temp_column_name(df: "DataFrame", column_name_or_label: Name) -> Label:
    ...


def verify_temp_column_name(
    df: Union["DataFrame", PySparkDataFrame],
    column_name_or_label: Union[str, Name],
) -> Union[str, Label]:
    """
    Verify that the given column name does not exist in the given pandas-on-Spark or
    Spark DataFrame.

    The temporary column names should start and end with `__`. In addition, `column_name_or_label`
    expects a single string, or column labels when `df` is a pandas-on-Spark DataFrame.

    >>> psdf = ps.DataFrame({("x", "a"): ['a', 'b', 'c']})
    >>> psdf["__dummy__"] = 0
    >>> psdf[("", "__dummy__")] = 1
    >>> psdf  # doctest: +NORMALIZE_WHITESPACE
       x __dummy__
       a           __dummy__
    0  a         0         1
    1  b         0         1
    2  c         0         1

    >>> verify_temp_column_name(psdf, '__tmp__')
    ('__tmp__', '')
    >>> verify_temp_column_name(psdf, ('', '__tmp__'))
    ('', '__tmp__')
    >>> verify_temp_column_name(psdf, '__dummy__')
    Traceback (most recent call last):
    ...
    AssertionError: ... `(__dummy__, )` ...
    >>> verify_temp_column_name(psdf, ('', '__dummy__'))
    Traceback (most recent call last):
    ...
    AssertionError: ... `(, __dummy__)` ...
    >>> verify_temp_column_name(psdf, 'dummy')
    Traceback (most recent call last):
    ...
    AssertionError: ... should be empty or start and end with `__`: ('dummy', '')
    >>> verify_temp_column_name(psdf, ('', 'dummy'))
    Traceback (most recent call last):
    ...
    AssertionError: ... should be empty or start and end with `__`: ('', 'dummy')

    >>> internal = psdf._internal.resolved_copy
    >>> sdf = internal.spark_frame
    >>> sdf.select(internal.data_spark_columns).show()  # doctest: +NORMALIZE_WHITESPACE
    +------+---------+-------------+
    |(x, a)|__dummy__|(, __dummy__)|
    +------+---------+-------------+
    |     a|        0|            1|
    |     b|        0|            1|
    |     c|        0|            1|
    +------+---------+-------------+

    >>> verify_temp_column_name(sdf, '__tmp__')
    '__tmp__'
    >>> verify_temp_column_name(sdf, '__dummy__')
    Traceback (most recent call last):
    ...
    AssertionError: ... `__dummy__` ... '(x, a)', '__dummy__', '(, __dummy__)', ...
    >>> verify_temp_column_name(sdf, ('', '__dummy__'))
    Traceback (most recent call last):
    ...
    AssertionError: <class 'tuple'>
    >>> verify_temp_column_name(sdf, 'dummy')
    Traceback (most recent call last):
    ...
    AssertionError: ... should start and end with `__`: dummy
    """
    from pyspark.pandas.frame import DataFrame

    if isinstance(df, DataFrame):
        if isinstance(column_name_or_label, str):
            column_name = column_name_or_label

            level = df._internal.column_labels_level
            column_name_or_label = tuple([column_name_or_label] + ([""] * (level - 1)))
        else:
            column_name = name_like_string(column_name_or_label)

        assert any(len(label) > 0 for label in column_name_or_label) and all(
            label == "" or (label.startswith("__") and label.endswith("__"))
            for label in column_name_or_label
        ), "The temporary column name should be empty or start and end with `__`: {}".format(
            column_name_or_label
        )
        assert all(
            column_name_or_label != label for label in df._internal.column_labels
        ), "The given column name `{}` already exists in the pandas-on-Spark DataFrame: {}".format(
            name_like_string(column_name_or_label), df.columns
        )
        df = df._internal.resolved_copy.spark_frame
    else:
        assert isinstance(column_name_or_label, str), type(column_name_or_label)
        assert column_name_or_label.startswith("__") and column_name_or_label.endswith(
            "__"
        ), "The temporary column name should start and end with `__`: {}".format(
            column_name_or_label
        )
        column_name = column_name_or_label

    assert isinstance(df, PySparkDataFrame), type(df)
    assert (
        column_name not in df.columns
    ), "The given column name `{}` already exists in the Spark DataFrame: {}".format(
        column_name, df.columns
    )

    return column_name_or_label


def spark_column_equals(left: Column, right: Column) -> bool:
    """
    Check both `left` and `right` have the same expressions.

    >>> spark_column_equals(sf.lit(0), sf.lit(0))
    True
    >>> spark_column_equals(sf.lit(0) + 1, sf.lit(0) + 1)
    True
    >>> spark_column_equals(sf.lit(0) + 1, sf.lit(0) + 2)
    False
    >>> sdf1 = ps.DataFrame({"x": ['a', 'b', 'c']}).to_spark()
    >>> spark_column_equals(sdf1["x"] + 1, sdf1["x"] + 1)
    True
    >>> sdf2 = ps.DataFrame({"x": ['a', 'b', 'c']}).to_spark()
    >>> spark_column_equals(sdf1["x"] + 1, sdf2["x"] + 1)
    False
    """
    if is_remote():
        from pyspark.sql.connect.column import Column as ConnectColumn

        if not isinstance(left, ConnectColumn):
            raise PySparkTypeError(
                error_class="NOT_COLUMN",
                message_parameters={"arg_name": "left", "arg_type": type(left).__name__},
            )
        if not isinstance(right, ConnectColumn):
            raise PySparkTypeError(
                error_class="NOT_COLUMN",
                message_parameters={"arg_name": "right", "arg_type": type(right).__name__},
            )
        return repr(left).replace("`", "") == repr(right).replace("`", "")
    else:
        return left._jc.equals(right._jc)


def compare_null_first(
    left: Column,
    right: Column,
    comp: Callable[
        [Column, Column],
        Column,
    ],
) -> Column:
    return (left.isNotNull() & right.isNotNull() & comp(left, right)) | (
        left.isNull() & right.isNotNull()
    )


def compare_null_last(
    left: Column,
    right: Column,
    comp: Callable[
        [Column, Column],
        Column,
    ],
) -> Column:
    return (left.isNotNull() & right.isNotNull() & comp(left, right)) | (
        left.isNotNull() & right.isNull()
    )


def compare_disallow_null(
    left: Column,
    right: Column,
    comp: Callable[
        [Column, Column],
        Column,
    ],
) -> Column:
    return left.isNotNull() & right.isNotNull() & comp(left, right)


def compare_allow_null(
    left: Column,
    right: Column,
    comp: Callable[
        [Column, Column],
        Column,
    ],
) -> Column:
    return left.isNull() | right.isNull() | comp(left, right)


def log_advice(message: str) -> None:
    """
    Display advisory logs for functions to be aware of when using pandas API on Spark
    for the existing pandas/PySpark users who may not be familiar with distributed environments
    or the behavior of pandas.
    """
    warnings.warn(message, PandasAPIOnSparkAdviceWarning)


def validate_index_loc(index: "Index", loc: int) -> None:
    """
    Raises IndexError if index is out of bounds
    """
    length = len(index)
    if loc < 0:
        loc = loc + length
        if loc < 0:
            raise IndexError(
                "index {} is out of bounds for axis 0 with size {}".format((loc - length), length)
            )
    else:
        if loc > length:
            raise IndexError(
                "index {} is out of bounds for axis 0 with size {}".format(loc, length)
            )


def xor(df1: PySparkDataFrame, df2: PySparkDataFrame) -> PySparkDataFrame:
    colNames = df1.columns

    tmp_tag_col = verify_temp_column_name(df1, "__temporary_tag__")
    tmp_max_col = verify_temp_column_name(df1, "__temporary_max_tag__")
    tmp_min_col = verify_temp_column_name(df1, "__temporary_min_tag__")

    return (
        df1.withColumn(tmp_tag_col, F.lit(0))
        .union(df2.withColumn(tmp_tag_col, F.lit(1)))
        .groupBy(*colNames)
        .agg(F.min(tmp_tag_col).alias(tmp_min_col), F.max(tmp_tag_col).alias(tmp_max_col))
        .where(F.col(tmp_min_col) == F.col(tmp_max_col))
        .select(*colNames)
    )


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.utils

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.utils.__dict__.copy()
    globs["ps"] = pyspark.pandas
    globs["sf"] = F
    spark = (
        SparkSession.builder.master("local[4]").appName("pyspark.pandas.utils tests").getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.utils,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
