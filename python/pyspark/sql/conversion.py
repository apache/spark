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

import array
import datetime
import decimal
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Sequence, Tuple, Union, overload

from pyspark.errors import PySparkValueError, PySparkTypeError, PySparkRuntimeError
from pyspark.sql.pandas.types import (
    _dedup_names,
    _deduplicate_field_names,
    _create_converter_to_pandas,
    _create_converter_from_pandas,
    from_arrow_type,
    to_arrow_schema,
)
from pyspark.sql.pandas.utils import require_minimum_pyarrow_version
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DataType,
    DecimalType,
    GeographyType,
    Geography,
    GeometryType,
    Geometry,
    MapType,
    NullType,
    Row,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
    UserDefinedType,
    VariantType,
    VariantVal,
    _create_row,
)

if TYPE_CHECKING:
    import pyarrow as pa
    import pandas as pd


class ArrowBatchTransformer:
    """
    Pure functions that transform Arrow data structures (Arrays, RecordBatches).
    They should have no side effects (no I/O, no writing to streams).

    This class provides utility methods for Arrow batch transformations used throughout
    PySpark's Arrow UDF implementation. All methods are static and handle common patterns
    like struct wrapping/unwrapping, schema conversions, and creating RecordBatches from Arrays.

    """

    @staticmethod
    def flatten_struct(batch: "pa.RecordBatch", column_index: int = 0) -> "pa.RecordBatch":
        """
        Flatten a struct column at given index into a RecordBatch.

        Used by
        -------
        - ArrowStreamGroupSerializer
        - ArrowStreamArrowUDTFSerializer
        - SQL_MAP_ARROW_ITER_UDF mapper
        - SQL_GROUPED_MAP_ARROW_UDF mapper
        - SQL_GROUPED_MAP_ARROW_ITER_UDF mapper
        - SQL_COGROUPED_MAP_ARROW_UDF mapper
        """
        import pyarrow as pa

        struct = batch.column(column_index)
        return pa.RecordBatch.from_arrays(struct.flatten(), schema=pa.schema(struct.type))

    @staticmethod
    def wrap_struct(batch: "pa.RecordBatch") -> "pa.RecordBatch":
        """
        Wrap a RecordBatch's columns into a single struct column.

        Used by
        -------
        - wrap_grouped_map_arrow_udf
        - wrap_grouped_map_arrow_iter_udf
        - wrap_cogrouped_map_arrow_udf
        - wrap_arrow_batch_iter_udf
        - ArrowStreamArrowUDTFSerializer.dump_stream
        - TransformWithStateInPySparkRowSerializer.dump_stream
        """
        import pyarrow as pa

        if batch.num_columns == 0:
            # When batch has no column, it should still create
            # an empty batch with the number of rows set.
            struct = pa.array([{}] * batch.num_rows)
        else:
            struct = pa.StructArray.from_arrays(batch.columns, fields=pa.struct(list(batch.schema)))
        return pa.RecordBatch.from_arrays([struct], ["_0"])

    @classmethod
    def concat_batches(cls, batches: List["pa.RecordBatch"]) -> "pa.RecordBatch":
        """
        Concatenate multiple RecordBatches into a single RecordBatch.

        This method handles both modern and legacy PyArrow versions.

        Parameters
        ----------
        batches : List[pa.RecordBatch]
            List of RecordBatches with the same schema

        Returns
        -------
        pa.RecordBatch
            Single RecordBatch containing all rows from input batches

        Used by
        -------
        - SQL_GROUPED_AGG_ARROW_UDF mapper
        - SQL_WINDOW_AGG_ARROW_UDF mapper

        Examples
        --------
        >>> import pyarrow as pa
        >>> batch1 = pa.RecordBatch.from_arrays([pa.array([1, 2])], ['a'])
        >>> batch2 = pa.RecordBatch.from_arrays([pa.array([3, 4])], ['a'])
        >>> result = ArrowBatchTransformer.concat_batches([batch1, batch2])
        >>> result.to_pydict()
        {'a': [1, 2, 3, 4]}
        """
        import pyarrow as pa

        if not batches:
            raise PySparkValueError(
                errorClass="INVALID_ARROW_BATCH_CONCAT",
                messageParameters={"reason": "Cannot concatenate empty list of batches"},
            )

        # Assert all batches have the same schema
        first_schema = batches[0].schema
        for i, batch in enumerate(batches[1:], start=1):
            if batch.schema != first_schema:
                raise PySparkValueError(
                    errorClass="INVALID_ARROW_BATCH_CONCAT",
                    messageParameters={
                        "reason": (
                            f"All batches must have the same schema. "
                            f"Batch 0 has schema {first_schema}, but batch {i} has schema {batch.schema}."
                        )
                    },
                )

        if hasattr(pa, "concat_batches"):
            return pa.concat_batches(batches)
        else:
            # pyarrow.concat_batches not supported in old versions
            return pa.RecordBatch.from_struct_array(
                pa.concat_arrays([b.to_struct_array() for b in batches])
            )

    @classmethod
    def zip_batches(
        cls,
        items: Union[
            List["pa.RecordBatch"],
            List["pa.Array"],
            List[Tuple["pa.Array", "pa.DataType"]],
        ],
        safecheck: bool = True,
    ) -> "pa.RecordBatch":
        """
        Zip multiple RecordBatches or Arrays horizontally by combining their columns.

        This is different from concat_batches which concatenates rows vertically.
        This method combines columns from multiple batches/arrays into a single batch,
        useful when multiple UDFs each produce a RecordBatch or when combining arrays.

        Parameters
        ----------
        items : List[pa.RecordBatch], List[pa.Array], or List[Tuple[pa.Array, pa.DataType]]
            - List of RecordBatches to zip (must have same number of rows)
            - List of Arrays to combine directly
            - List of (array, type) tuples for type casting (always attempts cast if types don't match)
        safecheck : bool, default True
            If True, use safe casting (fails on overflow/truncation) (only used when items are tuples).

        Returns
        -------
        pa.RecordBatch
            Single RecordBatch with all columns from input batches/arrays

        Used by
        -------
        - SQL_GROUPED_AGG_ARROW_UDF mapper
        - SQL_WINDOW_AGG_ARROW_UDF mapper
        - wrap_scalar_arrow_udf
        - wrap_grouped_agg_arrow_udf
        - ArrowBatchUDFSerializer.dump_stream

        Examples
        --------
        >>> import pyarrow as pa
        >>> batch1 = pa.RecordBatch.from_arrays([pa.array([1, 2])], ['a'])
        >>> batch2 = pa.RecordBatch.from_arrays([pa.array([3, 4])], ['b'])
        >>> result = ArrowBatchTransformer.zip_batches([batch1, batch2])
        >>> result.to_pydict()
        {'_0': [1, 2], '_1': [3, 4]}
        >>> # Can also zip arrays directly
        >>> result = ArrowBatchTransformer.zip_batches([pa.array([1, 2]), pa.array([3, 4])])
        >>> result.to_pydict()
        {'_0': [1, 2], '_1': [3, 4]}
        >>> # Can also zip with type casting
        >>> result = ArrowBatchTransformer.zip_batches(
        ...     [(pa.array([1, 2]), pa.int64()), (pa.array([3, 4]), pa.int64())]
        ... )
        """
        import pyarrow as pa

        if not items:
            raise PySparkValueError(
                errorClass="INVALID_ARROW_BATCH_ZIP",
                messageParameters={"reason": "Cannot zip empty list"},
            )

        # Check if items are RecordBatches, Arrays, or (array, type) tuples
        first_item = items[0]

        if isinstance(first_item, pa.RecordBatch):
            # Handle RecordBatches
            batches = items
            if len(batches) == 1:
                return batches[0]

            # Assert all batches have the same number of rows
            num_rows = batches[0].num_rows
            for i, batch in enumerate(batches[1:], start=1):
                assert batch.num_rows == num_rows, (
                    f"All batches must have the same number of rows. "
                    f"Batch 0 has {num_rows} rows, but batch {i} has {batch.num_rows} rows."
                )

            # Combine all columns from all batches
            all_columns = []
            for batch in batches:
                all_columns.extend(batch.columns)
        elif isinstance(first_item, tuple) and len(first_item) == 2:
            # Handle (array, type) tuples with type casting (always attempt cast if types don't match)
            all_columns = [
                cls._cast_array(
                    array,
                    arrow_type,
                    safecheck=safecheck,
                    error_message=(
                        "Arrow UDFs require the return type to match the expected Arrow type. "
                        f"Expected: {arrow_type}, but got: {array.type}."
                    ),
                )
                for array, arrow_type in items
            ]
        else:
            # Handle Arrays directly
            all_columns = list(items)

        # Create RecordBatch from columns
        return pa.RecordBatch.from_arrays(all_columns, ["_%d" % i for i in range(len(all_columns))])

    @classmethod
    def reorder_columns(
        cls, batch: "pa.RecordBatch", target_schema: Union["pa.StructType", "StructType"]
    ) -> "pa.RecordBatch":
        """
        Reorder columns in a RecordBatch to match target schema field order.

        This method is useful when columns need to be arranged in a specific order
        for schema compatibility, particularly when assign_cols_by_name is enabled.

        Parameters
        ----------
        batch : pa.RecordBatch
            Input RecordBatch with columns to reorder
        target_schema : pa.StructType or pyspark.sql.types.StructType
            Target schema defining the desired column order.
            Can be either PyArrow StructType or Spark StructType.

        Returns
        -------
        pa.RecordBatch
            New RecordBatch with columns reordered to match target schema

        Used by
        -------
        - wrap_grouped_map_arrow_udf
        - wrap_grouped_map_arrow_iter_udf
        - wrap_cogrouped_map_arrow_udf

        Examples
        --------
        >>> import pyarrow as pa
        >>> from pyspark.sql.types import StructType, StructField, IntegerType
        >>> batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array([3, 4])], ['b', 'a'])
        >>> # Using PyArrow schema
        >>> target_pa = pa.struct([pa.field('a', pa.int64()), pa.field('b', pa.int64())])
        >>> result = ArrowBatchTransformer.reorder_columns(batch, target_pa)
        >>> result.schema.names
        ['a', 'b']
        >>> # Using Spark schema
        >>> target_spark = StructType([StructField('a', IntegerType()), StructField('b', IntegerType())])
        >>> result = ArrowBatchTransformer.reorder_columns(batch, target_spark)
        >>> result.schema.names
        ['a', 'b']
        """
        import pyarrow as pa

        # Convert Spark StructType to PyArrow StructType if needed
        if hasattr(target_schema, "fields") and hasattr(target_schema.fields[0], "dataType"):
            # This is Spark StructType - convert to PyArrow
            from pyspark.sql.pandas.types import to_arrow_schema

            arrow_schema = to_arrow_schema(target_schema)
            field_names = [field.name for field in arrow_schema]
        else:
            # This is PyArrow StructType
            field_names = [field.name for field in target_schema]

        return pa.RecordBatch.from_arrays(
            [batch.column(name) for name in field_names],
            names=field_names,
        )

    @classmethod
    def to_pandas(
        cls,
        batch: Union["pa.RecordBatch", "pa.Table"],
        timezone: str,
        schema: Optional["StructType"] = None,
        struct_in_pandas: str = "dict",
        ndarray_as_list: bool = False,
        df_for_struct: bool = False,
    ) -> List[Union["pd.Series", "pd.DataFrame"]]:
        """
        Convert a RecordBatch or Table to a list of pandas Series.

        Parameters
        ----------
        batch : pa.RecordBatch or pa.Table
            The Arrow RecordBatch or Table to convert.
        timezone : str
            Timezone for timestamp conversion.
        schema : StructType, optional
            Spark schema for type conversion. If None, types are inferred from Arrow.
        struct_in_pandas : str
            How to represent struct in pandas ("dict", "row", etc.)
        ndarray_as_list : bool
            Whether to convert ndarray as list.
        df_for_struct : bool
            If True, convert struct columns to DataFrame instead of Series.

        Returns
        -------
        List[Union[pd.Series, pd.DataFrame]]
            List of pandas Series (or DataFrame if df_for_struct=True), one for each column.
        """
        import pandas as pd

        import pyspark
        from pyspark.sql.pandas.types import from_arrow_type

        if batch.num_columns == 0:
            return [pd.Series([pyspark._NoValue] * batch.num_rows)]

        return [
            ArrowArrayToPandasConversion.convert(
                batch.column(i),
                schema[i].dataType if schema is not None else from_arrow_type(batch.column(i).type),
                timezone=timezone,
                struct_in_pandas=struct_in_pandas,
                ndarray_as_list=ndarray_as_list,
                df_for_struct=df_for_struct,
            )
            for i in range(batch.num_columns)
        ]

    @classmethod
    def _cast_array(
        cls,
        arr: "pa.Array",
        target_type: "pa.DataType",
        safecheck: bool = True,
        error_message: Optional[str] = None,
    ) -> "pa.Array":
        """
        Cast an Arrow Array to a target type with type checking.

        This is a private method used internally by zip_batches.

        Parameters
        ----------
        arr : pa.Array
            The Arrow Array to cast.
        target_type : pa.DataType
            Target Arrow data type.
        safecheck : bool
            If True, use safe casting (fails on overflow/truncation).
        error_message : str, optional
            Custom error message for type mismatch (used if cast fails).

        Returns
        -------
        pa.Array
            The casted array if types differ, or original array if types match.
        """
        import pyarrow as pa

        assert isinstance(arr, pa.Array)
        assert isinstance(target_type, pa.DataType)

        if arr.type == target_type:
            return arr

        try:
            return arr.cast(target_type=target_type, safe=safecheck)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
            if error_message:
                raise PySparkTypeError(error_message) from e
            else:
                raise PySparkTypeError(
                    f"Arrow type mismatch. Expected: {target_type}, but got: {arr.type}."
                ) from e


class PandasBatchTransformer:
    """
    Pure functions that transform between pandas DataFrames/Series and Arrow RecordBatches.
    They should have no side effects (no I/O, no writing to streams).

    This class provides utility methods for converting between pandas and Arrow formats,
    used primarily by Pandas UDF wrappers and serializers.

    """

    @classmethod
    def concat_series_batches(
        cls, series_batches: List[List["pd.Series"]], arg_offsets: Optional[List[int]] = None
    ) -> List["pd.Series"]:
        """
        Concatenate multiple batches of pandas Series column-wise.

        Takes a list of batches where each batch is a list of Series (one per column),
        and concatenates all Series column-by-column to produce a single list of
        concatenated Series.

        Parameters
        ----------
        series_batches : List[List[pd.Series]]
            List of batches, each batch is a list of Series (one Series per column)
        arg_offsets : Optional[List[int]]
            If provided and series_batches is empty, determines the number of empty Series to create

        Returns
        -------
        List[pd.Series]
            List of concatenated Series, one per column

        Used by
        -------
        - SQL_GROUPED_AGG_PANDAS_UDF mapper
        - SQL_WINDOW_AGG_PANDAS_UDF mapper

        Examples
        --------
        >>> import pandas as pd
        >>> batch1 = [pd.Series([1, 2]), pd.Series([3, 4])]
        >>> batch2 = [pd.Series([5, 6]), pd.Series([7, 8])]
        >>> result = PandasBatchTransformer.concat_series_batches([batch1, batch2])
        >>> len(result)
        2
        >>> result[0].tolist()
        [1, 2, 5, 6]
        """
        import pandas as pd

        if not series_batches:
            # Empty batches - create empty Series
            if arg_offsets:
                num_columns = max(arg_offsets) + 1 if arg_offsets else 0
            else:
                num_columns = 0
            return [pd.Series(dtype=object) for _ in range(num_columns)]

        # Concatenate Series by column
        num_columns = len(series_batches[0])
        return [
            pd.concat([batch[i] for batch in series_batches], ignore_index=True)
            for i in range(num_columns)
        ]

    @classmethod
    def series_batches_to_dataframe(cls, series_batches) -> "pd.DataFrame":
        """
        Convert an iterator of Series lists to a single DataFrame.

        Each batch is a list of Series (one per column). This method concatenates
        Series within each batch horizontally (axis=1), then concatenates all
        resulting DataFrames vertically (axis=0).

        Parameters
        ----------
        series_batches : Iterator[List[pd.Series]]
            Iterator where each element is a list of Series representing one batch

        Returns
        -------
        pd.DataFrame
            Combined DataFrame with all data, or empty DataFrame if no batches

        Used by
        -------
        - wrap_grouped_map_pandas_udf
        - wrap_grouped_map_pandas_iter_udf

        Examples
        --------
        >>> import pandas as pd
        >>> batch1 = [pd.Series([1, 2], name='a'), pd.Series([3, 4], name='b')]
        >>> batch2 = [pd.Series([5, 6], name='a'), pd.Series([7, 8], name='b')]
        >>> df = PandasBatchTransformer.series_batches_to_dataframe([batch1, batch2])
        >>> df.shape
        (4, 2)
        >>> df.columns.tolist()
        ['a', 'b']
        """
        import pandas as pd

        # Materialize iterator and convert each batch to DataFrame
        dataframes = [pd.concat(series_list, axis=1) for series_list in series_batches]

        # Concatenate all DataFrames vertically
        return pd.concat(dataframes, axis=0) if dataframes else pd.DataFrame()

    @classmethod
    def to_arrow(
        cls,
        series: Union["pd.Series", "pd.DataFrame", List],
        timezone: str,
        safecheck: bool = True,
        int_to_decimal_coercion_enabled: bool = False,
        as_struct: bool = False,
        struct_in_pandas: Optional[str] = None,
        ignore_unexpected_complex_type_values: bool = False,
        error_class: Optional[str] = None,
        assign_cols_by_name: bool = True,
        arrow_cast: bool = False,
    ) -> "pa.RecordBatch":
        """
        Convert pandas.Series/DataFrame or list to Arrow RecordBatch.

        Parameters
        ----------
        series : pandas.Series, pandas.DataFrame or list
            A single series/dataframe, list of series/dataframes, or list of
            (data, arrow_type) or (data, arrow_type, spark_type)
        timezone : str
            Timezone for timestamp conversion
        safecheck : bool
            Whether to perform safe type checking
        int_to_decimal_coercion_enabled : bool
            Whether to enable int to decimal coercion
        as_struct : bool
            If True, treat all inputs as DataFrames and create struct arrays (for UDTF)
        struct_in_pandas : str, optional
            How struct types are represented. If "dict", struct types require DataFrame input.
        ignore_unexpected_complex_type_values : bool
            Whether to ignore unexpected complex type values (for UDTF)
        error_class : str, optional
            Custom error class for type cast errors (for UDTF)
        assign_cols_by_name : bool
            If True, assign columns by name; otherwise by position (for struct)
        arrow_cast : bool
            Whether to apply Arrow casting when type mismatches

        Returns
        -------
        pyarrow.RecordBatch
            Arrow RecordBatch
        """
        import pandas as pd
        import pyarrow as pa
        from pyspark.sql.pandas.types import is_variant

        # Normalize input to a consistent format
        # Make input conform to [(series1, arrow_type1, spark_type1), (series2, arrow_type2, spark_type2), ...]
        if (
            not isinstance(series, (list, tuple))
            or (len(series) == 2 and isinstance(series[1], pa.DataType))
            or (
                len(series) == 3
                and isinstance(series[1], pa.DataType)
                and isinstance(series[2], DataType)
            )
        ):
            series = [series]
        series = ((s, None) if not isinstance(s, (list, tuple)) else s for s in series)
        normalized_series = ((s[0], s[1], None) if len(s) == 2 else s for s in series)

        arrs = []
        for s, arrow_type, spark_type in normalized_series:
            if as_struct or (
                struct_in_pandas == "dict"
                and arrow_type is not None
                and pa.types.is_struct(arrow_type)
                and not is_variant(arrow_type)
            ):
                # Struct mode: require DataFrame, create struct array
                if not isinstance(s, pd.DataFrame):
                    error_msg = (
                        "Output of an arrow-optimized Python UDTFs expects "
                        if as_struct
                        else "Invalid return type. Please make sure that the UDF returns a "
                    )
                    if not as_struct:
                        error_msg += (
                            "pandas.DataFrame when the specified return type is StructType."
                        )
                    else:
                        error_msg += f"a pandas.DataFrame but got: {type(s)}"
                    raise PySparkValueError(error_msg)

                # Create struct array from DataFrame
                if len(s.columns) == 0:
                    struct_arr = pa.array([{}] * len(s), arrow_type)
                else:
                    # Determine column selection strategy
                    use_name_matching = assign_cols_by_name and any(
                        isinstance(name, str) for name in s.columns
                    )

                    struct_arrs = []
                    for i, field in enumerate(arrow_type):
                        # Get Series and spark_type based on matching strategy
                        if use_name_matching:
                            series = s[field.name]
                            field_spark_type = (
                                spark_type[field.name].dataType if spark_type is not None else None
                            )
                        else:
                            series = s[s.columns[i]].rename(field.name)
                            field_spark_type = (
                                spark_type[i].dataType if spark_type is not None else None
                            )

                        struct_arrs.append(
                            PandasSeriesToArrowConversion.create_array(
                                series,
                                field.type,
                                timezone=timezone,
                                safecheck=safecheck,
                                spark_type=field_spark_type,
                                arrow_cast=arrow_cast,
                                int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
                                ignore_unexpected_complex_type_values=ignore_unexpected_complex_type_values,
                                error_class=error_class,
                            )
                        )

                    struct_arr = pa.StructArray.from_arrays(struct_arrs, fields=list(arrow_type))

                arrs.append(struct_arr)
            else:
                # Normal mode: create array from Series
                arrs.append(
                    PandasSeriesToArrowConversion.create_array(
                        s,
                        arrow_type,
                        timezone=timezone,
                        safecheck=safecheck,
                        spark_type=spark_type,
                        int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
                        arrow_cast=arrow_cast,
                        ignore_unexpected_complex_type_values=ignore_unexpected_complex_type_values,
                        error_class=error_class,
                    )
                )
        return pa.RecordBatch.from_arrays(arrs, ["_%d" % i for i in range(len(arrs))])


class LocalDataToArrowConversion:
    """
    Conversion from local data (except pandas DataFrame and numpy ndarray) to Arrow.
    """

    @staticmethod
    def _need_converter(
        dataType: DataType,
        nullable: bool = True,
    ) -> bool:
        if not nullable:
            # always check the nullability
            return True
        elif isinstance(dataType, NullType):
            # always check the nullability
            return True
        elif isinstance(dataType, StructType):
            # Struct maybe rows, should convert to dict.
            return True
        elif isinstance(dataType, ArrayType):
            return LocalDataToArrowConversion._need_converter(
                dataType.elementType, dataType.containsNull
            )
        elif isinstance(dataType, MapType):
            # Different from PySpark, here always needs conversion,
            # since an Arrow Map requires a list of tuples.
            return True
        elif isinstance(dataType, BinaryType):
            return True
        elif isinstance(dataType, (TimestampType, TimestampNTZType)):
            # Always truncate
            return True
        elif isinstance(dataType, DecimalType):
            # Convert Decimal('NaN') to None
            # Rescale Decimal values
            return True
        elif isinstance(dataType, StringType):
            # Coercion to StringType is allowed
            return True
        elif isinstance(dataType, UserDefinedType):
            return True
        elif isinstance(dataType, VariantType):
            return True
        elif isinstance(dataType, GeometryType):
            return True
        elif isinstance(dataType, GeographyType):
            return True
        else:
            return False

    @overload
    @staticmethod
    def _create_converter(
        dataType: DataType, nullable: bool = True, *, int_to_decimal_coercion_enabled: bool = False
    ) -> Callable:
        pass

    @overload
    @staticmethod
    def _create_converter(
        dataType: DataType,
        nullable: bool = True,
        *,
        none_on_identity: bool = False,
        int_to_decimal_coercion_enabled: bool = False,
    ) -> Optional[Callable]:
        pass

    @staticmethod
    def _create_converter(
        dataType: DataType,
        nullable: bool = True,
        *,
        none_on_identity: bool = False,
        int_to_decimal_coercion_enabled: bool = False,
    ) -> Optional[Callable]:
        assert dataType is not None and isinstance(dataType, DataType)
        assert isinstance(nullable, bool)

        if not LocalDataToArrowConversion._need_converter(dataType, nullable):
            if none_on_identity:
                return None
            else:
                return lambda value: value

        if isinstance(dataType, NullType):

            def convert_null(value: Any) -> Any:
                if value is not None:
                    raise PySparkValueError(f"input for {dataType} must be None, but got {value}")
                return None

            return convert_null

        elif isinstance(dataType, StructType):
            field_names = dataType.fieldNames()
            len_field_names = len(field_names)
            dedup_field_names = _dedup_names(dataType.names)

            field_convs = [
                LocalDataToArrowConversion._create_converter(
                    field.dataType,
                    field.nullable,
                    none_on_identity=True,
                    int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
                )
                for field in dataType.fields
            ]

            def convert_struct(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                else:
                    # The `value` should be tuple, dict, or have `__dict__`.
                    if isinstance(value, tuple):  # `Row` inherits `tuple`
                        if len(value) != len_field_names:
                            raise PySparkValueError(
                                errorClass="AXIS_LENGTH_MISMATCH",
                                messageParameters={
                                    "expected_length": str(len_field_names),
                                    "actual_length": str(len(value)),
                                },
                            )
                        return {
                            dedup_field_names[i]: (
                                field_convs[i](value[i])  # type: ignore[misc]
                                if field_convs[i] is not None
                                else value[i]
                            )
                            for i in range(len_field_names)
                        }
                    elif isinstance(value, dict):
                        return {
                            dedup_field_names[i]: (
                                field_convs[i](value.get(field))  # type: ignore[misc]
                                if field_convs[i] is not None
                                else value.get(field)
                            )
                            for i, field in enumerate(field_names)
                        }
                    else:
                        assert hasattr(value, "__dict__"), f"{type(value)} {value}"
                        value = value.__dict__
                        return {
                            dedup_field_names[i]: (
                                field_convs[i](value.get(field))  # type: ignore[misc]
                                if field_convs[i] is not None
                                else value.get(field)
                            )
                            for i, field in enumerate(field_names)
                        }

            return convert_struct

        elif isinstance(dataType, ArrayType):
            element_conv = LocalDataToArrowConversion._create_converter(
                dataType.elementType,
                dataType.containsNull,
                none_on_identity=True,
                int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
            )

            if element_conv is None:

                def convert_array(value: Any) -> Any:
                    if value is None:
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None
                    else:
                        assert isinstance(value, (list, array.array))
                        return list(value)

            else:

                def convert_array(value: Any) -> Any:
                    if value is None:
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None
                    else:
                        assert isinstance(value, (list, array.array))
                        return [element_conv(v) for v in value]

            return convert_array

        elif isinstance(dataType, MapType):
            key_conv = LocalDataToArrowConversion._create_converter(
                dataType.keyType,
                nullable=False,
                int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
            )
            value_conv = LocalDataToArrowConversion._create_converter(
                dataType.valueType,
                dataType.valueContainsNull,
                none_on_identity=True,
                int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
            )

            if value_conv is None:

                def convert_map(value: Any) -> Any:
                    if value is None:
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None
                    else:
                        assert isinstance(value, dict)
                        return [(key_conv(k), v) for k, v in value.items()]

            else:

                def convert_map(value: Any) -> Any:
                    if value is None:
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None
                    else:
                        assert isinstance(value, dict)
                        return [(key_conv(k), value_conv(v)) for k, v in value.items()]

            return convert_map

        elif isinstance(dataType, BinaryType):

            def convert_binary(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                else:
                    assert isinstance(value, (bytes, bytearray))
                    return bytes(value)

            return convert_binary

        elif isinstance(dataType, TimestampType):

            def convert_timestamp(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                else:
                    assert isinstance(value, datetime.datetime)
                    return value.astimezone(datetime.timezone.utc)

            return convert_timestamp

        elif isinstance(dataType, TimestampNTZType):

            def convert_timestamp_ntz(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                else:
                    assert isinstance(value, datetime.datetime) and value.tzinfo is None
                    return value

            return convert_timestamp_ntz

        elif isinstance(dataType, DecimalType):
            exp = decimal.Decimal(f"1E-{dataType.scale}")
            ctx = decimal.Context(prec=dataType.precision, rounding=decimal.ROUND_HALF_EVEN)

            def convert_decimal(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                else:
                    if int_to_decimal_coercion_enabled and isinstance(value, int):
                        value = decimal.Decimal(value)

                    assert isinstance(value, decimal.Decimal)
                    if value.is_nan():
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None

                    return value.quantize(exp, context=ctx)

            return convert_decimal

        elif isinstance(dataType, StringType):

            def convert_string(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                else:
                    if isinstance(value, bool):
                        # To match the PySpark Classic which convert bool to string in
                        # the JVM side (python.EvaluatePython.makeFromJava)
                        return str(value).lower()
                    else:
                        return str(value)

            return convert_string

        elif isinstance(dataType, UserDefinedType):
            udt: UserDefinedType = dataType

            conv = LocalDataToArrowConversion._create_converter(
                udt.sqlType(),
                nullable=nullable,
                none_on_identity=True,
                int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
            )

            if conv is None:

                def convert_udt(value: Any) -> Any:
                    if value is None:
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None
                    else:
                        return udt.serialize(value)

            else:

                def convert_udt(value: Any) -> Any:
                    if value is None:
                        if not nullable:
                            raise PySparkValueError(f"input for {dataType} must not be None")
                        return None
                    else:
                        return conv(udt.serialize(value))

            return convert_udt

        elif isinstance(dataType, VariantType):

            def convert_variant(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                elif isinstance(value, VariantVal):
                    return VariantType().toInternal(value)
                else:
                    raise PySparkValueError(errorClass="MALFORMED_VARIANT")

            return convert_variant

        elif isinstance(dataType, GeographyType):

            def convert_geography(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                elif isinstance(value, Geography):
                    return dataType.toInternal(value)
                else:
                    raise PySparkValueError(errorClass="MALFORMED_GEOGRAPHY")

            return convert_geography

        elif isinstance(dataType, GeometryType):

            def convert_geometry(value: Any) -> Any:
                if value is None:
                    if not nullable:
                        raise PySparkValueError(f"input for {dataType} must not be None")
                    return None
                elif isinstance(value, Geometry):
                    return dataType.toInternal(value)
                else:
                    raise PySparkValueError(errorClass="MALFORMED_GEOMETRY")

            return convert_geometry

        elif not nullable:

            def convert_other(value: Any) -> Any:
                if value is None:
                    raise PySparkValueError(f"input for {dataType} must not be None")
                return value

            return convert_other
        else:  # pragma: no cover
            assert False, f"Need converter for {dataType} but failed to find one."

    @staticmethod
    def create_array(
        results: Sequence[Any],
        arrow_type: "pa.DataType",
        spark_type: DataType,
        safecheck: bool = True,
        int_to_decimal_coercion_enabled: bool = False,
    ) -> "pa.Array":
        """
        Create an Arrow Array from a sequence of Python values.

        Parameters
        ----------
        results : Sequence[Any]
            Sequence of Python values to convert
        arrow_type : pa.DataType
            Target Arrow data type
        spark_type : DataType
            Spark data type for conversion
        safecheck : bool
            If True, use safe casting (fails on overflow/truncation)
        int_to_decimal_coercion_enabled : bool
            If True, applies additional coercions in Python before converting to Arrow

        Returns
        -------
        pa.Array
            Arrow Array with converted values
        """
        import pyarrow as pa

        conv = LocalDataToArrowConversion._create_converter(
            spark_type,
            none_on_identity=True,
            int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
        )
        converted = [conv(res) for res in results] if conv is not None else results
        try:
            return pa.array(converted, type=arrow_type)
        except pa.lib.ArrowInvalid:
            return pa.array(converted).cast(target_type=arrow_type, safe=safecheck)

    @staticmethod
    def convert(data: Sequence[Any], schema: StructType, use_large_var_types: bool) -> "pa.Table":
        require_minimum_pyarrow_version()
        import pyarrow as pa

        assert isinstance(data, list) and len(data) > 0

        assert schema is not None and isinstance(schema, StructType)

        column_names = schema.fieldNames()
        len_column_names = len(column_names)

        def to_row(item: Any) -> tuple:
            if item is None:
                return tuple([None] * len_column_names)
            elif isinstance(item, tuple):  # `Row` inherits `tuple`
                if len(item) != len_column_names:
                    raise PySparkValueError(
                        errorClass="AXIS_LENGTH_MISMATCH",
                        messageParameters={
                            "expected_length": str(len_column_names),
                            "actual_length": str(len(item)),
                        },
                    )
                return tuple(item)
            elif isinstance(item, dict):
                return tuple([item.get(col) for col in column_names])
            elif isinstance(item, VariantVal):
                raise PySparkValueError("Rows cannot be of type VariantVal")
            elif hasattr(item, "__dict__"):
                item = item.__dict__
                return tuple([item.get(col) for col in column_names])
            else:
                if len(item) != len_column_names:
                    raise PySparkValueError(
                        errorClass="AXIS_LENGTH_MISMATCH",
                        messageParameters={
                            "expected_length": str(len_column_names),
                            "actual_length": str(len(item)),
                        },
                    )
                return tuple(item)

        rows = [to_row(item) for item in data]

        if len_column_names > 0:
            column_convs = [
                LocalDataToArrowConversion._create_converter(
                    field.dataType,
                    field.nullable,
                    none_on_identity=True,
                    # Default to False for general data conversion
                    int_to_decimal_coercion_enabled=False,
                )
                for field in schema.fields
            ]

            pylist = [
                [conv(row[i]) for row in rows] if conv is not None else [row[i] for row in rows]
                for i, conv in enumerate(column_convs)
            ]

            pa_schema = to_arrow_schema(
                StructType(
                    [
                        StructField(
                            field.name, _deduplicate_field_names(field.dataType), field.nullable
                        )
                        for field in schema.fields
                    ]
                ),
                timezone="UTC",
                prefers_large_types=use_large_var_types,
            )

            return pa.Table.from_arrays(pylist, schema=pa_schema)
        else:
            return pa.Table.from_struct_array(pa.array([{}] * len(rows)))


class ArrowTableToRowsConversion:
    """
    Conversion from Arrow Table to Rows.
    """

    @staticmethod
    def _need_converter(dataType: DataType) -> bool:
        if isinstance(dataType, NullType):
            return True
        elif isinstance(dataType, StructType):
            return True
        elif isinstance(dataType, ArrayType):
            return ArrowTableToRowsConversion._need_converter(dataType.elementType)
        elif isinstance(dataType, MapType):
            # Different from PySpark, here always needs conversion,
            # since the input from Arrow is a list of tuples.
            return True
        elif isinstance(dataType, BinaryType):
            return True
        elif isinstance(dataType, (TimestampType, TimestampNTZType)):
            # Always remove the time zone info for now
            return True
        elif isinstance(dataType, UserDefinedType):
            return True
        elif isinstance(dataType, VariantType):
            return True
        elif isinstance(dataType, GeographyType):
            return True
        elif isinstance(dataType, GeometryType):
            return True
        else:
            return False

    @overload
    @staticmethod
    def _create_converter(dataType: DataType) -> Callable:
        pass

    @overload
    @staticmethod
    def _create_converter(
        dataType: DataType, *, none_on_identity: bool = True, binary_as_bytes: bool = True
    ) -> Optional[Callable]:
        pass

    @staticmethod
    def _create_converter(
        dataType: DataType, *, none_on_identity: bool = False, binary_as_bytes: bool = True
    ) -> Optional[Callable]:
        assert dataType is not None and isinstance(dataType, DataType)

        if not ArrowTableToRowsConversion._need_converter(dataType):
            if none_on_identity:
                return None
            else:
                return lambda value: value

        if isinstance(dataType, NullType):
            return lambda value: None

        elif isinstance(dataType, StructType):
            field_names = dataType.names
            dedup_field_names = _dedup_names(field_names)

            field_convs = [
                ArrowTableToRowsConversion._create_converter(
                    f.dataType, none_on_identity=True, binary_as_bytes=binary_as_bytes
                )
                for f in dataType.fields
            ]

            def convert_struct(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, dict)

                    _values = [
                        field_convs[i](value.get(name, None))  # type: ignore[misc]
                        if field_convs[i] is not None
                        else value.get(name, None)
                        for i, name in enumerate(dedup_field_names)
                    ]
                    return _create_row(field_names, _values)

            return convert_struct

        elif isinstance(dataType, ArrayType):
            element_conv = ArrowTableToRowsConversion._create_converter(
                dataType.elementType, none_on_identity=True, binary_as_bytes=binary_as_bytes
            )

            assert (
                element_conv is not None
            ), f"_need_converter() returned True for ArrayType of {dataType.elementType}"

            def convert_array(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, list)
                    return [element_conv(v) for v in value]

            return convert_array

        elif isinstance(dataType, MapType):
            key_conv = ArrowTableToRowsConversion._create_converter(
                dataType.keyType, none_on_identity=True, binary_as_bytes=binary_as_bytes
            )
            value_conv = ArrowTableToRowsConversion._create_converter(
                dataType.valueType, none_on_identity=True, binary_as_bytes=binary_as_bytes
            )

            if key_conv is None:
                if value_conv is None:

                    def convert_map(value: Any) -> Any:
                        if value is None:
                            return None
                        else:
                            assert isinstance(value, list)
                            assert all(isinstance(t, tuple) and len(t) == 2 for t in value)
                            return dict(value)

                else:

                    def convert_map(value: Any) -> Any:
                        if value is None:
                            return None
                        else:
                            assert isinstance(value, list)
                            assert all(isinstance(t, tuple) and len(t) == 2 for t in value)
                            return dict((t[0], value_conv(t[1])) for t in value)

            else:
                if value_conv is None:

                    def convert_map(value: Any) -> Any:
                        if value is None:
                            return None
                        else:
                            assert isinstance(value, list)
                            assert all(isinstance(t, tuple) and len(t) == 2 for t in value)
                            return dict((key_conv(t[0]), t[1]) for t in value)

                else:

                    def convert_map(value: Any) -> Any:
                        if value is None:
                            return None
                        else:
                            assert isinstance(value, list)
                            assert all(isinstance(t, tuple) and len(t) == 2 for t in value)
                            return dict((key_conv(t[0]), value_conv(t[1])) for t in value)

            return convert_map

        elif isinstance(dataType, BinaryType):

            def convert_binary(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, bytes)
                    return value if binary_as_bytes else bytearray(value)

            return convert_binary

        elif isinstance(dataType, TimestampType):

            def convert_timestample(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, datetime.datetime)
                    return value.astimezone().replace(tzinfo=None)

            return convert_timestample

        elif isinstance(dataType, TimestampNTZType):

            def convert_timestample_ntz(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, datetime.datetime)
                    return value

            return convert_timestample_ntz

        elif isinstance(dataType, UserDefinedType):
            udt: UserDefinedType = dataType

            conv = ArrowTableToRowsConversion._create_converter(
                udt.sqlType(), none_on_identity=True, binary_as_bytes=binary_as_bytes
            )

            if conv is None:

                def convert_udt(value: Any) -> Any:
                    if value is None:
                        return None
                    else:
                        return udt.deserialize(value)

            else:

                def convert_udt(value: Any) -> Any:
                    if value is None:
                        return None
                    else:
                        return udt.deserialize(conv(value))

            return convert_udt

        elif isinstance(dataType, VariantType):

            def convert_variant(value: Any) -> Any:
                if value is None:
                    return None
                elif (
                    isinstance(value, dict)
                    and all(key in value for key in ["value", "metadata"])
                    and all(isinstance(value[key], bytes) for key in ["value", "metadata"])
                ):
                    return VariantVal(value["value"], value["metadata"])
                else:
                    raise PySparkValueError(errorClass="MALFORMED_VARIANT")

            return convert_variant

        elif isinstance(dataType, GeographyType):

            def convert_geography(value: Any) -> Any:
                if value is None:
                    return None
                elif (
                    isinstance(value, dict)
                    and all(key in value for key in ["wkb", "srid"])
                    and isinstance(value["wkb"], bytes)
                    and isinstance(value["srid"], int)
                ):
                    return Geography.fromWKB(value["wkb"], value["srid"])
                else:
                    raise PySparkValueError(errorClass="MALFORMED_GEOGRAPHY")

            return convert_geography

        elif isinstance(dataType, GeometryType):

            def convert_geometry(value: Any) -> Any:
                if value is None:
                    return None
                elif (
                    isinstance(value, dict)
                    and all(key in value for key in ["wkb", "srid"])
                    and isinstance(value["wkb"], bytes)
                    and isinstance(value["srid"], int)
                ):
                    return Geometry.fromWKB(value["wkb"], value["srid"])
                else:
                    raise PySparkValueError(errorClass="MALFORMED_GEOMETRY")

            return convert_geometry

        else:  # pragma: no cover
            assert False, f"Need converter for {dataType} but failed to find one."

    @overload
    @staticmethod
    def convert(table: "pa.Table", schema: StructType) -> List[Row]:
        pass

    @overload
    @staticmethod
    def convert(table: "pa.Table", schema: StructType, *, binary_as_bytes: bool) -> List[Row]:
        pass

    @overload
    @staticmethod
    def convert(
        table: "pa.Table", schema: StructType, *, return_as_tuples: bool
    ) -> List[Row | tuple]:
        pass

    @staticmethod  # type: ignore[misc]
    def convert(
        table: "pa.Table",
        schema: StructType,
        *,
        return_as_tuples: bool = False,
        binary_as_bytes: bool = True,
    ) -> List[Union[Row, tuple]]:
        require_minimum_pyarrow_version()
        import pyarrow as pa

        assert isinstance(table, pa.Table)

        assert schema is not None and isinstance(schema, StructType)

        fields = schema.fieldNames()

        if len(fields) > 0:
            field_converters = [
                ArrowTableToRowsConversion._create_converter(
                    f.dataType, none_on_identity=True, binary_as_bytes=binary_as_bytes
                )
                for f in schema.fields
            ]

            columnar_data = [
                [conv(v) for v in column.to_pylist()] if conv is not None else column.to_pylist()
                for column, conv in zip(table.columns, field_converters)
            ]

            if return_as_tuples:
                rows = [tuple(cols) for cols in zip(*columnar_data)]
            else:
                rows = [_create_row(fields, tuple(cols)) for cols in zip(*columnar_data)]
            assert len(rows) == table.num_rows, f"{len(rows)}, {table.num_rows}"
            return rows
        else:
            if return_as_tuples:
                return [tuple()] * table.num_rows
            else:
                return [_create_row(fields, tuple())] * table.num_rows


class ArrowTimestampConversion:
    @classmethod
    def _need_localization(cls, at: "pa.DataType") -> bool:
        import pyarrow.types as types

        if types.is_timestamp(at) and at.tz is not None:
            return True
        elif (
            types.is_list(at)
            or types.is_large_list(at)
            or types.is_fixed_size_list(at)
            or types.is_dictionary(at)
        ):
            return cls._need_localization(at.value_type)
        elif types.is_map(at):
            return any(cls._need_localization(dt) for dt in [at.key_type, at.item_type])
        elif types.is_struct(at):
            return any(cls._need_localization(field.type) for field in at)
        else:
            return False

    @staticmethod
    def localize_tz(arr: "pa.Array") -> "pa.Array":
        """
        Convert Arrow timezone-aware timestamps to timezone-naive in the specified timezone.
        This function works on Arrow Arrays, and it recurses to convert nested types.
        This function is dedicated for Pandas UDF execution.

        Differences from _create_converter_to_pandas + _check_series_convert_timestamps_local_tz:
        1, respect the timezone field in pyarrow timestamp type;
        2, do not use local time at any time;
        3, handle nested types in a consistent way. (_create_converter_to_pandas handles
        simple timestamp series with session timezone, but handles nested series with
        datetime.timezone.utc)

        Differences from _check_arrow_array_timestamps_localize:
        1, respect the timezone field in pyarrow timestamp type;
        2, do not handle timezone-naive timestamp;
        3, do not support unit coercion which won't happen in UDF execution.

        Parameters
        ----------
        arr : :class:`pyarrow.Array`

        Returns
        -------
        :class:`pyarrow.Array`

        Notes
        -----
        Arrow UDF (@arrow_udf/mapInArrow/etc) always preserve the original timezone, and thus
        doesn't need this conversion.
        """
        import pyarrow as pa
        import pyarrow.types as types
        import pyarrow.compute as pc

        pa_type = arr.type
        if not ArrowTimestampConversion._need_localization(pa_type):
            return arr

        if types.is_timestamp(pa_type) and pa_type.tz is not None:
            # import datetime
            # from zoneinfo import ZoneInfo
            # ts = datetime.datetime(2022, 1, 5, 15, 0, 1, tzinfo=ZoneInfo('Asia/Singapore'))
            # arr = pa.array([ts])
            # arr[0]
            # <pyarrow.TimestampScalar: '2022-01-05T15:00:01.000000+0800'>
            # arr = pc.local_timestamp(arr)
            # arr[0]
            # <pyarrow.TimestampScalar: '2022-01-05T15:00:01.000000'>

            return pc.local_timestamp(arr)
        elif types.is_list(pa_type):
            return pa.ListArray.from_arrays(
                offsets=arr.offsets,
                values=ArrowTimestampConversion.localize_tz(arr.values),
            )
        elif types.is_large_list(pa_type):
            return pa.LargeListType.from_arrays(
                offsets=arr.offsets,
                values=ArrowTimestampConversion.localize_tz(arr.values),
            )
        elif types.is_fixed_size_list(pa_type):
            return pa.FixedSizeListArray.from_arrays(
                values=ArrowTimestampConversion.localize_tz(arr.values),
            )
        elif types.is_dictionary(pa_type):
            return pa.DictionaryArray.from_arrays(
                indices=arr.indices,
                dictionary=ArrowTimestampConversion.localize_tz(arr.dictionary),
            )
        elif types.is_map(pa_type):
            return pa.MapArray.from_arrays(
                offsets=arr.offsets,
                keys=ArrowTimestampConversion.localize_tz(arr.keys),
                items=ArrowTimestampConversion.localize_tz(arr.items),
            )
        elif types.is_struct(pa_type):
            return pa.StructArray.from_arrays(
                arrays=[
                    ArrowTimestampConversion.localize_tz(arr.field(i)) for i in range(len(arr.type))
                ],
                names=arr.type.names,
            )
        else:  # pragma: no cover
            assert False, f"Need converter for {pa_type} but failed to find one."


class PandasSeriesToArrowConversion:
    """
    Conversion utilities for converting pandas Series to PyArrow Arrays.

    This class provides methods to convert pandas Series to PyArrow Arrays,
    with support for Spark-specific type handling and conversions.

    The class is primarily used by PySpark's Pandas UDF wrappers and serializers,
    where pandas data needs to be converted to Arrow for efficient serialization.
    """

    @classmethod
    def create_array(
        cls,
        series: "pd.Series",
        arrow_type: Optional["pa.DataType"],
        timezone: str,
        safecheck: bool = True,
        spark_type: Optional[DataType] = None,
        arrow_cast: bool = False,
        int_to_decimal_coercion_enabled: bool = False,
        ignore_unexpected_complex_type_values: bool = False,
        error_class: Optional[str] = None,
    ) -> "pa.Array":
        """
        Create an Arrow Array from the given pandas.Series and optional type.

        Parameters
        ----------
        series : pandas.Series
            A single series
        arrow_type : pyarrow.DataType, optional
            If None, pyarrow's inferred type will be used
        timezone : str
            Timezone for timestamp conversion
        safecheck : bool
            Whether to perform safe type checking
        spark_type : DataType, optional
            If None, spark type converted from arrow_type will be used
        arrow_cast : bool
            Whether to apply Arrow casting when type mismatches
        int_to_decimal_coercion_enabled : bool
            Whether to enable int to decimal coercion
        ignore_unexpected_complex_type_values : bool
            Whether to ignore unexpected complex type values during conversion
        error_class : str, optional
            Custom error class for arrow type cast errors (e.g., "UDTF_ARROW_TYPE_CAST_ERROR")

        Returns
        -------
        pyarrow.Array
        """
        import pyarrow as pa
        import pandas as pd

        if isinstance(series.dtype, pd.CategoricalDtype):
            series = series.astype(series.dtypes.categories.dtype)

        if arrow_type is not None:
            dt = spark_type or from_arrow_type(arrow_type, prefer_timestamp_ntz=True)
            conv = _create_converter_from_pandas(
                dt,
                timezone=timezone,
                error_on_duplicated_field_names=False,
                int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
                ignore_unexpected_complex_type_values=ignore_unexpected_complex_type_values,
            )
            series = conv(series)

        if hasattr(series.array, "__arrow_array__"):
            mask = None
        else:
            mask = series.isnull()

        # For UDTF (error_class is set), use Arrow-specific error handling
        if error_class is not None:
            try:
                try:
                    return pa.Array.from_pandas(series, mask=mask, type=arrow_type, safe=safecheck)
                except pa.lib.ArrowException:
                    if arrow_cast:
                        return pa.Array.from_pandas(series, mask=mask).cast(
                            target_type=arrow_type, safe=safecheck
                        )
                    else:
                        raise
            except pa.lib.ArrowException:
                raise PySparkRuntimeError(
                    errorClass=error_class,
                    messageParameters={
                        "col_name": series.name,
                        "col_type": str(series.dtype),
                        "arrow_type": str(arrow_type),
                    },
                ) from None

        # For regular UDF, use standard error handling
        try:
            try:
                return pa.Array.from_pandas(series, mask=mask, type=arrow_type, safe=safecheck)
            except pa.lib.ArrowInvalid:
                # Only catch ArrowInvalid for arrow_cast, not ArrowTypeError
                # ArrowTypeError should propagate to the TypeError handler
                if arrow_cast:
                    return pa.Array.from_pandas(series, mask=mask).cast(
                        target_type=arrow_type, safe=safecheck
                    )
                else:
                    raise
        except (TypeError, pa.lib.ArrowTypeError) as e:
            error_msg = (
                "Exception thrown when converting pandas.Series (%s) "
                "with name '%s' to Arrow Array (%s)."
            )
            raise PySparkTypeError(error_msg % (series.dtype, series.name, arrow_type)) from e
        except (ValueError, pa.lib.ArrowException) as e:
            error_msg = (
                "Exception thrown when converting pandas.Series (%s) "
                "with name '%s' to Arrow Array (%s)."
            )
            if safecheck:
                error_msg += (
                    " It can be caused by overflows or other "
                    "unsafe conversions warned by Arrow. Arrow safe type check "
                    "can be disabled by using SQL config "
                    "`spark.sql.execution.pandas.convertToArrowArraySafely`."
                )
            raise PySparkValueError(error_msg % (series.dtype, series.name, arrow_type)) from e


class ArrowArrayToPandasConversion:
    """
    Conversion utilities for converting PyArrow Arrays and ChunkedArrays to pandas.

    This class provides methods to convert PyArrow columnar data structures to pandas
    Series or DataFrames, with support for Spark-specific type handling and conversions.

    The class is primarily used by PySpark's Arrow-based serializers for UDF execution,
    where Arrow data needs to be converted to pandas for Python UDF processing.
    """

    @classmethod
    def convert_legacy(
        cls,
        arr: Union["pa.Array", "pa.ChunkedArray"],
        spark_type: DataType,
        *,
        timezone: Optional[str] = None,
        struct_in_pandas: Optional[str] = None,
        ndarray_as_list: bool = False,
        df_for_struct: bool = False,
    ) -> Union["pd.Series", "pd.DataFrame"]:
        """
        Convert a PyArrow Array or ChunkedArray to a pandas Series or DataFrame.

        This is the lower-level conversion method that requires explicit Spark type
        specification. For a more convenient API, see :meth:`convert`.

        Parameters
        ----------
        arr : pa.Array or pa.ChunkedArray
            The arrow column to convert.
        spark_type : DataType
            Target Spark type. Must be specified and should match the Arrow array type.
        timezone : str, optional
            The timezone to use for timestamp conversion. Required if the data contains
            timestamp types.
        struct_in_pandas : str, optional
            How to handle struct types in pandas. Valid values are "dict", "row", or "legacy".
            Required if the data contains struct types.
        ndarray_as_list : bool, optional
            Whether to convert numpy ndarrays to Python lists. Default is False.
        df_for_struct : bool, optional
            If True and spark_type is a StructType, return a DataFrame with columns
            corresponding to struct fields instead of a Series. Default is False.

        Returns
        -------
        pd.Series or pd.DataFrame
            Converted pandas Series. If df_for_struct is True and spark_type is StructType,
            returns a DataFrame with columns corresponding to struct fields.

        Notes
        -----
        This method handles date type columns specially to avoid overflow issues with
        datetime64[ns] intermediate representations.
        """
        import pyarrow as pa
        import pandas as pd

        assert isinstance(arr, (pa.Array, pa.ChunkedArray))

        if df_for_struct and isinstance(spark_type, StructType):
            import pyarrow.types as types

            assert types.is_struct(arr.type)
            assert len(spark_type.names) == len(arr.type.names), (
                f"Schema mismatch: spark_type has {len(spark_type.names)} fields, "
                f"but arrow type has {len(arr.type.names)} fields. "
                f"spark_type={spark_type}, arrow_type={arr.type}"
            )

            series = [
                cls.convert_legacy(
                    field_arr,
                    spark_type=field.dataType,
                    timezone=timezone,
                    struct_in_pandas=struct_in_pandas,
                    ndarray_as_list=ndarray_as_list,
                    df_for_struct=False,  # always False for child fields
                )
                for field_arr, field in zip(arr.flatten(), spark_type)
            ]
            pdf = pd.concat(series, axis=1)
            pdf.columns = spark_type.names  # type: ignore[assignment]
            return pdf

        # Convert Arrow array to pandas Series with specific options:
        # - date_as_object: Convert date types to Python datetime.date objects directly
        #   instead of datetime64[ns] to avoid overflow issues
        # - coerce_temporal_nanoseconds: Handle nanosecond precision timestamps correctly
        # - integer_object_nulls: Use object dtype for integer arrays with nulls
        pandas_options = {
            "date_as_object": True,
            "coerce_temporal_nanoseconds": True,
            "integer_object_nulls": True,
        }
        ser = arr.to_pandas(**pandas_options)

        converter = _create_converter_to_pandas(
            data_type=spark_type,
            nullable=True,
            timezone=timezone,
            struct_in_pandas=struct_in_pandas,
            error_on_duplicated_field_names=True,
            ndarray_as_list=ndarray_as_list,
            integer_object_nulls=True,
        )
        return converter(ser)

    @classmethod
    def convert(
        cls,
        arrow_column: Union["pa.Array", "pa.ChunkedArray"],
        target_type: DataType,
        *,
        timezone: Optional[str] = None,
        struct_in_pandas: str = "dict",
        ndarray_as_list: bool = False,
        df_for_struct: bool = False,
    ) -> Union["pd.Series", "pd.DataFrame"]:
        """
        Convert a PyArrow Array or ChunkedArray to a pandas Series or DataFrame.

        Parameters
        ----------
        arrow_column : pa.Array or pa.ChunkedArray
            The Arrow column to convert.
        target_type : DataType
            The target Spark type for the column to be converted to.
        timezone : str, optional
            Timezone for timestamp conversion. Required if the data contains timestamp types.
        struct_in_pandas : str, optional
            How to represent struct types in pandas. Valid values are "dict", "row", or "legacy".
            Default is "dict".
        ndarray_as_list : bool, optional
            Whether to convert numpy ndarrays to Python lists. Default is False.
        df_for_struct : bool, optional
            If True, convert struct columns to a DataFrame with columns corresponding
            to struct fields instead of a Series. Default is False.

        Returns
        -------
        pd.Series or pd.DataFrame
            Converted pandas Series. If df_for_struct is True and the type is StructType,
            returns a DataFrame with columns corresponding to struct fields.
        """
        return cls.convert_legacy(
            arrow_column,
            target_type,
            timezone=timezone,
            struct_in_pandas=struct_in_pandas,
            ndarray_as_list=ndarray_as_list,
            df_for_struct=df_for_struct,
        )
