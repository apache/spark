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
Grouped and cogrouped UDF serializer classes.
"""

from pyspark.sql.conversion import ArrowBatchTransformer
from pyspark.sql.pandas.types import from_arrow_schema

from pyspark.sql.pandas.serializers._udf import (
    ArrowStreamUDFSerializer,
    ArrowStreamArrowUDFSerializer,
    ArrowStreamPandasUDFSerializer,
)


class ArrowStreamGroupUDFSerializer(ArrowStreamUDFSerializer):
    """
    Serializer for grouped Arrow UDFs.

    Deserializes:
        ``Iterator[Iterator[pa.RecordBatch]]`` - one inner iterator per group.
        Each batch contains a single struct column.

    Serializes:
        ``Iterator[Tuple[Iterator[pa.RecordBatch], pa.DataType]]``
        Each tuple contains iterator of flattened batches and their Arrow type.

    Used by:
        - SQL_GROUPED_MAP_ARROW_UDF
        - SQL_GROUPED_MAP_ARROW_ITER_UDF

    Parameters
    ----------
    assign_cols_by_name : bool
        If True, reorder serialized columns by schema name.
    """

    def __init__(self, assign_cols_by_name):
        super().__init__()
        self._assign_cols_by_name = assign_cols_by_name

    def load_stream(self, stream):
        """
        Load grouped Arrow record batches from stream.
        """
        return self._load_single_group_stream(stream)

    def dump_stream(self, iterator, stream):
        import pyarrow as pa

        # flatten inner list [([pa.RecordBatch], arrow_type)] into [(pa.RecordBatch, arrow_type)]
        # so strip off inner iterator induced by ArrowStreamUDFSerializer.load_stream
        batch_iter = (
            (batch, arrow_type)
            for batches, arrow_type in iterator  # tuple constructed in wrap_grouped_map_arrow_udf
            for batch in batches
        )

        if self._assign_cols_by_name:
            batch_iter = (
                (
                    pa.RecordBatch.from_arrays(
                        [batch.column(field.name) for field in arrow_type],
                        names=[field.name for field in arrow_type],
                    ),
                    arrow_type,
                )
                for batch, arrow_type in batch_iter
            )

        super().dump_stream(batch_iter, stream)


# Serializer for SQL_GROUPED_AGG_ARROW_UDF, SQL_WINDOW_AGG_ARROW_UDF,
# and SQL_GROUPED_AGG_ARROW_ITER_UDF
class ArrowStreamAggArrowUDFSerializer(ArrowStreamArrowUDFSerializer):
    def load_stream(self, stream):
        """
        Yield an iterator that produces one tuple of column arrays per batch.
        Each group yields Iterator[Tuple[pa.Array, ...]], allowing UDF to process batches one by one
        without consuming all batches upfront.
        """
        return self._load_single_group_stream(stream)

    def _transform_group_batches(self, batches):
        # Lazily read and convert Arrow batches one at a time from the stream.
        # This avoids loading all batches into memory for the group.
        return (batch.columns for batch in batches)

    def __repr__(self):
        return "ArrowStreamAggArrowUDFSerializer"


# Serializer for SQL_GROUPED_AGG_PANDAS_UDF, SQL_WINDOW_AGG_PANDAS_UDF,
# and SQL_GROUPED_AGG_PANDAS_ITER_UDF
class ArrowStreamAggPandasUDFSerializer(ArrowStreamPandasUDFSerializer):
    def __init__(
        self,
        timezone,
        safecheck,
        assign_cols_by_name,
        int_to_decimal_coercion_enabled,
    ):
        super().__init__(
            timezone=timezone,
            safecheck=safecheck,
            assign_cols_by_name=assign_cols_by_name,
            df_for_struct=False,
            struct_in_pandas="dict",
            ndarray_as_list=False,
            arrow_cast=True,
            input_type=None,
            int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
        )

    def load_stream(self, stream):
        """
        Yield an iterator that produces one tuple of pandas.Series per batch.
        Each group yields Iterator[Tuple[pd.Series, ...]], allowing UDF to
        process batches one by one without consuming all batches upfront.
        """
        return self._load_single_group_stream(stream)

    def _transform_group_batches(self, batches):
        # Lazily read and convert Arrow batches to pandas Series one at a time
        # from the stream. This avoids loading all batches into memory for the group.
        return (
            tuple(
                ArrowBatchTransformer.to_pandas(
                    batch,
                    timezone=self._timezone,
                    schema=self._input_type,
                    struct_in_pandas=self._struct_in_pandas,
                    ndarray_as_list=self._ndarray_as_list,
                    df_for_struct=self._df_for_struct,
                )
            )
            for batch in batches
        )

    def __repr__(self):
        return "ArrowStreamAggPandasUDFSerializer"


# Serializer for SQL_GROUPED_MAP_PANDAS_UDF, SQL_GROUPED_MAP_PANDAS_ITER_UDF
class GroupPandasUDFSerializer(ArrowStreamPandasUDFSerializer):
    def __init__(
        self,
        timezone,
        safecheck,
        assign_cols_by_name,
        int_to_decimal_coercion_enabled,
    ):
        super().__init__(
            timezone=timezone,
            safecheck=safecheck,
            assign_cols_by_name=assign_cols_by_name,
            df_for_struct=False,
            struct_in_pandas="dict",
            ndarray_as_list=False,
            arrow_cast=True,
            input_type=None,
            int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
        )

    def load_stream(self, stream):
        """
        Deserialize Grouped ArrowRecordBatches and yield raw Iterator[pa.RecordBatch].
        Each outer iterator element represents a group.
        """
        return self._load_single_group_stream(stream)

    def dump_stream(self, iterator, stream):
        """
        Flatten the grouped iterator structure.
        """
        # Flatten: Iterator[Iterator[[(df, spark_type)]]] -> Iterator[[(df, spark_type)]]
        flattened_iter = (batch for generator in iterator for batch in generator)
        super().dump_stream(flattened_iter, stream)

    def __repr__(self):
        return "GroupPandasUDFSerializer"


class CogroupArrowUDFSerializer(ArrowStreamGroupUDFSerializer):
    """
    Serializes pyarrow.RecordBatch data with Arrow streaming format.

    Loads Arrow record batches as `[([pa.RecordBatch], [pa.RecordBatch])]` (one tuple per group)
    and serializes `[([pa.RecordBatch], arrow_type)]`.

    Parameters
    ----------
    assign_cols_by_name : bool
        If True, then DataFrames will get columns by name
    """

    def load_stream(self, stream):
        """
        Deserialize Cogrouped ArrowRecordBatches and yield as two `pyarrow.RecordBatch`es.
        """
        return self._load_cogroup_stream(stream)


class CogroupPandasUDFSerializer(ArrowStreamPandasUDFSerializer):
    def load_stream(self, stream):
        """
        Deserialize Cogrouped ArrowRecordBatches to a tuple of Arrow tables and yield as two
        lists of pandas.Series.
        """
        return self._load_cogroup_stream(stream)

    def _transform_cogroup_batches(self, left_batches, right_batches):
        import pyarrow as pa

        def batches_to_pandas(batches):
            table = pa.Table.from_batches(batches)
            return ArrowBatchTransformer.to_pandas(
                table,
                timezone=self._timezone,
                schema=from_arrow_schema(table.schema),
                struct_in_pandas=self._struct_in_pandas,
                ndarray_as_list=self._ndarray_as_list,
                df_for_struct=self._df_for_struct,
            )

        return batches_to_pandas(left_batches), batches_to_pandas(right_batches)
