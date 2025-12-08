import dataclasses
from typing import Iterable, Optional, Iterator, Any, Tuple
import pyarrow
from pyarrow.interchange.column import DtypeKind, _PyArrowColumn, ColumnBuffers, ColumnNullType, CategoricalDescription
from pyarrow.interchange.dataframe import _PyArrowDataFrame

import pyspark.sql
from pyspark.sql.types import StructType, StructField, BinaryType
from pyspark.sql.pandas.types import to_arrow_schema


def _get_arrow_array_partition_stream(df: pyspark.sql.DataFrame) -> Iterator[pyarrow.RecordBatch]:
    """Return all the partitions as Arrow arrays in an Iterator."""
    # We will be using mapInArrow to convert each partition to Arrow RecordBatches.
    # The return type of the function will be a single binary column containing
    # the serialized RecordBatch in Arrow IPC format.
    binary_schema = StructType([StructField("interchange_arrow_bytes", BinaryType(), nullable=False)])

    def batch_to_bytes_iter(batch_iter):
        """
        A generator function that converts RecordBatches to serialized Arrow IPC format.

        Spark sends each partition as an iterator of RecordBatches. In order to return
        the entire partition as a stream of Arrow RecordBatches, we need to serialize
        each RecordBatch to Arrow IPC format and yield it as a single binary blob.
        """
        # The size of the batch can be controlled by the Spark config
        # `spark.sql.execution.arrow.maxRecordsPerBatch`.
        for arrow_batch in batch_iter:
            # We create an in-memory byte stream to hold the serialized batch
            sink = pyarrow.BufferOutputStream()
            # Write the batch to the stream using Arrow IPC format
            with pyarrow.ipc.new_stream(sink, arrow_batch.schema) as writer:
                writer.write_batch(arrow_batch)
            buf = sink.getvalue().to_pybytes()
            # Wrap the bytes in a new 1-row, 1-column RecordBatch to satisfy mapInArrow return signature
            # This serializes the whole batch into a single pyarrow serialized cell.
            storage_arr = pyarrow.array([buf])
            yield pyarrow.RecordBatch.from_arrays([storage_arr], names=["interchange_arrow_bytes"])

    # Convert all partitions to Arrow RecordBatches and map to binary blobs.
    byte_df = df.mapInArrow(batch_to_bytes_iter, binary_schema)

    # A row is actually a batch of data in Arrow IPC format. Fetch the batches one by one.
    for row in byte_df.toLocalIterator():
        with pyarrow.ipc.open_stream(row.interchange_arrow_bytes) as reader:
            for batch in reader:
                # Each batch corresponds to a chunk of data in the partition.
                yield batch


class SparkArrowCStreamer:
    """
    A class that implements that __arrow_c_stream__ protocol for Spark partitions.

    This class is implemented in a way that allows consumers to consume each partition
    one at a time without materializing all partitions at once on the driver side.
    """
    def __init__(self, df: pyspark.sql.DataFrame):
        self._df = df
        self._schema = to_arrow_schema(df.schema)

    def __arrow_c_stream__(self, requested_schema=None):
        """
        Return the Arrow C stream for the dataframe partitions.
        """
        reader: pyarrow.RecordBatchReader = pyarrow.RecordBatchReader.from_batches(
            self._schema, _get_arrow_array_partition_stream(self._df)
        )
        return reader.__arrow_c_stream__(requested_schema=requested_schema)


@dataclasses.dataclass(frozen=True)
class SparkInterchangeColumn(_PyArrowColumn):
    """
    A class that conforms to the dataframe interchange protocol column interface.

    This class leverages the Arrow-based dataframe interchange protocol by returning
    Spark partitions (chunks) in Arrow's dataframe interchange format.
    """
    _spark_dataframe: "pyspark.sql.DataFrame"
    _spark_column: "pyspark.sql.Column"
    _allow_copy: bool

    def size(self) -> Optional[int]:
        """
        The number of values in the column.

        This would trigger computation to get the size, so we return None.
        """
        return None

    def offset(self) -> int:
        """
        Return the offset of the first element, which is always 0 in Spark.

        The only case where the offset would not be 0 would be when this object
        represents a chunk. Since we have a separate class for the column chunks,
        we can safely return 0 here.
        """
        return 0

    @property
    def dtype(self) -> Tuple[DtypeKind, int, str, str]:
        """Return the Dtype of the column."""
        return self._dtype_from_arrowdtype(
            to_arrow_schema(self._spark_dataframe.select(self._spark_column).schema).field(0).type,
            bit_width=8,
        )

    @property
    def describe_categorical(self) -> CategoricalDescription:
        """Return the categorical description of the column, if applicable."""
        raise NotImplementedError("Categorical description is not implemented for Spark columns.")

    @property
    def describe_null(self) -> Tuple[ColumnNullType, Any]:
        """Return the null description of the column."""
        raise NotImplementedError("Null description is not implemented for Spark columns.")

    @property
    def null_count(self) -> Optional[int]:
        """Return the number of nulls in the column, or None if not known."""
        # Always return None to avoid triggering computation
        return None

    @property
    def num_chunks(self) -> int:
        """Return the number of chunks in the column (partitions in this case)."""
        return self._spark_dataframe.rdd.getNumPartitions()

    def get_chunks(self, n_chunks: Optional[int] = None) -> Iterable[_PyArrowColumn]:
        """
        Return an iterator yielding the chunks of the column. See
        SparkInterchangeDataframe.get_chunks for details.
        """
        if n_chunks is not None:
            raise NotImplementedError("n_chunks would require repartitioning, which is not implemented.")
        arrow_array_partitions = _get_arrow_array_partition_stream(self._spark_dataframe.select(self._spark_column))
        for part in arrow_array_partitions:
            yield _PyArrowColumn(
                column=part.column(0),
                allow_copy=self._allow_copy,
            )

    def get_buffers(self) -> ColumnBuffers:
        """Return a dictionary of buffers for the column."""
        raise NotImplementedError("get_buffers would force materialization, so it is not implemented.")


@dataclasses.dataclass(frozen=True)
class SparkInterchangeDataframe(_PyArrowDataFrame):
    """
    A class that conforms to the dataframe interchange protocol.

    This class leverages the Arrow-based dataframe interchange protocol by returning
    Spark partitions (chunks) in Arrow's dataframe interchange format. This
    implementation attempts to avoid materializing all the data on the driver side at
    once.
    """
    _spark_dataframe: "pyspark.sql.DataFrame"
    _allow_copy: bool
    _nan_as_null: bool

    def __dataframe__(self, nan_as_null: bool = False, allow_copy: bool = True) -> "SparkInterchangeDataframe":
        """Construct a new interchange dataframe, potentially changing the options."""
        return SparkInterchangeDataframe(
            _spark_dataframe=self._spark_dataframe,
            _allow_copy=allow_copy,
            _nan_as_null=nan_as_null,
        )

    @property
    def metadata(self) -> dict[str, Any]:
        """
        The metadata for the dataframe.

        In Spark's case, there is no additional metadata to provide.
        """
        return {}

    def num_columns(self) -> int:
        return len(self._spark_dataframe.columns)

    def num_chunks(self) -> int:
        """Return the number of chunks in the dataframe (partitions in this case)."""
        return self._spark_dataframe.rdd.getNumPartitions()

    def column_names(self) -> Iterable[str]:
        return self._spark_dataframe.columns

    def get_column(self, i: int) -> "SparkInterchangeColumn":
        """Get a column by the 0-based index."""
        col_name = self._spark_dataframe.columns[i]
        return SparkInterchangeColumn(
            _spark_dataframe=self._spark_dataframe,
            _spark_column=self._spark_dataframe[col_name],
            _allow_copy=self._allow_copy,
        )

    def get_column_by_name(self, name: str) -> "SparkInterchangeColumn":
        """Get a column by name."""
        return SparkInterchangeColumn(
            _spark_dataframe=self._spark_dataframe,
            _spark_column=self._spark_dataframe[name],
            _allow_copy=self._allow_copy,
        )

    def get_columns(self) -> Iterable[_PyArrowColumn]:
        """Return an iterator yielding the columns."""
        for col_name in self._spark_dataframe.columns:
            yield SparkInterchangeColumn(
                _spark_dataframe=self._spark_dataframe,
                _spark_column=self._spark_dataframe[col_name],
                _allow_copy=self._allow_copy,
            )

    def select_columns(self, indices: Iterable[int]) -> "SparkInterchangeDataframe":
        """Create a new DataFrame by selecting a subset of columns by index."""
        selected_column_names = [self._spark_dataframe.columns[i] for i in indices]
        new_spark_df = self._spark_dataframe.select(selected_column_names)
        return SparkInterchangeDataframe(
            _spark_dataframe=new_spark_df,
            _allow_copy=self._allow_copy,
            _nan_as_null=self._nan_as_null,
        )

    def select_columns_by_name(self, names: Iterable[str]) -> "SparkInterchangeDataframe":
        """Create a new DataFrame by selecting a subset of columns by name."""
        new_spark_df = self._spark_dataframe.select(list(names))
        return SparkInterchangeDataframe(
            _spark_dataframe=new_spark_df,
            _allow_copy=self._allow_copy,
            _nan_as_null=self._nan_as_null,
        )

    def get_chunks(
        self, n_chunks: Optional[int] = None
    ) -> Iterable[_PyArrowDataFrame]:
        """Return an iterator yielding the chunks of the dataframe."""
        if n_chunks is not None:
            raise NotImplementedError("n_chunks would require repartitioning, which is not implemented.")
        arrow_array_partitions = _get_arrow_array_partition_stream(self._spark_dataframe)
        for part in arrow_array_partitions:
            yield _PyArrowDataFrame(
                part,
                allow_copy=self._allow_copy,
            )
