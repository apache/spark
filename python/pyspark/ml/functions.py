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
from __future__ import annotations

import inspect
import numpy as np
import pandas as pd
import uuid
from pyspark import SparkContext
from pyspark.sql.functions import pandas_udf
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import ArrayType, DataType, StructType
from typing import Callable, Iterator, List, Mapping, TYPE_CHECKING, Tuple, Union

if TYPE_CHECKING:
    from pyspark.sql._typing import UserDefinedFunctionLike


def vector_to_array(col: Column, dtype: str = "float64") -> Column:
    """
    Converts a column of MLlib sparse/dense vectors into a column of dense arrays.

    .. versionadded:: 3.0.0

    Parameters
    ----------
    col : :py:class:`pyspark.sql.Column` or str
        Input column
    dtype : str, optional
        The data type of the output array. Valid values: "float64" or "float32".

    Returns
    -------
    :py:class:`pyspark.sql.Column`
        The converted column of dense arrays.

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.functions import vector_to_array
    >>> from pyspark.mllib.linalg import Vectors as OldVectors
    >>> df = spark.createDataFrame([
    ...     (Vectors.dense(1.0, 2.0, 3.0), OldVectors.dense(10.0, 20.0, 30.0)),
    ...     (Vectors.sparse(3, [(0, 2.0), (2, 3.0)]),
    ...      OldVectors.sparse(3, [(0, 20.0), (2, 30.0)]))],
    ...     ["vec", "oldVec"])
    >>> df1 = df.select(vector_to_array("vec").alias("vec"),
    ...                 vector_to_array("oldVec").alias("oldVec"))
    >>> df1.collect()
    [Row(vec=[1.0, 2.0, 3.0], oldVec=[10.0, 20.0, 30.0]),
     Row(vec=[2.0, 0.0, 3.0], oldVec=[20.0, 0.0, 30.0])]
    >>> df2 = df.select(vector_to_array("vec", "float32").alias("vec"),
    ...                 vector_to_array("oldVec", "float32").alias("oldVec"))
    >>> df2.collect()
    [Row(vec=[1.0, 2.0, 3.0], oldVec=[10.0, 20.0, 30.0]),
     Row(vec=[2.0, 0.0, 3.0], oldVec=[20.0, 0.0, 30.0])]
    >>> df1.schema.fields
    [StructField('vec', ArrayType(DoubleType(), False), False),
     StructField('oldVec', ArrayType(DoubleType(), False), False)]
    >>> df2.schema.fields
    [StructField('vec', ArrayType(FloatType(), False), False),
     StructField('oldVec', ArrayType(FloatType(), False), False)]
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.org.apache.spark.ml.functions.vector_to_array(_to_java_column(col), dtype)
    )


def array_to_vector(col: Column) -> Column:
    """
    Converts a column of array of numeric type into a column of pyspark.ml.linalg.DenseVector
    instances

    .. versionadded:: 3.1.0

    Parameters
    ----------
    col : :py:class:`pyspark.sql.Column` or str
        Input column

    Returns
    -------
    :py:class:`pyspark.sql.Column`
        The converted column of dense vectors.

    Examples
    --------
    >>> from pyspark.ml.functions import array_to_vector
    >>> df1 = spark.createDataFrame([([1.5, 2.5],),], schema='v1 array<double>')
    >>> df1.select(array_to_vector('v1').alias('vec1')).collect()
    [Row(vec1=DenseVector([1.5, 2.5]))]
    >>> df2 = spark.createDataFrame([([1.5, 3.5],),], schema='v1 array<float>')
    >>> df2.select(array_to_vector('v1').alias('vec1')).collect()
    [Row(vec1=DenseVector([1.5, 3.5]))]
    >>> df3 = spark.createDataFrame([([1, 3],),], schema='v1 array<int>')
    >>> df3.select(array_to_vector('v1').alias('vec1')).collect()
    [Row(vec1=DenseVector([1.0, 3.0]))]
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.org.apache.spark.ml.functions.array_to_vector(_to_java_column(col)))


def _batched(
    data: pd.Series | pd.DataFrame | Tuple[pd.Series], batch_size: int
) -> Iterator[pd.DataFrame]:
    """Generator that splits a pandas dataframe/series into batches."""
    if isinstance(data, pd.DataFrame):
        for _, batch in data.groupby(np.arange(len(data)) // batch_size):
            yield batch
    else:
        # convert (tuple of) pd.Series into pd.DataFrame
        if isinstance(data, pd.Series):
            df = pd.concat((data,), axis=1)
        else:  # isinstance(data, Tuple[pd.Series]):
            df = pd.concat(data, axis=1)
        for _, batch in df.groupby(np.arange(len(df)) // batch_size):
            yield batch


def _has_tensor_cols(data: pd.Series | pd.DataFrame | Tuple[pd.Series]) -> bool:
    """Check if input DataFrame contains any tensor-valued columns"""
    if isinstance(data, pd.Series):
        return data.dtype == np.object_ and isinstance(data.iloc[0], (np.ndarray, list))
    elif isinstance(data, pd.DataFrame):
        return any(data.dtypes == np.object_) and any(
            [isinstance(d, (np.ndarray, list)) for d in data.iloc[0]]
        )
    else:  # isinstance(data, Tuple):
        return any([d.dtype == np.object_ for d in data]) and any(
            [isinstance(d.iloc[0], (np.ndarray, list)) for d in data]
        )


def predict_batch_udf(
    predict_batch_fn: Callable[
        [],
        Callable[
            [np.ndarray | List[np.ndarray]],
            np.ndarray | Mapping[str, np.ndarray] | List[Mapping[str, np.dtype]],
        ],
    ],
    *,
    return_type: DataType,
    batch_size: int,
    input_tensor_shapes: list[list[int] | None] | Mapping[int, list[int]] | None = None,
) -> UserDefinedFunctionLike:
    """Given a function which loads a model, returns a pandas_udf for inferencing over that model.

    This will handle:
    - conversion of the Spark DataFrame to numpy arrays.
    - batching of the inputs sent to the model predict() function.
    - caching of the model and prediction function on the executors.

    This assumes that the `predict_batch_fn` encapsulates all of the necessary dependencies for
    running the model or the Spark executor environment already satisfies all runtime requirements.

    For the conversion of Spark DataFrame to numpy, the following table describes the behavior,
    where tensor columns in the Spark DataFrame must be represented as a flattened 1-D array/list.

    | dataframe \\ model | single input | multiple inputs |
    | :----------------- | :----------- | :-------------- |
    | single-col scalar  | 1            | N/A             |
    | single-col tensor  | 1,2          | N/A             |
    | multi-col scalar   | 3            | 4               |
    | multi-col tensor   | N/A          | 4,2             |

    Notes:
    1. pass thru dataframe column => model input as single numpy array.
    2. reshape flattened tensors into expected tensor shapes.
    3. user must use `pyspark.sql.functions.struct()` or `pyspark.sql.functions.array()` to
       combine multiple input columns into the equivalent of a single-col tensor.
    4. pass thru dataframe column => model input as an ordered list of numpy arrays.

    Example (single-col tensor):

    Input DataFrame has a single column with a flattened tensor value, represented as an array of
    float.
    ```
    from pyspark.ml.functions import predict_batch_udf

    def predict_batch_fn():
        # load/init happens once per python worker
        import tensorflow as tf
        model = tf.keras.models.load_model('/path/to/mnist_model')

        # predict on batches of tasks/partitions, using cached model
        def predict(inputs: np.ndarray) -> np.ndarray:
            # inputs.shape = [batch_size, 784]
            # outputs.shape = [batch_size, 10], return_type = ArrayType(FloatType())
            return model.predict(inputs)

        return predict

    mnist = predict_batch_udf(predict_batch_fn,
                              return_type=ArrayType(FloatType()),
                              batch_size=100,
                              input_tensor_shapes=[[784]])

    df = spark.read.parquet("/path/to/mnist_data")
    df.show(5)
    # +--------------------+
    # |                data|
    # +--------------------+
    # |[0.0, 0.0, 0.0, 0...|
    # |[0.0, 0.0, 0.0, 0...|
    # |[0.0, 0.0, 0.0, 0...|
    # |[0.0, 0.0, 0.0, 0...|
    # |[0.0, 0.0, 0.0, 0...|
    # +--------------------+

    df.withColumn("preds", mnist("data")).show(5)
    # +--------------------+--------------------+
    # |                data|               preds|
    # +--------------------+--------------------+
    # |[0.0, 0.0, 0.0, 0...|[-13.511008, 8.84...|
    # |[0.0, 0.0, 0.0, 0...|[-5.3957458, -2.2...|
    # |[0.0, 0.0, 0.0, 0...|[-7.2014456, -8.8...|
    # |[0.0, 0.0, 0.0, 0...|[-19.466187, -13....|
    # |[0.0, 0.0, 0.0, 0...|[-5.7757926, -7.8...|
    # +--------------------+--------------------+
    ```

    Example (single-col scalar):

    Input DataFrame has a single scalar column, which will be passed to the `predict` function as
    a 1-D numpy array.
    ```
    import numpy as np
    import pandas as pd
    from pyspark.ml.functions import predict_batch_udf
    from pyspark.sql.types import FloatType

    df = spark.createDataFrame(pd.DataFrame(np.arange(100)))
    df.show(5)
    # +---+
    # |  0|
    # +---+
    # |  0|
    # |  1|
    # |  2|
    # |  3|
    # |  4|
    # +---+

    def predict_batch_fn():
        def predict(inputs: np.ndarray) -> np.ndarray:
            # inputs.shape = [batch_size]
            # outputs.shape = [batch_size], return_type = FloatType()
            return inputs * 2

        return predict

    times_two = predict_batch_udf(predict_batch_fn,
                                  return_type=FloatType(),
                                  batch_size=10)

    df = spark.createDataFrame(pd.DataFrame(np.arange(100)))
    df.withColumn("x2", times_two("0")).show(5)
    # +---+---+
    # |  0| x2|
    # +---+---+
    # |  0|0.0|
    # |  1|2.0|
    # |  2|4.0|
    # |  3|6.0|
    # |  4|8.0|
    # +---+---+
    ```

    Example (multi-col scalar):

    Input DataFrame has muliple columns of scalar values.  If the user-provided `predict` function
    expects a single input, then the user should combine multiple columns into a single tensor using
    `pyspark.sql.functions.struct` or `pyspark.sql.functions.array`.
    ```
    import numpy as np
    import pandas as pd
    from pyspark.ml.functions import predict_batch_udf
    from pyspark.sql.functions import struct

    data = np.arange(0, 1000, dtype=np.float64).reshape(-1, 4)
    pdf = pd.DataFrame(data, columns=['a','b','c','d'])
    df = spark.createDataFrame(pdf)
    # +----+----+----+----+
    # |   a|   b|   c|   d|
    # +----+----+----+----+
    # | 0.0| 1.0| 2.0| 3.0|
    # | 4.0| 5.0| 6.0| 7.0|
    # | 8.0| 9.0|10.0|11.0|
    # |12.0|13.0|14.0|15.0|
    # |16.0|17.0|18.0|19.0|
    # +----+----+----+----+

    def predict_batch_fn():
        def predict(inputs: np.ndarray) -> np.ndarray:
            # inputs.shape = [batch_size, 4]
            # outputs.shape = [batch_size], return_type = FloatType()
            return np.sum(inputs, axis=1)

        return predict

    sum_rows = predict_batch_udf(predict_batch_fn,
                                 return_type=FloatType(),
                                 batch_size=10,
                                 input_tensor_shapes=[[4]])

    df.withColumn("sum", sum_rows(struct("a", "b", "c", "d"))).show(5)
    # +----+----+----+----+----+
    # |   a|   b|   c|   d| sum|
    # +----+----+----+----+----+
    # | 0.0| 1.0| 2.0| 3.0| 6.0|
    # | 4.0| 5.0| 6.0| 7.0|22.0|
    # | 8.0| 9.0|10.0|11.0|38.0|
    # |12.0|13.0|14.0|15.0|54.0|
    # |16.0|17.0|18.0|19.0|70.0|
    # +----+----+----+----+----+

    # Note: if the `predict` function expects multiple inputs, then the number of selected columns
    # must match the number of expected inputs.

    def predict_batch_fn():
        def predict(x1: np.ndarray, x2: np.ndarray, x3: np.ndarray, x4: np.ndarray) -> np.ndarray:
            # xN.shape = [batch_size]
            # outputs.shape = [batch_size], return_type = FloatType()
            return x1 + x2 + x3 + x4

        return predict

    sum_rows = predict_batch_udf(predict_batch_fn,
                                 return_type=FloatType(),
                                 batch_size=10)

    df.withColumn("sum", sum_rows("a", "b", "c", "d")).show(5)
    # +----+----+----+----+----+
    # |   a|   b|   c|   d| sum|
    # +----+----+----+----+----+
    # | 0.0| 1.0| 2.0| 3.0| 6.0|
    # | 4.0| 5.0| 6.0| 7.0|22.0|
    # | 8.0| 9.0|10.0|11.0|38.0|
    # |12.0|13.0|14.0|15.0|54.0|
    # |16.0|17.0|18.0|19.0|70.0|
    # +----+----+----+----+----+
    ```

    Example (multi-col tensor):

    Input DataFrame has multiple columns, where each column is a tensor.  The number of columns
    should match the number of expected inputs for the user-provided `predict` function.
    ```
    import numpy as np
    import pandas as pd
    from pyspark.ml.functions import predict_batch_udf
    from pyspark.sql.types import FloatType, StructType, StructField
    from typing import Mapping

    data = np.arange(0, 1000, dtype=np.float64).reshape(-1, 4)
    pdf = pd.DataFrame(data, columns=['a','b','c','d'])
    pdf_tensor = pd.DataFrame()
    pdf_tensor['t1'] = pdf.values.tolist()
    pdf_tensor['t2'] = pdf.drop(columns='d').values.tolist()
    df = spark.createDataFrame(pdf_tensor)
    df.show(5)
    # +--------------------+------------------+
    # |                  t1|                t2|
    # +--------------------+------------------+
    # |[0.0, 1.0, 2.0, 3.0]|   [0.0, 1.0, 2.0]|
    # |[4.0, 5.0, 6.0, 7.0]|   [4.0, 5.0, 6.0]|
    # |[8.0, 9.0, 10.0, ...|  [8.0, 9.0, 10.0]|
    # |[12.0, 13.0, 14.0...|[12.0, 13.0, 14.0]|
    # |[16.0, 17.0, 18.0...|[16.0, 17.0, 18.0]|
    # +--------------------+------------------+

    def multi_sum_fn():
        def predict(x1: np.ndarray, x2: np.ndarray) -> np.ndarray:
            # x1.shape = [batch_size, 4]
            # x2.shape = [batch_size, 3]
            # outputs.shape = [batch_size], result_type = FloatType()
            return np.sum(x1, axis=1) + np.sum(x2, axis=1)

        return predict

    sum_cols = predict_batch_udf(
        multi_sum_fn,
        return_type=FloatType(),
        batch_size=5,
        input_tensor_shapes=[[4], [3]],
    )

    df.withColumn("sum", sum_cols("t1", "t2")).show(5)
    # +--------------------+------------------+-----+
    # |                  t1|                t2|  sum|
    # +--------------------+------------------+-----+
    # |[0.0, 1.0, 2.0, 3.0]|   [0.0, 1.0, 2.0]|  9.0|
    # |[4.0, 5.0, 6.0, 7.0]|   [4.0, 5.0, 6.0]| 37.0|
    # |[8.0, 9.0, 10.0, ...|  [8.0, 9.0, 10.0]| 65.0|
    # |[12.0, 13.0, 14.0...|[12.0, 13.0, 14.0]| 93.0|
    # |[16.0, 17.0, 18.0...|[16.0, 17.0, 18.0]|121.0|
    # +--------------------+------------------+-----+

    # Note that some models can provide multiple outputs.  These can be returned as a dictionary
    # of named values, which can be represented in columnar (or row-based) formats.

    def multi_sum_fn():
        def predict_columnar(x1: np.ndarray, x2: np.ndarray) -> Mapping[str, np.ndarray]:
            # x1.shape = [batch_size, 4]
            # x2.shape = [batch_size, 3]
            return {
                "sum1": np.sum(x1, axis=1),
                "sum2": np.sum(x2, axis=1)
            }  # return_type = StructType()

        return predict_columnar

    sum_cols = predict_batch_udf(
        multi_sum_fn,
        return_type=StructType([
            StructField("sum1", FloatType(), True),
            StructField("sum2", FloatType(), True)
        ])
        batch_size=5,
        input_tensor_shapes=[[4], [3]],
    )

    df.withColumn("preds", sum_cols("t1", "t2")).select("t1", "t2", "preds.*").show(5)
    # +--------------------+------------------+----+----+
    # |                  t1|                t2|sum1|sum2|
    # +--------------------+------------------+----+----+
    # |[0.0, 1.0, 2.0, 3.0]|   [0.0, 1.0, 2.0]| 6.0| 3.0|
    # |[4.0, 5.0, 6.0, 7.0]|   [4.0, 5.0, 6.0]|22.0|15.0|
    # |[8.0, 9.0, 10.0, ...|  [8.0, 9.0, 10.0]|38.0|27.0|
    # |[12.0, 13.0, 14.0...|[12.0, 13.0, 14.0]|54.0|39.0|
    # |[16.0, 17.0, 18.0...|[16.0, 17.0, 18.0]|70.0|51.0|
    # +--------------------+------------------+----+----+
    ```

    Parameters
    ----------
    predict_batch_fn : Callable[[],
        Callable[..., np.ndarray | Mapping[str, np.ndarray] | List[Mapping[str, np.dtype]] ]
        Function which is responsible for loading a model and returning a `predict` function which
        takes one or more numpy arrays as input and returns either a numpy array (for a single
        output), a dictionary of named numpy arrays (for multiple outputs), or a row-oriented list
        of dictionaries (for multiple outputs).
    return_type : :class:`pspark.sql.types.DataType` or str.
        Spark SQL datatype for the expected output.
    batch_size : int
        Batch size to use for inference, note that this is typically a limitation of the model
        and/or the hardware resources and is usually smaller than the Spark partition size.
    input_tensor_shapes: list[list[int] | None] | Mapping[int, list[int]] | None
        Optional input tensor shapes for models with tensor inputs.  This can be a list of shapes,
        where each shape is a list of integers or None (for scalar inputs).  Alternatively, this
        can be represented by a "sparse" dictionary, where the keys are the integer indices of the
        inputs, and the values are the shapes.  Each tensor input value in the Spark DataFrame must
        be represented as a single column containing a flattened 1-D array.  The provided
        input_tensor_shapes will be used to reshape the flattened array into expected tensor shape.
        For the list form, the order of the tensor shapes must match the order of the selected
        DataFrame columns.  The batch dimension (typically -1 or None in the first dimension) should
        not be included, since it will be determined by the batch_size argument.  Tabular datasets
        with scalar-valued columns should not provide this argument.

    Returns
    -------
    A pandas_udf for predicting a batch.
    """
    # generate a new uuid each time this is invoked on the driver to invalidate executor-side cache.
    model_uuid = uuid.uuid4()

    def predict(data: Iterator[Union[pd.Series, pd.DataFrame]]) -> Iterator[pd.DataFrame]:
        from pyspark.ml.model_cache import ModelCache

        # get predict function (from cache or from running user-provided predict_batch_fn)
        predict_fn = ModelCache.get(model_uuid)
        if not predict_fn:
            predict_fn = predict_batch_fn()
            ModelCache.add(model_uuid, predict_fn)

        # get number of expected parameters for predict function
        signature = inspect.signature(predict_fn)
        num_expected = len(signature.parameters)

        # convert sparse input_tensor_shapes to dense if needed
        input_shapes: list[list[int] | None]
        if isinstance(input_tensor_shapes, Mapping):
            input_shapes = [None] * num_expected
            for index, shape in input_tensor_shapes.items():
                input_shapes[index] = shape
        else:
            input_shapes = input_tensor_shapes  # type: ignore

        # iterate over partition, invoking predict_fn with ndarrays
        for partition in data:
            has_tuple = isinstance(partition, Tuple)  # type: ignore
            has_tensors = _has_tensor_cols(partition)

            # require input_tensor_shapes for any tensor columns
            if has_tensors and not input_shapes:
                raise ValueError("Tensor columns require input_tensor_shapes")

            for batch in _batched(partition, batch_size):
                num_actual = len(batch.columns)
                if num_actual == num_expected and num_expected > 1:
                    # input column per expected input, convert each column into param
                    multi_inputs = [batch[col].to_numpy() for col in batch.columns]
                    if input_shapes:
                        if len(input_shapes) == num_actual:
                            multi_inputs = [
                                np.vstack(v).reshape([-1] + input_shapes[i])  # type: ignore
                                if input_shapes[i]
                                else v
                                for i, v in enumerate(multi_inputs)
                            ]
                        else:
                            raise ValueError("input_tensor_shapes must match columns")

                    # run model prediction function on transformed (numpy) inputs
                    preds = predict_fn(*multi_inputs)
                elif num_expected == 1:
                    # multiple input columns for single expected input
                    if has_tensors:
                        # tensor columns
                        if len(batch.columns) == 1:
                            # one tensor column and one expected input, vstack rows
                            single_input = np.vstack(batch.iloc[:, 0])
                        else:
                            raise ValueError(
                                "Multiple input columns found, but model expected a single "
                                "input, use `struct` or `array` to combine columns into tensors."
                            )
                    else:
                        # scalar columns
                        if len(batch.columns) == 1:
                            # single scalar column, remove extra dim
                            single_input = np.squeeze(batch.to_numpy())
                        elif not has_tuple:
                            # columns grouped via struct/array, convert to single tensor
                            single_input = batch.to_numpy()
                        else:
                            raise ValueError(
                                "Multiple input columns found, but model expected a single "
                                "input, use `struct` or `array` to combine columns into tensors."
                            )

                    # if input_tensor_shapes provided, try to reshape input
                    if input_shapes:
                        if len(input_shapes) == 1:
                            single_input = single_input.reshape(
                                [-1] + input_shapes[0]  # type: ignore
                            )
                        else:
                            raise ValueError(
                                "Multiple input_tensor_shapes found, but model expected one input"
                            )

                    # run model prediction function on transformed (numpy) inputs
                    preds = predict_fn(single_input)
                else:
                    msg = "Model expected {} inputs, but received {} columns"
                    raise ValueError(msg.format(num_expected, num_actual))

                # return predictions to Spark
                if isinstance(return_type, StructType):
                    yield pd.DataFrame(preds)
                elif isinstance(return_type, ArrayType):
                    yield pd.Series(list(preds))  # type: ignore[misc]
                else:
                    yield pd.Series(np.squeeze(preds))  # type: ignore

    return pandas_udf(predict, return_type)  # type: ignore[call-overload]


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.ml.functions
    import sys

    globs = pyspark.ml.functions.__dict__.copy()
    spark = SparkSession.builder.master("local[2]").appName("ml.functions tests").getOrCreate()
    sc = spark.sparkContext
    globs["sc"] = sc
    globs["spark"] = spark

    (failure_count, test_count) = doctest.testmod(
        pyspark.ml.functions,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
