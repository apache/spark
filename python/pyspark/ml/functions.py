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
from pyspark.sql.types import (
    ArrayType,
    ByteType,
    DataType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructType,
)
from typing import Any, Callable, Iterator, List, Mapping, TYPE_CHECKING, Tuple, Union, Optional

if TYPE_CHECKING:
    from pyspark.sql._typing import UserDefinedFunctionLike

supported_scalar_types = (
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
)

# Callable type for end user predict functions that take a variable number of ndarrays as
# input and returns one of the following as output:
# - single ndarray (single output)
# - dictionary of named ndarrays (multiple outputs represented in columnar form)
# - list of dictionaries of named ndarrays (multiple outputs represented in row form)
PredictBatchFunction = Callable[
    [np.ndarray], Union[np.ndarray, Mapping[str, np.ndarray], List[Mapping[str, np.dtype]]]
]


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
    data: Union[pd.Series, pd.DataFrame, Tuple[pd.Series]], batch_size: int
) -> Iterator[pd.DataFrame]:
    """Generator that splits a pandas dataframe/series into batches."""
    if isinstance(data, pd.DataFrame):
        df = data
    elif isinstance(data, pd.Series):
        df = pd.concat((data,), axis=1)
    else:  # isinstance(data, Tuple[pd.Series]):
        df = pd.concat(data, axis=1)

    index = 0
    data_size = len(df)
    while index < data_size:
        yield df.iloc[index : index + batch_size]
        index += batch_size


def _is_tensor_col(data: Union[pd.Series, pd.DataFrame]) -> bool:
    if isinstance(data, pd.Series):
        return data.dtype == np.object_ and isinstance(data.iloc[0], (np.ndarray, list))
    elif isinstance(data, pd.DataFrame):
        return any(data.dtypes == np.object_) and any(
            [isinstance(d, (np.ndarray, list)) for d in data.iloc[0]]
        )
    else:
        raise ValueError(
            "Unexpected data type: {}, expected pd.Series or pd.DataFrame.".format(type(data))
        )


def _has_tensor_cols(data: Union[pd.Series, pd.DataFrame, Tuple[pd.Series]]) -> bool:
    """Check if input Series/DataFrame/Tuple contains any tensor-valued columns."""
    if isinstance(data, (pd.Series, pd.DataFrame)):
        return _is_tensor_col(data)
    else:  # isinstance(data, Tuple):
        return any(_is_tensor_col(elem) for elem in data)


def _validate_and_transform_multiple_inputs(
    batch: pd.DataFrame, input_shapes: List[Optional[List[int]]], num_input_cols: int
) -> List[np.ndarray]:
    multi_inputs = [batch[col].to_numpy() for col in batch.columns]
    if input_shapes:
        if len(input_shapes) == num_input_cols:
            multi_inputs = [
                np.vstack(v).reshape([-1] + input_shapes[i])  # type: ignore
                if input_shapes[i]
                else v
                for i, v in enumerate(multi_inputs)
            ]
            if not all([len(x) == len(batch) for x in multi_inputs]):
                raise ValueError("Input data does not match expected shape.")
        else:
            raise ValueError("input_tensor_shapes must match columns")

    return multi_inputs


def _validate_and_transform_single_input(
    batch: pd.DataFrame,
    input_shapes: List[List[int] | None],
    has_tensors: bool,
    has_tuple: bool,
) -> np.ndarray:
    # multiple input columns for single expected input
    if has_tensors:
        # tensor columns
        if len(batch.columns) == 1:
            # one tensor column and one expected input, vstack rows
            single_input = np.vstack(batch.iloc[:, 0])
        else:
            raise ValueError(
                "Multiple input columns found, but model expected a single "
                "input, use `array` to combine columns into tensors."
            )
    else:
        # scalar columns
        if len(batch.columns) == 1:
            # single scalar column, remove extra dim
            single_input = np.squeeze(batch.to_numpy())
            if input_shapes and input_shapes[0] not in [None, [], [1]]:
                raise ValueError("Invalid input_tensor_shape for scalar column.")
        elif not has_tuple:
            # columns grouped via `array`, convert to single tensor
            single_input = batch.to_numpy()
            if input_shapes and input_shapes[0] != [len(batch.columns)]:
                raise ValueError("Input data does not match expected shape.")
        else:
            raise ValueError(
                "Multiple input columns found, but model expected a single "
                "input, use `array` to combine columns into tensors."
            )

    # if input_tensor_shapes provided, try to reshape input
    if input_shapes:
        if len(input_shapes) == 1:
            single_input = single_input.reshape([-1] + input_shapes[0])  # type: ignore
            if len(single_input) != len(batch):
                raise ValueError("Input data does not match expected shape.")
        else:
            raise ValueError("Multiple input_tensor_shapes found, but model expected one input")

    return single_input


def _validate_and_transform_prediction_result(
    preds: np.ndarray | Mapping[str, np.ndarray] | List[Mapping[str, Any]],
    num_input_rows: int,
    return_type: DataType,
) -> pd.DataFrame | pd.Series:
    """Validate numpy-based model predictions against the expected pandas_udf return_type and
    transforms the predictions into an equivalent pandas DataFrame or Series."""
    if isinstance(return_type, StructType):
        struct_rtype: StructType = return_type
        fieldNames = struct_rtype.names
        if isinstance(preds, dict):
            # dictionary of columns
            predNames = list(preds.keys())
            for field in struct_rtype.fields:
                if isinstance(field.dataType, ArrayType):
                    if len(preds[field.name].shape) == 2:
                        preds[field.name] = list(preds[field.name])
                    else:
                        raise ValueError(
                            "Prediction results for ArrayType must be two-dimensional."
                        )
                elif isinstance(field.dataType, supported_scalar_types):
                    if len(preds[field.name].shape) != 1:
                        raise ValueError(
                            "Prediction results for scalar types must be one-dimensional."
                        )
                else:
                    raise ValueError("Unsupported field type in return struct type.")

                if len(preds[field.name]) != num_input_rows:
                    raise ValueError("Prediction results must have same length as input data")

        elif isinstance(preds, list) and isinstance(preds[0], dict):
            # rows of dictionaries
            predNames = list(preds[0].keys())
            if len(preds) != num_input_rows:
                raise ValueError("Prediction results must have same length as input data.")
            for field in struct_rtype.fields:
                if isinstance(field.dataType, ArrayType):
                    if len(preds[0][field.name].shape) != 1:
                        raise ValueError(
                            "Prediction results for ArrayType must be one-dimensional."
                        )
                elif isinstance(field.dataType, supported_scalar_types):
                    if not np.isscalar(preds[0][field.name]):
                        raise ValueError("Invalid scalar prediction result.")
                else:
                    raise ValueError("Unsupported field type in return struct type.")
        else:
            raise ValueError(
                "Prediction results for StructType must be a dictionary or "
                "a list of dictionary, got: {}".format(type(preds))
            )

        # check column names
        if set(predNames) != set(fieldNames):
            raise ValueError(
                "Prediction result columns did not match expected return_type "
                "columns: expected {}, got: {}".format(fieldNames, predNames)
            )

        return pd.DataFrame(preds)
    elif isinstance(return_type, ArrayType):
        if isinstance(preds, np.ndarray):
            if len(preds) != num_input_rows:
                raise ValueError("Prediction results must have same length as input data.")
            if len(preds.shape) != 2:
                raise ValueError("Prediction results for ArrayType must be two-dimensional.")
        else:
            raise ValueError("Prediction results for ArrayType must be an ndarray.")

        return pd.Series(list(preds))
    elif isinstance(return_type, supported_scalar_types):
        preds_array: np.ndarray = preds  # type: ignore
        if len(preds_array) != num_input_rows:
            raise ValueError("Prediction results must have same length as input data.")
        if not (
            (len(preds_array.shape) == 2 and preds_array.shape[1] == 1)
            or len(preds_array.shape) == 1
        ):
            raise ValueError("Invalid shape for scalar prediction result.")

        output = np.squeeze(preds)  # type: ignore[arg-type]
        return pd.Series(output).astype(output.dtype)
    else:
        raise ValueError("Unsupported return type")


def predict_batch_udf(
    make_predict_fn: Callable[
        [],
        PredictBatchFunction,
    ],
    *,
    return_type: DataType,
    batch_size: int,
    input_tensor_shapes: Optional[Union[List[Optional[List[int]]], Mapping[int, List[int]]]] = None,
) -> UserDefinedFunctionLike:
    """Given a function which loads a model and returns a `predict` function for inference over a
    batch of numpy inputs, returns a Pandas UDF wrapper for inference over a Spark DataFrame.

    The returned Pandas UDF does the following on each DataFrame partition:

    * calls the `make_predict_fn` to load the model and cache its `predict` function.
    * batches the input records as numpy arrays and invokes `predict` on each batch.

    Note: this assumes that the `make_predict_fn` encapsulates all of the necessary dependencies for
    running the model, or the Spark executor environment already satisfies all runtime requirements.

    For the conversion of the Spark DataFrame to numpy arrays, there is a one-to-one mapping between
    the input arguments of the `predict` function (returned by the `make_predict_fn`) and the input
    columns sent to the Pandas UDF (returned by the `predict_batch_udf`) at runtime.  Each input
    column will be converted as follows:

    * scalar column -> 1-dim np.ndarray
    * tensor column + tensor shape -> N-dim np.ndarray

    Note that any tensor columns in the Spark DataFrame must be represented as a flattened
    one-dimensional array, and multiple scalar columns can be combined into a single tensor column
    using the standard :py:func:`pyspark.sql.functions.array()` function.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    make_predict_fn : callable
        Function which is responsible for loading a model and returning a
        :py:class:`PredictBatchFunction` which takes one or more numpy arrays as input and returns
        one of the following:

        * a numpy array (for a single output)
        * a dictionary of named numpy arrays (for multiple outputs)
        * a row-oriented list of dictionaries (for multiple outputs).

        For a dictionary of named numpy arrays, the arrays can only be one or two dimensional, since
        higher dimensional arrays are not supported.  For a row-oriented list of dictionaries, each
        element in the dictionary must be either a scalar or one-dimensional array.
    return_type : :py:class:`pyspark.sql.types.DataType` or str.
        Spark SQL datatype for the expected output:

        * Scalar (e.g. IntegerType, FloatType) --> 1-dim numpy array.
        * ArrayType --> 2-dim numpy array.
        * StructType --> dict with keys matching struct fields.
        * StructType --> list of dict with keys matching struct fields, for models like the
          `Huggingface pipeline for sentiment analysis
          <https://huggingface.co/docs/transformers/quicktour#pipeline-usage>`_.

    batch_size : int
        Batch size to use for inference.  This is typically a limitation of the model
        and/or available hardware resources and is usually smaller than the Spark partition size.
    input_tensor_shapes : list, dict, optional.
        A list of ints or a dictionary of ints (key) and list of ints (value).
        Input tensor shapes for models with tensor inputs.  This can be a list of shapes,
        where each shape is a list of integers or None (for scalar inputs).  Alternatively, this
        can be represented by a "sparse" dictionary, where the keys are the integer indices of the
        inputs, and the values are the shapes.  Each tensor input value in the Spark DataFrame must
        be represented as a single column containing a flattened 1-D array.  The provided
        `input_tensor_shapes` will be used to reshape the flattened array into the expected tensor
        shape.  For the list form, the order of the tensor shapes must match the order of the
        selected DataFrame columns.  The batch dimension (typically -1 or None in the first
        dimension) should not be included, since it will be determined by the batch_size argument.
        Tabular datasets with scalar-valued columns should not provide this argument.

    Returns
    -------
    :py:class:`UserDefinedFunctionLike`
        A Pandas UDF for model inference on a Spark DataFrame.

    Examples
    --------
    For a pre-trained TensorFlow MNIST model with two-dimensional input images represented as a
    flattened tensor value stored in a single Spark DataFrame column of type `array<float>`.

    .. code-block:: python

        from pyspark.ml.functions import predict_batch_udf

        def make_mnist_fn():
            # load/init happens once per python worker
            import tensorflow as tf
            model = tf.keras.models.load_model('/path/to/mnist_model')

            # predict on batches of tasks/partitions, using cached model
            def predict(inputs: np.ndarray) -> np.ndarray:
                # inputs.shape = [batch_size, 784], see input_tensor_shapes
                # outputs.shape = [batch_size, 10], see return_type
                return model.predict(inputs)

            return predict

        mnist_udf = predict_batch_udf(make_mnist_fn,
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

        df.withColumn("preds", mnist_udf("data")).show(5)
        # +--------------------+--------------------+
        # |                data|               preds|
        # +--------------------+--------------------+
        # |[0.0, 0.0, 0.0, 0...|[-13.511008, 8.84...|
        # |[0.0, 0.0, 0.0, 0...|[-5.3957458, -2.2...|
        # |[0.0, 0.0, 0.0, 0...|[-7.2014456, -8.8...|
        # |[0.0, 0.0, 0.0, 0...|[-19.466187, -13....|
        # |[0.0, 0.0, 0.0, 0...|[-5.7757926, -7.8...|
        # +--------------------+--------------------+

    To demonstrate usage with different combinations of input and output types, the following
    examples just use simple mathematical transforms as the models.

    * Single scalar column
        Input DataFrame has a single scalar column, which will be passed to the `predict`
        function as a 1-D numpy array.

        >>> import numpy as np
        >>> import pandas as pd
        >>> from pyspark.ml.functions import predict_batch_udf
        >>> from pyspark.sql.types import FloatType
        >>>
        >>> df = spark.createDataFrame(pd.DataFrame(np.arange(100)))
        >>> df.show(5)
        +---+
        |  0|
        +---+
        |  0|
        |  1|
        |  2|
        |  3|
        |  4|
        +---+
        only showing top 5 rows

        >>> def make_times_two_fn():
        ...     def predict(inputs: np.ndarray) -> np.ndarray:
        ...         # inputs.shape = [batch_size]
        ...         # outputs.shape = [batch_size]
        ...         return inputs * 2
        ...     return predict
        >>>
        >>> times_two_udf = predict_batch_udf(make_times_two_fn,
        ...                                   return_type=FloatType(),
        ...                                   batch_size=10)
        >>>
        >>> df = spark.createDataFrame(pd.DataFrame(np.arange(100)))
        >>> df.withColumn("x2", times_two_udf("0")).show(5)
        +---+---+
        |  0| x2|
        +---+---+
        |  0|0.0|
        |  1|2.0|
        |  2|4.0|
        |  3|6.0|
        |  4|8.0|
        +---+---+
        only showing top 5 rows

    * Multiple scalar columns
        Input DataFrame has muliple columns of scalar values.  If the user-provided `predict`
        function expects a single input, then the user must combine the multiple columns into a
        single tensor using `pyspark.sql.functions.array`.

        >>> import numpy as np
        >>> import pandas as pd
        >>> from pyspark.ml.functions import predict_batch_udf
        >>> from pyspark.sql.functions import array
        >>>
        >>> data = np.arange(0, 1000, dtype=np.float64).reshape(-1, 4)
        >>> pdf = pd.DataFrame(data, columns=['a','b','c','d'])
        >>> df = spark.createDataFrame(pdf)
        >>> df.show(5)
        +----+----+----+----+
        |   a|   b|   c|   d|
        +----+----+----+----+
        | 0.0| 1.0| 2.0| 3.0|
        | 4.0| 5.0| 6.0| 7.0|
        | 8.0| 9.0|10.0|11.0|
        |12.0|13.0|14.0|15.0|
        |16.0|17.0|18.0|19.0|
        +----+----+----+----+
        only showing top 5 rows

        >>> def make_sum_fn():
        ...     def predict(inputs: np.ndarray) -> np.ndarray:
        ...         # inputs.shape = [batch_size, 4]
        ...         # outputs.shape = [batch_size]
        ...         return np.sum(inputs, axis=1)
        ...     return predict
        >>>
        >>> sum_udf = predict_batch_udf(make_sum_fn,
        ...                             return_type=FloatType(),
        ...                             batch_size=10,
        ...                             input_tensor_shapes=[[4]])
        >>>
        >>> df.withColumn("sum", sum_udf(array("a", "b", "c", "d"))).show(5)
        +----+----+----+----+----+
        |   a|   b|   c|   d| sum|
        +----+----+----+----+----+
        | 0.0| 1.0| 2.0| 3.0| 6.0|
        | 4.0| 5.0| 6.0| 7.0|22.0|
        | 8.0| 9.0|10.0|11.0|38.0|
        |12.0|13.0|14.0|15.0|54.0|
        |16.0|17.0|18.0|19.0|70.0|
        +----+----+----+----+----+
        only showing top 5 rows

        If the `predict` function expects multiple inputs, then the number of selected input columns
        must match the number of expected inputs.

        >>> def make_sum_fn():
        ...     def predict(x1: np.ndarray,
        ...                 x2: np.ndarray,
        ...                 x3: np.ndarray,
        ...                 x4: np.ndarray) -> np.ndarray:
        ...         # xN.shape = [batch_size]
        ...         # outputs.shape = [batch_size]
        ...         return x1 + x2 + x3 + x4
        ...     return predict
        >>>
        >>> sum_udf = predict_batch_udf(make_sum_fn,
        ...                             return_type=FloatType(),
        ...                             batch_size=10)
        >>>
        >>> df.withColumn("sum", sum_udf("a", "b", "c", "d")).show(5)
        +----+----+----+----+----+
        |   a|   b|   c|   d| sum|
        +----+----+----+----+----+
        | 0.0| 1.0| 2.0| 3.0| 6.0|
        | 4.0| 5.0| 6.0| 7.0|22.0|
        | 8.0| 9.0|10.0|11.0|38.0|
        |12.0|13.0|14.0|15.0|54.0|
        |16.0|17.0|18.0|19.0|70.0|
        +----+----+----+----+----+
        only showing top 5 rows

    * Multiple tensor columns
        Input DataFrame has multiple columns, where each column is a tensor.  The number of columns
        should match the number of expected inputs for the user-provided `predict` function.

        >>> import numpy as np
        >>> import pandas as pd
        >>> from pyspark.ml.functions import predict_batch_udf
        >>> from pyspark.sql.types import ArrayType, FloatType, StructType, StructField
        >>> from typing import Mapping
        >>>
        >>> data = np.arange(0, 1000, dtype=np.float64).reshape(-1, 4)
        >>> pdf = pd.DataFrame(data, columns=['a','b','c','d'])
        >>> pdf_tensor = pd.DataFrame()
        >>> pdf_tensor['t1'] = pdf.values.tolist()
        >>> pdf_tensor['t2'] = pdf.drop(columns='d').values.tolist()
        >>> df = spark.createDataFrame(pdf_tensor)
        >>> df.show(5)
        +--------------------+------------------+
        |                  t1|                t2|
        +--------------------+------------------+
        |[0.0, 1.0, 2.0, 3.0]|   [0.0, 1.0, 2.0]|
        |[4.0, 5.0, 6.0, 7.0]|   [4.0, 5.0, 6.0]|
        |[8.0, 9.0, 10.0, ...|  [8.0, 9.0, 10.0]|
        |[12.0, 13.0, 14.0...|[12.0, 13.0, 14.0]|
        |[16.0, 17.0, 18.0...|[16.0, 17.0, 18.0]|
        +--------------------+------------------+
        only showing top 5 rows

        >>> def make_multi_sum_fn():
        ...     def predict(x1: np.ndarray, x2: np.ndarray) -> np.ndarray:
        ...         # x1.shape = [batch_size, 4]
        ...         # x2.shape = [batch_size, 3]
        ...         # outputs.shape = [batch_size]
        ...         return np.sum(x1, axis=1) + np.sum(x2, axis=1)
        ...     return predict
        >>>
        >>> multi_sum_udf = predict_batch_udf(
        ...     make_multi_sum_fn,
        ...     return_type=FloatType(),
        ...     batch_size=5,
        ...     input_tensor_shapes=[[4], [3]],
        ... )
        >>>
        >>> df.withColumn("sum", multi_sum_udf("t1", "t2")).show(5)
        +--------------------+------------------+-----+
        |                  t1|                t2|  sum|
        +--------------------+------------------+-----+
        |[0.0, 1.0, 2.0, 3.0]|   [0.0, 1.0, 2.0]|  9.0|
        |[4.0, 5.0, 6.0, 7.0]|   [4.0, 5.0, 6.0]| 37.0|
        |[8.0, 9.0, 10.0, ...|  [8.0, 9.0, 10.0]| 65.0|
        |[12.0, 13.0, 14.0...|[12.0, 13.0, 14.0]| 93.0|
        |[16.0, 17.0, 18.0...|[16.0, 17.0, 18.0]|121.0|
        +--------------------+------------------+-----+
        only showing top 5 rows

    * Multiple outputs
        Some models can provide multiple outputs.  These can be returned as a dictionary of named
        values, which can be represented in either columnar or row-based formats.

        >>> def make_multi_sum_fn():
        ...     def predict_columnar(x1: np.ndarray, x2: np.ndarray) -> Mapping[str, np.ndarray]:
        ...         # x1.shape = [batch_size, 4]
        ...         # x2.shape = [batch_size, 3]
        ...         return {
        ...             "sum1": np.sum(x1, axis=1),
        ...             "sum2": np.sum(x2, axis=1)
        ...         }
        ...     return predict_columnar
        >>>
        >>> multi_sum_udf = predict_batch_udf(
        ...     make_multi_sum_fn,
        ...     return_type=StructType([
        ...         StructField("sum1", FloatType(), True),
        ...         StructField("sum2", FloatType(), True)
        ...     ]),
        ...     batch_size=5,
        ...     input_tensor_shapes=[[4], [3]],
        ... )
        >>>
        >>> df.withColumn("preds", multi_sum_udf("t1", "t2")).select("t1", "t2", "preds.*").show(5)
        +--------------------+------------------+----+----+
        |                  t1|                t2|sum1|sum2|
        +--------------------+------------------+----+----+
        |[0.0, 1.0, 2.0, 3.0]|   [0.0, 1.0, 2.0]| 6.0| 3.0|
        |[4.0, 5.0, 6.0, 7.0]|   [4.0, 5.0, 6.0]|22.0|15.0|
        |[8.0, 9.0, 10.0, ...|  [8.0, 9.0, 10.0]|38.0|27.0|
        |[12.0, 13.0, 14.0...|[12.0, 13.0, 14.0]|54.0|39.0|
        |[16.0, 17.0, 18.0...|[16.0, 17.0, 18.0]|70.0|51.0|
        +--------------------+------------------+----+----+
        only showing top 5 rows

        >>> def make_multi_sum_fn():
        ...     def predict_row(x1: np.ndarray, x2: np.ndarray) -> list[Mapping[str, float]]:
        ...         # x1.shape = [batch_size, 4]
        ...         # x2.shape = [batch_size, 3]
        ...         return [{'sum1': np.sum(x1[i]), 'sum2': np.sum(x2[i])} for i in range(len(x1))]
        ...     return predict_row
        >>>
        >>> multi_sum_udf = predict_batch_udf(
        ...     make_multi_sum_fn,
        ...     return_type=StructType([
        ...         StructField("sum1", FloatType(), True),
        ...         StructField("sum2", FloatType(), True)
        ...     ]),
        ...     batch_size=5,
        ...     input_tensor_shapes=[[4], [3]],
        ... )
        >>>
        >>> df.withColumn("sum", multi_sum_udf("t1", "t2")).select("t1", "t2", "sum.*").show(5)
        +--------------------+------------------+----+----+
        |                  t1|                t2|sum1|sum2|
        +--------------------+------------------+----+----+
        |[0.0, 1.0, 2.0, 3.0]|   [0.0, 1.0, 2.0]| 6.0| 3.0|
        |[4.0, 5.0, 6.0, 7.0]|   [4.0, 5.0, 6.0]|22.0|15.0|
        |[8.0, 9.0, 10.0, ...|  [8.0, 9.0, 10.0]|38.0|27.0|
        |[12.0, 13.0, 14.0...|[12.0, 13.0, 14.0]|54.0|39.0|
        |[16.0, 17.0, 18.0...|[16.0, 17.0, 18.0]|70.0|51.0|
        +--------------------+------------------+----+----+
        only showing top 5 rows

        Note that the multiple outputs can be arrays as well.

        >>> def make_multi_times_two_fn():
        ...     def predict(x1: np.ndarray, x2: np.ndarray) -> Mapping[str, np.ndarray]:
        ...         # x1.shape = [batch_size, 4]
        ...         # x2.shape = [batch_size, 3]
        ...         return {"t1x2": x1 * 2, "t2x2": x2 * 2}
        ...     return predict
        >>>
        >>> multi_times_two_udf = predict_batch_udf(
        ...     make_multi_times_two_fn,
        ...     return_type=StructType([
        ...         StructField("t1x2", ArrayType(FloatType()), True),
        ...         StructField("t2x2", ArrayType(FloatType()), True)
        ...     ]),
        ...     batch_size=5,
        ...     input_tensor_shapes=[[4], [3]],
        ... )
        >>>
        >>> df.withColumn("x2", multi_times_two_udf("t1", "t2")).select("t1", "t2", "x2.*").show(5)
        +--------------------+------------------+--------------------+------------------+
        |                  t1|                t2|                t1x2|              t2x2|
        +--------------------+------------------+--------------------+------------------+
        |[0.0, 1.0, 2.0, 3.0]|   [0.0, 1.0, 2.0]|[0.0, 2.0, 4.0, 6.0]|   [0.0, 2.0, 4.0]|
        |[4.0, 5.0, 6.0, 7.0]|   [4.0, 5.0, 6.0]|[8.0, 10.0, 12.0,...| [8.0, 10.0, 12.0]|
        |[8.0, 9.0, 10.0, ...|  [8.0, 9.0, 10.0]|[16.0, 18.0, 20.0...|[16.0, 18.0, 20.0]|
        |[12.0, 13.0, 14.0...|[12.0, 13.0, 14.0]|[24.0, 26.0, 28.0...|[24.0, 26.0, 28.0]|
        |[16.0, 17.0, 18.0...|[16.0, 17.0, 18.0]|[32.0, 34.0, 36.0...|[32.0, 34.0, 36.0]|
        +--------------------+------------------+--------------------+------------------+
        only showing top 5 rows
    """
    # generate a new uuid each time this is invoked on the driver to invalidate executor-side cache.
    model_uuid = uuid.uuid4()

    def predict(data: Iterator[Union[pd.Series, pd.DataFrame]]) -> Iterator[pd.DataFrame]:
        # TODO: adjust return type hint when Iterator[Union[pd.Series, pd.DataFrame]] is supported
        from pyspark.ml.model_cache import ModelCache

        # get predict function (from cache or from running user-provided make_predict_fn)
        predict_fn = ModelCache.get(model_uuid)
        if not predict_fn:
            predict_fn = make_predict_fn()
            ModelCache.add(model_uuid, predict_fn)

        # get number of expected parameters for predict function
        signature = inspect.signature(predict_fn)
        num_expected_cols = len(signature.parameters)

        # convert sparse input_tensor_shapes to dense if needed
        input_shapes: List[List[int] | None]
        if isinstance(input_tensor_shapes, Mapping):
            input_shapes = [None] * num_expected_cols
            for index, shape in input_tensor_shapes.items():
                input_shapes[index] = shape
        else:
            input_shapes = input_tensor_shapes  # type: ignore

        # iterate over pandas batch, invoking predict_fn with ndarrays
        for pandas_batch in data:
            has_tuple = isinstance(pandas_batch, Tuple)  # type: ignore
            has_tensors = _has_tensor_cols(pandas_batch)

            # require input_tensor_shapes for any tensor columns
            if has_tensors and not input_shapes:
                raise ValueError("Tensor columns require input_tensor_shapes")

            for batch in _batched(pandas_batch, batch_size):
                num_input_rows = len(batch)
                num_input_cols = len(batch.columns)
                if num_input_cols == num_expected_cols and num_expected_cols > 1:
                    # input column per expected input for multiple inputs
                    multi_inputs = _validate_and_transform_multiple_inputs(
                        batch, input_shapes, num_input_cols
                    )
                    # run model prediction function on multiple (numpy) inputs
                    preds = predict_fn(*multi_inputs)
                elif num_expected_cols == 1:
                    # one or more input columns for single expected input
                    single_input = _validate_and_transform_single_input(
                        batch, input_shapes, has_tensors, has_tuple
                    )
                    # run model prediction function on single (numpy) inputs
                    preds = predict_fn(single_input)
                else:
                    msg = "Model expected {} inputs, but received {} columns"
                    raise ValueError(msg.format(num_expected_cols, num_input_cols))

                # return transformed predictions to Spark
                yield _validate_and_transform_prediction_result(
                    preds, num_input_rows, return_type
                )  # type: ignore

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
