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

import numpy as np
import pandas as pd
import uuid
from pyspark import SparkContext
from pyspark.sql.functions import pandas_udf
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import ArrayType, DataType, FloatType, StructType
from typing import Callable, Iterator, Union


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


def batched(
    df: Union[pd.Series, pd.DataFrame],
    batch_size: int = -1
) -> Iterator[Union[pd.DataFrame, pd.Series]]:
    """Generator that splits a pandas dataframe/series into batches."""
    if batch_size <= 0 or batch_size >= len(df):
        yield df
    else:
        # for batch in np.array_split(df, (len(df.index) + batch_size - 1) // batch_size):
        for _, batch in df.groupby(np.arange(len(df)) // batch_size):
            yield batch


def has_tensor_cols(df: pd.DataFrame):
    """Check if input DataFrame contains any tensor-valued columns"""
    if any(df.dtypes == np.object_):
        # pd.DataFrame object types can contain different types, e.g. string, dates, etc.
        # so inspect a row and check for array/list type
        sample = df.iloc[0]
        return any([isinstance(x, np.ndarray) or isinstance(x, list) for x in sample])
    else:
        return False


def batch_infer_udf(
        predict_batch_fn: Callable,
        return_type: DataType = ArrayType(FloatType()),
        batch_size: int = -1,
        input_names: list[str] = [],
        input_tensor_shapes: list[list[int]] = [],
        **kwargs):
    """Given a function which loads a model, returns a pandas_udf for inferencing over that model.

    This will handle:
    - conversion of the Spark DataFrame to numpy arrays.
    - batching of the inputs sent to the model predict() function.
    - caching of the model and prediction function on the executors.

    This assumes that the `predict_batch_fn` encapsulates all of the necessary dependencies for
    running the model or the Spark executor environment already satisfies all runtime requirements.

    When selecting columns in pyspark SQL, users are required to always use `struct` for simplicity.

    For the conversion of Spark DataFrame to numpy, the following table describes the behavior,
    where tensor columns in the Spark DataFrame must be represented as a flattened 1-D array/list.

    | dataframe \ model | single input | multiple inputs |
    | :---------------- | :----------- | :-------------- |
    | single-col scalar | 1            | N/A             |
    | single-col tensor | 1,2          | N/A             |
    | multi-col scalar  | 3            | 4               |
    | multi-col tensor  | N/A          | 4,2             |

    Notes:
    1. pass thru dataframe column => model input as single numpy array.
    2. reshape flattened tensors into expected tensor shapes.
    3. convert entire dataframe into single numpy array via df.to_numpy(), or user can use
       `pyspark.sql.functions.array()` to transform the input into a single-col tensor first.
    4. pass thru dataframe column => model input as an (ordered) dictionary of numpy arrays.

    Parameters
    ----------
    predict_batch_fn : Callable
        Function which is responsible for loading a model and returning a `predict` function.
    return_type : DataType
        Spark SQL datatype for the expected output.
        Default: ArrayType(FloatType())
    batch_size : int
        Batch size to use for inference, note that this is typically a limitation of the model
        and/or the hardware resources and is usually smaller than the Spark partition size.
        Default: -1, which sends the entire Spark partition to the model.
    input_names: list[str]
        Optional list of input names which will be used to map DataFrame column names to model
        input names.  The order of names must match the order of the selected DataFrame columns.
        If provided, the `predict()` function will be passed a dictionary of named inputs.
    input_tensor_shapes: list[list[int]]
        Optional list of input tensor shapes for models with tensor inputs.  Each tensor
        input must be represented as a single DataFrame column containing a flattened 1-D array.
        The order of the tensor shapes must match the order of the selected DataFrame columns.
        Tabular datasets with scalar-valued columns should not supply this argument.

    Returns
    -------
    A pandas_udf for predicting a batch.
    """
    # generate a new uuid each time this is invoked on the driver to invalidate executor-side cache.
    model_uuid = uuid.uuid4()

    def predict(data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        import pyspark.ml.executor_globals as exec_global

        if exec_global.predict_fn and exec_global.model_uuid == model_uuid:
            predict_fn = exec_global.predict_fn
        else:
            predict_fn = predict_batch_fn(**kwargs)
            exec_global.predict_fn = predict_fn
            exec_global.model_uuid = model_uuid

        for partition in data:
            has_tensors = has_tensor_cols(partition)
            for batch in batched(partition, batch_size):
                if input_names:
                    # input names provided, expect a dictionary of named numpy arrays
                    # check if the number of inputs matches expected
                    num_expected = len(input_names)
                    num_actual = len(batch.columns)
                    if num_actual != num_expected:
                        msg = "Model expected {} inputs, but received {} columns"
                        raise ValueError(msg.format(num_expected, num_actual))

                    # rename dataframe column names to match model input names, if needed
                    if input_names != list(batch.columns):
                        batch.columns = input_names

                    if has_tensors:
                        raise ValueError("Tensor columns require an input_tensor_shape")

                    # create a dictionary of named inputs
                    inputs = batch.to_dict('series')

                    # reshape inputs, if needed
                    if input_tensor_shapes:
                        if len(input_tensor_shapes) == num_actual:
                            for i, (k, v) in enumerate(inputs.items()):
                                inputs[k] = v.reshape(input_tensor_shapes[i])
                        else:
                            raise ValueError("input_tensor_shapes must match columns")
                else:
                    # no input names provided, expect a single numpy array
                    if input_tensor_shapes:
                        if len(input_tensor_shapes) == 1:
                            if len(batch.columns) == 1:
                                # if one tensor input and one column, vstack and reshape the batch
                                input_shape = input_tensor_shapes[0]
                                input_shape[0] = -1     # replace None with -1 in batch dimension
                                # input = np.vstack(batch).reshape(input_shape)     # name, col
                                inputs = np.vstack(batch.iloc[:, 0]).reshape(input_shape)  # struct
                            else:
                                # otherwise, try to convert entire dataframe to a single np array
                                inputs = batch.to_numpy()
                        else:
                            msg = "Multiple input_tensor_shapes require associated input_names: {}"
                            raise ValueError(msg.format(input_tensor_shapes))
                    else:
                        if has_tensors:
                            raise ValueError("Tensor columns require an input_tensor_shape")
                        inputs = batch.to_numpy()

                # run model prediction function on transformed (numpy) inputs
                preds = predict_fn(inputs)

                # return predictions to Spark
                if isinstance(return_type, StructType):
                    yield pd.DataFrame(list(preds))
                elif isinstance(return_type, ArrayType):
                    yield pd.Series(list(preds))
                else:
                    yield pd.Series(np.squeeze(preds))

    return pandas_udf(predict, return_type)


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
