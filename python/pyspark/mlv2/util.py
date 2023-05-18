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

import pandas as pd
import cloudpickle

from pyspark.sql.functions import col, pandas_udf


def aggregate_dataframe(
        dataframe,
        cols,
        local_agg_fn,
        merge_agg_status,
        agg_status_to_result
):
    """
    The function can be used to run arbitrary aggregation logic on a spark dataframe
    or a pandas dataframe.

    Parameters
    ----------
    dataframe :
        A spark dataframe or a pandas dataframe

    cols :
        The name of columns that are used in aggregation

    local_agg_fn :
        A user-defined function that converts a pandas dataframe to an object holding
        aggregation status. The aggregation status object must be pickle-able by
        `cloudpickle`.

    merge_agg_status :
        A user-defined function that merges 2 aggregation status objects into one and
        return the merged status. Either in-place modifying the first input status object
        and returning it or creating a new status object are acceptable.

    agg_status_to_result :
        A user-defined function that converts aggregation status object to final aggregation
        result.

    Returns
    -------
    Aggregation result.
    """

    if isinstance(dataframe, pd.DataFrame):
        dataframe = dataframe[list(cols)]
        agg_status = local_agg_fn(dataframe)
        return agg_status_to_result(agg_status)

    dataframe = dataframe.select(*cols)

    def compute_status(iterator):
        status = None

        for batch_pandas_df in iterator:
            new_batch_status = local_agg_fn(batch_pandas_df)
            if status is None:
                status = new_batch_status
            else:
                status = merge_agg_status(status, new_batch_status)

        return pd.DataFrame({'status': [cloudpickle.dumps(status)]})

    result_pdf = dataframe.mapInPandas(compute_status, schema='status binary').toPandas()

    merged_status = None
    for status in result_pdf.status:
        status = cloudpickle.loads(status)
        if merged_status is None:
            merged_status = status
        else:
            merged_status = merge_agg_status(merged_status, status)

    return agg_status_to_result(merged_status)


def transform_dataframe_column(
        dataframe,
        input_col_name,
        transform_fn,
        output_cols,
):
    """
    Transform specified column of the input spark dataframe or pandas dataframe,
    returns a new dataframe

    Parameters
    ----------
    dataframe :
        A spark dataframe or a pandas dataframe

    input_col_name :
        The name of columns to be transformed

    transform_fn:
        A transforming function with one arguments of `pandas.Series` type,
        if the transform function output is only one column data,
        return transformed result as a `pandas.Series` object,
        otherwise return transformed result as a `pandas.DataFrame` object
        with corresponding column names defined in `output_cols` argument.
        The output pandas Series/DataFrame object must have the same index
        with the input series.

    output_cols:
        a list of output transformed columns, each elements in the list
        is a tuple of (column_name, column_spark_type)

    Returns
    -------
    If the input dataframe is a spark dataframe, return a new spark dataframe
    with the transformed result column appended.
    If the input dataframe is a pandas dataframe, return a new pandas dataframe
    with only one column of the the transformed result, but the result
    pandas dataframe has the same index with the input dataframe.
    """

    if len(output_cols) > 1:
        output_col_name = "__spark_ml_transformer_output_tmp__"
        spark_udf_return_type = (
            ",".join([f"{col_name} {col_type}" for col_name, col_type in output_cols])
        )
    else:
        output_col_name, spark_udf_return_type = output_cols[0]

    if isinstance(dataframe, pd.DataFrame):
        result_data = transform_fn(dataframe[input_col_name])
        if isinstance(result_data, pd.Series):
            assert len(output_cols) == 1
            return pd.DataFrame({output_col_name: result_data})
        else:
            assert set(result_data.columns) == set(col_name for col_name, _ in output_cols)
            return result_data

    @pandas_udf(returnType=spark_udf_return_type)
    def transform_fn_pandas_udf(s: pd.Series) -> pd.Series:
        return transform_fn(s)

    input_col = col(input_col_name)
    input_col_type = dict(dataframe.dtypes)[input_col_name]

    if input_col_type == "vector":
        from pyspark.ml.functions import vector_to_array
        # pandas UDF does not support vector type for now,
        # we convert it into vector type
        input_col = vector_to_array(input_col)

    result_spark_df = dataframe.withColumn(
        output_col_name, transform_fn_pandas_udf(input_col)
    )

    if len(output_cols) > 1:
        return result_spark_df.select(
            *[f"{output_col_name}.{col_name}" for col_name, _ in output_cols]
        ).drop(output_col_name)
    else:
        return result_spark_df
