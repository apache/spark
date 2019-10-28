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
A simple example demonstrating Arrow in Spark.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/arrow.py
"""

from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql.utils import require_minimum_pandas_version, require_minimum_pyarrow_version

require_minimum_pandas_version()
require_minimum_pyarrow_version()


def dataframe_with_arrow_example(spark):
    # $example on:dataframe_with_arrow$
    import numpy as np
    import pandas as pd

    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # Generate a Pandas DataFrame
    pdf = pd.DataFrame(np.random.rand(100, 3))

    # Create a Spark DataFrame from a Pandas DataFrame using Arrow
    df = spark.createDataFrame(pdf)

    # Convert the Spark DataFrame back to a Pandas DataFrame using Arrow
    result_pdf = df.select("*").toPandas()
    # $example off:dataframe_with_arrow$
    print("Pandas DataFrame result statistics:\n%s\n" % str(result_pdf.describe()))


def scalar_pandas_udf_example(spark):
    # $example on:scalar_pandas_udf$
    import pandas as pd

    from pyspark.sql.functions import col, pandas_udf
    from pyspark.sql.types import LongType

    # Declare the function and create the UDF
    def multiply_func(a, b):
        return a * b

    multiply = pandas_udf(multiply_func, returnType=LongType())

    # The function for a pandas_udf should be able to execute with local Pandas data
    x = pd.Series([1, 2, 3])
    print(multiply_func(x, x))
    # 0    1
    # 1    4
    # 2    9
    # dtype: int64

    # Create a Spark DataFrame, 'spark' is an existing SparkSession
    df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))

    # Execute function as a Spark vectorized UDF
    df.select(multiply(col("x"), col("x"))).show()
    # +-------------------+
    # |multiply_func(x, x)|
    # +-------------------+
    # |                  1|
    # |                  4|
    # |                  9|
    # +-------------------+
    # $example off:scalar_pandas_udf$


def scalar_iter_pandas_udf_example(spark):
    # $example on:scalar_iter_pandas_udf$
    import pandas as pd

    from pyspark.sql.functions import col, pandas_udf, struct, PandasUDFType

    pdf = pd.DataFrame([1, 2, 3], columns=["x"])
    df = spark.createDataFrame(pdf)

    # When the UDF is called with a single column that is not StructType,
    # the input to the underlying function is an iterator of pd.Series.
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def plus_one(batch_iter):
        for x in batch_iter:
            yield x + 1

    df.select(plus_one(col("x"))).show()
    # +-----------+
    # |plus_one(x)|
    # +-----------+
    # |          2|
    # |          3|
    # |          4|
    # +-----------+

    # When the UDF is called with more than one columns,
    # the input to the underlying function is an iterator of pd.Series tuple.
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def multiply_two_cols(batch_iter):
        for a, b in batch_iter:
            yield a * b

    df.select(multiply_two_cols(col("x"), col("x"))).show()
    # +-----------------------+
    # |multiply_two_cols(x, x)|
    # +-----------------------+
    # |                      1|
    # |                      4|
    # |                      9|
    # +-----------------------+

    # When the UDF is called with a single column that is StructType,
    # the input to the underlying function is an iterator of pd.DataFrame.
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def multiply_two_nested_cols(pdf_iter):
        for pdf in pdf_iter:
            yield pdf["a"] * pdf["b"]

    df.select(
        multiply_two_nested_cols(
            struct(col("x").alias("a"), col("x").alias("b"))
        ).alias("y")
    ).show()
    # +---+
    # |  y|
    # +---+
    # |  1|
    # |  4|
    # |  9|
    # +---+

    # In the UDF, you can initialize some states before processing batches.
    # Wrap your code with try/finally or use context managers to ensure
    # the release of resources at the end.
    y_bc = spark.sparkContext.broadcast(1)

    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def plus_y(batch_iter):
        y = y_bc.value  # initialize states
        try:
            for x in batch_iter:
                yield x + y
        finally:
            pass  # release resources here, if any

    df.select(plus_y(col("x"))).show()
    # +---------+
    # |plus_y(x)|
    # +---------+
    # |        2|
    # |        3|
    # |        4|
    # +---------+
    # $example off:scalar_iter_pandas_udf$


def grouped_map_pandas_udf_example(spark):
    # $example on:grouped_map_pandas_udf$
    from pyspark.sql.functions import pandas_udf, PandasUDFType

    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ("id", "v"))

    @pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
    def subtract_mean(pdf):
        # pdf is a pandas.DataFrame
        v = pdf.v
        return pdf.assign(v=v - v.mean())

    df.groupby("id").apply(subtract_mean).show()
    # +---+----+
    # | id|   v|
    # +---+----+
    # |  1|-0.5|
    # |  1| 0.5|
    # |  2|-3.0|
    # |  2|-1.0|
    # |  2| 4.0|
    # +---+----+
    # $example off:grouped_map_pandas_udf$


def grouped_agg_pandas_udf_example(spark):
    # $example on:grouped_agg_pandas_udf$
    from pyspark.sql.functions import pandas_udf, PandasUDFType
    from pyspark.sql import Window

    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ("id", "v"))

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(v):
        return v.mean()

    df.groupby("id").agg(mean_udf(df['v'])).show()
    # +---+-----------+
    # | id|mean_udf(v)|
    # +---+-----------+
    # |  1|        1.5|
    # |  2|        6.0|
    # +---+-----------+

    w = Window \
        .partitionBy('id') \
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df.withColumn('mean_v', mean_udf(df['v']).over(w)).show()
    # +---+----+------+
    # | id|   v|mean_v|
    # +---+----+------+
    # |  1| 1.0|   1.5|
    # |  1| 2.0|   1.5|
    # |  2| 3.0|   6.0|
    # |  2| 5.0|   6.0|
    # |  2|10.0|   6.0|
    # +---+----+------+
    # $example off:grouped_agg_pandas_udf$


def map_iter_pandas_udf_example(spark):
    # $example on:map_iter_pandas_udf$
    import pandas as pd

    from pyspark.sql.functions import pandas_udf, PandasUDFType

    df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

    @pandas_udf(df.schema, PandasUDFType.MAP_ITER)
    def filter_func(batch_iter):
        for pdf in batch_iter:
            yield pdf[pdf.id == 1]

    df.mapInPandas(filter_func).show()
    # +---+---+
    # | id|age|
    # +---+---+
    # |  1| 21|
    # +---+---+
    # $example off:map_iter_pandas_udf$


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark example") \
        .getOrCreate()

    print("Running Pandas to/from conversion example")
    dataframe_with_arrow_example(spark)
    print("Running pandas_udf scalar example")
    scalar_pandas_udf_example(spark)
    print("Running pandas_udf scalar iterator example")
    scalar_iter_pandas_udf_example(spark)
    print("Running pandas_udf grouped map example")
    grouped_map_pandas_udf_example(spark)
    print("Running pandas_udf grouped agg example")
    grouped_agg_pandas_udf_example(spark)
    print("Running pandas_udf map iterator example")
    map_iter_pandas_udf_example(spark)

    spark.stop()
