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
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

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


def grouped_map_pandas_udf_example(spark):
    # $example on:grouped_map_pandas_udf$
    from pyspark.sql.functions import pandas_udf, PandasUDFType

    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ("id", "v"))

    @pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
    def substract_mean(pdf):
        # pdf is a pandas.DataFrame
        v = pdf.v
        return pdf.assign(v=v - v.mean())

    df.groupby("id").apply(substract_mean).show()
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


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark example") \
        .getOrCreate()

    print("Running Pandas to/from conversion example")
    dataframe_with_arrow_example(spark)
    print("Running pandas_udf scalar example")
    scalar_pandas_udf_example(spark)
    print("Running pandas_udf grouped map example")
    grouped_map_pandas_udf_example(spark)

    spark.stop()
