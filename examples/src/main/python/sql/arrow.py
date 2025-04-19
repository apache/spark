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

# NOTE that this file is imported in tutorials in PySpark documentation.
# The codes are referred via line numbers. See also `literalinclude` directive in Sphinx.
import pandas as pd
from typing import Iterable

from pyspark.sql import SparkSession
from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version

require_minimum_pandas_version()
require_minimum_pyarrow_version()


def dataframe_to_from_arrow_table_example(spark: SparkSession) -> None:
    import pyarrow as pa
    import numpy as np

    # Create a PyArrow Table
    table = pa.table([pa.array(np.random.rand(100)) for i in range(3)], names=["a", "b", "c"])

    # Create a Spark DataFrame from the PyArrow Table
    df = spark.createDataFrame(table)

    # Convert the Spark DataFrame to a PyArrow Table
    result_table = df.select("*").toArrow()

    print(result_table.schema)
    # a: double
    # b: double
    # c: double


def dataframe_with_arrow_example(spark: SparkSession) -> None:
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

    print("Pandas DataFrame result statistics:\n%s\n" % str(result_pdf.describe()))


def ser_to_frame_pandas_udf_example(spark: SparkSession) -> None:
    import pandas as pd

    from pyspark.sql.functions import pandas_udf

    @pandas_udf("col1 string, col2 long")  # type: ignore[call-overload]
    def func(s1: pd.Series, s2: pd.Series, s3: pd.DataFrame) -> pd.DataFrame:
        s3['col2'] = s1 + s2.str.len()
        return s3

    # Create a Spark DataFrame that has three columns including a struct column.
    df = spark.createDataFrame(
        [[1, "a string", ("a nested string",)]],
        "long_col long, string_col string, struct_col struct<col1:string>")

    df.printSchema()
    # root
    # |-- long_column: long (nullable = true)
    # |-- string_column: string (nullable = true)
    # |-- struct_column: struct (nullable = true)
    # |    |-- col1: string (nullable = true)

    df.select(func("long_col", "string_col", "struct_col")).printSchema()
    # |-- func(long_col, string_col, struct_col): struct (nullable = true)
    # |    |-- col1: string (nullable = true)
    # |    |-- col2: long (nullable = true)


def ser_to_ser_pandas_udf_example(spark: SparkSession) -> None:
    import pandas as pd

    from pyspark.sql.functions import col, pandas_udf
    from pyspark.sql.types import LongType

    # Declare the function and create the UDF
    def multiply_func(a: pd.Series, b: pd.Series) -> pd.Series:
        return a * b

    multiply = pandas_udf(multiply_func, returnType=LongType())  # type: ignore[call-overload]

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


def iter_ser_to_iter_ser_pandas_udf_example(spark: SparkSession) -> None:
    from typing import Iterator

    import pandas as pd

    from pyspark.sql.functions import pandas_udf

    pdf = pd.DataFrame([1, 2, 3], columns=["x"])
    df = spark.createDataFrame(pdf)

    # Declare the function and create the UDF
    @pandas_udf("long")  # type: ignore[call-overload]
    def plus_one(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
        for x in iterator:
            yield x + 1

    df.select(plus_one("x")).show()
    # +-----------+
    # |plus_one(x)|
    # +-----------+
    # |          2|
    # |          3|
    # |          4|
    # +-----------+


def iter_sers_to_iter_ser_pandas_udf_example(spark: SparkSession) -> None:
    from typing import Iterator, Tuple

    import pandas as pd

    from pyspark.sql.functions import pandas_udf

    pdf = pd.DataFrame([1, 2, 3], columns=["x"])
    df = spark.createDataFrame(pdf)

    # Declare the function and create the UDF
    @pandas_udf("long")  # type: ignore[call-overload]
    def multiply_two_cols(
            iterator: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
        for a, b in iterator:
            yield a * b

    df.select(multiply_two_cols("x", "x")).show()
    # +-----------------------+
    # |multiply_two_cols(x, x)|
    # +-----------------------+
    # |                      1|
    # |                      4|
    # |                      9|
    # +-----------------------+


def ser_to_scalar_pandas_udf_example(spark: SparkSession) -> None:
    import pandas as pd

    from pyspark.sql.functions import pandas_udf
    from pyspark.sql import Window

    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ("id", "v"))

    # Declare the function and create the UDF
    @pandas_udf("double")  # type: ignore[call-overload]
    def mean_udf(v: pd.Series) -> float:
        return v.mean()

    df.select(mean_udf(df['v'])).show()
    # +-----------+
    # |mean_udf(v)|
    # +-----------+
    # |        4.2|
    # +-----------+

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


def grouped_apply_in_pandas_example(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ("id", "v"))

    def subtract_mean(pdf: pd.DataFrame) -> pd.DataFrame:
        # pdf is a pandas.DataFrame
        v = pdf.v
        return pdf.assign(v=v - v.mean())

    df.groupby("id").applyInPandas(subtract_mean, schema="id long, v double").show()
    # +---+----+
    # | id|   v|
    # +---+----+
    # |  1|-0.5|
    # |  1| 0.5|
    # |  2|-3.0|
    # |  2|-1.0|
    # |  2| 4.0|
    # +---+----+


def map_in_pandas_example(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

    def filter_func(iterator: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
        for pdf in iterator:
            yield pdf[pdf.id == 1]

    df.mapInPandas(filter_func, schema=df.schema).show()
    # +---+---+
    # | id|age|
    # +---+---+
    # |  1| 21|
    # +---+---+


def cogrouped_apply_in_pandas_example(spark: SparkSession) -> None:
    import pandas as pd

    df1 = spark.createDataFrame(
        [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
        ("time", "id", "v1"))

    df2 = spark.createDataFrame(
        [(20000101, 1, "x"), (20000101, 2, "y")],
        ("time", "id", "v2"))

    def merge_ordered(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
        return pd.merge_ordered(left, right)

    df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
        merge_ordered, schema="time int, id int, v1 double, v2 string").show()
    # +--------+---+---+----+
    # |    time| id| v1|  v2|
    # +--------+---+---+----+
    # |20000101|  1|1.0|   x|
    # |20000102|  1|3.0|null|
    # |20000101|  2|2.0|   y|
    # |20000102|  2|4.0|null|
    # +--------+---+---+----+


def arrow_python_udf_example(spark: SparkSession) -> None:
    from pyspark.sql.functions import udf

    @udf(returnType='int')  # A default, pickled Python UDF
    def slen(s):  # type: ignore[no-untyped-def]
        return len(s)

    @udf(returnType='int', useArrow=True)  # An Arrow Python UDF
    def arrow_slen(s):  # type: ignore[no-untyped-def]
        return len(s)

    df = spark.createDataFrame([(1, "John Doe", 21)], ("id", "name", "age"))

    df.select(slen("name"), arrow_slen("name")).show()
    # +----------+----------------+
    # |slen(name)|arrow_slen(name)|
    # +----------+----------------+
    # |         8|               8|
    # +----------+----------------+


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark example") \
        .getOrCreate()

    print("Running Arrow conversion example: DataFrame to Table")
    dataframe_to_from_arrow_table_example(spark)
    print("Running Pandas to/from conversion example")
    dataframe_with_arrow_example(spark)
    print("Running pandas_udf example: Series to Frame")
    ser_to_frame_pandas_udf_example(spark)
    print("Running pandas_udf example: Series to Series")
    ser_to_ser_pandas_udf_example(spark)
    print("Running pandas_udf example: Iterator of Series to Iterator of Series")
    iter_ser_to_iter_ser_pandas_udf_example(spark)
    print("Running pandas_udf example: Iterator of Multiple Series to Iterator of Series")
    iter_sers_to_iter_ser_pandas_udf_example(spark)
    print("Running pandas_udf example: Series to Scalar")
    ser_to_scalar_pandas_udf_example(spark)
    print("Running pandas function example: Grouped Map")
    grouped_apply_in_pandas_example(spark)
    print("Running pandas function example: Map")
    map_in_pandas_example(spark)
    print("Running pandas function example: Co-grouped Map")
    cogrouped_apply_in_pandas_example(spark)

    spark.stop()
