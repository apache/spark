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
A simple example demonstrating Python UDTFs in Spark
Run with:
  ./bin/spark-submit examples/src/main/python/sql/udtf.py
"""

# NOTE that this file is imported in the User Guides in PySpark documentation.
# The codes are referred via line numbers. See also `literalinclude` directive in Sphinx.
from pyspark.sql import SparkSession
from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version

# Python UDTFs use Arrow by default.
require_minimum_pandas_version()
require_minimum_pyarrow_version()


def python_udtf_simple_example(spark: SparkSession) -> None:

    # Define the UDTF class and implement the required `eval` method.
    class SquareNumbers:
        def eval(self, start: int, end: int):
            for num in range(start, end + 1):
                yield (num, num * num)

    from pyspark.sql.functions import udtf

    # Create a UDTF using the class definition and the `udtf` function.
    square_num = udtf(SquareNumbers, returnType="num: int, squared: int")

    # Invoke the UDTF in PySpark.
    square_num(1, 3).show()
    # +---+-------+
    # |num|squared|
    # +---+-------+
    # |  1|      1|
    # |  2|      4|
    # |  3|      9|
    # +---+-------+


def python_udtf_decorator_example(spark: SparkSession) -> None:

    from pyspark.sql.functions import udtf

    # Define a UDTF using the `udtf` decorator directly on the class.
    @udtf(returnType="num: int, squared: int")
    class SquareNumbers:
        def eval(self, start: int, end: int):
            for num in range(start, end + 1):
                yield (num, num * num)

    # Invoke the UDTF in PySpark using the SquareNumbers class directly.
    SquareNumbers(1, 3).show()
    # +---+-------+
    # |num|squared|
    # +---+-------+
    # |  1|      1|
    # |  2|      4|
    # |  3|      9|
    # +---+-------+


def python_udtf_registration(spark: SparkSession) -> None:

    from pyspark.sql.functions import udtf

    @udtf(returnType="word: string")
    class WordSplitter:
        def eval(self, text: str):
            for word in text.split(" "):
                yield (word.strip(),)

    # Register the UDTF for use in Spark SQL.
    spark.udtf.register("split_words", WordSplitter)

    # Example: Using the UDTF in SQL.
    spark.sql("SELECT * FROM split_words('hello world')").show()
    # +-----+
    # | word|
    # +-----+
    # |hello|
    # |world|
    # +-----+

    # Example: Using the UDTF with a lateral join in SQL.
    # The lateral join allows us to reference the columns and aliases
    # in the previous FROM clause items as inputs to the UDTF.
    spark.sql(
        "SELECT * FROM VALUES ('Hello World'), ('Apache Spark') t(text), "
        "LATERAL split_words(text)"
    ).show()
    # +------------+------+
    # |        text|  word|
    # +------------+------+
    # | Hello World| Hello|
    # | Hello World| World|
    # |Apache Spark|Apache|
    # |Apache Spark| Spark|
    # +------------+------+


def python_udtf_arrow_example(spark: SparkSession) -> None:

    from pyspark.sql.functions import udtf

    @udtf(returnType="c1: int, c2: int", useArrow=True)
    class PlusOne:
        def eval(self, x: int):
            yield x, x + 1


def python_udtf_date_expander_example(spark: SparkSession) -> None:

    from datetime import datetime, timedelta
    from pyspark.sql.functions import udtf

    @udtf(returnType="date: string")
    class DateExpander:
        def eval(self, start_date: str, end_date: str):
            current = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            while current <= end:
                yield (current.strftime('%Y-%m-%d'),)
                current += timedelta(days=1)

    DateExpander("2023-02-25", "2023-03-01").show()
    # +----------+
    # |      date|
    # +----------+
    # |2023-02-25|
    # |2023-02-26|
    # |2023-02-27|
    # |2023-02-28|
    # |2023-03-01|
    # +----------+


def python_udtf_terminate_example(spark: SparkSession) -> None:

    from pyspark.sql.functions import udtf

    @udtf(returnType="cnt: int")
    class CountUDTF:
        def __init__(self):
            # Initialize the counter to 0 when an instance of the class is created.
            self.count = 0

        def eval(self, x: int):
            # Increment the counter by 1 for each input value received.
            self.count += 1

        def terminate(self):
            # Yield the final count when the UDTF is done processing.
            yield self.count,

    spark.udtf.register("count_udtf", CountUDTF)
    spark.sql("SELECT * FROM range(0, 10, 1, 1), LATERAL count_udtf(id)").show()
    # +---+---+
    # | id|cnt|
    # +---+---+
    # |  9| 10|
    # +---+---+
    spark.sql("SELECT * FROM range(0, 10, 1, 2), LATERAL count_udtf(id)").show()
    # +---+---+
    # | id|cnt|
    # +---+---+
    # |  4|  5|
    # |  9|  5|
    # +---+---+


def python_udtf_table_argument(spark: SparkSession) -> None:

    from pyspark.sql.functions import udtf
    from pyspark.sql.types import Row

    @udtf(returnType="id: int")
    class FilterUDTF:
        def eval(self, row: Row):
            if row["id"] > 5:
                yield row["id"],

    spark.udtf.register("filter_udtf", FilterUDTF)

    spark.sql("SELECT * FROM filter_udtf(TABLE(SELECT * FROM range(10)))").show()
    # +---+
    # | id|
    # +---+
    # |  6|
    # |  7|
    # |  8|
    # |  9|
    # +---+


def python_udtf_table_argument_with_partitioning(spark: SparkSession) -> None:

    from pyspark.sql.functions import udtf
    from pyspark.sql.types import Row

    # Define and register a UDTF.
    @udtf(returnType="a: string, b: int")
    class FilterUDTF:
        def __init__(self):
            self.key = ""
            self.max = 0

        def eval(self, row: Row):
            self.key = row["a"]
            self.max = max(self.max, row["b"])

        def terminate(self):
            yield self.key, self.max

    spark.udtf.register("filter_udtf", FilterUDTF)

    # Create an input table with some example values.
    spark.sql("DROP TABLE IF EXISTS values_table")
    spark.sql("CREATE TABLE values_table (a STRING, b INT)")
    spark.sql("INSERT INTO values_table VALUES ('abc', 2), ('abc', 4), ('def', 6), ('def', 8)")
    spark.table("values_table").show()
    # +-------+----+
    # |     a |  b |
    # +-------+----+
    # | "abc" | 2  |
    # | "abc" | 4  |
    # | "def" | 6  |
    # | "def" | 8  |
    # +-------+----+

    # Query the UDTF with the input table as an argument, and a directive to partition the input
    # rows such that all rows with each unique value of the `a` column are processed by the same
    # instance of the UDTF class. Within each partition, the rows are ordered by the `b` column.
    spark.sql("""
        SELECT * FROM filter_udtf(TABLE(values_table) PARTITION BY a ORDER BY b) ORDER BY 1
        """).show()
    # +-------+----+
    # |     a |  b |
    # +-------+----+
    # | "abc" | 4  |
    # | "def" | 8  |
    # +-------+----+

    # Query the UDTF with the input table as an argument, and a directive to partition the input
    # rows such that all rows with each unique result of evaluating the "LENGTH(a)" expression are
    # processed by the same instance of the UDTF class. Within each partition, the rows are ordered
    # by the `b` column.
    spark.sql("""
        SELECT * FROM filter_udtf(TABLE(values_table) PARTITION BY LENGTH(a) ORDER BY b) ORDER BY 1
        """).show()
    # +-------+---+
    # |     a | b |
    # +-------+---+
    # | "def" | 8 |
    # +-------+---+

    # Query the UDTF with the input table as an argument, and a directive to consider all the input
    # rows in one single partition such that exactly once instance of the UDTF class consumes all of
    # the input rows. Within each partition, the rows are ordered by the `b` column.
    spark.sql("""
        SELECT * FROM filter_udtf(TABLE(values_table) WITH SINGLE PARTITION ORDER BY b) ORDER BY 1
        """).show()
    # +-------+----+
    # |     a |  b |
    # +-------+----+
    # | "def" | 8 |
    # +-------+----+

    # Clean up.
    spark.sql("DROP TABLE values_table")


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python UDTF example") \
        .getOrCreate()

    print("Running Python UDTF single example")
    python_udtf_simple_example(spark)

    print("Running Python UDTF decorator example")
    python_udtf_decorator_example(spark)

    print("Running Python UDTF registration example")
    python_udtf_registration(spark)

    print("Running Python UDTF arrow example")
    python_udtf_arrow_example(spark)

    print("Running Python UDTF date expander example")
    python_udtf_date_expander_example(spark)

    print("Running Python UDTF terminate example")
    python_udtf_terminate_example(spark)

    print("Running Python UDTF table argument example")
    python_udtf_table_argument(spark)

    print("Running Python UDTF table argument with partitioning example")
    python_udtf_table_argument_with_partitioning(spark)

    spark.stop()
