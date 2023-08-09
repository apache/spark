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
from typing import Iterator, Any

from pyspark.sql import SparkSession
from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version

# Python UDTFs use Arrow by default.
require_minimum_pandas_version()
require_minimum_pyarrow_version()


def python_udtf_simple_example(spark: SparkSession) -> None:

    class SimpleUDTF:
        def eval(self, x: int, y: int) -> Iterator[Any]:
            yield x + y, x - y

    from pyspark.sql.functions import lit, udtf

    func = udtf(SimpleUDTF, returnType="c1: int, c2: int")

    func(lit(1), lit(2)).show()  # type: ignore
    # +---+---+
    # | c1| c2|
    # +---+---+
    # |  3| -1|
    # +---+---+


def python_udtf_decorator_example(spark: SparkSession) -> None:

    from pyspark.sql.functions import lit, udtf

    @udtf(returnType="c1: int, c2: int")  # type: ignore
    class SimpleUDTF:
        def eval(self, x: int, y: int) -> Iterator[Any]:
            yield x + y, x - y

    SimpleUDTF(lit(1), lit(2)).show()  # type: ignore
    # +---+---+
    # | c1| c2|
    # +---+---+
    # |  3| -1|
    # +---+---+


def python_udtf_registration(spark: SparkSession) -> None:

    from pyspark.sql.functions import udtf

    @udtf(returnType="c1: int, c2: int")  # type: ignore
    class PlusOne:
        def eval(self, x: int) -> Iterator[Any]:
            yield x, x + 1

    # Register the UDTF
    spark.udtf.register("plus_one", PlusOne)  # type: ignore

    # Use the UDTF in SQL
    spark.sql("SELECT * FROM plus_one(1)").show()
    # +---+---+
    # | c1| c2|
    # +---+---+
    # |  1|  2|
    # +---+---+

    # Use the UDTF in SQL with lateral join
    spark.sql("SELECT * FROM VALUES (0, 1), (1, 2) t(x, y), LATERAL plus_one(x)").show()
    # +---+---+---+---+
    # |  x|  y| c1| c2|
    # +---+---+---+---+
    # |  0|  1|  0|  1|
    # |  1|  2|  1|  2|
    # +---+---+---+---+


def python_udtf_arrow_example(spark: SparkSession) -> None:

    from pyspark.sql.functions import udtf

    @udtf(returnType="c1: int, c2: int", useArrow=True)  # type: ignore
    class PlusOne:
        def eval(self, x: int) -> Iterator[Any]:
            yield x, x + 1


def python_udtf_terminate_example(spark: SparkSession) -> None:

    from pyspark.sql.functions import udtf

    @udtf(returnType="cnt: int")  # type: ignore
    class CountUDTF:
        def __init__(self) -> None:
            self.count = 0

        def eval(self, x: int) -> None:
            self.count += 1

        def terminate(self) -> Iterator[Any]:
            yield self.count,

    spark.udtf.register("count_udtf", CountUDTF)  # type: ignore
    spark.sql("SELECT * FROM range(0, 10, 1, 1), LATERAL count_udtf(id)").show()
    # +---+---+
    # | id|cnt|
    # +---+---+
    # |  9| 10|
    # +---+---+


def python_udtf_calendar_example(spark: SparkSession) -> None:

    import datetime
    from pyspark.sql.functions import lit, udtf

    @udtf(returnType=(  # type: ignore
        "date: string, year: int, month: int, "
        "day: int, day_of_week: string"
    ))
    class Calendar:
        def eval(self, start_date: str, end_date: str) -> Iterator[Any]:
            start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
            delta = end - start
            dates = [start + datetime.timedelta(days=i) for i in range(delta.days + 1)]
            for date in dates:
                yield (
                    date.strftime("%Y-%m-%d"),
                    date.year,
                    date.month,
                    date.day,
                    date.strftime("%A"),
                )

    Calendar(lit("2023-01-01"), lit("2023-01-10")).show()  # type: ignore
    # +----------+----+-----+---+-----------+
    # |      date|year|month|day|day_of_week|
    # +----------+----+-----+---+-----------+
    # |2023-01-01|2023|    1|  1|     Sunday|
    # |2023-01-02|2023|    1|  2|     Monday|
    # |2023-01-03|2023|    1|  3|    Tuesday|
    # |2023-01-04|2023|    1|  4|  Wednesday|
    # |2023-01-05|2023|    1|  5|   Thursday|
    # |2023-01-06|2023|    1|  6|     Friday|
    # |2023-01-07|2023|    1|  7|   Saturday|
    # |2023-01-08|2023|    1|  8|     Sunday|
    # |2023-01-09|2023|    1|  9|     Monday|
    # |2023-01-10|2023|    1| 10|    Tuesday|
    # +----------+----+-----+---+-----------+


def python_udtf_table_argument(spark: SparkSession) -> None:

    from pyspark.sql.functions import udtf
    from pyspark.sql.types import Row

    @udtf(returnType="id: int")  # type: ignore
    class FilterUDTF:
        def eval(self, row: Row) -> Iterator[Any]:
            if row["id"] > 5:
                yield row["id"],

    spark.udtf.register("filter_udtf", FilterUDTF)  # type: ignore

    spark.sql("SELECT * FROM filter_udtf(TABLE(SELECT * FROM range(10)))").show()
    # +---+
    # | id|
    # +---+
    # |  6|
    # |  7|
    # |  8|
    # |  9|
    # +---+


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python UDTF example") \
        .getOrCreate()

    print("Running simple Python UDTF example")
    python_udtf_simple_example(spark)

    print("Running simple Python UDTF decorator example")
    python_udtf_decorator_example(spark)

    print("Running Python UDTF registration example")
    python_udtf_registration(spark)

    print("Running Python UDTF arrow example")
    python_udtf_arrow_example(spark)

    print("Running Python UDTF terminate example")
    python_udtf_terminate_example(spark)

    print("Running Python UDTF calendar example")
    python_udtf_calendar_example(spark)

    print("Running Python UDTF table argument example")
    python_udtf_table_argument(spark)

    spark.stop()
