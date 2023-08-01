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

# NOTE that this file is imported in user guide in PySpark documentation.
# The codes are referred via line numbers. See also `literalinclude` directive in Sphinx.
import pandas as pd
from typing import Iterator, Any

from pyspark.sql import SparkSession
from pyspark.sql.pandas.utils import require_minimum_pandas_version, require_minimum_pyarrow_version

# Python UDTFs use Arrow by default.
require_minimum_pandas_version()
require_minimum_pyarrow_version()


def python_udtf_simple_example(spark: SparkSession):

    from pyspark.sql.functions import lit, udtf

    class SimpleUDTF:
        def eval(self, x: int, y: int):
            yield x + y, x - y

    # Now, create a Python UDTF using the defined class and specify a return type
    func = udtf(SimpleUDTF, returnType="c1: int, c2: int")

    func(lit(1), lit(2)).show()
    # +---+---+
    # | c1| c2|
    # +---+---+
    # |  3| -1|
    # +---+---+


def python_udtf_registration(spark: SparkSession):

    from pyspark.sql.functions import udtf

    # Use the decorator to define the UDTF.
    @udtf(returnType="c1: int, c2: int")
    class PlusOne:
        def eval(self, x: int):
            yield x, x + 1

    # Register the UDTF
    spark.udtf.register("plus_one", PlusOne)
    
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


def python_udtf_terminate_example(spark: SparkSession):

    from pyspark.sql.functions import udtf

    @udtf(returnType="cnt: int")
    class CountUDTF:
        def __init__(self):
            self.count = 0

        def eval(self, x):
            self.count += 1
    
        def terminate(self):
            yield self.count,


    spark.udtf.register("count_udtf", CountUDTF)
    spark.sql("SELECT * FROM range(0, 10, 1, 1), LATERAL count_udtf(id)")
    # +---+---+
    # | id|cnt|
    # +---+---+
    # |  9| 10|
    # +---+---+


def python_udtf_calendar_example(spark: SparkSession):
    
    import datetime
    from pyspark.sql.functions import udtf

    @udtf(returnType="date: string, year: int, month: int, day: int, day_of_week: string")
    class Calendar:
        def eval(self, start_date: str, end_date: str):
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
            delta = end_date - start_date
            dates = [start_date + datetime.timedelta(days=i) for i in range(delta.days + 1)]
            for date in dates:
                date_str = date.strftime("%Y-%m-%d")
                yield (
                    date.strftime("%Y-%m-%d"),
                    date.year,
                    date.month,
                    date.day,
                    date.strftime("%A"),
                )

    Calendar(lit("2023-01-01"), lit("2023-01-10")).show()
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


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python UDTF example") \
        .getOrCreate()

    print("Running simple Python UDTF example")
    python_udtf_simple_example(spark)

    print("Running Python UDTF registration example")
    python_udtf_registration(spark)

    print("Running Python UDTF terminate example")
    python_udtf_terminate_example(spark)

    print("Running Python UDTF calendar example")
    python_udtf_calendar_example(spark)

    spark.stop()
