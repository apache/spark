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

import time
from contextlib import contextmanager
from typing import Callable, Iterator

import pyspark.pandas as ps
from pyspark.pandas.frame import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


RUNS = 3
ROW_COUNTS = [1_000_000, 5_000_000, 10_000_000]


def make_psdf(spark: SparkSession, rows: int) -> ps.DataFrame:
    sdf = spark.range(rows).select(
        F.rand(seed=11).alias("a"),
        F.rand(seed=17).alias("b"),
        F.rand(seed=23).alias("c"),
    )
    return sdf.pandas_api()


def materialize(psdf: ps.DataFrame) -> None:
    psdf.to_spark().count()


def time_average(operation: Callable[[], ps.DataFrame]) -> float:
    durations = []
    for _ in range(RUNS):
        started = time.perf_counter()
        materialize(operation())
        durations.append(time.perf_counter() - started)
    return sum(durations) / len(durations)


@contextmanager
def old_diff_path() -> Iterator[None]:
    original_diff = DataFrame.diff

    def diff_via_window(self: DataFrame, periods: int = 1, axis: object = 0) -> DataFrame:
        if axis not in (0, "index"):
            raise NotImplementedError('axis should be either 0 or "index" currently.')
        if not isinstance(periods, int):
            raise TypeError(
                "periods should be an int; however, got [%s]" % type(periods).__name__
            )
        return self._apply_series_op(lambda psser: psser._diff(periods), should_resolve=True)

    DataFrame.diff = diff_via_window
    try:
        yield
    finally:
        DataFrame.diff = original_diff


def main() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("pandas-on-Spark diff benchmark")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(
        "Note: the old path triggers a full shuffle via an unpartitioned Window "
        "and may OOM on large datasets."
    )
    print("rows | old_wall_s | new_wall_s | speedup")
    print("-----|------------|------------|--------")

    for rows in ROW_COUNTS:
        psdf = make_psdf(spark, rows).spark.cache()
        materialize(psdf)

        with old_diff_path():
            old_wall_s = time_average(lambda: psdf.diff())

        new_wall_s = time_average(lambda: psdf.diff())
        speedup = old_wall_s / new_wall_s if new_wall_s else float("inf")
        print(f"{rows:,} | {old_wall_s:.3f} | {new_wall_s:.3f} | {speedup:.2f}x")

        psdf.spark.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
