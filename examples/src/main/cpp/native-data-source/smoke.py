#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run with Java 22+ and spark-submit from a distribution that includes PR #59424."""

import argparse
from pathlib import Path
import tempfile

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    args = parser.parse_args()
    package = args.package.resolve(strict=True)

    spark = (
        SparkSession.builder.appName("C++ native data source smoke")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    try:
        spark.addArtifact(str(package))
        df = (
            spark.read.format("cpp_sdk_range")
            .option("start", 0)
            .option("end", 31)
            .option("numpartitions", 4)
            .option("batchsize", 3)
            .load()
        )
        assert [tuple(row) for row in df.orderBy("id").collect()] == [
            (i, f"row-{i}") for i in range(31)
        ]

        # The native framework pushes limits only for scans without residual filters.
        unfiltered_limit = df.limit(4).collect()
        assert len(unfiltered_limit) == 4
        assert all(0 <= row.id < 31 for row in unfiltered_limit)

        projected = df.where((F.col("id") >= 7) & (F.col("id") < 17)).select("label", "id")
        assert [tuple(row) for row in projected.orderBy("id").collect()] == [
            (f"row-{i}", i) for i in range(7, 17)
        ]
        limited = df.where((F.col("id") >= 10) & (F.col("id") < 20)).limit(4).collect()
        assert len(limited) == 4
        assert all(10 <= row.id < 20 for row in limited)

        # Count needs row counts even when column pruning produces zero-column Arrow batches.
        assert df.select().count() == 31
        assert df.where(F.col("id") < 0).count() == 0

        # Spark rechecks every predicate, including those the connector evaluates exactly.
        # These expressions also exercise filters the connector does not evaluate.
        arithmetic = df.where((F.col("id") % 3) == 1).select("id").orderBy("id").collect()
        assert [row.id for row in arithmetic] == list(range(1, 31, 3))
        labels = df.where(F.col("label").endswith("3")).select("id").orderBy("id").collect()
        assert [row.id for row in labels] == [3, 13, 23]
        residual_limit = df.where(F.col("label") == "row-21").limit(1).select("id").collect()
        assert [row.id for row in residual_limit] == [21]

        # A streaming read of the fixed range reads all its rows in its first micro-batch, with
        # the reader of batch scans.
        with tempfile.TemporaryDirectory(prefix="cpp-native-range-stream-") as checkpoint:
            query = (
                spark.readStream.format("cpp_sdk_range")
                .option("start", 0)
                .option("end", 31)
                .option("numpartitions", 4)
                .option("batchsize", 3)
                .load()
                .writeStream.format("memory")
                .queryName("cpp_sdk_range_stream")
                .option("checkpointLocation", checkpoint)
                .trigger(availableNow=True)
                .start()
            )
            query.awaitTermination()
            streamed = spark.table("cpp_sdk_range_stream").orderBy("id").collect()
            assert [tuple(row) for row in streamed] == [(i, f"row-{i}") for i in range(31)]

        # Analysis must reject both modes before AQE can run the input shuffle.
        failing_input = (
            spark.range(10)
            .selectExpr(
                "if(id >= 0, raise_error('input was evaluated'), id) AS id", "'row' AS label"
            )
            .repartition(2)
        )
        for mode in ("append", "overwrite"):
            try:
                failing_input.write.format("cpp_sdk_range").mode(mode).save()
            except AnalysisException as error:
                assert error.getCondition() == "UNSUPPORTED_FEATURE.TABLE_OPERATION", str(error)
                assert error.getMessageParameters() == {
                    "tableName": "`cpp_sdk_range`",
                    "operation": "batch write",
                }, str(error)
            else:
                raise AssertionError(f"{mode} should reject this batch-read-only source")
        print("C++ native data source Spark smoke checks passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
