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

"""Run with spark-submit from a Spark distribution that includes PR #59313."""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    args = parser.parse_args()
    package = args.package.resolve(strict=True)

    spark = SparkSession.builder.appName("Rust native data source smoke").getOrCreate()
    try:
        spark.addArtifact(str(package))
        df = (
            spark.read.format("rust_sdk_range")
            .option("start", 0)
            .option("end", 31)
            .option("numpartitions", 4)
            .option("batchsize", 3)
            .load()
        )
        assert [tuple(row) for row in df.orderBy("id").collect()] == [
            (i, f"row-{i}") for i in range(31)
        ]

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

        # The connector does not evaluate these expressions; Spark must retain the filters.
        arithmetic = df.where((F.col("id") % 3) == 1).select("id").orderBy("id").collect()
        assert [row.id for row in arithmetic] == list(range(1, 31, 3))
        labels = df.where(F.col("label").endswith("3")).select("id").orderBy("id").collect()
        assert [row.id for row in labels] == [3, 13, 23]
        residual_limit = df.where(F.col("label") == "row-21").limit(1).select("id").collect()
        assert [row.id for row in residual_limit] == [21]
        print("Rust native data source Spark smoke checks passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
