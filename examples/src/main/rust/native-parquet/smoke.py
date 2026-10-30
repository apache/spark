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

"""Run with Java 22+ and local spark-submit from a build that includes PR #59424.

Writes Parquet files with Spark, and checks that rust_sdk_parquet reads what Spark's own
Parquet reader reads, with and without its row filter.
"""

import argparse
from pathlib import Path
import re
import tempfile

from pyspark.sql import SparkSession

# The columns cover the types that Spark writes by default, including INT96 timestamps, decimals
# stored as INT32, INT64 and fixed-length binary, nulls, and floats that Spark compares
# differently from Arrow, which the source compares as Spark does: NaN, -0.0, and 0.0.
COLUMNS = [
    "id",
    "cast(id % 10 as int) as k",
    "cast(id % 100 as tinyint) as tiny",
    "cast(id % 1000 as smallint) as small",
    "if(id % 11 = 0, null, id * 1.5) as score",
    "case id % 4 when 0 then double('NaN') when 1 then -0.0D when 2 then 0.0D else id end as odd",
    "cast(id as float) / 3 as ratio",
    "case id % 4 when 0 then float('NaN') when 1 then cast(-0.0D as float) "
    "when 2 then cast(0.0D as float) else cast(id as float) end as single",
    "if(id % 7 = 0, null, concat('row-', id)) as label",
    "id % 3 = 0 as flag",
    "cast((id - 10000) / 100 as decimal(7, 2)) as price",
    "cast((id - 10000) / 100 as decimal(12, 2)) as amount",
    "cast(id - 10000 as decimal(38, 2)) * 1000000000000 as big",
    "date_add(date'1999-12-25', cast(id % 4000 as int)) as day",
    "timestamp_micros(id * 3600000123 - 86400000000000) as ts",
    "cast(timestamp_micros(id * 1000003) as timestamp_ntz) as ntz",
    "cast(concat('b', id % 5) as binary) as bytes",
    "array(id, if(id % 2 = 0, null, id + 1)) as numbers",
    "named_struct('x', cast(id as int), 'y', if(id % 7 = 0, null, concat('p', id))) as point",
    "map('a', id, 'b', if(id % 5 = 0, null, id * 2)) as attrs",
]

# Queries over a view `t`, with predicates that the source evaluates and ones that it leaves
# to Spark.
QUERIES = [
    "SELECT * FROM t",
    "SELECT label, id FROM t",
    "SELECT * FROM t WHERE k = 3",
    "SELECT id FROM t WHERE k <> 3 AND id < 100",
    "SELECT id, tiny, small FROM t WHERE tiny >= 50 AND small < 20",
    "SELECT id FROM t WHERE 5 > k",
    "SELECT id FROM t WHERE label LIKE 'row-1%'",
    "SELECT id FROM t WHERE label LIKE '%99'",
    "SELECT id FROM t WHERE label LIKE '%23%'",
    "SELECT id FROM t WHERE label IS NULL",
    "SELECT id FROM t WHERE label IS NOT NULL AND k IN (1, 2)",
    "SELECT id FROM t WHERE k IN (1, NULL)",
    "SELECT id FROM t WHERE NOT (k IN (1, NULL))",
    "SELECT id FROM t WHERE NOT (k IN (1, 2) OR label = 'row-5')",
    "SELECT id FROM t WHERE label <=> 'row-8'",
    "SELECT id FROM t WHERE flag",
    "SELECT id FROM t WHERE NOT flag",
    "SELECT id, price FROM t WHERE price <= -50.25",
    "SELECT id, amount FROM t WHERE amount > 50.25",
    "SELECT id, big FROM t WHERE big < -100",
    "SELECT id, day FROM t WHERE day BETWEEN date'2000-01-01' AND date'2000-12-31'",
    "SELECT id, ts FROM t WHERE ts >= timestamp'1970-01-01 00:00:00' "
    "AND ts < timestamp'1970-06-01 00:00:00'",
    "SELECT id, ntz FROM t WHERE ntz < timestamp_ntz'1970-01-01 00:00:10'",
    "SELECT id FROM t WHERE bytes = X'6233'",
    "SELECT id FROM t WHERE score > 100",
    "SELECT id FROM t WHERE odd = 0.0",
    "SELECT id FROM t WHERE odd > 1e300",
    "SELECT id FROM t WHERE odd < 0",
    "SELECT id FROM t WHERE odd <=> -0.0D",
    "SELECT id FROM t WHERE NOT (odd <= 2 OR odd IN (1e300, 3))",
    "SELECT id, single FROM t WHERE single = 0.0",
    "SELECT id FROM t WHERE single > 3.5 AND single < 100",
    "SELECT id FROM t WHERE single >= float('NaN')",
    "SELECT id FROM t WHERE single IN (cast(-0.0D as float), 7)",
    "SELECT id FROM t WHERE point.x = 5",
    "SELECT id FROM t WHERE array_contains(numbers, 5)",
    "SELECT id FROM t WHERE attrs['b'] IS NULL",
    "SELECT count(*) FROM t",
    "SELECT count(*) FROM t WHERE k = 3",
    "SELECT k, count(*), sum(id), max(label) FROM t GROUP BY k",
]


def rows(df):
    # repr keeps NaN comparable.
    return sorted(repr(row) for row in df.collect())


def pushed_predicates(df):
    # The formatted plan shows the metadata of the scan, except empty values.
    plan = df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")
    pushed = re.search(r"PushedPredicates: (\[.*\])", plan)
    return pushed.group(1) if pushed else "[]"


def check_reads(spark, path, read):
    spark.read.parquet(str(path)).createOrReplaceTempView("t")
    expected = {query: rows(spark.sql(query)) for query in QUERIES}
    for options in [{}, {"rowFilter": "false"}]:
        read(options).createOrReplaceTempView("t")
        for query in QUERIES:
            assert rows(spark.sql(query)) == expected[query], (options, query)
        pushed = pushed_predicates(spark.sql("SELECT id FROM t WHERE k = 3 AND odd < 0"))
        if options:
            assert pushed == "[]", pushed
        else:
            assert "k = 3" in pushed and "odd < 0" in pushed, pushed
    limited = spark.sql("SELECT id FROM t LIMIT 10").collect()
    assert len(limited) == 10, limited


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    args = parser.parse_args()
    package = args.package.resolve(strict=True)

    spark = (
        SparkSession.builder.appName("Rust native Parquet source smoke")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        if not spark.sparkContext.master.startswith("local"):
            raise ValueError(
                "This smoke script uses driver-local temporary files; use --master local"
            )
        spark.addArtifact(str(package))
        data = spark.range(0, 20000, 1, 4).selectExpr(*COLUMNS)
        with tempfile.TemporaryDirectory(prefix="rust-native-parquet-") as directory:
            base = Path(directory)

            def native(path):
                def read(options):
                    reader = spark.read.format("rust_sdk_parquet")
                    for key, value in options.items():
                        reader = reader.option(key, value)
                    return reader.load(str(path))

                return read

            # Spark writes timestamps as INT96 by default.
            int96 = base / "int96"
            data.write.parquet(str(int96))
            check_reads(spark, int96, native(int96))

            # Small row groups and splits give each task several row groups, or none.
            groups = base / "groups"
            data.write.option("parquet.block.size", 16384).parquet(str(groups))
            read_groups = native(groups)
            check_reads(
                spark,
                groups,
                lambda options: read_groups({**options, "maxPartitionBytes": "65536"}),
            )

            micros = base / "micros"
            spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            data.coalesce(1).write.parquet(str(micros))
            single = next(micros.glob("*.parquet"))
            check_reads(spark, single, native(single))

            millis = base / "millis"
            spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
            data.select("id", "ts").write.parquet(str(millis))
            try:
                native(millis)({}).collect()
            except Exception as error:
                assert "The column ts contains the Arrow type" in str(error), error
            else:
                raise AssertionError("Millisecond timestamps should be rejected")
        print("Rust native Parquet source Spark smoke checks passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
