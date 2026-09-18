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

"""End-to-end in-process, worker Arrow, and pandas UDF benchmarks.

See README.md for the required Spark build and JEP launch environment. These
measure steady-state queries, including JVM row/Arrow conversion and Python
execution. Worker Arrow UDFs are the primary baseline and use the same Arrow
operations as in-process UDFs. The supplementary pandas baseline also includes
pandas conversion costs; neither comparison isolates IPC overhead alone.
Historical standalone-script timings are a separate baseline.
"""

from importlib.util import find_spec


class InProcessUDFTimeBench:
    # One query per sample, with explicit full-query warmup in setup.
    number = 1
    rounds = 1
    repeat = 5
    warmup_time = 0
    timeout = 300
    params = [
        ["arrow", "inprocess", "pandas"],
        [
            ("narrow", 100_000),
            ("narrow", 1_000_000),
            ("narrow", 5_000_000),
            ("wide", 1_000_000),
            ("wide", 5_000_000),
            ("wide", 10_000_000),
            ("short_string", 1_000_000),
            ("short_string", 5_000_000),
            ("short_string", 10_000_000),
            ("long_string", 500_000),
            ("long_string", 1_000_000),
            ("long_string", 2_000_000),
        ],
    ]
    param_names = ["udf_type", "workload"]

    def setup(self, udf_type, workload):
        # JEP cannot be imported from standalone CPython. Check availability
        # without loading it; broken native/JVM setup must fail, not be skipped.
        if udf_type == "inprocess" and find_spec("jep") is None:
            raise NotImplementedError("Install JEP and configure its JVM launch paths")

        import pyarrow.compute as pc
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import arrow_udf, col, lpad, pandas_udf
        from pyspark.sql.types import LongType, StringType

        use_arrow = udf_type != "pandas"
        scenario, n_rows = workload
        n_cols = 10 if scenario == "wide" else 1
        batch_size = {"narrow": 10_000, "wide": 1_000_000}.get(scenario, 100_000)
        self.spark = (
            SparkSession.builder.master("local[1]")
            .appName("InProcessUDFTimeBench")
            .config("spark.ui.enabled", "false")
            .config("spark.python.worker.reuse", "true")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", batch_size)
            .config("spark.sql.execution.arrow.maxBytesPerBatch", 128 * 1024 * 1024)
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        try:
            base = self.spark.range(n_rows, numPartitions=1)
            if scenario in ("narrow", "wide"):
                self.data = base.select(*[col("id").alias(f"c{i}") for i in range(n_cols)])
                return_type = LongType()

                def operation(*columns):
                    result = columns[0]
                    for column in columns[1:]:
                        if use_arrow:
                            result = pc.add(result, column)
                        else:
                            result = result + column
                    return result

            else:
                value = col("id").cast("string")
                if scenario == "long_string":
                    value = lpad(value, 1000, "x")
                self.data = base.select(value.alias("s"))
                return_type = StringType()

                def operation(value):
                    if scenario == "long_string":
                        return value
                    return pc.utf8_upper(value) if use_arrow else value.str.upper()

            if udf_type == "inprocess":
                from pyspark.inprocess.udf import inprocess_udf

                udf = inprocess_udf(return_type=return_type)(operation)
            elif udf_type == "arrow":
                udf = arrow_udf(return_type)(operation)
            else:
                udf = pandas_udf(return_type)(operation)
            self.data.cache()
            self.data.count()
            self.query = self.data.select(udf(*[self.data[c] for c in self.data.columns]))
            for _ in range(2):
                self.time_query(udf_type, workload)
        except Exception:
            self.teardown(udf_type, workload)
            raise

    def time_query(self, udf_type, workload):
        # count() could prune the UDF projection; noop consumes its output.
        self.query.write.format("noop").mode("overwrite").save()

    def teardown(self, udf_type, workload):
        try:
            if hasattr(self, "data"):
                self.data.unpersist(blocking=True)
        finally:
            if hasattr(self, "spark"):
                self.spark.stop()
