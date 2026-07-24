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
End-to-end benchmarks for Pickled Python UDFs vs Arrow Python UDFs.
"""

import os
from datetime import timedelta
from decimal import Decimal


def _plus_bool(v):
    return not v if v is not None else None


def _plus_num(v):
    return v + 1 if v is not None else None


def _plus_float(v):
    return float(v) + 1.0 if v is not None else None


def _plus_decimal(v):
    return v + Decimal("1.00") if v is not None else None


def _plus_string(v):
    return v + "_x" if v is not None else None


def _plus_binary(v):
    return bytes(v) + b"x" if v is not None else None


def _plus_date_or_timestamp(v):
    return v + timedelta(days=1) if v is not None else None


def _plus_timestamp(v):
    return v + timedelta(seconds=1) if v is not None else None


def _plus_array_int(v):
    return [x + 1 if x is not None else None for x in v] if v is not None else None


def _plus_array_string(v):
    return [x + "_x" if x is not None else None for x in v] if v is not None else None


def _plus_map_string_int(v):
    return {key: value + 1 for key, value in v.items()} if v is not None else None


def _plus_struct(v):
    if v is None:
        return None
    from pyspark.sql import Row

    return Row(i=v["i"] + 1, s=v["s"] + "_x", d=v["d"] + 1.0)


def _plus_array_struct(v):
    if v is None:
        return None
    from pyspark.sql import Row

    return [Row(i=x["i"] + 1, s=x["s"] + "_x") for x in v]


def _plus_struct_nested(v):
    if v is None:
        return None
    from pyspark.sql import Row

    return Row(
        id=v["id"] + 1,
        values=[x + 1 for x in v["values"]],
        attrs={key: value + "_x" for key, value in v["attrs"].items()},
    )


SCALAR_CASES = {
    "boolean": ("(id % 2) = 0", "boolean", _plus_bool),
    "byte": ("CAST(id % 100 AS TINYINT)", "tinyint", _plus_num),
    "short": ("CAST(id % 30000 AS SMALLINT)", "smallint", _plus_num),
    "integer": ("CAST(id AS INT)", "int", _plus_num),
    "long": ("id", "bigint", _plus_num),
    "float": ("CAST(id * 0.5 AS FLOAT)", "float", _plus_float),
    "double": ("CAST(id * 0.5 AS DOUBLE)", "double", _plus_float),
    "decimal": ("CAST((id % 100000) / 100.0 AS DECIMAL(20, 2))", "decimal(20,2)", _plus_decimal),
    "string": ("CONCAT('s', CAST(id AS STRING))", "string", _plus_string),
    "binary": ("ENCODE(CONCAT('s', CAST(id AS STRING)), 'UTF-8')", "binary", _plus_binary),
    "date": ("DATE_ADD(DATE'2020-01-01', CAST(id % 1000 AS INT))", "date", _plus_date_or_timestamp),
    "timestamp_ntz": (
        "TIMESTAMP_NTZ'2020-01-01 00:00:00' + CAST(id % 100000 AS INT) * INTERVAL 1 SECOND",
        "timestamp_ntz",
        _plus_timestamp,
    ),
}

ROW_COUNTS = [100_000, 200_000, 400_000, 800_000, 1_600_000, 3_200_000, 6_400_000]
END_TO_END_UDF_COUNTS = [2]
END_TO_END_WORKLOAD_ITERATIONS = [2]
END_TO_END_SCALAR_CASE_NAMES = os.environ.get(
    "PYSPARK_ASV_E2E_TYPES",
    "integer,double,decimal,string,binary,date,timestamp_ntz",
).split(",")
_CACHED_SPARK = None

TIMESTAMP_CASES = {
    "timestamp": (
        "TIMESTAMP'2020-01-01 00:00:00' + CAST(id % 100000 AS INT) * INTERVAL 1 SECOND",
        "timestamp",
        _plus_timestamp,
    )
}

NESTED_CASES = {
    "array_int": (
        "ARRAY(CAST(id AS INT), CAST(id + 1 AS INT), CAST(id + 2 AS INT))",
        "array<int>",
        _plus_array_int,
    ),
    "array_string": (
        "ARRAY(CONCAT('a', CAST(id AS STRING)), CONCAT('b', CAST(id AS STRING)))",
        "array<string>",
        _plus_array_string,
    ),
    "map_string_int": (
        "MAP('a', CAST(id AS INT), 'b', CAST(id + 1 AS INT))",
        "map<string,int>",
        _plus_map_string_int,
    ),
    "struct_mixed": (
        "NAMED_STRUCT('i', CAST(id AS INT), "
        "'s', CONCAT('s', CAST(id AS STRING)), "
        "'d', CAST(id * 0.5 AS DOUBLE))",
        "struct<i:int,s:string,d:double>",
        _plus_struct,
    ),
    "array_struct": (
        "ARRAY("
        "NAMED_STRUCT('i', CAST(id AS INT), 's', CONCAT('a', CAST(id AS STRING))), "
        "NAMED_STRUCT('i', CAST(id + 1 AS INT), 's', CONCAT('b', CAST(id AS STRING))))",
        "array<struct<i:int,s:string>>",
        _plus_array_struct,
    ),
    "struct_nested": (
        "NAMED_STRUCT("
        "'id', CAST(id AS INT), "
        "'values', ARRAY(CAST(id AS INT), CAST(id + 1 AS INT)), "
        "'attrs', MAP('k', CONCAT('v', CAST(id AS STRING))))",
        "struct<id:int,values:array<int>,attrs:map<string,string>>",
        _plus_struct_nested,
    ),
}
NESTED_CASE_NAMES = os.environ.get(
    "PYSPARK_ASV_NESTED_TYPES",
    ",".join(NESTED_CASES),
).split(",")


class _PythonUDFTypeBenchBase:
    timeout = 120

    def _spark_conf(self):
        from pyspark import SparkConf

        return (
            SparkConf()
            .setMaster(os.environ.get("PYSPARK_ASV_MASTER", "local[4]"))
            .setAppName(self.__class__.__name__)
            .set("spark.python.worker.reuse", "true")
            .set("spark.ui.enabled", "false")
            .set("spark.sql.shuffle.partitions", "4")
            .set("spark.sql.execution.pythonUDF.arrow.enabled", "true")
            .set("spark.sql.execution.pythonUDTF.arrow.enabled", "true")
            .set("spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled", "false")
        )

    def _setup_spark(self):
        from pyspark.sql import SparkSession

        self.spark = SparkSession.builder.config(conf=self._spark_conf()).getOrCreate()

    def _teardown_spark(self):
        if hasattr(self, "spark"):
            from pyspark.sql import SparkSession

            self.spark.stop()
            SparkSession.builder._options = {}

    def _setup_case(self, cases, case_name, use_arrow, n_rows):
        from pyspark.sql.functions import expr, udf

        self._setup_spark()
        input_expr, return_type, func = cases[case_name]
        self.df = self.spark.range(n_rows).select(expr(input_expr).alias("v"))
        self.df.cache()
        self.df.count()
        self.udf = udf(func, return_type, useArrow=use_arrow)

    def _run_case(self):
        from pyspark.sql.functions import col

        self.df.select(self.udf(col("v")).alias("out")).write.format("noop").mode(
            "overwrite"
        ).save()


class _CachedSparkPythonUDFTypeBenchBase(_PythonUDFTypeBenchBase):
    def _setup_spark(self):
        global _CACHED_SPARK

        if _CACHED_SPARK is None:
            from pyspark.sql import SparkSession

            _CACHED_SPARK = SparkSession.builder.config(conf=self._spark_conf()).getOrCreate()
            _CACHED_SPARK.sparkContext.setLogLevel("WARN")
        self.spark = _CACHED_SPARK

    def _teardown_spark(self):
        if hasattr(self, "spark"):
            self.spark.catalog.clearCache()


def _burn_cpu(workload_iterations, salt):
    acc = salt
    for i in range(workload_iterations):
        acc = (acc * 1_103_515_245 + i + 12_345) & 0xFFFF
    return acc


def _make_workload_func(data_type, workload_iterations, salt):
    def _workload(v):
        if v is None:
            return None

        acc = _burn_cpu(workload_iterations, salt)
        delta = acc & 1

        if data_type == "boolean":
            return not v if delta else v
        if data_type == "byte":
            return int((int(v) + delta) % 100)
        if data_type == "short":
            return int((int(v) + delta) % 30000)
        if data_type == "integer":
            return int(v) + delta
        if data_type == "long":
            return int(v) + delta
        if data_type == "float":
            return float(v) + float(delta)
        if data_type == "double":
            return float(v) + float(delta)
        if data_type == "decimal":
            return v + Decimal(delta)
        if data_type == "string":
            return v + str(delta)
        if data_type == "binary":
            return bytes(v) + bytes([delta])
        if data_type == "date":
            return v + timedelta(days=delta)
        if data_type == "timestamp_ntz":
            return v + timedelta(seconds=delta)
        raise ValueError(f"Unsupported data type: {data_type}")

    return _workload


class ScalarPythonUDFTypeBench(_PythonUDFTypeBenchBase):
    """Benchmark scalar input and output types for Python UDFs."""

    params = [list(SCALAR_CASES), [False, True], ROW_COUNTS]
    param_names = ["data_type", "use_arrow", "n_rows"]

    def setup(self, data_type, use_arrow, n_rows):
        self._setup_case(SCALAR_CASES, data_type, use_arrow, n_rows)

    def teardown(self, data_type, use_arrow, n_rows):
        self._teardown_spark()

    def time_scalar_python_udf_type(self, data_type, use_arrow, n_rows):
        self._run_case()


class ScalarPythonUDFEndToEndWorkloadBench(_CachedSparkPythonUDFTypeBenchBase):
    """Benchmark scalar Python UDFs with multiple UDFs and non-trivial work."""

    params = [
        END_TO_END_SCALAR_CASE_NAMES,
        [False, True],
        ROW_COUNTS,
        END_TO_END_UDF_COUNTS,
        END_TO_END_WORKLOAD_ITERATIONS,
    ]
    param_names = ["data_type", "use_arrow", "n_rows", "udf_count", "workload_iterations"]

    def setup(self, data_type, use_arrow, n_rows, udf_count, workload_iterations):
        from pyspark.sql.functions import expr, udf

        self._setup_spark()
        input_expr, return_type, _ = SCALAR_CASES[data_type]
        self.df = self.spark.range(n_rows).select(expr(input_expr).alias("v"))
        self.df.cache()
        self.df.count()
        self.udfs = [
            udf(
                _make_workload_func(data_type, workload_iterations, i + 1),
                return_type,
                useArrow=use_arrow,
            )
            for i in range(udf_count)
        ]

    def teardown(self, data_type, use_arrow, n_rows, udf_count, workload_iterations):
        self._teardown_spark()

    def time_scalar_python_udf_end_to_end_workload(
        self, data_type, use_arrow, n_rows, udf_count, workload_iterations
    ):
        from pyspark.sql.functions import col

        self.df.select(
            *[func(col("v")).alias(f"out_{i}") for i, func in enumerate(self.udfs)]
        ).write.format("noop").mode("overwrite").save()


class TimestampPythonUDFTypeBench(_PythonUDFTypeBenchBase):
    """Benchmark timestamp separately because timezone-aware timestamp is slow."""

    params = [list(TIMESTAMP_CASES), [False, True], ROW_COUNTS]
    param_names = ["data_type", "use_arrow", "n_rows"]

    def setup(self, data_type, use_arrow, n_rows):
        self._setup_case(TIMESTAMP_CASES, data_type, use_arrow, n_rows)

    def teardown(self, data_type, use_arrow, n_rows):
        self._teardown_spark()

    def time_timestamp_python_udf_type(self, data_type, use_arrow, n_rows):
        self._run_case()


class NestedPythonUDFTypeBench(_PythonUDFTypeBenchBase):
    """Benchmark common nested input and output types for Python UDFs."""

    params = [NESTED_CASE_NAMES, [False, True], ROW_COUNTS]
    param_names = ["data_type", "use_arrow", "n_rows"]

    def setup(self, data_type, use_arrow, n_rows):
        self._setup_case(NESTED_CASES, data_type, use_arrow, n_rows)

    def teardown(self, data_type, use_arrow, n_rows):
        self._teardown_spark()

    def time_nested_python_udf_type(self, data_type, use_arrow, n_rows):
        self._run_case()
