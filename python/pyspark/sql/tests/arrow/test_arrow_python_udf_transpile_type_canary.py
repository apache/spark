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
A canary over every Spark data type, for UDF transpilation at each transpilable eval type.

The transpiler admits return types by an allowlist in ``python/pyspark/sql/transpile.py`` and
input types by one in ``ResolveTranspiledPythonUDFOptions.scala``, so a new data type falls
back to interpreted Python -- safely, but without anyone deciding so. This module enumerates
the concrete ``DataType`` classes ``pyspark.sql.types`` exports and demands a verdict for
each. ``_atomic_types`` is not used because it is not exhaustive.

To triage a new type, add it to ``_SAMPLES`` and run this module, which prints the observed
verdicts. Record a refusal as is. A type that TRANSPILES also needs a ``_PARITY_FIXTURES``
entry, so that it is compared with the interpreted path of every eval type.

The return-type table is written for the pickled eval type; the Arrow-backed eval types
must admit the same types except those Arrow cannot express.
"""

import unittest

from pyspark.errors import PySparkNotImplementedError
from pyspark.sql import types as T
from pyspark.sql.tests.arrow.test_arrow_python_udf_transpile import (
    _TRANSPILE_ON,
    ArrowUDFTranspileTestsMixin,
    _eval_python_nodes,
)
from pyspark.sql.udf import UserDefinedFunction
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.util import PythonEvalType, is_remote_only

TRANSPILES = "TRANSPILES"
NO_TRANSPILE = "NO_TRANSPILE"
# The return type cannot be expressed as an Arrow type at all, so an Arrow-optimized UDF
# declaring it fails in _check_return_type before transpilation is even a question.
REJECTED_BY_ARROW = "REJECTED_BY_ARROW"
# CHAR/VARCHAR are rejected at UDF construction for every eval type
# (CHAR_VARCHAR_NOT_SUPPORTED_IN_PYTHON): Python has only unconstrained strings, so there
# is no return verdict to have.
REJECTED_CHAR_VARCHAR = "REJECTED_CHAR_VARCHAR"
# The type cannot appear in a DataFrame schema, so there is no input-side verdict to have.
NOT_A_COLUMN_TYPE = "NOT_A_COLUMN_TYPE"

_PICKLED = PythonEvalType.SQL_BATCHED_UDF
_ARROW = PythonEvalType.SQL_ARROW_BATCHED_UDF
_PANDAS = PythonEvalType.SQL_SCALAR_PANDAS_UDF

# Exported DataType subclasses that are not column types.
_NOT_COLUMN_TYPES = frozenset({T.DataType, T.StructField, T.UserDefinedType})


# UDF bodies are named defs: inspect.getsource cannot recover a lambda passed as an argument.


def numeric_body(x):
    return x + 1


def string_body(x: str):
    return x


def bool_body(x: bool):
    return x


def binary_body(x: bytes):
    return x


def null_probe(x):
    # Unannotated, so the parameter is only tried as numeric and string.
    return 1 if x is None else 2


def series_null_probe(x):
    return x.isnull()


_BODIES = {
    "numeric": numeric_body,
    "string": string_body,
    "bool": bool_body,
    "binary": binary_body,
    # Refused by the return-type allowlist before the body matters.
    "other": numeric_body,
}


# (label, instance, body category whose lowering could plausibly target this return type)
_SAMPLES = (
    ("void", T.NullType(), "other"),
    ("char", T.CharType(10), "string"),
    ("string", T.StringType(), "string"),
    ("string_lcase", T.StringType("UTF8_LCASE"), "string"),
    ("varchar", T.VarcharType(10), "string"),
    ("binary", T.BinaryType(), "binary"),
    ("boolean", T.BooleanType(), "bool"),
    ("date", T.DateType(), "other"),
    ("time", T.TimeType(6), "other"),
    ("timestamp", T.TimestampType(), "other"),
    ("timestamp_ntz", T.TimestampNTZType(), "other"),
    ("timestamp_ltz_nanos", T.TimestampLTZNanosType(), "other"),
    ("timestamp_ntz_nanos", T.TimestampNTZNanosType(), "other"),
    ("decimal", T.DecimalType(10, 2), "numeric"),
    ("double", T.DoubleType(), "numeric"),
    ("float", T.FloatType(), "numeric"),
    ("tinyint", T.ByteType(), "numeric"),
    ("smallint", T.ShortType(), "numeric"),
    ("int", T.IntegerType(), "numeric"),
    ("bigint", T.LongType(), "numeric"),
    ("day_time_interval", T.DayTimeIntervalType(), "other"),
    ("year_month_interval", T.YearMonthIntervalType(), "other"),
    ("calendar_interval", T.CalendarIntervalType(), "other"),
    ("array", T.ArrayType(T.LongType()), "other"),
    ("map", T.MapType(T.StringType(), T.LongType()), "other"),
    ("struct", T.StructType([T.StructField("f", T.LongType())]), "other"),
    ("variant", T.VariantType(), "other"),
    ("geography", T.GeographyType(4326), "other"),
    ("geometry", T.GeometryType(4326), "other"),
    ("file", T.FileType(), "other"),
    ("file_external", T.FileType.external(), "other"),
)

# Verdict per RETURN type, pickled eval type, with the body category paired above. `decimal`
# is a NumericType but refused by the body-vs-return-type check.
_EXPECTED_RETURN_VERDICT = {
    "void": NO_TRANSPILE,
    "char": REJECTED_CHAR_VARCHAR,
    "string": TRANSPILES,
    "string_lcase": TRANSPILES,
    "varchar": REJECTED_CHAR_VARCHAR,
    "binary": TRANSPILES,
    "boolean": TRANSPILES,
    "date": NO_TRANSPILE,
    "time": NO_TRANSPILE,
    "timestamp": NO_TRANSPILE,
    "timestamp_ntz": NO_TRANSPILE,
    "timestamp_ltz_nanos": NO_TRANSPILE,
    "timestamp_ntz_nanos": NO_TRANSPILE,
    "decimal": NO_TRANSPILE,
    "double": TRANSPILES,
    "float": TRANSPILES,
    "tinyint": TRANSPILES,
    "smallint": TRANSPILES,
    "int": TRANSPILES,
    "bigint": TRANSPILES,
    "day_time_interval": NO_TRANSPILE,
    "year_month_interval": NO_TRANSPILE,
    "calendar_interval": NO_TRANSPILE,
    "array": NO_TRANSPILE,
    "map": NO_TRANSPILE,
    "struct": NO_TRANSPILE,
    "variant": NO_TRANSPILE,
    "geography": NO_TRANSPILE,
    "geometry": NO_TRANSPILE,
    "file": NO_TRANSPILE,
    "file_external": NO_TRANSPILE,
}

# Return types Arrow cannot express, so an Arrow-backed UDF declaring one fails at
# _check_return_type. CHAR/VARCHAR are refused earlier, for every eval type.
_ARROW_REJECTED_RETURN_TYPES = frozenset(
    {"year_month_interval", "calendar_interval", "file", "file_external"}
)

# Verdict per RETURN type for the two Arrow-backed eval types (Arrow-optimized and scalar
# pandas): the pickled table, except that the return types Arrow cannot express are rejected.
_EXPECTED_ARROW_BACKED_RETURN_VERDICT = {
    label: (REJECTED_BY_ARROW if label in _ARROW_REJECTED_RETURN_TYPES else verdict)
    for label, verdict in _EXPECTED_RETURN_VERDICT.items()
}

# Verdict per INPUT column type, with the unannotated `null_probe` body (`.isnull()` for a
# pandas UDF); the same for every eval type, since the check is on the JVM side. boolean and
# binary match no category of an unannotated parameter, and string_lcase is refused because
# 'abc' == 'ABC' under UTF8_LCASE.
_EXPECTED_INPUT_VERDICT = {
    "void": NO_TRANSPILE,
    "char": NOT_A_COLUMN_TYPE,
    "string": TRANSPILES,
    "string_lcase": NO_TRANSPILE,
    "varchar": NOT_A_COLUMN_TYPE,
    "binary": NO_TRANSPILE,
    "boolean": NO_TRANSPILE,
    "date": NO_TRANSPILE,
    "time": NO_TRANSPILE,
    "timestamp": NO_TRANSPILE,
    "timestamp_ntz": NO_TRANSPILE,
    "timestamp_ltz_nanos": NO_TRANSPILE,
    "timestamp_ntz_nanos": NO_TRANSPILE,
    "decimal": NO_TRANSPILE,
    "double": TRANSPILES,
    "float": TRANSPILES,
    "tinyint": TRANSPILES,
    "smallint": TRANSPILES,
    "int": TRANSPILES,
    "bigint": TRANSPILES,
    "day_time_interval": NO_TRANSPILE,
    "year_month_interval": NOT_A_COLUMN_TYPE,
    "calendar_interval": NOT_A_COLUMN_TYPE,
    "array": NO_TRANSPILE,
    "map": NO_TRANSPILE,
    "struct": NO_TRANSPILE,
    "variant": NO_TRANSPILE,
    "geography": NO_TRANSPILE,
    "geometry": NO_TRANSPILE,
    "file": NOT_A_COLUMN_TYPE,
    "file_external": NOT_A_COLUMN_TYPE,
}

# Parity fixture per TRANSPILES return type: an input column whose body result already has
# the declared return type, so the pickled path's NULL for a mismatched result is not hit.
_PARITY_FIXTURES = {
    "string": (T.StringType(), "ab", string_body),
    "string_lcase": (T.StringType(), "ab", string_body),
    "binary": (T.BinaryType(), b"ab", binary_body),
    "boolean": (T.BooleanType(), True, bool_body),
    "tinyint": (T.LongType(), 7, numeric_body),
    "smallint": (T.LongType(), 7, numeric_body),
    "int": (T.LongType(), 7, numeric_body),
    "bigint": (T.LongType(), 7, numeric_body),
    "double": (T.DoubleType(), 7.0, numeric_body),
    "float": (T.DoubleType(), 7.0, numeric_body),
}


def _concrete_data_type_classes():
    """Every concrete ``DataType`` subclass exported from ``pyspark.sql.types``."""
    classes = set()
    for name in T.__all__:
        obj = getattr(T, name)
        if isinstance(obj, type) and issubclass(obj, T.DataType) and obj not in _NOT_COLUMN_TYPES:
            classes.add(obj)
    return classes


@unittest.skipIf(is_remote_only(), "UDF transpilation is only supported in non-Connect Spark.")
@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,  # type: ignore[arg-type]
)
class ArrowUDFTranspileTypeCanaryTests(ArrowUDFTranspileTestsMixin, ReusedSQLTestCase):
    """Every Spark data type is either refused by the transpiler or verified against it."""

    def test_data_type_inventory_is_complete(self):
        sampled = {type(dt) for _label, dt, _cat in _SAMPLES}
        missing = sorted(cls.__name__ for cls in _concrete_data_type_classes() - sampled)
        self.assertEqual(
            [],
            missing,
            f"pyspark.sql.types now exports {missing}, which this canary has never "
            "considered. Add an instance to _SAMPLES and triage it as this module's "
            "docstring describes.",
        )

    def test_sample_tables_are_complete(self):
        labels = {label for label, _dt, _cat in _SAMPLES}
        self.assertEqual(labels, set(_EXPECTED_RETURN_VERDICT))
        self.assertEqual(labels, set(_EXPECTED_INPUT_VERDICT))
        self.assertLessEqual(_ARROW_REJECTED_RETURN_TYPES, labels)
        transpiling = {
            label for label, verdict in _EXPECTED_RETURN_VERDICT.items() if verdict == TRANSPILES
        }
        self.assertEqual(transpiling, set(_PARITY_FIXTURES))

    def _return_verdict(self, dt, body, eval_type):
        try:
            pudf = UserDefinedFunction(body, dt, evalType=eval_type)
            _ = pudf.returnType  # runs _check_return_type
        except Exception as e:  # classified below
            if (
                isinstance(e, PySparkNotImplementedError)
                and e.getCondition() == "CHAR_VARCHAR_NOT_SUPPORTED_IN_PYTHON"
            ):
                return REJECTED_CHAR_VARCHAR
            if any(
                message in str(e)
                for message in (
                    "Invalid return type with Arrow-optimized Python UDF",
                    "Invalid return type with scalar Pandas UDFs",
                )
            ):
                return REJECTED_BY_ARROW
            raise
        return TRANSPILES if pudf.transpiled else NO_TRANSPILE

    def _assert_verdicts(self, expected, observed, table_name):
        """Compare verdict maps, showing the observed table in copy-pasteable form."""
        if expected == observed:
            return
        changed = {
            label: (expected.get(label), observed.get(label))
            for label in sorted(set(expected) | set(observed))
            if expected.get(label) != observed.get(label)
        }
        rendered = "\n".join(f'    "{label}": {verdict},' for label, verdict in observed.items())
        self.fail(
            f"{table_name} disagrees with what the transpiler now does.\n"
            f"changed (label: expected -> observed): {changed}\n"
            f"observed table:\n{rendered}"
        )

    def test_return_type_verdicts(self):
        # Every eval type admits the same return types, except those Arrow cannot express.
        for eval_type, table_name, expected in (
            (_PICKLED, "_EXPECTED_RETURN_VERDICT", _EXPECTED_RETURN_VERDICT),
            (
                _ARROW,
                "_EXPECTED_ARROW_BACKED_RETURN_VERDICT",
                _EXPECTED_ARROW_BACKED_RETURN_VERDICT,
            ),
            (
                _PANDAS,
                "_EXPECTED_ARROW_BACKED_RETURN_VERDICT",
                _EXPECTED_ARROW_BACKED_RETURN_VERDICT,
            ),
        ):
            with self.subTest(evalType=eval_type), self.sql_conf(_TRANSPILE_ON):
                observed = {
                    label: self._return_verdict(dt, _BODIES[cat], eval_type)
                    for label, dt, cat in _SAMPLES
                }
                self._assert_verdicts(expected, observed, table_name)

    def _null_row_df(self, dt):
        schema = T.StructType([T.StructField("a", dt)])
        return self.spark.createDataFrame([(None,)], schema=schema)

    def _input_verdict(self, dt, eval_type):
        try:
            df = self._null_row_df(dt)
            df.count()
        except Exception:  # the type simply cannot be a column
            return NOT_A_COLUMN_TYPE
        body = series_null_probe if eval_type == _PANDAS else null_probe
        return_type = T.BooleanType() if eval_type == _PANDAS else T.LongType()
        pudf = UserDefinedFunction(body, return_type, evalType=eval_type)
        # Plan only: an option that survives JVM-side pruning leaves no Python-eval operator.
        counts = _eval_python_nodes(df.select(pudf("a")))
        return NO_TRANSPILE if sum(counts.values()) else TRANSPILES

    def test_input_type_verdicts(self):
        for eval_type in (_PICKLED, _ARROW, _PANDAS):
            with self.subTest(evalType=eval_type), self.sql_conf(_TRANSPILE_ON):
                observed = {
                    label: self._input_verdict(dt, eval_type) for label, dt, _cat in _SAMPLES
                }
                self._assert_verdicts(_EXPECTED_INPUT_VERDICT, observed, "_EXPECTED_INPUT_VERDICT")

    def test_transpiling_return_types_have_verified_parity(self):
        for eval_type in (_PICKLED, _ARROW, _PANDAS):
            for label, dt, _cat in _SAMPLES:
                if _EXPECTED_RETURN_VERDICT[label] != TRANSPILES:
                    continue
                with self.subTest(evalType=eval_type, label=label):
                    column_dtype, value, body = _PARITY_FIXTURES[label]
                    df = self.spark.createDataFrame(
                        [(value,)], schema=T.StructType([T.StructField("a", column_dtype)])
                    )
                    self._assert_matches_interpreted(body, dt, df, "a", eval_type=eval_type)

    def test_transpiling_input_types_have_verified_parity_pandas(self):
        for label, dt, _cat in _SAMPLES:
            if _EXPECTED_INPUT_VERDICT[label] != TRANSPILES:
                continue
            value = "ab" if label == "string" else (7.0 if label in ("float", "double") else 7)
            df = self.spark.createDataFrame([(value,), (None,)], f"a {dt.simpleString()}")
            with self.subTest(label=label):
                self._assert_matches_interpreted(
                    series_null_probe, T.BooleanType(), df, "a", eval_type=_PANDAS
                )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
