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
A canary over every Spark data type, for UDF transpilation.

Two allowlists decide what the transpiler rewrites: a return-type check in
``python/pyspark/sql/transpile.py`` (numeric / string / boolean / binary), and an input-type
category check in ``ResolveTranspiledPythonUDFOptions.scala`` with a ``case _ => false``
catch-all. Being allowlists, a data type added tomorrow falls back to interpreted Python
automatically -- safe, but silent: nobody is told the new type was never considered.

So this module enumerates the concrete ``DataType`` subclasses ``pyspark.sql.types`` exports
and demands an entry for each, making a new type fail
``test_data_type_inventory_is_complete`` with instructions.

Enumeration is introspective because ``pyspark.sql.types._atomic_types`` is NOT exhaustive
(it omits ``TimeType``, ``GeometryType``, ``GeographyType``, ``CalendarIntervalType``), so a
canary keyed on it would stop covering new types -- the exact failure this file prevents.

Triaging a new type
-------------------
1. Add an instance to ``_SAMPLES`` with the body category whose lowering could target it.
2. Run this module; the verdict tests report the observed verdict.
3. ``NO_TRANSPILE``, ``REJECTED_BY_ARROW`` or ``REJECTED_CHAR_VARCHAR``: record it and
   stop.
4. ``TRANSPILES``: verify parity against the interpreted path on both eval types *before*
   recording. The pickled path NULLs a mismatched return value where Arrow raises or
   converts; the golden matrix in ``python/pyspark/sql/tests/coercion/`` is the authority. If
   they disagree, tighten the body-vs-return-type check in ``transpile.py`` instead.

The tables were derived by observation. They are expressed for the pickled eval type, with
the Arrow expectations derived from them, which pins the property that matters: Arrow UDFs
admit exactly the same types as pickled ones, except where Arrow cannot express the return
type at all.
"""

import unittest

from pyspark.errors import PySparkNotImplementedError
from pyspark.sql import types as T
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.tests.arrow.test_arrow_python_udf_transpile import (
    _TRANSPILE_OFF,
    _TRANSPILE_ON,
    ARROW_TRANSPILE_SUPPORTED,
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

# Classes exported from pyspark.sql.types that are not usable column types: the abstract
# root, StructField (a field descriptor rather than a type), and the bare UserDefinedType
# base (its sqlType() raises NotImplementedError; real UDTs are user-authored subclasses
# this module can't enumerate). None of these are in __all__, so _concrete_data_type_classes
# never sees them anyway -- named here only so a reader doesn't wonder why they're missing.
_NOT_COLUMN_TYPES = frozenset({T.DataType, T.StructField, T.UserDefinedType})


# --------------------------------------------------------------------------------------
# UDF bodies. Named defs because the transpiler recovers source via inspect.getsource and
# cannot extract a lambda that appears as a call argument.
# --------------------------------------------------------------------------------------


def numeric_body(x):
    return x + 1


def string_body(x: str):
    return x


def bool_body(x: bool):
    return x


def binary_body(x: bytes):
    return x


def null_probe(x):
    # Deliberately unannotated: this is the realistic default, and an unannotated parameter
    # is only tried as numeric and string, which is what the input-type table records.
    return 1 if x is None else 2


_BODIES = {
    "numeric": numeric_body,
    "string": string_body,
    "bool": bool_body,
    "binary": binary_body,
    # For a return type no lowering can category-match, the body is irrelevant: the
    # return-type allowlist refuses before any lowering is attempted.
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
)

# Verdict per RETURN type, pickled eval type, with the body category paired above.
# Two refusals have non-obvious reasons: `decimal` is inside the return-type allowlist (a
# NumericType) but refused by the body-vs-return-type check, because Python receives
# decimal.Decimal with different precision semantics; `void` is outside the allowlist even
# though Arrow can express it, so widening the allowlist to follow Arrow would let it in.
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
}

# Return types Arrow cannot express, so an Arrow-optimized UDF declaring one fails at
# _check_return_type regardless of transpilation. Derived from to_arrow_type and pinned
# independently by test_transpiler_return_allowlist_is_a_subset_of_arrow_support.
# CHAR/VARCHAR are absent even though to_arrow_type rejects them too: UDF construction
# now refuses them for every eval type (REJECTED_CHAR_VARCHAR) before the Arrow check
# is reached.
_ARROW_REJECTED_RETURN_TYPES = frozenset({"year_month_interval", "calendar_interval"})

# Verdict per INPUT column type, pickled eval type, with the unannotated `null_probe` body.
# boolean and binary are NO_TRANSPILE here but TRANSPILES as return types: an unannotated
# parameter is only tried as numeric and string, so those columns match no category (see
# test_annotated_parameters_widen_input_coverage). string_lcase is refused because the check
# requires UTF8_BINARY -- under UTF8_LCASE 'abc' == 'ABC' is true where Python says False.
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
}

# Parity fixture per TRANSPILES return type: an input column whose body result already
# matches the declared return type. Pairing them avoids a known, accepted divergence -- a
# bigint column with `x + 1` declared DoubleType() gives the pickled path a Python int, which
# it NULLs, while the transpiled cast returns 8.0 (see
# test_udf_transpile_casts_to_return_type) -- masquerading as a canary failure.
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

# Column type + value + matching annotated body, per body category, to show that annotating a
# parameter is what unlocks the bool and binary columns.
_ANNOTATED_CATEGORY_FIXTURES = {
    "numeric": (T.LongType(), 7, numeric_body),
    "string": (T.StringType(), "ab", string_body),
    "bool": (T.BooleanType(), True, bool_body),
    "binary": (T.BinaryType(), b"ab", binary_body),
}


def _concrete_data_type_classes():
    """Every concrete ``DataType`` subclass exported from ``pyspark.sql.types``.

    Introspective rather than a hand-written list: see the module docstring on why
    ``_atomic_types`` is not a safe basis. Abstract bases are excluded implicitly -- they are
    not in ``__all__`` -- so only ``DataType`` itself and ``StructField`` need naming.
    """
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

    # -- inventory ----------------------------------------------------------------------

    def test_data_type_inventory_is_complete(self):
        # The tripwire for a newly added Spark type.
        sampled = {type(dt) for _label, dt, _cat in _SAMPLES}
        missing = sorted(cls.__name__ for cls in _concrete_data_type_classes() - sampled)
        self.assertEqual(
            [],
            missing,
            f"pyspark.sql.types now exports {missing}, which this canary has never "
            "considered. Add an instance to _SAMPLES in this file, then follow the "
            "'Triaging a new type' runbook in this module's docstring. Falling back to "
            "interpreted Python is the safe default and is probably the right answer, but "
            "it should be a recorded decision rather than an accident.",
        )

    def test_sample_tables_are_complete(self):
        # Adding a sample must force a verdict to be recorded for it, in both directions.
        labels = {label for label, _dt, _cat in _SAMPLES}
        self.assertEqual(
            set(),
            labels - set(_EXPECTED_RETURN_VERDICT),
            "these samples have no entry in _EXPECTED_RETURN_VERDICT",
        )
        self.assertEqual(
            set(),
            labels - set(_EXPECTED_INPUT_VERDICT),
            "these samples have no entry in _EXPECTED_INPUT_VERDICT",
        )
        self.assertEqual(
            set(),
            set(_EXPECTED_RETURN_VERDICT) - labels,
            "these verdicts refer to samples that no longer exist",
        )
        self.assertEqual(
            set(),
            set(_EXPECTED_INPUT_VERDICT) - labels,
            "these verdicts refer to samples that no longer exist",
        )
        self.assertEqual(
            set(),
            _ARROW_REJECTED_RETURN_TYPES - labels,
            "these Arrow-rejected labels refer to samples that no longer exist",
        )

    # -- static invariant ---------------------------------------------------------------

    def test_transpiler_return_allowlist_is_a_subset_of_arrow_support(self):
        # Why the existing return-type allowlist can be reused unchanged for the Arrow eval
        # type: everything it admits, Arrow can express. If this ever fails, widening
        # transpilation to Arrow UDFs would start producing return types the Arrow worker
        # cannot serialize.
        for label, dt, _cat in _SAMPLES:
            with self.subTest(label=label):
                in_allowlist = isinstance(
                    dt, (T.NumericType, T.StringType, T.BooleanType, T.BinaryType)
                )
                if not in_allowlist:
                    continue
                try:
                    to_arrow_type(dt, timezone="UTC")
                except Exception as e:  # any failure is the finding
                    self.fail(
                        f"{label} is inside the transpiler's return-type allowlist "
                        "(python/pyspark/sql/transpile.py) but to_arrow_type "
                        f"(python/pyspark/sql/pandas/types.py) rejects it: {e!r}. Either "
                        "narrow the transpiler's allowlist or teach to_arrow_type about "
                        "this type; until then Arrow-optimized UDFs must not transpile it."
                    )

    # -- return-type verdicts -----------------------------------------------------------

    def _return_verdict(self, dt, body, eval_type):
        try:
            pudf = UserDefinedFunction(body, dt, evalType=eval_type)
            # Force _check_return_type, which only runs when returnType is read. Doing it
            # explicitly keeps the verdict stable regardless of whether the transpile block
            # happens to have read it during construction.
            _ = pudf.returnType
        except Exception as e:  # classified below
            if (
                isinstance(e, PySparkNotImplementedError)
                and e.getCondition() == "CHAR_VARCHAR_NOT_SUPPORTED_IN_PYTHON"
            ):
                return REJECTED_CHAR_VARCHAR
            if "Invalid return type with Arrow-optimized Python UDF" in str(e):
                return REJECTED_BY_ARROW
            raise
        return TRANSPILES if pudf.transpiled else NO_TRANSPILE

    def _assert_verdicts(self, expected, observed, table_name, guidance):
        """Compare verdict maps, showing the observed table in copy-pasteable form."""
        self.maxDiff = None
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
            f"changed (label: expected -> observed): {changed}\n\n"
            f"{guidance}\n\n"
            f"observed table, if the change is intended and verified:\n{rendered}"
        )

    def test_return_type_verdicts_pickled(self):
        with self.sql_conf(_TRANSPILE_ON):
            observed = {}
            for label, dt, cat in _SAMPLES:
                observed[label] = self._return_verdict(dt, _BODIES[cat], _PICKLED)
        self._assert_verdicts(
            _EXPECTED_RETURN_VERDICT,
            observed,
            "_EXPECTED_RETURN_VERDICT",
            "See 'Triaging a new type' in this module's docstring: if something now "
            "TRANSPILES, verify parity against the interpreted path BEFORE updating the "
            "table, using the golden matrix in python/pyspark/sql/tests/coercion/ as the "
            "authority. A Cast that is analysis-valid but that the interpreted path would "
            "never perform is a silent divergence, not a feature.",
        )

    def test_return_type_verdicts_arrow(self):
        # Correct in both states: before the gate flip nothing transpiles on the Arrow eval
        # type, and afterwards the verdicts must equal the pickled ones -- except for the
        # return types Arrow cannot express at all, which fail earlier and either way.
        expected = {}
        for label, verdict in _EXPECTED_RETURN_VERDICT.items():
            if label in _ARROW_REJECTED_RETURN_TYPES:
                expected[label] = REJECTED_BY_ARROW
            elif verdict == REJECTED_CHAR_VARCHAR:
                # Rejected at UDF construction for every eval type, gate or no gate.
                expected[label] = REJECTED_CHAR_VARCHAR
            elif ARROW_TRANSPILE_SUPPORTED:
                expected[label] = verdict
            else:
                expected[label] = NO_TRANSPILE

        with self.sql_conf(_TRANSPILE_ON):
            observed = {}
            for label, dt, cat in _SAMPLES:
                observed[label] = self._return_verdict(dt, _BODIES[cat], _ARROW)
        self._assert_verdicts(
            expected,
            observed,
            "the Arrow return verdicts derived from _EXPECTED_RETURN_VERDICT",
            "Transpilation should admit exactly the same return types on both eval types; "
            "the only Arrow-specific difference is the return types Arrow cannot express, "
            "listed in _ARROW_REJECTED_RETURN_TYPES.",
        )

    # -- input-type verdicts ------------------------------------------------------------

    def _null_row_df(self, dt):
        schema = T.StructType([T.StructField("a", dt)])
        return self.spark.createDataFrame([(None,)], schema=schema)

    def _input_verdict(self, dt, eval_type):
        try:
            df = self._null_row_df(dt)
            df.count()
        except Exception:  # the type simply cannot be a column
            return NOT_A_COLUMN_TYPE
        pudf = UserDefinedFunction(null_probe, T.LongType(), evalType=eval_type)
        # Plan only: whether an option survived JVM-side pruning shows up as the presence
        # or absence of a Python-eval operator, and needs no Python execution.
        counts = _eval_python_nodes(df.select(pudf("a")))
        return NO_TRANSPILE if sum(counts.values()) else TRANSPILES

    def test_input_type_verdicts_pickled(self):
        with self.sql_conf(_TRANSPILE_ON):
            observed = {label: self._input_verdict(dt, _PICKLED) for label, dt, _cat in _SAMPLES}
        self._assert_verdicts(
            _EXPECTED_INPUT_VERDICT,
            observed,
            "_EXPECTED_INPUT_VERDICT",
            "The input-type check is ResolveTranspiledPythonUDFOptions.optionMatchesTypes; "
            "its `case _ => false` is what makes an unrecognised column type fall back. If a "
            "type now TRANSPILES, verify parity for it before updating the table.",
        )

    def test_input_type_verdicts_arrow(self):
        expected = {
            label: (
                verdict
                if (ARROW_TRANSPILE_SUPPORTED or verdict == NOT_A_COLUMN_TYPE)
                else NO_TRANSPILE
            )
            for label, verdict in _EXPECTED_INPUT_VERDICT.items()
        }
        with self.sql_conf(_TRANSPILE_ON):
            observed = {label: self._input_verdict(dt, _ARROW) for label, dt, _cat in _SAMPLES}
        self._assert_verdicts(
            expected,
            observed,
            "the Arrow input verdicts derived from _EXPECTED_INPUT_VERDICT",
            "The set of column types a transpiled option can bind to is decided on the JVM "
            "side and must not depend on the eval type.",
        )

    def test_no_transpile_input_types_still_compute_correctly(self):
        # The point of a fallback: a column type the transpiler cannot bind to must still
        # produce the interpreted answer rather than an error or a wrong value.
        for label, dt, _cat in _SAMPLES:
            if _EXPECTED_INPUT_VERDICT[label] != NO_TRANSPILE:
                continue
            with self.subTest(label=label):
                df = self._null_row_df(dt)
                pudf = UserDefinedFunction(null_probe, T.LongType(), evalType=_PICKLED)
                with self.sql_conf(_TRANSPILE_ON):
                    on = df.select(pudf("a")).collect()[0][0]
                with self.sql_conf(_TRANSPILE_OFF):
                    off = df.select(pudf("a")).collect()[0][0]
                self.assertEqual(1, off, f"{label}: interpreted UDF should see None")
                self.assertEqual(off, on, f"{label}: fallback changed the answer")

    # -- parity for the types that DO transpile -----------------------------------------

    def _fixture_df(self, fixtures, key):
        column_dtype, value, body = fixtures[key]
        df = self.spark.createDataFrame(
            [(value,)], schema=T.StructType([T.StructField("a", column_dtype)])
        )
        return df, body, column_dtype

    def test_parity_fixtures_cover_every_transpiling_return_type(self):
        # A return type may not be recorded as TRANSPILES without a parity fixture, or the
        # parity tests below would silently skip it.
        expected = {
            label for label, verdict in _EXPECTED_RETURN_VERDICT.items() if verdict == TRANSPILES
        }
        self.assertEqual(
            expected,
            set(_PARITY_FIXTURES),
            "every return type recorded as TRANSPILES needs an entry in _PARITY_FIXTURES "
            "(an input column and body whose Python result type matches that return type), "
            "and vice versa",
        )

    def test_transpiling_return_types_have_verified_parity_pickled(self):
        # Every return type the table records as TRANSPILES is compared against the
        # interpreted path, so the table can never say TRANSPILES on the strength of the
        # rewrite merely existing.
        for label, dt, _cat in _SAMPLES:
            if _EXPECTED_RETURN_VERDICT[label] != TRANSPILES:
                continue
            with self.subTest(label=label):
                df, body, _column_dtype = self._fixture_df(_PARITY_FIXTURES, label)
                self._assert_matches_interpreted(body, dt, df, "a", eval_type=_PICKLED)

    def test_transpiling_return_types_have_verified_parity_arrow(self):
        for label, dt, _cat in _SAMPLES:
            if _EXPECTED_RETURN_VERDICT[label] != TRANSPILES:
                continue
            with self.subTest(label=label):
                df, body, _column_dtype = self._fixture_df(_PARITY_FIXTURES, label)
                self._assert_matches_interpreted(body, dt, df, "a", eval_type=_ARROW)

    def test_annotated_parameters_widen_input_coverage(self):
        # Explains the boolean and binary NO_TRANSPILE rows in _EXPECTED_INPUT_VERDICT:
        # they are a consequence of the parameter being unannotated, not of the column type
        # being unsupported. With an annotation pinning the category, the option binds.
        for category in _ANNOTATED_CATEGORY_FIXTURES:
            with self.subTest(category=category):
                df, body, column_dtype = self._fixture_df(_ANNOTATED_CATEGORY_FIXTURES, category)
                # Transpilation happens at construction, so the confs have to be on before
                # the UDF is built -- not merely before it is used.
                with self.sql_conf(_TRANSPILE_ON):
                    pudf = UserDefinedFunction(body, column_dtype, evalType=_PICKLED)
                    self._assert_transpiled(pudf, f"an annotated {category} body")
                    counts = _eval_python_nodes(df.select(pudf("a")))
                self.assertEqual(
                    0,
                    sum(counts.values()),
                    f"an annotated {category} parameter should let the transpiled option "
                    "bind, leaving no Python-eval operator in the plan",
                )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
