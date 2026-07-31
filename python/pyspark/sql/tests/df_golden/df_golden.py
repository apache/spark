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
Framework for DataFrame API golden file tests, analogous to SQLQueryTestSuite for SQL.

A golden test is a pair of files: a Python test module declaring the cases, and
a ``.test`` file next to it holding their expected outputs.  Everything a case
needs in order to run -- the DataFrame program, its name, its tags -- lives in
the Python class, so cases are ordinary code that can be read, imported and
stepped through; the ``.test`` file is purely generated output, rewritten in
place when golden files are regenerated (``SPARK_GENERATE_GOLDEN_FILES=1``).

Test module::

    class GroupByGoldenTests(DFGoldenTestMixin, ReusedConnectTestCase):
        golden_file = "group_by.test"

        @classmethod
        def setup_session(cls, spark):
            spark.sql("CREATE OR REPLACE TEMPORARY VIEW testData AS ...")

        @unordered
        def _test_group_by_count(self, spark):
            \"\"\"Aggregate with non-empty GroupBy expressions.\"\"\"
            return spark.table("testData").groupBy(col("a")).agg(count(col("b")))

Each ``_test_<case>`` method builds and returns the DataFrame of one case, and
:class:`DFGoldenTestMixin` registers a ``test_<case>`` method for it.  Cases are
therefore ordinary unittest tests: they are reported individually and can be run
one at a time::

    python/run-tests --testnames \\
      "pyspark.sql.tests.df_golden.test_group_by GroupByGoldenTests.test_group_by_count"

``.test`` file format::

    --! name
    __file_metadata__
    --! source
    pyspark.sql.tests.df_golden.test_group_by.GroupByGoldenTests
    !-- end


    --! name
    group_by_count
    --! tags
    unordered
    --! expected_analysis_output
    Sort [k#x ASC NULLS FIRST], true
    +- ...
    --! expected_optimized_output
    ...
    --! expected_output_schema
    struct<k:bigint>
    --! expected_result
    +---+
    | k |
    +---+
    | 1 |
    +---+
    printed all 1 rows.
    --! expected_result_hash
    <sha256 over the result rows>
    !-- end

The first block is named ``__file_metadata__``; its remaining sections are
file-level metadata, matching the convention used by the Scala
``SqlHiFiTestRunner`` framework.  ``source`` records the class the file is
generated from, so the ``.test`` file always points back at the code that
produces it.

Sections:

- ``name``: the case method name without its ``_test_`` prefix, which is what
  ties a block to the method that produced it (required).
- ``tags``: whitespace/comma separated, written from the case method's
  decorators.  Row order is asserted by default; ``@unordered`` sorts result
  rows before comparison, for cases whose result has no deterministic order
  (aggregate/join/distinct/... without a global sort).
- ``expected_analysis_output``: the analyzed logical plan.
- ``expected_optimized_output``: the optimized logical plan.
- ``expected_output_schema``: ``df.schema.simpleString()``.
- ``expected_result``: pretty-printed result table plus a ``printed all N
  rows.`` trailer.
- ``expected_result_hash``: sha256 over the (normalized, post-sort) result
  rows -- a compact checksum of the same rows rendered in ``expected_result``
  (the table is not truncated), co-required with it.
- ``expected_error``: expected error message when analysis or execution fails.
  Mutually exclusive with the plan/schema/result sections: as in the SQL golden
  suite, any error records only the message and discards the plan and schema.

At comparison time only the ``expected_*`` sections present in the file are
checked, so optional sections (e.g. ``expected_optimized_output``) may be
omitted.  Regeneration writes all sections the case produces.

Running the tests::

    python/run-tests --testnames pyspark.sql.tests.df_golden.test_group_by

Regenerating golden files
-------------------------
Set ``SPARK_GENERATE_GOLDEN_FILES=1`` before running the tests, or use the
wrapper script, which covers every golden test module in this directory::

    python/pyspark/sql/tests/df_golden/regenerate.sh [--verify]

With ``--verify`` the wrapper re-runs the tests afterwards against the
regenerated files.  Regeneration rewrites a whole file at once, so it has to
run a whole test class: a filtered run refuses to write rather than drop the
cases it skipped.

Adding golden tests
-------------------
Add a case method to an existing class, or add a ``test_<topic>.py`` module
with a new class (registering it in ``dev/sparktestsupport/modules.py``, as for
any PySpark test module).  Then regenerate: the ``.test`` file is created or
extended from the class, so it needs no hand-editing.
"""

import hashlib
import inspect
import math
import os
import re
from decimal import Decimal


_CASE_END = "!-- end"
_SECTION_PREFIX = "--! "
_FILE_METADATA_NAME = "__file_metadata__"

# Canonical section order used when (re)generating a ``.test`` file.
_CASE_SECTION_ORDER = [
    "name",
    "tags",
    "expected_analysis_output",
    "expected_optimized_output",
    "expected_output_schema",
    "expected_result",
    "expected_result_hash",
    "expected_error",
]

_RESULT_SECTIONS = [s for s in _CASE_SECTION_ORDER if s.startswith("expected_")]

_KNOWN_HEADER_SECTIONS = {"source"}
_KNOWN_TAGS = {"unordered"}

# Methods with this prefix declare golden cases; the mixin registers a real
# ``test_<case>`` method for each one.  The prefix keeps them out of unittest's
# own collection, which only picks up names starting with ``test``.
_CASE_METHOD_PREFIX = "_test_"


# ---------------------------------------------------------------------------
# .test file parsing / serialization
# ---------------------------------------------------------------------------


def parse_test_file(filepath):
    """
    Parse a ``.test`` file.

    A file whose last case is missing its ``!-- end`` terminator is rejected:
    an unclosed final case is corruption (e.g. a truncating bad merge) that
    could otherwise pass by matching a partial case or silently merging two.

    Returns
    -------
    header : dict
        File-level metadata sections from the ``__file_metadata__`` block
        (excluding ``name``), e.g. ``{"source": ...}``.
    cases : list[dict]
        One dict per test case, mapping section name to content.
    """
    with open(filepath, "r") as f:
        lines = f.read().split("\n")

    cases = []
    current = None
    section_key = None
    section_lines = []

    def flush():
        nonlocal section_key, section_lines
        if section_key is not None and current is not None:
            # A repeated section name is a copy/paste or merge mistake; last-wins
            # would silently discard one of the two, so fail loudly instead.
            assert section_key not in current, "{}: duplicate section `{}`".format(
                filepath, section_key
            )
            current[section_key] = "\n".join(section_lines).strip("\n")
        section_key = None
        section_lines = []

    for line in lines:
        stripped = line.rstrip()
        if stripped == _CASE_END:
            flush()
            if current:
                cases.append(current)
            current = None
        elif stripped.startswith(_SECTION_PREFIX):
            flush()
            if current is None:
                current = {}
            section_key = stripped[len(_SECTION_PREFIX) :].strip()
        elif stripped.startswith("--!"):
            # Reaches here only because the space after "--!" is missing, i.e. a
            # typo'd section marker. Left as body it would silently turn an
            # assertion into inert prose, so reject it.
            raise AssertionError(
                "{}: malformed section marker (expected `{}`): {!r}".format(
                    filepath, _SECTION_PREFIX, line
                )
            )
        elif section_key is not None:
            section_lines.append(line)
        elif stripped:
            # Non-blank content outside any section (before the first marker or
            # between cases) is dropped by the original loop; that hides stray
            # text, so fail loudly. Blank separator lines are fine.
            raise AssertionError("{}: content outside any section: {!r}".format(filepath, line))

    # A case still open here never hit "!-- end", i.e. the file was truncated.
    flush()
    assert not current, "{}: file does not end with `{}` (last case is unterminated)".format(
        filepath, _CASE_END
    )

    header = {}
    if cases and cases[0].get("name") == _FILE_METADATA_NAME:
        header = cases.pop(0)
        del header["name"]

    return header, cases


def write_test_file(filepath, header, cases):
    """Serialize *header* and *cases* back into ``.test`` file format."""
    blocks = []
    if header:
        header_lines = [_SECTION_PREFIX + "name", _FILE_METADATA_NAME]
        for key, value in header.items():
            header_lines.append(_SECTION_PREFIX + key)
            header_lines.append(value)
        header_lines.append(_CASE_END)
        blocks.append("\n".join(header_lines))

    for case in cases:
        case_lines = []
        for key in _CASE_SECTION_ORDER:
            value = case.get(key)
            if value is not None:
                case_lines.append(_SECTION_PREFIX + key)
                case_lines.append(value)
        case_lines.append(_CASE_END)
        blocks.append("\n".join(case_lines))

    with open(filepath, "w") as f:
        f.write("\n\n\n".join(blocks) + "\n")


def parse_tags(case):
    """Return the set of tags declared on *case*."""
    return {tag for tag in re.split(r"[,\s]+", case.get("tags", "")) if tag}


def unordered(method):
    """
    Mark a case method whose result rows have no deterministic order.

    Row order is asserted by default; a case whose result has no deterministic
    order (aggregate/join/distinct/... without a global sort) opts out with this
    decorator, which sorts the rows before they are compared or written.
    Deriving orderedness from the rendered plan text was rejected as too loose:
    it silently sorts genuinely order-sensitive results, hiding real ordering
    regressions from the golden.
    """
    method.df_golden_unordered = True
    return method


def is_unordered(method):
    """Whether *method* is decorated with :func:`unordered`."""
    return getattr(method, "df_golden_unordered", False)


# ---------------------------------------------------------------------------
# Output normalisation (mirrors SQLQueryTestHelper.replaceNotIncludedMsg)
# ---------------------------------------------------------------------------

# Compiled once for performance.
_NORMALIZATION_RULES = [
    (re.compile(r"#\d+"), "#x"),
    (re.compile(r"plan_id=\d+"), "plan_id=x"),
    (re.compile(r"joinId=\d+"), "joinId=x"),
    (re.compile(r"repartitionId=\d+"), "repartitionId=x"),
    (re.compile(r"uuid\(Some\(-?\d+\)\)"), "uuid(Some(x))"),
    (re.compile(r"CTERelationDef \d+,"), "CTERelationDef xxxx,"),
    (re.compile(r"CTERelationRef \d+,"), "CTERelationRef xxxx,"),
    (re.compile(r"cterelationdef \d+,"), "cterelationdef xxxx,"),
    (re.compile(r"cterelationref \d+,"), "cterelationref xxxx,"),
    (re.compile(r"UnionLoop \d+"), "UnionLoop xxxx"),
    (re.compile(r"UnionLoopRef \d+,"), "UnionLoopRef xxxx,"),
    (re.compile(r"Loop id: \d+"), "Loop id: xxxx"),
    (re.compile(r"@\w*,"), "@xxxxxxxx,"),
    (re.compile(r"\*\(\d+\) "), "*"),
]


def replace_not_included(text):
    """Normalise environment-dependent fragments in *text*."""
    for pattern, repl in _NORMALIZATION_RULES:
        text = pattern.sub(repl, text)
    return text


def format_error(e):
    """
    Format an exception message for golden file comparison.

    Uses ``str(e)``, which for connect exceptions is the server-side message
    (``[ERROR_CLASS] message SQLSTATE: xxxxx``).  Stripped to keep the output
    deterministic:

    - the appended JVM stacktrace;
    - the ``== DataFrame ==`` query context block, which embeds the absolute
      script path and line number of the DataFrame call (editing a script
      comment must not break golden files);
    - the trailing logical plan dump (a ``;\\n`` followed by the plan tree);

    and expression ids are normalized.
    """
    msg = str(e)
    msg = msg.split("\n\nJVM stacktrace:")[0]
    msg = msg.split("\n== DataFrame ==")[0]
    # Drop a trailing logical-plan dump: Spark appends it as ";\n" followed by
    # the plan tree, whose root line starts with an (optionally "'"-prefixed)
    # uppercase operator name. Anchoring on that lookahead avoids truncating a
    # message body that merely contains ";\n" (splitting on the first ";\n"
    # unconditionally would lose the remainder of such a message).
    msg = re.split(r";\n(?=['A-Z])", msg, maxsplit=1)[0]
    return replace_not_included(msg).strip()


# ---------------------------------------------------------------------------
# Plan extraction
# ---------------------------------------------------------------------------


_EXPLAIN_HEADER = re.compile(r"^== .+ ==$", re.MULTILINE)


def _extract_explain_section(explain, marker):
    """
    Return the body of the *marker* section of an extended explain output,
    ending at the next ``== ... ==`` header.
    """
    start = explain.find(marker)
    if start < 0:
        return None
    start = explain.find("\n", start + len(marker))
    if start < 0:
        return None
    start += 1
    match = _EXPLAIN_HEADER.search(explain, start)
    end = match.start() if match else len(explain)
    return explain[start:end].strip("\n")


def get_plan_strings(df):
    """
    Return ``(analyzed, optimized)`` normalized logical plan strings.

    Uses ``df._explain_string(mode="extended")``, which exists on Spark
    Connect only - the framework runs over connect (see ``DFGoldenTestMixin``).
    Triggers analysis, so analysis errors surface here.
    """
    explain = df._explain_string(mode="extended")
    analyzed = _extract_explain_section(explain, "== Analyzed Logical Plan ==")
    optimized = _extract_explain_section(explain, "== Optimized Logical Plan ==")
    if analyzed is None:
        raise AssertionError("explain output has no analyzed plan section:\n" + explain)

    # When the output schema is non-empty, the analyzed section starts with a
    # schema header line (possibly truncated by spark.sql.debug.maxToStringFields).
    # The schema has its own golden section, so drop the header by position.
    if df.schema.fields:
        analyzed = "\n".join(analyzed.split("\n")[1:]).strip("\n")

    optimized = replace_not_included(optimized) if optimized is not None else None
    return replace_not_included(analyzed), optimized


# ---------------------------------------------------------------------------
# Result formatting
# ---------------------------------------------------------------------------


def format_double(value):
    """
    Render *value* (a Python ``float`` from a ``double`` column) exactly as Java
    ``Double.toString`` does, which is what Hive output uses for numeric types
    (``HiveResult.toHiveStringDefault``: ``case (n, _: NumericType) =>
    n.toString``).  Matching it keeps double results comparable with the SQL
    ``.sql.out`` goldens; Python's own ``str``/``repr`` differs for special
    values (``nan``/``inf``) and for the scientific-notation regime.

    Java's rules: ``NaN``/``Infinity``/``-Infinity`` spelled out; a signed
    ``0.0``; plain decimal (always with a fractional digit) when
    ``1e-3 <= |x| < 1e7``; otherwise ``d.ddddEexp`` scientific notation with a
    single leading digit.  The shortest round-tripping digits come from Python's
    ``repr`` (normalized to drop the artificial trailing zero of values like
    ``1e7`` -> ``10000000.0``); only their placement is reformatted.
    """
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    if value == 0.0:
        return "-0.0" if math.copysign(1.0, value) < 0 else "0.0"

    sign = "-" if value < 0 else ""
    digit_tuple, exp = Decimal(repr(abs(value))).normalize().as_tuple()[1:]
    digits = "".join(map(str, digit_tuple))
    nd = len(digits)
    # Power of ten of the leading significant digit.
    leading_exp = exp + nd - 1

    if -3 <= leading_exp < 7:
        if leading_exp >= 0:
            if nd <= leading_exp + 1:
                body = digits + "0" * (leading_exp + 1 - nd) + ".0"
            else:
                body = digits[: leading_exp + 1] + "." + digits[leading_exp + 1 :]
        else:
            body = "0." + "0" * (-leading_exp - 1) + digits
    else:
        body = digits[0] + "." + (digits[1:] or "0") + "E" + str(leading_exp)
    return sign + body


def _format_value(value, data_type, nested=False):
    """
    Format a single cell value for golden file output, mirroring
    ``HiveResult.toHiveStringDefault`` so values line up with the SQL
    ``.sql.out`` goldens: structs carry quoted field names, strings are quoted
    when nested, and a top-level null (``NULL``) differs from a nested one
    (``null``).
    """
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        FloatType,
        MapType,
        StringType,
        StructType,
        TimestampNTZType,
        TimestampType,
    )

    if value is None:
        return "null" if nested else "NULL"

    if isinstance(data_type, BooleanType):
        return "true" if value else "false"
    if isinstance(data_type, StringType):
        # A tab or newline in a cell would desync the rendered table (cells are
        # tab-joined and re-split, the file is newline-delimited) while the hash
        # stayed self-consistent, so --verify could not catch the misrender.
        # Refuse loudly rather than bake a corrupt golden; add escaping with the
        # first case that legitimately needs such a value.
        if "\t" in value or "\n" in value:
            raise AssertionError(
                "df_golden: result string contains a tab or newline, which is "
                "not supported yet (would desync the rendered table): {!r}".format(value)
            )
        return '"' + value + '"' if nested else value
    if isinstance(data_type, DecimalType):
        # BigDecimal.toPlainString: never scientific notation, scale preserved.
        return format(value, "f")
    if isinstance(data_type, DoubleType):
        return format_double(value)
    if isinstance(data_type, StructType):
        parts = [
            '"{}":{}'.format(f.name, _format_value(value[i], f.dataType, nested=True))
            for i, f in enumerate(data_type.fields)
        ]
        return "{" + ",".join(parts) + "}"
    if isinstance(data_type, ArrayType):
        parts = [_format_value(v, data_type.elementType, nested=True) for v in value]
        return "[" + ",".join(parts) + "]"
    if isinstance(data_type, MapType):
        parts = [
            _format_value(k, data_type.keyType, nested=True)
            + ":"
            + _format_value(v, data_type.valueType, nested=True)
            for k, v in value.items()
        ]
        # Hive sorts map entries by their rendered string, not by key.
        return "{" + ",".join(sorted(parts)) + "}"
    # These types have no faithful ``str()`` rendering and must not fall through
    # to the generic branch below:
    #   - float: Python's repr is the double-precision shortest form, not Java
    #     ``Float.toString``'s float32-shortest form, so str() would diverge.
    #     (``double`` is handled above via ``format_double``; ``float`` waits for
    #     the first float-column case, which needs float32-shortest rendering.)
    #   - temporal/binary: need a Hive-style formatter and (for LTZ timestamps) a
    #     pinned session time zone this framework does not set up yet.
    # Refuse them loudly rather than silently emit a wrong/non-deterministic
    # value; add real formatting together with the first such test case.
    if isinstance(
        data_type,
        (FloatType, DateType, TimestampType, TimestampNTZType, BinaryType),
    ):
        raise AssertionError(
            "df_golden: result column of type {} is not supported yet (needs a "
            "Hive-style formatter)".format(data_type.simpleString())
        )
    return str(value)


def get_result_rows(df):
    """
    Collect *df* and format each row as a tab-separated string matching hive
    output conventions (``NULL`` for None, lowercase booleans, etc.).

    Cells are joined with ``\\t`` and later re-split on ``\\t`` by
    ``render_result_table``, and the ``.test`` format is newline-delimited, so a
    literal tab or newline inside a string value would desync the rendered table
    while the hash stayed self-consistent (``--verify`` could not flag it).
    ``_format_value`` therefore rejects such strings loudly rather than let a
    corrupt golden through.
    """
    schema = df.schema
    return [
        "\t".join(_format_value(row[i], field.dataType) for i, field in enumerate(schema.fields))
        for row in df.collect()
    ]


def render_result_table(columns, rows):
    """
    Render *rows* (tab-separated strings) as a pretty-printed table::

        +----+----+
        | c1 | c2 |
        +----+----+
        | 1  | 10 |
        +----+----+
        printed all 1 rows.
    """
    trailer = "printed all {} rows.".format(len(rows))
    if not columns:
        return trailer

    cells = [r.split("\t") for r in rows]
    widths = [len(c) for c in columns]
    for row_cells in cells:
        for i, cell in enumerate(row_cells[: len(widths)]):
            widths[i] = max(widths[i], len(cell))

    border = "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    def fmt(values):
        padded = [v.ljust(w) for v, w in zip(values, widths)]
        return "| " + " | ".join(padded) + " |"

    lines = [border, fmt(columns), border]
    lines.extend(fmt(row_cells) for row_cells in cells)
    lines.append(border)
    lines.append(trailer)
    return "\n".join(lines)


def hash_result_rows(rows):
    """sha256 over the normalized result rows; verifies the full result."""
    return hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Test execution engine
# ---------------------------------------------------------------------------


def _build_dataframe(spark, build_df):
    """Call a case method and return the DataFrame it built."""
    from pyspark.sql import DataFrame

    df = build_df(spark)
    if not isinstance(df, DataFrame):
        raise AssertionError(
            "case method {} must return the DataFrame under test, got: {!r}".format(
                getattr(build_df, "__name__", build_df), df
            )
        )
    return df


def compute_case_outputs(spark, build_df, sort_rows=False):
    """
    Run a single test case and return a dict of actual ``expected_*`` sections.

    *build_df* is the case method: it takes the session and returns the
    DataFrame under test.  *sort_rows* sorts the result rows before they are
    rendered (see :func:`unordered`).
    """
    from pyspark.errors import PySparkException

    # Only Spark errors are legitimate expected outputs.  Anything else
    # (NameError, TypeError, ... from a buggy case method) must fail the test;
    # capturing it would write the Python error into the golden file as the
    # expected output on regeneration.
    try:
        df = _build_dataframe(spark, build_df)
        analyzed, optimized = get_plan_strings(df)
        schema = df.schema.simpleString()
    except PySparkException as e:
        return {"expected_error": format_error(e)}

    actual = {
        "expected_analysis_output": analyzed,
        "expected_output_schema": schema,
    }
    if optimized is not None:
        actual["expected_optimized_output"] = optimized

    try:
        rows = get_result_rows(df)
    except PySparkException as e:
        # Match the SQL golden suite: on any error keep only the message and
        # discard the analyzed plan / schema captured before execution.
        return {"expected_error": format_error(e)}

    rows = [replace_not_included(r) for r in rows]
    if sort_rows:
        rows = sorted(rows)
    actual["expected_result"] = render_result_table(df.columns, rows)
    actual["expected_result_hash"] = hash_result_rows(rows)
    return actual


def validate_test_file(test_file, header, cases):
    """
    Fail loudly on malformed ``.test`` content.  A misspelled section or tag
    that is silently ignored makes a case assert less than it appears to (or
    nothing at all), so unknown names are errors, not noise.

    Only the golden files being checked go through here: regeneration writes a
    file from scratch and never reads the old one, so a file this rejects can
    always be repaired by regenerating it.
    """
    unknown_header = set(header) - _KNOWN_HEADER_SECTIONS
    assert not unknown_header, "{}: unknown header sections: {}".format(
        test_file, ", ".join(sorted(unknown_header))
    )
    assert cases, "{}: no test cases found".format(test_file)
    for case in cases:
        assert case.get("name"), "{}: every test case needs a name".format(test_file)
        name = case["name"]
        unknown = set(case) - set(_CASE_SECTION_ORDER)
        assert not unknown, "{}: case `{}` has unknown sections: {}".format(
            test_file, name, ", ".join(sorted(unknown))
        )
        unknown_tags = parse_tags(case) - _KNOWN_TAGS
        assert not unknown_tags, "{}: case `{}` has unknown tags: {}".format(
            test_file, name, ", ".join(sorted(unknown_tags))
        )
        # A case with no expected_* section would pass vacuously.
        assert any(case.get(key) is not None for key in _RESULT_SECTIONS), (
            "{}: case `{}` has no expected_* sections and would assert "
            "nothing; regenerate the golden files".format(test_file, name)
        )
        # ``compare_case`` only checks sections present in the file, so a
        # dropped section (merge/manual edit) silently shrinks coverage
        # without failing. Pin down what a well-formed case must look like:
        has_error = case.get("expected_error") is not None
        has_result = case.get("expected_result") is not None
        has_hash = case.get("expected_result_hash") is not None
        if has_error:
            # An error case records only the error (the run discards plan,
            # schema and result on failure); anything else is a corrupt file.
            conflicting = sorted(
                key
                for key in _RESULT_SECTIONS
                if key != "expected_error" and case.get(key) is not None
            )
            assert not conflicting, (
                "{}: error case `{}` must carry only `expected_error`, not also: {}".format(
                    test_file, name, ", ".join(conflicting)
                )
            )
        else:
            # The result table and its hash are a pair; dropping one leaves
            # the other asserting half the result, so require both or neither.
            assert has_result == has_hash, (
                "{}: case `{}` must have both `expected_result` and "
                "`expected_result_hash` or neither".format(test_file, name)
            )


def build_golden_case(name, sort_rows, actual):
    """
    Build the ``.test`` block for a case from this run's *actual* outputs.

    Only the outputs are golden: the case's identity (*name*) and its ordering
    guard (*sort_rows*) are declared by the case method, so regeneration writes
    them from the class rather than carrying them over from the old file.
    """
    case = {"name": name}
    if sort_rows:
        case["tags"] = "unordered"
    case.update(actual)
    return case


def compare_case(test_case, case, actual):
    """Compare the ``expected_*`` sections of *case* against *actual*."""
    name = case["name"]
    for key in _RESULT_SECTIONS:
        expected = case.get(key)
        if expected is None:
            continue
        got = actual.get(key)
        if got is None:
            produced = ", ".join(sorted(actual)) or "<nothing>"
            test_case.fail(
                "[{}] expected section `{}` but the case produced: {}".format(name, key, produced)
            )
        test_case.assertEqual(
            expected.strip("\n"),
            got.strip("\n"),
            "[{}] mismatch in `{}`".format(name, key),
        )


def is_generating_golden():
    """Whether this run regenerates the golden files instead of checking them."""
    return os.environ.get("SPARK_GENERATE_GOLDEN_FILES") is not None


def check_cases_in_sync(test_file, declared, golden):
    """
    Check that the golden file describes exactly the declared cases, in order.

    Each test asserts against its own block, so a case the golden file has never
    heard of (or one it still remembers after the method was deleted or renamed)
    would otherwise go unnoticed.
    """
    if declared == golden:
        return
    missing = [name for name in declared if name not in golden]
    extra = [name for name in golden if name not in declared]
    if missing:
        detail = "cases with no block in the golden file: " + ", ".join(missing)
    elif extra:
        detail = "blocks in the golden file with no case method: " + ", ".join(extra)
    else:
        detail = "the golden file lists the cases in a different order"
    raise AssertionError(
        "{}: golden file is out of sync with the test class ({}); "
        "regenerate the golden files".format(test_file, detail)
    )


# ---------------------------------------------------------------------------
# Test class integration
# ---------------------------------------------------------------------------


class DFGoldenTestMixin:
    """
    Mixin turning a class of case methods into DataFrame golden file tests.

    Mix into a session-providing test case, listing this class first so its
    ``setUpClass`` runs once the session exists::

        class GroupByGoldenTests(DFGoldenTestMixin, ReusedConnectTestCase):
            golden_file = "group_by.test"

            def _test_group_by_count(self, spark):
                return spark.table("testData").groupBy(col("a")).agg(count(col("b")))

    Every ``_test_<case>`` method declares one case and returns the DataFrame
    under test; a ``test_<case>`` method is registered for each, so cases run,
    report and can be selected individually like any other unittest test.

    The cases of a class share one Spark Connect session (``newSession()`` off
    the session the test class provides, the Connect counterpart of
    ``SQLQueryTestSuite``'s per-file ``newSession()``), prepared once by
    :meth:`setup_session`.  State created there -- temp views, UDFs, session
    confs -- is discarded with the session and cannot leak into other classes.
    """

    #: Name of the ``.test`` golden file, resolved next to the test module.
    golden_file = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # This mixin's setUpClass must run after the session-providing class has
        # created the session, i.e. its setUpClass must be the outer one, which
        # is only true when the mixin is listed first.
        for base in cls.__mro__:
            if base is DFGoldenTestMixin:
                break
            if base is not cls and "setUpClass" in vars(base):
                raise TypeError(
                    "{} has incorrect inheritance order: DFGoldenTestMixin must be "
                    "listed before {}. Use: class {}(DFGoldenTestMixin, {}, ...)".format(
                        cls.__name__, base.__name__, cls.__name__, base.__name__
                    )
                )
        for name in cls.case_names():
            setattr(cls, "test_" + name, _make_case_test(name, cls.case_method(name)))

    @classmethod
    def case_names(cls):
        """The declared case names, in declaration order."""
        names = []
        for klass in reversed(cls.__mro__):
            for attr, value in vars(klass).items():
                if not attr.startswith(_CASE_METHOD_PREFIX) or not callable(value):
                    continue
                name = attr[len(_CASE_METHOD_PREFIX) :]
                if name not in names:
                    names.append(name)
        return names

    @classmethod
    def case_method(cls, name):
        """The case method declaring the case *name*."""
        return getattr(cls, _CASE_METHOD_PREFIX + name)

    @classmethod
    def golden_file_path(cls):
        """Absolute path of the golden file, resolved next to the test module."""
        assert cls.golden_file, "{}: set `golden_file` to its .test file".format(cls.__name__)
        module_file = os.path.abspath(inspect.getfile(cls))
        return os.path.join(os.path.dirname(module_file), cls.golden_file)

    @classmethod
    def setup_session(cls, spark):
        """
        Hook: prepare the session shared by this class's cases.

        Override to create the temp views and other session state the cases
        build on.
        """

    @classmethod
    def setUpClass(cls):
        cls._golden_regenerating = is_generating_golden()
        cls._golden_actual = {}
        cls._golden_cases = {}
        cls._golden_session = None
        case_names = cls.case_names()

        # Read the golden file before the session is started: a malformed or
        # stale file is worth failing on right away, and failing here leaves
        # nothing to clean up (unittest skips tearDownClass when setUpClass
        # raises).  Regeneration writes the file from the class alone, so it
        # neither reads nor requires an existing one.
        if case_names:
            cls._golden_path = cls.golden_file_path()
            if not cls._golden_regenerating:
                header, cases = parse_test_file(cls._golden_path)
                validate_test_file(cls._golden_path, header, cases)
                golden_names = [case["name"] for case in cases]
                check_cases_in_sync(cls._golden_path, case_names, golden_names)
                cls._golden_cases = {case["name"]: case for case in cases}

        super().setUpClass()

        if not case_names:
            return
        try:
            cls._golden_session = cls.spark.newSession()
            # Golden files are generated with ANSI mode on, matching the SQL
            # golden tests.  The session is discarded with the class, so there
            # is nothing to restore.
            cls._golden_session.conf.set("spark.sql.ansi.enabled", "true")
            cls.setup_session(cls._golden_session)
        except BaseException:
            # tearDownClass does not run when setUpClass raises, and leaving the
            # session behind would hang the test process at exit.
            cls._close_golden_session()
            super().tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls):
        try:
            if cls._golden_regenerating and cls.case_names():
                cls._write_golden_file()
        finally:
            try:
                cls._close_golden_session()
            finally:
                super().tearDownClass()

    @classmethod
    def _close_golden_session(cls):
        session = cls._golden_session
        cls._golden_session = None
        if session is None:
            return
        # Release only this sub-session server-side and close its client
        # channel.  We must NOT call ``session.stop()``: under
        # ``SPARK_LOCAL_REMOTE`` (the test harness) ``stop()`` terminates the
        # shared local Connect server, breaking the rest of the suite and
        # hanging the session-providing class's ``spark.stop()`` in release
        # retries against the dead server until the test times out.
        client = session.client
        try:
            client.release_session()
        except Exception:
            pass
        try:
            client.close()
        except Exception:
            pass

    @classmethod
    def _write_golden_file(cls):
        names = cls.case_names()
        missing = [name for name in names if name not in cls._golden_actual]
        # Writing what a partial run produced would silently drop the cases that
        # did not run, so refuse rather than truncate the golden file.
        assert not missing, (
            "{}: cannot regenerate, these cases did not run (a failing case, or a run "
            "filtered to a subset): {}".format(cls._golden_path, ", ".join(missing))
        )
        header = {"source": "{}.{}".format(cls.__module__, cls.__qualname__)}
        cases = [
            build_golden_case(name, is_unordered(cls.case_method(name)), cls._golden_actual[name])
            for name in names
        ]
        write_test_file(cls._golden_path, header, cases)

    def _run_golden_case(self, name):
        cls = type(self)
        build_df = getattr(self, _CASE_METHOD_PREFIX + name)
        actual = compute_case_outputs(
            cls._golden_session, build_df, sort_rows=is_unordered(build_df)
        )
        if cls._golden_regenerating:
            cls._golden_actual[name] = actual
        else:
            compare_case(self, cls._golden_cases[name], actual)


def _make_case_test(name, case_method):
    """Build the unittest method running the case *name*."""

    def test_case(self):
        self._run_golden_case(name)

    test_case.__name__ = "test_" + name
    # Carry the case method's docstring over so verbose runs describe the case.
    test_case.__doc__ = case_method.__doc__
    return test_case
