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

A test is described by a ``.test`` file which doubles as the golden file: the
expected outputs are stored inline and rewritten in place when golden files are
regenerated (``SPARK_GENERATE_GOLDEN_FILES=1``).

``.test`` file format::

    --! name
    __file_metadata__
    --! source
    df_golden/group_by
    !-- end


    --! name
    range + select + filter + order
    --! script
    scripts/group_by/range_select.py
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

The first block may be named ``__file_metadata__``; its remaining sections
(e.g. ``source``) are file-level metadata, matching the convention used by the
Scala ``SqlHiFiTestRunner`` framework.  Each test case references a standalone
Python script (path relative to the ``.test`` file) that is executed with
``spark`` in scope and must assign the DataFrame under test to a variable
named ``df``.  Cases run in file order against the same session, so earlier
cases can set up temp views for later ones.

Sections:

- ``name``: human-readable test case name (required).
- ``tags``: optional, whitespace/comma separated. Row order is asserted by
  default; add the ``unordered`` tag to sort result rows before comparison for
  cases whose result has no deterministic order (aggregate/join/distinct/...
  without a global sort).
- ``script``: path to the Python script (required).
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
"""

import hashlib
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
    "script",
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


# ---------------------------------------------------------------------------
# .test file parsing / serialization
# ---------------------------------------------------------------------------


def parse_test_file(filepath, require_terminated=False):
    """
    Parse a ``.test`` file.

    When *require_terminated* is set (verify mode), a file whose last case is
    missing its ``!-- end`` terminator is rejected: in verify mode an unclosed
    final case is corruption (e.g. a truncating bad merge) that could otherwise
    pass by matching a partial case or silently merging two. Regeneration leaves
    it unset and stays lenient, since it rewrites the file with terminators.

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
            section_key = stripped[len(_SECTION_PREFIX):].strip()
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
            raise AssertionError(
                "{}: content outside any section: {!r}".format(filepath, line)
            )

    # A case still open here never hit "!-- end". In verify mode that is
    # corruption; under regeneration stay lenient and keep it so the rewrite can
    # fix the formatting.
    flush()
    if current:
        assert not require_terminated, (
            "{}: file does not end with `{}` (last case is unterminated)".format(
                filepath, _CASE_END
            )
        )
        cases.append(current)

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
    Connect only - the framework runs over connect (see ``DFGoldenTestBase``).
    Triggers analysis, so analysis errors surface here.
    """
    explain = df._explain_string(mode="extended")
    analyzed = _extract_explain_section(explain, "== Analyzed Logical Plan ==")
    optimized = _extract_explain_section(explain, "== Optimized Logical Plan ==")
    if analyzed is None:
        raise AssertionError(
            "explain output has no analyzed plan section:\n" + explain
        )

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
        "\t".join(
            _format_value(row[i], field.dataType)
            for i, field in enumerate(schema.fields)
        )
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


def run_script(spark, script_path):
    """
    Execute the test case script and return the DataFrame it assigns to ``df``.

    The script runs with ``spark`` in scope and is responsible for its own
    imports.
    """
    with open(script_path, "r") as f:
        code = f.read()
    namespace = {"spark": spark}
    exec(compile(code, script_path, "exec"), namespace)
    if "df" not in namespace:
        raise AssertionError(
            "Test script {} must assign a DataFrame to a variable named `df`".format(
                script_path
            )
        )
    return namespace["df"]


def compute_case_outputs(spark, case, base_dir):
    """
    Run a single test case and return a dict of actual ``expected_*`` sections.
    """
    from pyspark.errors import PySparkException

    tags = parse_tags(case)
    script_path = os.path.join(base_dir, case["script"])

    # Only Spark errors are legitimate expected outputs.  Anything else
    # (NameError, ImportError, ... from a buggy script) must fail the test;
    # capturing it would write the Python error into the golden file as the
    # expected output on regeneration.
    try:
        df = run_script(spark, script_path)
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
    # Sort the rows only when the case is explicitly tagged ``unordered``.  Row
    # order is asserted by default; a case whose result has no deterministic
    # order (aggregate/join/distinct/... without a global sort) must opt out via
    # the tag.  Deriving orderedness from the rendered plan text was rejected as
    # too loose: it silently sorts genuinely order-sensitive results, hiding real
    # ordering regressions from the golden.
    if "unordered" in tags:
        rows = sorted(rows)
    actual["expected_result"] = render_result_table(df.columns, rows)
    actual["expected_result_hash"] = hash_result_rows(rows)
    return actual


def _validate_test_file(test_file, header, cases, regenerate):
    """
    Fail loudly on malformed ``.test`` content.  A misspelled section or tag
    that is silently ignored makes a case assert less than it appears to (or
    nothing at all), so unknown names are errors, not noise.
    """
    unknown_header = set(header) - _KNOWN_HEADER_SECTIONS
    assert not unknown_header, "{}: unknown header sections: {}".format(
        test_file, ", ".join(sorted(unknown_header))
    )
    assert cases, "{}: no test cases found".format(test_file)
    for case in cases:
        assert case.get("name"), "{}: every test case needs a name".format(test_file)
        name = case["name"]
        assert case.get("script"), "{}: case `{}` needs a script".format(test_file, name)
        # Unknown sections are dropped and rewritten by regeneration, so only
        # reject them in verify mode.  Enforcing this during regeneration would
        # block the very migration regeneration exists to perform: a section
        # renamed or removed in the framework (e.g. the old
        # ``expected_analysis_error``/``expected_execution_error`` split folded
        # into ``expected_error``) leaves the on-disk file carrying a name no
        # longer in ``_CASE_SECTION_ORDER`` until it is regenerated.
        if not regenerate:
            unknown = set(case) - set(_CASE_SECTION_ORDER)
            assert not unknown, "{}: case `{}` has unknown sections: {}".format(
                test_file, name, ", ".join(sorted(unknown))
            )
        # Tags are preserved verbatim across regeneration, so an unknown tag
        # would persist; reject it in both modes.
        unknown_tags = parse_tags(case) - _KNOWN_TAGS
        assert not unknown_tags, "{}: case `{}` has unknown tags: {}".format(
            test_file, name, ", ".join(sorted(unknown_tags))
        )
        # In regenerate mode new cases legitimately have no expected_*
        # sections yet; in verify mode such a case would pass vacuously.
        if not regenerate:
            assert any(case.get(key) is not None for key in _RESULT_SECTIONS), (
                "{}: case `{}` has no expected_* sections and would assert "
                "nothing; regenerate the golden files".format(test_file, name)
            )
            # ``_compare_case`` only checks sections present in the file, so a
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
                    "{}: error case `{}` must carry only `expected_error`, "
                    "not also: {}".format(test_file, name, ", ".join(conflicting))
                )
            else:
                # The result table and its hash are a pair; dropping one leaves
                # the other asserting half the result, so require both or neither.
                assert has_result == has_hash, (
                    "{}: case `{}` must have both `expected_result` and "
                    "`expected_result_hash` or neither".format(test_file, name)
                )


def run_golden_test(test_case, spark, test_file):
    """
    Run all cases of a ``.test`` file.

    Parameters
    ----------
    test_case : unittest.TestCase
        The test case instance (for assertions).
    spark : SparkSession
        The session to run against.  The caller provides a fresh session per
        ``.test`` file (the connect counterpart of ``SQLQueryTestSuite``'s
        per-file ``newSession()``), so state created by case scripts - temp
        views, UDFs, confs - is discarded with the session and cannot leak
        into other files.
    test_file : str
        Absolute path to the ``.test`` file.
    """
    regenerate = os.environ.get("SPARK_GENERATE_GOLDEN_FILES") is not None
    base_dir = os.path.dirname(test_file)

    header, cases = parse_test_file(test_file, require_terminated=not regenerate)
    _validate_test_file(test_file, header, cases, regenerate)

    # Golden files are generated with ANSI mode on, matching the SQL golden
    # tests.  The session is discarded after the file, so nothing to restore.
    spark.conf.set("spark.sql.ansi.enabled", "true")

    regenerated_cases = []
    for case in cases:
        actual = compute_case_outputs(spark, case, base_dir)
        if regenerate:
            regenerated_cases.append(_regenerate_case(case, actual))
        else:
            _compare_case(test_case, case, actual)

    if regenerate:
        write_test_file(test_file, header, regenerated_cases)


def _regenerate_case(old_case, actual):
    """
    Build the regenerated form of *old_case* from this run's *actual* outputs.

    Every populated ``expected_*`` section from this run replaces the on-disk
    value; ``name`` / ``tags`` / ``script`` are carried over unchanged so the
    case's identity, ordering guard, and script pointer survive regeneration.
    """
    carried = {key: old_case.get(key) for key in ("name", "tags", "script")}
    carried.update(actual)
    return carried


def _compare_case(test_case, case, actual):
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
                "[{}] expected section `{}` but the case produced: {}".format(
                    name, key, produced
                )
            )
        test_case.assertEqual(
            expected.strip("\n"),
            got.strip("\n"),
            "[{}] mismatch in `{}`".format(name, key),
        )
