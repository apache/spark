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
Unit tests for the ``pyspark.testing.df_golden`` write/validation machinery.

These exercise the pure ``.test`` file plumbing -- parsing, serialization,
validation, output normalization and result rendering -- without a Spark
session, so they are fast and run anywhere.  The end-to-end golden runs that
need a Spark Connect server live in ``test_df_golden.py``.
"""

import os
import tempfile
import unittest

from pyspark.testing.df_golden import (
    _compare_case,
    _regenerate_case,
    _validate_test_file,
    format_double,
    format_error,
    hash_result_rows,
    parse_tags,
    parse_test_file,
    render_result_table,
    replace_not_included,
    write_test_file,
)


class DFGoldenFrameworkTests(unittest.TestCase):
    # -- parse / serialize ------------------------------------------------

    def _write(self, text):
        """Write *text* to a temp ``.test`` file and return its path."""
        fd, path = tempfile.mkstemp(suffix=".test")
        os.close(fd)
        with open(path, "w") as f:
            f.write(text)
        self.addCleanup(os.remove, path)
        return path

    def test_parse_basic_case(self):
        path = self._write(
            "--! name\n"
            "my case\n"
            "--! script\n"
            "scripts/x.py\n"
            "--! expected_output_schema\n"
            "struct<k:bigint>\n"
            "!-- end\n"
        )
        header, cases = parse_test_file(path)
        self.assertEqual(header, {})
        self.assertEqual(len(cases), 1)
        self.assertEqual(cases[0]["name"], "my case")
        self.assertEqual(cases[0]["script"], "scripts/x.py")
        self.assertEqual(cases[0]["expected_output_schema"], "struct<k:bigint>")

    def test_parse_extracts_file_metadata_header(self):
        path = self._write(
            "--! name\n"
            "__file_metadata__\n"
            "--! source\n"
            "df_golden/group_by\n"
            "!-- end\n"
            "\n\n"
            "--! name\n"
            "c1\n"
            "--! script\n"
            "scripts/a.py\n"
            "!-- end\n"
        )
        header, cases = parse_test_file(path)
        # The header block is lifted out and its synthetic name dropped.
        self.assertEqual(header, {"source": "df_golden/group_by"})
        self.assertEqual(len(cases), 1)
        self.assertEqual(cases[0]["name"], "c1")

    def test_parse_preserves_multiline_section_body(self):
        path = self._write(
            "--! name\n"
            "c\n"
            "--! script\n"
            "scripts/a.py\n"
            "--! expected_analysis_output\n"
            "Sort [k#x ASC], true\n"
            "+- Project\n"
            "   +- Range\n"
            "!-- end\n"
        )
        _, cases = parse_test_file(path)
        self.assertEqual(
            cases[0]["expected_analysis_output"],
            "Sort [k#x ASC], true\n+- Project\n   +- Range",
        )

    def test_round_trip_parse_write_parse(self):
        header = {"source": "df_golden/group_by"}
        cases = [
            {
                "name": "ordered case",
                "script": "scripts/a.py",
                "expected_analysis_output": "Sort [k#x ASC], true\n+- Range",
                "expected_output_schema": "struct<k:bigint>",
                "expected_result": "+---+\n| k |\n+---+\n| 1 |\n+---+\nprinted all 1 rows.",
                "expected_result_hash": "abc123",
            },
            {
                "name": "error case",
                "tags": "unordered",
                "script": "scripts/b.py",
                "expected_error": "[SOME_ERROR] boom",
            },
        ]
        path = self._write("")
        write_test_file(path, header, cases)
        header2, cases2 = parse_test_file(path)
        self.assertEqual(header2, header)
        self.assertEqual(cases2, cases)

    def test_write_only_emits_known_sections_in_order(self):
        path = self._write("")
        # ``junk`` is not in the canonical section order and must be dropped.
        write_test_file(
            path,
            {},
            [{"script": "scripts/a.py", "name": "c", "junk": "ignored"}],
        )
        with open(path) as f:
            body = f.read()
        self.assertNotIn("junk", body)
        # name precedes script in the canonical order regardless of dict order.
        self.assertLess(body.index("--! name"), body.index("--! script"))

    # -- tags -------------------------------------------------------------

    def test_parse_tags_splits_on_whitespace_and_commas(self):
        self.assertEqual(
            parse_tags({"tags": "unordered, foo  bar"}), {"unordered", "foo", "bar"}
        )
        self.assertEqual(parse_tags({}), set())
        self.assertEqual(parse_tags({"tags": ""}), set())

    # -- validation -------------------------------------------------------

    def _valid_case(self, **overrides):
        case = {
            "name": "c",
            "script": "scripts/a.py",
            "expected_output_schema": "struct<k:bigint>",
        }
        case.update(overrides)
        return case

    def test_validate_accepts_well_formed_file(self):
        # Should not raise.
        _validate_test_file(
            "f.test", {"source": "x"}, [self._valid_case()], regenerate=False
        )

    def test_validate_rejects_unknown_header_section(self):
        with self.assertRaisesRegex(AssertionError, "unknown header sections: bogus"):
            _validate_test_file(
                "f.test", {"bogus": "x"}, [self._valid_case()], regenerate=False
            )

    def test_validate_rejects_no_cases(self):
        with self.assertRaisesRegex(AssertionError, "no test cases found"):
            _validate_test_file("f.test", {}, [], regenerate=False)

    def test_validate_rejects_case_without_name(self):
        with self.assertRaisesRegex(AssertionError, "every test case needs a name"):
            _validate_test_file(
                "f.test", {}, [{"script": "scripts/a.py"}], regenerate=False
            )

    def test_validate_rejects_case_without_script(self):
        with self.assertRaisesRegex(AssertionError, "needs a script"):
            _validate_test_file(
                "f.test",
                {},
                [{"name": "c", "expected_output_schema": "x"}],
                regenerate=False,
            )

    def test_validate_rejects_unknown_section(self):
        with self.assertRaisesRegex(AssertionError, "unknown sections: expected_bogus"):
            _validate_test_file(
                "f.test", {}, [self._valid_case(expected_bogus="x")], regenerate=False
            )

    def test_validate_rejects_unknown_tag(self):
        with self.assertRaisesRegex(AssertionError, "unknown tags: wat"):
            _validate_test_file(
                "f.test", {}, [self._valid_case(tags="wat")], regenerate=False
            )

    def test_validate_accepts_known_unordered_tag(self):
        _validate_test_file(
            "f.test", {}, [self._valid_case(tags="unordered")], regenerate=False
        )

    def test_validate_rejects_vacuous_case_in_verify_mode(self):
        # A case with no expected_* section asserts nothing.
        with self.assertRaisesRegex(AssertionError, "would assert\n?.*nothing"):
            _validate_test_file(
                "f.test", {}, [{"name": "c", "script": "scripts/a.py"}], regenerate=False
            )

    def test_validate_allows_vacuous_case_in_regenerate_mode(self):
        # New cases legitimately have no expected_* sections before regeneration.
        _validate_test_file(
            "f.test", {}, [{"name": "c", "script": "scripts/a.py"}], regenerate=True
        )

    def test_validate_accepts_error_only_case(self):
        # A case carrying only ``expected_error`` is not vacuous: the error is a
        # recognized result section, the single output an error case produces.
        _validate_test_file(
            "f.test",
            {},
            [{"name": "c", "script": "scripts/a.py", "expected_error": "[ERR] boom"}],
            regenerate=False,
        )

    def test_validate_rejects_legacy_split_error_sections(self):
        # The old analysis/execution split was collapsed into ``expected_error``;
        # the legacy names are now unknown sections in verify mode.
        for legacy in ("expected_analysis_error", "expected_execution_error"):
            with self.assertRaisesRegex(AssertionError, "unknown sections: " + legacy):
                _validate_test_file(
                    "f.test", {}, [self._valid_case(**{legacy: "x"})], regenerate=False
                )

    def test_validate_tolerates_unknown_sections_in_regenerate_mode(self):
        # Regeneration drops and rewrites unknown sections, so a file still
        # carrying a renamed/removed section (e.g. the legacy error split) must
        # not be rejected during regeneration - otherwise it could never be
        # migrated.
        _validate_test_file(
            "f.test",
            {},
            [self._valid_case(expected_analysis_error="x")],
            regenerate=True,
        )

    def test_validate_rejects_unknown_tag_even_in_regenerate_mode(self):
        # Tags are preserved verbatim across regeneration, so an unknown tag
        # would persist; it is rejected in both modes.
        with self.assertRaisesRegex(AssertionError, "unknown tags: wat"):
            _validate_test_file(
                "f.test", {}, [self._valid_case(tags="wat")], regenerate=True
            )

    # -- output normalization --------------------------------------------

    def test_replace_not_included_normalizes_volatile_ids(self):
        self.assertEqual(replace_not_included("k#1234 + v#5"), "k#x + v#x")
        self.assertEqual(replace_not_included("plan_id=42"), "plan_id=x")
        self.assertEqual(
            replace_not_included("CTERelationDef 17, false"),
            "CTERelationDef xxxx, false",
        )

    def test_format_error_strips_volatile_trailers(self):
        msg = (
            "[DIVIDE_BY_ZERO] Division by zero. SQLSTATE: 22012\n"
            "== DataFrame ==\n"
            '"__truediv__" was called from /abs/path/script.py:7\n'
            "\n"
            "JVM stacktrace:\n"
            "org.apache.spark.SparkArithmeticException: ..."
        )
        self.assertEqual(
            format_error(Exception(msg)),
            "[DIVIDE_BY_ZERO] Division by zero. SQLSTATE: 22012",
        )

    def test_format_error_strips_trailing_plan_dump(self):
        self.assertEqual(
            format_error(Exception("[ERR] bad column;\nProject [a#1]\n+- Range")),
            "[ERR] bad column",
        )

    def test_format_error_keeps_message_with_internal_semicolon_newline(self):
        # ";\n" not followed by a plan root (uppercase / "'") is part of the
        # message and must be preserved, not treated as the plan separator.
        msg = "[ERR] first clause;\nand the second clause continues"
        self.assertEqual(format_error(Exception(msg)), msg)

    # -- result rendering -------------------------------------------------

    def test_render_result_table_pads_columns(self):
        table = render_result_table(["k", "v"], ["1\t10", "200\t3"])
        self.assertEqual(
            table,
            "\n".join(
                [
                    "+-----+----+",
                    "| k   | v  |",
                    "+-----+----+",
                    "| 1   | 10 |",
                    "| 200 | 3  |",
                    "+-----+----+",
                    "printed all 2 rows.",
                ]
            ),
        )

    def test_render_result_table_no_columns_is_trailer_only(self):
        self.assertEqual(render_result_table([], []), "printed all 0 rows.")

    def test_hash_result_rows_is_stable_and_order_sensitive(self):
        h1 = hash_result_rows(["a", "b"])
        self.assertEqual(h1, hash_result_rows(["a", "b"]))
        self.assertNotEqual(h1, hash_result_rows(["b", "a"]))

    # -- case comparison --------------------------------------------------

    def test_compare_case_passes_on_match(self):
        case = {
            "name": "c",
            "expected_output_schema": "struct<k:bigint>",
        }
        _compare_case(self, case, {"expected_output_schema": "struct<k:bigint>"})

    def test_compare_case_ignores_sections_absent_from_golden(self):
        # Only sections present in the golden file are checked; extras in
        # ``actual`` are ignored.
        _compare_case(
            self,
            {"name": "c", "expected_output_schema": "struct<k:bigint>"},
            {
                "expected_output_schema": "struct<k:bigint>",
                "expected_optimized_output": "Range",
            },
        )

    def test_compare_case_fails_on_value_mismatch(self):
        with self.assertRaises(AssertionError):
            _compare_case(
                self,
                {"name": "c", "expected_output_schema": "struct<k:bigint>"},
                {"expected_output_schema": "struct<v:string>"},
            )

    def test_compare_case_fails_when_expected_section_not_produced(self):
        with self.assertRaisesRegex(AssertionError, "expected section `expected_result`"):
            _compare_case(
                self,
                {"name": "c", "expected_result": "printed all 0 rows."},
                {"expected_error": "[ERR] boom"},
            )

    # -- under-assertion guards ------------------------------------------

    def test_validate_accepts_result_with_hash(self):
        _validate_test_file(
            "f.test",
            {},
            [
                self._valid_case(
                    expected_result="printed all 0 rows.", expected_result_hash="h"
                )
            ],
            regenerate=False,
        )

    def test_validate_rejects_result_without_hash(self):
        with self.assertRaisesRegex(AssertionError, "or neither"):
            _validate_test_file(
                "f.test",
                {},
                [self._valid_case(expected_result="printed all 0 rows.")],
                regenerate=False,
            )

    def test_validate_rejects_hash_without_result(self):
        with self.assertRaisesRegex(AssertionError, "or neither"):
            _validate_test_file(
                "f.test", {}, [self._valid_case(expected_result_hash="h")], regenerate=False
            )

    def test_validate_rejects_error_case_mixed_with_result(self):
        with self.assertRaisesRegex(AssertionError, "must carry only `expected_error`"):
            _validate_test_file(
                "f.test",
                {},
                [
                    {
                        "name": "c",
                        "script": "scripts/a.py",
                        "expected_error": "[ERR] boom",
                        "expected_result": "printed all 0 rows.",
                        "expected_result_hash": "h",
                    }
                ],
                regenerate=False,
            )

    def test_validate_rejects_error_case_mixed_with_plan(self):
        with self.assertRaisesRegex(AssertionError, "must carry only `expected_error`"):
            _validate_test_file(
                "f.test",
                {},
                [
                    {
                        "name": "c",
                        "script": "scripts/a.py",
                        "expected_error": "[ERR] boom",
                        "expected_analysis_output": "Range",
                    }
                ],
                regenerate=False,
            )

    # -- double formatting / float refusal --------------------------------

    def test_format_double_matches_java_double_to_string(self):
        # Hive renders doubles via Java Double.toString
        # (HiveResult: ``case (n, _: NumericType) => n.toString``); these are the
        # cases where it diverges from Python's str()/repr.
        # A list (not a dict): 0.0 and -0.0 compare equal and would collide as
        # dict keys.
        cases = [
            (1.0, "1.0"),
            (100.0, "100.0"),
            (0.5, "0.5"),
            (0.001, "0.001"),
            (2.142857142857, "2.142857142857"),
            (-0.272380105815, "-0.272380105815"),
            (1e7, "1.0E7"),
            (1e-4, "1.0E-4"),
            (12345678.0, "1.2345678E7"),
            (1234567.0, "1234567.0"),
            (1e20, "1.0E20"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (float("nan"), "NaN"),
            (float("inf"), "Infinity"),
            (float("-inf"), "-Infinity"),
        ]
        for value, expected in cases:
            self.assertEqual(format_double(value), expected, "format_double(%r)" % value)

    def test_format_value_refuses_float(self):
        # double is supported via format_double; float still needs float32-
        # shortest rendering to match Java Float.toString, so it is refused.
        try:
            from pyspark.sql.types import DoubleType, FloatType
        except Exception:
            self.skipTest("pyspark.sql.types unavailable in this environment")
        from pyspark.testing.df_golden import _format_value

        with self.assertRaisesRegex(AssertionError, "not supported yet"):
            _format_value(0.1, FloatType())
        # double does not raise.
        self.assertEqual(_format_value(0.5, DoubleType()), "0.5")

    # -- loud parser failures --------------------------------------------

    def test_parse_rejects_duplicate_section(self):
        path = self._write(
            "--! name\nc\n--! script\nscripts/a.py\n--! script\nscripts/b.py\n!-- end\n"
        )
        with self.assertRaisesRegex(AssertionError, "duplicate section `script`"):
            parse_test_file(path)

    def test_parse_rejects_malformed_marker(self):
        # Missing space after "--!": a typo'd marker, not body text.
        path = self._write(
            "--! name\nc\n--! script\nscripts/a.py\n--!expected_result\nx\n!-- end\n"
        )
        with self.assertRaisesRegex(AssertionError, "malformed section marker"):
            parse_test_file(path)

    def test_parse_rejects_stray_content_outside_section(self):
        path = self._write(
            "stray text\n--! name\nc\n--! script\nscripts/a.py\n!-- end\n"
        )
        with self.assertRaisesRegex(AssertionError, "content outside any section"):
            parse_test_file(path)

    def test_parse_allows_blank_lines_between_blocks(self):
        # Blank separators outside sections are fine (not stray content).
        path = self._write(
            "--! name\nc1\n--! script\nscripts/a.py\n!-- end\n\n\n"
            "--! name\nc2\n--! script\nscripts/b.py\n!-- end\n"
        )
        _, cases = parse_test_file(path)
        self.assertEqual([c["name"] for c in cases], ["c1", "c2"])

    def test_parse_unterminated_trailing_case(self):
        # Last case missing "!-- end".
        path = self._write("--! name\nc\n--! script\nscripts/a.py\n")
        # Lenient by default (regeneration rewrites with terminators):
        _, cases = parse_test_file(path)
        self.assertEqual(len(cases), 1)
        # Strict (verify mode) rejects it:
        with self.assertRaisesRegex(AssertionError, "does not end with"):
            parse_test_file(path, require_terminated=True)

    def test_format_value_refuses_tab_or_newline_in_string(self):
        try:
            from pyspark.sql.types import StringType
        except Exception:
            self.skipTest("pyspark.sql.types unavailable in this environment")
        from pyspark.testing.df_golden import _format_value

        self.assertEqual(_format_value("ok", StringType()), "ok")
        for bad in ("a\tb", "a\nb"):
            with self.assertRaisesRegex(AssertionError, "tab or newline"):
                _format_value(bad, StringType())

    # -- regeneration ----------------------------------------------------

    def test_regenerate_replaces_expected_sections_and_carries_identity(self):
        old = {
            "name": "c",
            "tags": "unordered",
            "script": "scripts/a.py",
            "expected_optimized_output": "old base",
            "expected_output_schema": "old schema",
        }
        actual = {
            "expected_optimized_output": "new base",
            "expected_output_schema": "new schema",
        }
        new = _regenerate_case(old, actual)
        # Name / tags / script are carried over; expected_* come from this run.
        self.assertEqual(new["name"], "c")
        self.assertEqual(new["tags"], "unordered")
        self.assertEqual(new["script"], "scripts/a.py")
        self.assertEqual(new["expected_optimized_output"], "new base")
        self.assertEqual(new["expected_output_schema"], "new schema")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
