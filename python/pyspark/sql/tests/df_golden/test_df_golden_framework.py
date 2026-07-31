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
Unit tests for the ``pyspark.sql.tests.df_golden.df_golden`` write/validation machinery.

These exercise the pure ``.test`` file plumbing -- parsing, serialization,
validation, output normalization, result rendering and case registration --
without a Spark session, so they are fast and run anywhere.  The end-to-end
golden runs that need a Spark Connect server live in the ``test_<topic>.py``
modules next to this one.
"""

import os
import tempfile
import unittest

from pyspark.sql.tests.df_golden.df_golden import (
    DFGoldenTestMixin,
    build_golden_case,
    check_cases_in_sync,
    compare_case,
    format_double,
    format_error,
    hash_result_rows,
    is_unordered,
    parse_tags,
    parse_test_file,
    render_result_table,
    replace_not_included,
    unordered,
    validate_test_file,
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
            "my_case\n"
            "--! expected_output_schema\n"
            "struct<k:bigint>\n"
            "!-- end\n"
        )
        header, cases = parse_test_file(path)
        self.assertEqual(header, {})
        self.assertEqual(len(cases), 1)
        self.assertEqual(cases[0]["name"], "my_case")
        self.assertEqual(cases[0]["expected_output_schema"], "struct<k:bigint>")

    def test_parse_extracts_file_metadata_header(self):
        path = self._write(
            "--! name\n"
            "__file_metadata__\n"
            "--! source\n"
            "pyspark.sql.tests.df_golden.test_group_by.GroupByGoldenTests\n"
            "!-- end\n"
            "\n\n"
            "--! name\n"
            "c1\n"
            "--! expected_output_schema\n"
            "struct<k:bigint>\n"
            "!-- end\n"
        )
        header, cases = parse_test_file(path)
        # The header block is lifted out and its synthetic name dropped.
        self.assertEqual(
            header,
            {"source": "pyspark.sql.tests.df_golden.test_group_by.GroupByGoldenTests"},
        )
        self.assertEqual(len(cases), 1)
        self.assertEqual(cases[0]["name"], "c1")

    def test_parse_preserves_multiline_section_body(self):
        path = self._write(
            "--! name\n"
            "c\n"
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
        header = {"source": "pyspark.sql.tests.df_golden.test_group_by.GroupByGoldenTests"}
        cases = [
            {
                "name": "ordered_case",
                "expected_analysis_output": "Sort [k#x ASC], true\n+- Range",
                "expected_output_schema": "struct<k:bigint>",
                "expected_result": "+---+\n| k |\n+---+\n| 1 |\n+---+\nprinted all 1 rows.",
                "expected_result_hash": "abc123",
            },
            {
                "name": "error_case",
                "tags": "unordered",
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
            [{"expected_output_schema": "struct<k:bigint>", "name": "c", "junk": "ignored"}],
        )
        with open(path) as f:
            body = f.read()
        self.assertNotIn("junk", body)
        # name precedes the outputs in the canonical order regardless of dict order.
        self.assertLess(body.index("--! name"), body.index("--! expected_output_schema"))

    # -- tags -------------------------------------------------------------

    def test_parse_tags_splits_on_whitespace_and_commas(self):
        self.assertEqual(parse_tags({"tags": "unordered, foo  bar"}), {"unordered", "foo", "bar"})
        self.assertEqual(parse_tags({}), set())
        self.assertEqual(parse_tags({"tags": ""}), set())

    def test_unordered_decorator_marks_the_case_method(self):
        def plain(self, spark):
            pass

        @unordered
        def marked(self, spark):
            pass

        self.assertFalse(is_unordered(plain))
        self.assertTrue(is_unordered(marked))

    # -- validation -------------------------------------------------------

    def _valid_case(self, **overrides):
        case = {
            "name": "c",
            "expected_output_schema": "struct<k:bigint>",
        }
        case.update(overrides)
        return case

    def test_validate_accepts_well_formed_file(self):
        # Should not raise.
        validate_test_file("f.test", {"source": "x"}, [self._valid_case()])

    def test_validate_rejects_unknown_header_section(self):
        with self.assertRaisesRegex(AssertionError, "unknown header sections: bogus"):
            validate_test_file("f.test", {"bogus": "x"}, [self._valid_case()])

    def test_validate_rejects_no_cases(self):
        with self.assertRaisesRegex(AssertionError, "no test cases found"):
            validate_test_file("f.test", {}, [])

    def test_validate_rejects_case_without_name(self):
        with self.assertRaisesRegex(AssertionError, "every test case needs a name"):
            validate_test_file("f.test", {}, [{"expected_output_schema": "x"}])

    def test_validate_rejects_unknown_section(self):
        with self.assertRaisesRegex(AssertionError, "unknown sections: expected_bogus"):
            validate_test_file("f.test", {}, [self._valid_case(expected_bogus="x")])

    def test_validate_rejects_unknown_tag(self):
        with self.assertRaisesRegex(AssertionError, "unknown tags: wat"):
            validate_test_file("f.test", {}, [self._valid_case(tags="wat")])

    def test_validate_accepts_known_unordered_tag(self):
        validate_test_file("f.test", {}, [self._valid_case(tags="unordered")])

    def test_validate_rejects_vacuous_case(self):
        # A case with no expected_* section asserts nothing.
        with self.assertRaisesRegex(AssertionError, "would assert\n?.*nothing"):
            validate_test_file("f.test", {}, [{"name": "c"}])

    def test_validate_accepts_error_only_case(self):
        # A case carrying only ``expected_error`` is not vacuous: the error is a
        # recognized result section, the single output an error case produces.
        validate_test_file("f.test", {}, [{"name": "c", "expected_error": "[ERR] boom"}])

    def test_validate_rejects_legacy_sections(self):
        # Sections dropped from the format (the analysis/execution error split
        # collapsed into ``expected_error``, the script pointer replaced by the
        # case method) are now unknown sections.
        for legacy in ("expected_analysis_error", "expected_execution_error", "script"):
            with self.assertRaisesRegex(AssertionError, "unknown sections: " + legacy):
                validate_test_file("f.test", {}, [self._valid_case(**{legacy: "x"})])

    # -- case / golden file sync ------------------------------------------

    def test_check_cases_in_sync_accepts_identical_lists(self):
        check_cases_in_sync("f.test", ["a", "b"], ["a", "b"])

    def test_check_cases_in_sync_rejects_case_missing_from_golden(self):
        with self.assertRaisesRegex(AssertionError, "no block in the golden file: b"):
            check_cases_in_sync("f.test", ["a", "b"], ["a"])

    def test_check_cases_in_sync_rejects_golden_block_without_case(self):
        with self.assertRaisesRegex(AssertionError, "no case method: b"):
            check_cases_in_sync("f.test", ["a"], ["a", "b"])

    def test_check_cases_in_sync_rejects_reordering(self):
        # The golden file is written in declaration order, so a different order
        # means the file no longer matches the class.
        with self.assertRaisesRegex(AssertionError, "different order"):
            check_cases_in_sync("f.test", ["a", "b"], ["b", "a"])

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
            '"__truediv__" was called from /abs/path/test_arithmetic.py:7\n'
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
        compare_case(self, case, {"expected_output_schema": "struct<k:bigint>"})

    def test_compare_case_ignores_sections_absent_from_golden(self):
        # Only sections present in the golden file are checked; extras in
        # ``actual`` are ignored.
        compare_case(
            self,
            {"name": "c", "expected_output_schema": "struct<k:bigint>"},
            {
                "expected_output_schema": "struct<k:bigint>",
                "expected_optimized_output": "Range",
            },
        )

    def test_compare_case_fails_on_value_mismatch(self):
        with self.assertRaises(AssertionError):
            compare_case(
                self,
                {"name": "c", "expected_output_schema": "struct<k:bigint>"},
                {"expected_output_schema": "struct<v:string>"},
            )

    def test_compare_case_fails_when_expected_section_not_produced(self):
        with self.assertRaisesRegex(AssertionError, "expected section `expected_result`"):
            compare_case(
                self,
                {"name": "c", "expected_result": "printed all 0 rows."},
                {"expected_error": "[ERR] boom"},
            )

    # -- under-assertion guards ------------------------------------------

    def test_validate_accepts_result_with_hash(self):
        validate_test_file(
            "f.test",
            {},
            [self._valid_case(expected_result="printed all 0 rows.", expected_result_hash="h")],
        )

    def test_validate_rejects_result_without_hash(self):
        with self.assertRaisesRegex(AssertionError, "or neither"):
            validate_test_file(
                "f.test", {}, [self._valid_case(expected_result="printed all 0 rows.")]
            )

    def test_validate_rejects_hash_without_result(self):
        with self.assertRaisesRegex(AssertionError, "or neither"):
            validate_test_file("f.test", {}, [self._valid_case(expected_result_hash="h")])

    def test_validate_rejects_error_case_mixed_with_result(self):
        with self.assertRaisesRegex(AssertionError, "must carry only `expected_error`"):
            validate_test_file(
                "f.test",
                {},
                [
                    {
                        "name": "c",
                        "expected_error": "[ERR] boom",
                        "expected_result": "printed all 0 rows.",
                        "expected_result_hash": "h",
                    }
                ],
            )

    def test_validate_rejects_error_case_mixed_with_plan(self):
        with self.assertRaisesRegex(AssertionError, "must carry only `expected_error`"):
            validate_test_file(
                "f.test",
                {},
                [
                    {
                        "name": "c",
                        "expected_error": "[ERR] boom",
                        "expected_analysis_output": "Range",
                    }
                ],
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
        from pyspark.sql.tests.df_golden.df_golden import _format_value

        with self.assertRaisesRegex(AssertionError, "not supported yet"):
            _format_value(0.1, FloatType())
        # double does not raise.
        self.assertEqual(_format_value(0.5, DoubleType()), "0.5")

    # -- loud parser failures --------------------------------------------

    def test_parse_rejects_duplicate_section(self):
        path = self._write(
            "--! name\nc\n--! expected_output_schema\nx\n--! expected_output_schema\ny\n!-- end\n"
        )
        with self.assertRaisesRegex(AssertionError, "duplicate section `expected_output_schema`"):
            parse_test_file(path)

    def test_parse_rejects_malformed_marker(self):
        # Missing space after "--!": a typo'd marker, not body text.
        path = self._write("--! name\nc\n--!expected_result\nx\n!-- end\n")
        with self.assertRaisesRegex(AssertionError, "malformed section marker"):
            parse_test_file(path)

    def test_parse_rejects_stray_content_outside_section(self):
        path = self._write("stray text\n--! name\nc\n!-- end\n")
        with self.assertRaisesRegex(AssertionError, "content outside any section"):
            parse_test_file(path)

    def test_parse_allows_blank_lines_between_blocks(self):
        # Blank separators outside sections are fine (not stray content).
        path = self._write("--! name\nc1\n!-- end\n\n\n--! name\nc2\n!-- end\n")
        _, cases = parse_test_file(path)
        self.assertEqual([c["name"] for c in cases], ["c1", "c2"])

    def test_parse_rejects_unterminated_trailing_case(self):
        # Last case missing "!-- end", i.e. a truncated file.
        path = self._write("--! name\nc\n--! expected_output_schema\nx\n")
        with self.assertRaisesRegex(AssertionError, "does not end with"):
            parse_test_file(path)

    def test_format_value_refuses_tab_or_newline_in_string(self):
        try:
            from pyspark.sql.types import StringType
        except Exception:
            self.skipTest("pyspark.sql.types unavailable in this environment")
        from pyspark.sql.tests.df_golden.df_golden import _format_value

        self.assertEqual(_format_value("ok", StringType()), "ok")
        for bad in ("a\tb", "a\nb"):
            with self.assertRaisesRegex(AssertionError, "tab or newline"):
                _format_value(bad, StringType())

    # -- regeneration ----------------------------------------------------

    def test_build_golden_case_writes_identity_from_the_case_method(self):
        actual = {
            "expected_optimized_output": "new base",
            "expected_output_schema": "new schema",
        }
        case = build_golden_case("c", False, actual)
        self.assertEqual(case["name"], "c")
        self.assertNotIn("tags", case)
        self.assertEqual(case["expected_optimized_output"], "new base")
        self.assertEqual(case["expected_output_schema"], "new schema")
        # An unordered case records the tag that explains its sorted rows.
        self.assertEqual(build_golden_case("c", True, actual)["tags"], "unordered")

    # -- case registration ------------------------------------------------

    def test_mixin_registers_a_test_per_case_in_declaration_order(self):
        class Cases(DFGoldenTestMixin, unittest.TestCase):
            golden_file = "x.test"

            def _test_second(self, spark):
                """Second case."""

            def _test_first(self, spark):
                pass

        # Declaration order, not alphabetical: the golden file is written in it.
        self.assertEqual(Cases.case_names(), ["second", "first"])
        self.assertTrue(callable(Cases.test_second))
        self.assertTrue(callable(Cases.test_first))
        self.assertEqual(Cases.test_second.__doc__, "Second case.")

    def test_mixin_inherits_cases_from_base_classes(self):
        class Base(DFGoldenTestMixin, unittest.TestCase):
            golden_file = "x.test"

            def _test_inherited(self, spark):
                pass

        class Derived(Base):
            def _test_added(self, spark):
                pass

        self.assertEqual(Derived.case_names(), ["inherited", "added"])

    def test_mixin_rejects_wrong_inheritance_order(self):
        # The mixin's setUpClass has to run after the session exists, which only
        # happens when it precedes the session-providing class.
        with self.assertRaisesRegex(TypeError, "must be listed before"):

            class Wrong(unittest.TestCase, DFGoldenTestMixin):
                golden_file = "x.test"

    def test_mixin_resolves_golden_file_next_to_the_test_module(self):
        class Cases(DFGoldenTestMixin, unittest.TestCase):
            golden_file = "group_by.test"

        self.assertEqual(
            Cases.golden_file_path(),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "group_by.test"),
        )

    def test_mixin_requires_a_golden_file(self):
        class Cases(DFGoldenTestMixin, unittest.TestCase):
            pass

        with self.assertRaisesRegex(AssertionError, "set `golden_file`"):
            Cases.golden_file_path()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
