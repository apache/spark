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

These exercise the pure golden file plumbing -- parsing, serialization,
validation, output normalization, result rendering and case registration --
without a Spark session, so they are fast and run anywhere.  The end-to-end
golden runs that need a Spark Connect server live in the ``inputs/test_<topic>.py``
modules.
"""

import os
import tempfile
import unittest

from pyspark.sql.tests.df_golden.df_golden import (
    DFGoldenTestMixin,
    _validate_test_file,
    assert_cases_in_sync,
    case_tags,
    compare_case,
    format_double,
    format_error,
    format_tags,
    golden_file_for_input,
    hash_result_rows,
    parse_tags,
    parse_test_file,
    render_result_table,
    replace_not_included,
    unordered,
    write_test_file,
)


class DFGoldenFrameworkTests(unittest.TestCase):
    # -- parse / serialize ------------------------------------------------

    def _write(self, text):
        """Write *text* to a temp golden file and return its path."""
        fd, path = tempfile.mkstemp(suffix=".py.out")
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
        cases = parse_test_file(path)
        self.assertEqual(list(cases), ["my_case"])
        self.assertEqual(cases["my_case"]["expected_output_schema"], "struct<k:bigint>")

    def test_parse_extracts_file_metadata_header(self):
        path = self._write(
            "--! name\n"
            "__file_metadata__\n"
            "--! source\n"
            "pyspark.sql.tests.df_golden.inputs.test_group_by.GroupByGoldenTests\n"
            "!-- end\n"
            "\n\n"
            "--! name\n"
            "c1\n"
            "--! expected_output_schema\n"
            "struct<k:bigint>\n"
            "!-- end\n"
        )
        # The header block is validated but not returned: regeneration writes it
        # from the test class, so only the cases come back.
        self.assertEqual(list(parse_test_file(path)), ["c1"])

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
        cases = parse_test_file(path)
        self.assertEqual(
            cases["c"]["expected_analysis_output"],
            "Sort [k#x ASC], true\n+- Project\n   +- Range",
        )

    def test_round_trip_parse_write_parse(self):
        header = {"source": "pyspark.sql.tests.df_golden.inputs.test_group_by.GroupByGoldenTests"}
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
        self.assertEqual(parse_test_file(path), {case["name"]: case for case in cases})

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

    def test_unordered_decorator_tags_the_case_method(self):
        def plain(self, spark):
            pass

        @unordered
        def marked(self, spark):
            pass

        self.assertEqual(case_tags(plain), set())
        self.assertEqual(case_tags(marked), {"unordered"})

    def test_format_tags_renders_a_tags_section(self):
        self.assertEqual(format_tags({"unordered"}), "unordered")
        # Sorted, so the section a run produces is stable.
        self.assertEqual(format_tags({"b", "a"}), "a b")

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
        _validate_test_file("f.py.out", {"source": "x"}, [self._valid_case()])

    def test_validate_rejects_unknown_header_section(self):
        with self.assertRaisesRegex(AssertionError, "unknown header sections: bogus"):
            _validate_test_file("f.py.out", {"bogus": "x"}, [self._valid_case()])

    def test_validate_rejects_no_cases(self):
        with self.assertRaisesRegex(AssertionError, "no test cases found"):
            _validate_test_file("f.py.out", {}, [])

    def test_validate_rejects_case_without_name(self):
        with self.assertRaisesRegex(AssertionError, "every test case needs a name"):
            _validate_test_file("f.py.out", {}, [{"expected_output_schema": "x"}])

    def test_validate_rejects_unknown_section(self):
        with self.assertRaisesRegex(AssertionError, "unknown sections: expected_bogus"):
            _validate_test_file("f.py.out", {}, [self._valid_case(expected_bogus="x")])

    def test_validate_rejects_unknown_tag(self):
        with self.assertRaisesRegex(AssertionError, "unknown tags: wat"):
            _validate_test_file("f.py.out", {}, [self._valid_case(tags="wat")])

    def test_validate_accepts_known_unordered_tag(self):
        _validate_test_file("f.py.out", {}, [self._valid_case(tags="unordered")])

    def test_validate_rejects_vacuous_case(self):
        # A case with no expected_* section asserts nothing.
        with self.assertRaisesRegex(AssertionError, "would assert\n?.*nothing"):
            _validate_test_file("f.py.out", {}, [{"name": "c"}])

    def test_validate_accepts_error_only_case(self):
        # A case carrying only ``expected_error`` is not vacuous: the error is a
        # recognized result section, the single output an error case produces.
        _validate_test_file("f.py.out", {}, [{"name": "c", "expected_error": "[ERR] boom"}])

    def test_validate_rejects_duplicate_case_name(self):
        # Cases are keyed by name, so a duplicate would shadow its twin.
        with self.assertRaisesRegex(AssertionError, "duplicate test case `c`"):
            _validate_test_file("f.py.out", {}, [self._valid_case(), self._valid_case()])

    def test_validate_rejects_legacy_sections(self):
        # Sections dropped from the format (the analysis/execution error split
        # collapsed into ``expected_error``, the script pointer replaced by the
        # case method) are now unknown sections.
        for legacy in ("expected_analysis_error", "expected_execution_error", "script"):
            with self.assertRaisesRegex(AssertionError, "unknown sections: " + legacy):
                _validate_test_file("f.py.out", {}, [self._valid_case(**{legacy: "x"})])

    # -- case / golden file sync ------------------------------------------

    def test_assert_cases_in_sync_accepts_identical_lists(self):
        assert_cases_in_sync("f.py.out", ["a", "b"], ["a", "b"])

    def test_assert_cases_in_sync_rejects_case_missing_from_golden(self):
        with self.assertRaisesRegex(AssertionError, "no block in the golden file: b"):
            assert_cases_in_sync("f.py.out", ["a", "b"], ["a"])

    def test_assert_cases_in_sync_rejects_golden_block_without_case(self):
        with self.assertRaisesRegex(AssertionError, "no case method: b"):
            assert_cases_in_sync("f.py.out", ["a"], ["a", "b"])

    def test_assert_cases_in_sync_rejects_reordering(self):
        # Both sides are sorted by name, so a different order means the file was
        # hand-edited (or badly merged) and no longer matches the class.
        with self.assertRaisesRegex(AssertionError, "different order"):
            assert_cases_in_sync("f.py.out", ["a", "b"], ["b", "a"])

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
        _validate_test_file(
            "f.py.out",
            {},
            [self._valid_case(expected_result="printed all 0 rows.", expected_result_hash="h")],
        )

    def test_validate_rejects_result_without_hash(self):
        with self.assertRaisesRegex(AssertionError, "or neither"):
            _validate_test_file(
                "f.py.out", {}, [self._valid_case(expected_result="printed all 0 rows.")]
            )

    def test_validate_rejects_hash_without_result(self):
        with self.assertRaisesRegex(AssertionError, "or neither"):
            _validate_test_file("f.py.out", {}, [self._valid_case(expected_result_hash="h")])

    def test_validate_rejects_error_case_mixed_with_result(self):
        with self.assertRaisesRegex(AssertionError, "must carry only `expected_error`"):
            _validate_test_file(
                "f.py.out",
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
            _validate_test_file(
                "f.py.out",
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
        path = self._write("stray text\n--! name\nc\n--! expected_output_schema\nx\n!-- end\n")
        with self.assertRaisesRegex(AssertionError, "content outside any section"):
            parse_test_file(path)

    def test_parse_allows_blank_lines_between_blocks(self):
        # Blank separators outside sections are fine (not stray content).
        path = self._write(
            "--! name\nc1\n--! expected_output_schema\nx\n!-- end\n\n\n"
            "--! name\nc2\n--! expected_output_schema\ny\n!-- end\n"
        )
        self.assertEqual(list(parse_test_file(path)), ["c1", "c2"])

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

    def test_compare_case_checks_tags(self):
        # Tags are data: a golden file recording a tag the case no longer carries
        # is a mismatch, reported like any other section.
        compare_case(self, {"name": "c", "tags": "unordered"}, {"tags": "unordered"})
        with self.assertRaisesRegex(AssertionError, "expected section `tags`"):
            compare_case(self, {"name": "c", "tags": "unordered"}, {})

    # -- case registration ------------------------------------------------

    def test_mixin_registers_a_test_per_case_sorted_by_name(self):
        class Cases(DFGoldenTestMixin, unittest.TestCase):
            def _test_second(self, spark):
                """Second case."""

            def _test_first(self, spark):
                pass

        # Sorted, which is also the order unittest runs the cases in.
        self.assertEqual(Cases.case_names(), ["first", "second"])
        self.assertTrue(callable(Cases.test_second))
        self.assertTrue(callable(Cases.test_first))
        self.assertEqual(Cases.test_second.__doc__, "Second case.")

    def test_mixin_inherits_cases_from_base_classes(self):
        class Base(DFGoldenTestMixin, unittest.TestCase):
            def _test_inherited(self, spark):
                pass

        class Derived(Base):
            def _test_added(self, spark):
                pass

        self.assertEqual(Derived.case_names(), ["added", "inherited"])
        # An inherited case keeps the test method of the class declaring it
        # rather than getting a second copy on the subclass.
        self.assertNotIn("test_inherited", vars(Derived))
        self.assertTrue(callable(Derived.test_inherited))
        self.assertIn("test_added", vars(Derived))

    def test_mixin_allows_a_subclass_to_define_set_up_class(self):
        # A -> B(setUpClass) -> DFGoldenTestMixin is a legitimate chain: the
        # ordering requirement is checked when the class runs, not declared.
        class Base(DFGoldenTestMixin, unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                super().setUpClass()

        class Derived(Base):
            def _test_case(self, spark):
                pass

        self.assertEqual(Derived.case_names(), ["case"])

    # -- session lifecycle -------------------------------------------------

    def _fake_golden_class(self, fail_setup=False):
        """
        Return a golden test class over a fake session, and the list its client
        records ``release``/``close`` calls in.

        The caller drives the class the way a test runner does: ``setUpClass``,
        then ``tearDownClass`` (which a runner skips when ``setUpClass`` raises),
        then ``doClassCleanups``.
        """
        calls = []

        class FakeClient:
            def release_session(self):
                calls.append("release")

            def close(self):
                calls.append("close")

        class FakeConf:
            def set(self, key, value):
                pass

        class FakeSession:
            client = FakeClient()
            conf = FakeConf()

        class FakeProviderSession:
            is_stopped = False

            def newSession(self):
                return FakeSession()

        golden = self._write("--! name\nc\n--! expected_output_schema\nx\n!-- end\n")

        class Cases(DFGoldenTestMixin, unittest.TestCase):
            spark = FakeProviderSession()

            @classmethod
            def golden_file_path(cls):
                return golden

            @classmethod
            def setup_session(cls, spark):
                if fail_setup:
                    raise RuntimeError("setup_session failed")

            def _test_c(self, spark):
                pass

        # A runner resets the class cleanups before calling setUpClass.
        Cases._class_cleanups = []
        # The class runs its checking path; an ambient SPARK_GENERATE_GOLDEN_FILES
        # would send teardown down the regeneration path, writing the file instead.
        generating = os.environ.pop("SPARK_GENERATE_GOLDEN_FILES", None)
        if generating is not None:
            self.addCleanup(os.environ.update, {"SPARK_GENERATE_GOLDEN_FILES": generating})
        return Cases, calls

    def test_session_is_released_by_the_class_cleanup(self):
        cases, calls = self._fake_golden_class()
        cases.setUpClass()
        cases.tearDownClass()
        # Nothing releases the session before the cleanup does.
        self.assertEqual(calls, [])
        cases.doClassCleanups()
        self.assertEqual(calls, ["release", "close"])

    def test_session_is_released_when_set_up_class_fails(self):
        # A runner skips tearDownClass when setUpClass raises, so the cleanup is
        # what releases the session, here before the provider stopped anything.
        cases, calls = self._fake_golden_class(fail_setup=True)
        with self.assertRaisesRegex(RuntimeError, "setup_session failed"):
            cases.setUpClass()
        cases.doClassCleanups()
        self.assertEqual(calls, ["release", "close"])

    def test_session_release_is_skipped_once_the_provider_session_stopped(self):
        # The session it was made from is gone, and with it the server holding
        # the session to release; only the client is left to close.
        cases, calls = self._fake_golden_class()
        cases.setUpClass()
        cases.tearDownClass()
        cases.spark.is_stopped = True
        cases.doClassCleanups()
        self.assertEqual(calls, ["close"])

    # -- golden file location ---------------------------------------------

    def test_golden_file_mirrors_the_input_module_under_results(self):
        self.assertEqual(
            golden_file_for_input(os.path.join("df_golden", "inputs", "test_group_by.py")),
            os.path.join(os.path.abspath("df_golden"), "results", "test_group_by.py.out"),
        )

    def test_golden_file_rejects_a_module_outside_inputs(self):
        # A module elsewhere has no ``results`` directory to pair with.
        with self.assertRaisesRegex(AssertionError, "must live in the `inputs` directory"):
            golden_file_for_input(os.path.join("df_golden", "test_group_by.py"))

    def test_mixin_derives_the_golden_file_from_its_module(self):
        # This test module is not under ``inputs``, which is what the mixin
        # resolves against, so the derivation reports that rather than guessing.
        class Cases(DFGoldenTestMixin, unittest.TestCase):
            pass

        with self.assertRaisesRegex(AssertionError, "must live in the `inputs` directory"):
            Cases.golden_file_path()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
