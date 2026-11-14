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
and the life cycle of a golden test class over a fake session, all without a
Spark session, so they are fast and run anywhere.  The end-to-end golden runs
that need a Spark Connect server live in the ``inputs/test_<topic>.py`` modules.
"""

import os
import tempfile
import unittest
from unittest import mock

from pyspark.sql.tests.df_golden import df_golden
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
            "--! name\nmy_case\n--! expected_output_schema\nstruct<k:bigint>\n!-- end\n"
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

    def test_compare_case_checks_tags(self):
        # Tags are data: a golden file recording a tag the case no longer carries
        # is a mismatch, reported like any other section.
        compare_case(self, {"name": "c", "tags": "unordered"}, {"tags": "unordered"})
        with self.assertRaisesRegex(AssertionError, "expected section `tags`"):
            compare_case(self, {"name": "c", "tags": "unordered"}, {})

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

    def test_mixin_registers_cases_declared_on_a_plain_base_class(self):
        # A class that knows nothing of the mixin registers no test methods, so
        # the golden test class mixing it in does that for the cases it brings.
        class Shared:
            def _test_shared(self, spark):
                """Shared case."""

        class Cases(Shared, DFGoldenTestMixin, unittest.TestCase):
            def _test_own(self, spark):
                pass

        self.assertEqual(Cases.case_names(), ["own", "shared"])
        # Every case has a test: one that is counted but never run would make
        # regeneration refuse, and the golden file demand a block no run writes.
        self.assertEqual(
            unittest.defaultTestLoader.getTestCaseNames(Cases), ["test_own", "test_shared"]
        )
        self.assertEqual(Cases.test_shared.__doc__, "Shared case.")

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


class DFGoldenTestMixinTests(unittest.TestCase):
    """
    The life cycle of a golden test class, run over a fake session.

    Each test declares a golden test class and hands it to a real unittest
    runner, so that its ``setUpClass``, registered tests, ``tearDownClass`` and
    class cleanups run in the order, and under the conditions, of an actual run.
    """

    # The golden file of a class whose only case is ``c``.
    GOLDEN_C = "--! name\nc\n--! expected_output_schema\nstruct<_test_c:int>\n!-- end\n"

    def setUp(self):
        # How a golden test class runs depends on the environment, so each test
        # starts from a known one, and the real one is put back afterwards.
        self._start(mock.patch.dict(os.environ))
        for variable in ("SPARK_GENERATE_GOLDEN_FILES", "SPARK_LOCAL_REMOTE"):
            os.environ.pop(variable, None)

        # A golden file stays claimed for as long as the process runs.
        self._start(mock.patch.dict(df_golden._golden_file_owners, clear=True))

        # The golden test classes of a test all resolve to this temp file,
        # rather than to a file under ``results`` next to their module.
        fd, self.golden = tempfile.mkstemp(suffix=".py.out")
        os.close(fd)
        self.addCleanup(os.remove, self.golden)
        self._start(mock.patch.object(df_golden, "golden_file_for_input", return_value=self.golden))

        # Running a case takes a real session, so a stand-in notes in ``ran``
        # the case it is asked to run instead.
        self.ran = []
        self._start(
            mock.patch.object(df_golden, "compute_case_outputs", self._compute_case_outputs)
        )

        # What happens to the sessions, in order: ``stop`` for the providing
        # one, ``release`` and ``close`` for the client of the golden one.
        self.calls = []

    def _start(self, patcher):
        patcher.start()
        self.addCleanup(patcher.stop)

    def _compute_case_outputs(self, spark, build_df):
        self.ran.append(build_df.__name__)
        # A schema naming the case, and its tags: no two cases produce the same.
        sections = {"expected_output_schema": "struct<{}:int>".format(build_df.__name__)}
        if case_tags(build_df):
            sections["tags"] = format_tags(case_tags(build_df))
        return sections

    def _provider(self, in_process_server=True):
        """
        Return a stand-in for ``ReusedConnectTestCase``: a test class whose
        ``setUpClass`` creates the session, without chaining to the next class
        (``PySparkBaseTestCase.setUpClass`` does not either), and whose
        ``tearDownClass`` stops it.

        With *in_process_server*, the session starts its server in this process
        and takes it down again when stopped, which is how the tests run by
        default.  Without, the server outlives the session, as a remote one does.
        """
        calls = self.calls

        class FakeClient:
            closed = False

            def release_session(self):
                # As for the real client, there is no releasing once it is closed.
                if self.closed:
                    raise RuntimeError("the client is closed")
                calls.append("release")

            def close(self):
                if not self.closed:
                    calls.append("close")
                self.closed = True

        class FakeConf:
            def set(self, key, value):
                pass

        class FakeSession:
            def __init__(self):
                self.client = FakeClient()
                self.conf = FakeConf()

            def __del__(self):
                # As the real session does when it is garbage collected.
                self.client.close()

        class FakeProviderSession:
            is_stopped = False

            def newSession(self):
                return FakeSession()

            def stop(self):
                calls.append("stop")
                self.is_stopped = True
                # As the real ``stop()`` does, once the server it started is down.
                os.environ.pop("SPARK_LOCAL_REMOTE", None)

        class Provider(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                cls.spark = FakeProviderSession()
                if in_process_server:
                    os.environ["SPARK_LOCAL_REMOTE"] = "1"

            @classmethod
            def tearDownClass(cls):
                cls.spark.stop()

        return Provider

    def _run(self, test_class, *names):
        """
        Run *test_class* -- all of its tests, or only those in *names* -- the
        way a test runner does, and return the result.
        """
        if names:
            suite = unittest.TestSuite(test_class(name) for name in names)
        else:
            suite = unittest.defaultTestLoader.loadTestsFromTestCase(test_class)
        result = unittest.TestResult()
        suite.run(result)
        return result

    def _run_passing(self, test_class, *names):
        """Run *test_class* like :meth:`_run`, expecting its tests to run and pass."""
        result = self._run(test_class, *names)
        self.assertEqual(result.errors + result.failures, [])
        self.assertGreater(result.testsRun, 0)

    def _write_golden(self, text):
        with open(self.golden, "w") as f:
            f.write(text)

    def _read_golden(self):
        with open(self.golden) as f:
            return f.read()

    # -- session release ---------------------------------------------------

    def test_session_is_released_when_the_server_outlives_the_provider(self):
        # The class cleanup runs after the providing class stopped its session.
        # A remote server, or the persistent local one, is still up by then, and
        # would hold the golden session and its temp views until they idle out.
        self._write_golden(self.GOLDEN_C)

        class Cases(DFGoldenTestMixin, self._provider(in_process_server=False)):
            def _test_c(self, spark):
                pass

        self._run_passing(Cases)
        self.assertEqual(self.calls, ["stop", "release", "close"])

    def test_session_release_is_skipped_once_the_in_process_server_stopped(self):
        # The providing session took the server it had started down with it, and
        # with that the session to release; only the client is left to close.
        self._write_golden(self.GOLDEN_C)

        class Cases(DFGoldenTestMixin, self._provider()):
            def _test_c(self, spark):
                pass

        self._run_passing(Cases)
        self.assertEqual(self.calls, ["stop", "close"])

    def test_session_is_released_when_set_up_class_fails(self):
        # A runner skips tearDownClass when setUpClass raises, so nothing has
        # stopped the providing session: its server is up, in process or not.
        self._write_golden(self.GOLDEN_C)

        class Cases(DFGoldenTestMixin, self._provider()):
            @classmethod
            def setup_session(cls, spark):
                raise RuntimeError("setup_session failed")

            def _test_c(self, spark):
                pass

        [(_, error)] = self._run(Cases).errors
        self.assertIn("setup_session failed", error)
        self.assertEqual(self.calls, ["release", "close"])

    # -- base classes ------------------------------------------------------

    def test_mixin_listed_after_the_session_provider_is_reported_by_each_case(self):
        # The provider's setUpClass does not chain to the mixin's, which never
        # runs; the cases are the first to notice, and say what to change.
        class Cases(self._provider(), DFGoldenTestMixin):
            def _test_a(self, spark):
                pass

            def _test_b(self, spark):
                pass

        result = self._run(Cases)
        self.assertEqual(result.errors, [])
        self.assertEqual(len(result.failures), 2)
        for _, failure in result.failures:
            self.assertIn("list DFGoldenTestMixin before the session-providing test class", failure)
        self.assertEqual(self.ran, [])

    def test_mixin_without_a_session_provider_is_reported(self):
        # Listed first, but no class after it creates a session: a different
        # mistake, which reordering the bases would not fix.
        self._write_golden(self.GOLDEN_C)

        class Cases(DFGoldenTestMixin, unittest.TestCase):
            def _test_c(self, spark):
                pass

        [(_, error)] = self._run(Cases).errors
        self.assertIn("mix DFGoldenTestMixin into a session-providing test class", error)

    def test_mixin_allows_a_subclass_to_define_set_up_class(self):
        # A -> B(setUpClass) -> DFGoldenTestMixin is a legitimate chain: the
        # ordering requirement is checked when the class runs, not declared.
        self._write_golden(self.GOLDEN_C)

        class Base(DFGoldenTestMixin, self._provider()):
            @classmethod
            def setUpClass(cls):
                super().setUpClass()

        class Derived(Base):
            def _test_c(self, spark):
                pass

        self._run_passing(Derived)
        self.assertEqual(self.ran, ["_test_c"])

    # -- case dispatch -----------------------------------------------------

    def test_registered_tests_each_run_their_own_case(self):
        self._write_golden(
            "--! name\na\n--! expected_output_schema\nstruct<_test_a:int>\n!-- end\n"
            "\n\n"
            "--! name\nb\n--! expected_output_schema\nstruct<_test_b:int>\n!-- end\n"
        )

        class Cases(DFGoldenTestMixin, self._provider()):
            def _test_a(self, spark):
                pass

            def _test_b(self, spark):
                pass

        self._run_passing(Cases)
        # Not the case registered last twice over, which is what tests sharing
        # the registration loop's variable would run.
        self.assertEqual(self.ran, ["_test_a", "_test_b"])

    # -- regeneration ------------------------------------------------------

    def test_regeneration_writes_the_golden_file_from_the_run(self):
        os.environ["SPARK_GENERATE_GOLDEN_FILES"] = "1"
        # The old file is replaced without being read, so it is no obstacle that
        # this one is no golden file at all, let alone in sync with the class.
        self._write_golden("not a golden file\n")

        class Cases(DFGoldenTestMixin, self._provider()):
            def _test_b(self, spark):
                pass

            @unordered
            def _test_a(self, spark):
                pass

        self._run_passing(Cases)
        self.assertEqual(
            self._read_golden(),
            "--! name\n__file_metadata__\n--! source\n{}.{}\n!-- end\n"
            "\n\n"
            "--! name\na\n--! tags\nunordered\n"
            "--! expected_output_schema\nstruct<_test_a:int>\n!-- end\n"
            "\n\n"
            "--! name\nb\n--! expected_output_schema\nstruct<_test_b:int>\n!-- end\n".format(
                Cases.__module__, Cases.__qualname__
            ),
        )

    def test_regeneration_refuses_a_run_that_skipped_a_case(self):
        os.environ["SPARK_GENERATE_GOLDEN_FILES"] = "1"
        self._write_golden("the previous golden file\n")

        class Cases(DFGoldenTestMixin, self._provider()):
            def _test_a(self, spark):
                pass

            def _test_b(self, spark):
                pass

        [(_, error)] = self._run(Cases, "test_a").errors
        self.assertIn("these cases did not run", error)
        # Writing the one case that ran would have dropped ``b`` from the file.
        self.assertEqual(self._read_golden(), "the previous golden file\n")

    # -- golden file ownership ---------------------------------------------

    def test_second_class_resolving_to_a_golden_file_is_refused(self):
        # As two golden test classes in one module would, these resolve to the
        # same file, which the second one to finish would otherwise overwrite.
        os.environ["SPARK_GENERATE_GOLDEN_FILES"] = "1"
        provider = self._provider()

        class First(DFGoldenTestMixin, provider):
            def _test_a(self, spark):
                pass

        class Second(DFGoldenTestMixin, provider):
            def _test_b(self, spark):
                pass

        self._run_passing(First)
        written = self._read_golden()

        [(_, error)] = self._run(Second).errors
        self.assertIn("already belongs to", error)
        self.assertIn(First.__qualname__, error)
        self.assertIn(Second.__qualname__, error)
        self.assertIn("its own `inputs/test_<topic>.py` module", error)
        # Refused before it started a session or ran a case.
        self.assertEqual(self.calls, ["stop", "close"])
        self.assertEqual(self.ran, ["_test_a"])
        self.assertEqual(self._read_golden(), written)

    def test_class_set_up_again_keeps_its_golden_file(self):
        self._write_golden(self.GOLDEN_C)

        class Cases(DFGoldenTestMixin, self._provider()):
            def _test_c(self, spark):
                pass

        self._run_passing(Cases)
        self._run_passing(Cases)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
