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
DataFrame API golden file tests.

Analogous to ``SQLQueryTestSuite`` but tests are written in native Python using
the PySpark DataFrame API.  Each ``.test`` file in this directory describes a
list of test cases; every case points to a standalone script under
``scripts/`` that builds the DataFrame under test.  The expected outputs live
inline in the ``.test`` file, which doubles as the golden file.  See
``pyspark.sql.tests.df_golden.df_golden`` for the file format.

Running the tests::

    python/run-tests --testnames pyspark.sql.tests.df_golden.test_df_golden

Regenerating golden files
-------------------------
Set ``SPARK_GENERATE_GOLDEN_FILES=1`` before running the tests, or use the
wrapper script::

    python/pyspark/sql/tests/df_golden/regenerate.sh [--verify]

Every run prints a unified diff between ``=== BEGIN GOLDEN DIFF ===`` and
``=== END GOLDEN DIFF ===`` markers when the generated file differs. A diff alone does not
fail a test; only the expected sections are compared. The environment variable still
controls whether the file is rewritten. With ``--verify`` the wrapper re-runs the tests
against the regenerated files.

Patches copied from ``python/run-tests`` logs have CRLF line endings. Strip carriage
returns before applying them: ``tr -d '\\r' < golden.patch | git apply``.

Adding a new test file
----------------------
Drop a new ``<name>.test`` file in this directory and its case scripts under
``scripts/<name>/`` -- the ``.test`` file is auto-discovered (a
``test_<name>`` method is registered here), no code changes needed.  Then
regenerate the golden files with the steps above.
"""

import io
import os
import tempfile
from contextlib import redirect_stderr
from pathlib import Path

from pyspark.sql.tests.df_golden.df_golden import run_golden_test, write_test_file
from pyspark.testing.connectutils import ReusedConnectTestCase

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class DFGoldenTestBase(ReusedConnectTestCase):
    """
    Base class for DataFrame golden file tests.

    Subclasses just need to exist -- test methods are auto-registered from
    the ``.test`` files found in ``test_file_dir``.

    Each ``.test`` file runs in its own Spark Connect session (a
    ``spark.newSession()`` call against the shared local Connect server that
    :class:`ReusedConnectTestCase` starts in ``setUpClass``), which is the
    Connect counterpart of ``SQLQueryTestSuite``'s per-file
    ``spark.newSession()``.  State created by a file's case scripts -- temp
    views, UDFs, session confs -- is discarded with the session and cannot
    leak into other files.
    """

    test_file_dir = _THIS_DIR

    def _run_golden_test(self, filename):
        session = self.spark.newSession()
        try:
            run_golden_test(self, session, os.path.join(self.test_file_dir, filename))
        finally:
            # Release only this sub-session server-side and close its client
            # channel. We must NOT call ``session.stop()``: under
            # ``SPARK_LOCAL_REMOTE`` (the test harness) ``stop()`` terminates the
            # shared local Connect server, breaking the rest of the suite and
            # hanging ``tearDownClass``'s ``spark.stop()`` in release retries
            # against the dead server until the test times out.
            client = session.client
            try:
                client.release_session()
            except Exception:
                pass
            try:
                client.close()
            except Exception:
                pass


def _register_tests(cls):
    """
    Discover ``.test`` files and create a test method for each one on *cls*.

    Finding no ``.test`` files registers a failing test instead of zero
    tests: a packaging problem that drops the data files must fail the
    suite, not silently turn it into a green no-op.
    """
    test_files = []
    if os.path.isdir(cls.test_file_dir):
        test_files = sorted(f for f in os.listdir(cls.test_file_dir) if f.endswith(".test"))

    if not test_files:

        def test_discovery_failed(self):
            self.fail("no .test files found in {}".format(cls.test_file_dir))

        cls.test_discovery_failed = test_discovery_failed
        return

    for filename in test_files:

        def _make_test(f=filename):
            def test_method(self):
                self._run_golden_test(f)

            return test_method

        name = filename[: -len(".test")]
        setattr(cls, "test_{}".format(name), _make_test())


class DFGoldenTest(DFGoldenTestBase):
    """DataFrame golden file tests for all ``.test`` files in this directory."""

    pass


_register_tests(DFGoldenTest)


class DFGoldenRunnerTests(DFGoldenTestBase):
    def _golden_file(self, root, schemas):
        self.test_file_dir = root
        root = Path(root)
        cases = []
        for index, schema in enumerate(schemas):
            script = root / f"case_{index}.py"
            # Record each execution to detect accidental reruns during comparison or regeneration.
            script.write_text(
                f"with open({str(root / 'executions')!r}, 'a') as f:\n"
                f"    f.write('{index}\\n')\n"
                "df = spark.range(1)\n"
            )
            case = {"name": f"case {index}", "script": script.name}
            if schema is not None:
                case["expected_output_schema"] = schema
            cases.append(case)
        path = root / "case.test"
        write_test_file(path, {}, cases)
        return path

    def test_passing_case_prints_diff_without_writing(self):
        with tempfile.TemporaryDirectory() as root:
            path = self._golden_file(root, ["struct<id:bigint>"])
            old_content = path.read_text()
            output = io.StringIO()
            with self.temp_env({"SPARK_GENERATE_GOLDEN_FILES": None}), redirect_stderr(output):
                self._run_golden_test(path.name)
            self.assertIn("=== BEGIN GOLDEN DIFF ===\n", output.getvalue())
            self.assertIn("+--! expected_result\n", output.getvalue())
            self.assertEqual(path.read_text(), old_content)
            self.assertEqual((path.parent / "executions").read_text().splitlines(), ["0"])

    def test_failure_prints_complete_diff_without_rerunning_cases(self):
        with tempfile.TemporaryDirectory() as root:
            path = self._golden_file(root, ["wrong first schema", "wrong second schema"])
            old_content = path.read_text()
            output = io.StringIO()
            with self.temp_env({"SPARK_GENERATE_GOLDEN_FILES": None}), redirect_stderr(output):
                with self.assertRaisesRegex(AssertionError, "mismatch in `expected_output_schema`"):
                    self._run_golden_test(path.name)
            self.assertIn("-wrong first schema\n", output.getvalue())
            self.assertIn("-wrong second schema\n", output.getvalue())
            self.assertIn("=== END GOLDEN DIFF ===\n", output.getvalue())
            self.assertEqual(path.read_text(), old_content)
            self.assertEqual((path.parent / "executions").read_text().splitlines(), ["0", "1"])

    def test_malformed_golden_prints_diff_before_verification_fails(self):
        for unterminated in (False, True):
            with self.subTest(unterminated=unterminated), tempfile.TemporaryDirectory() as root:
                schema = "struct<id:bigint>" if unterminated else None
                path = self._golden_file(root, [schema, schema])
                if unterminated:
                    path.write_bytes(path.read_bytes().removesuffix(b"!-- end\n"))
                old_content = path.read_bytes()
                message = "does not end with" if unterminated else "has no expected_"
                output = io.StringIO()
                with self.temp_env({"SPARK_GENERATE_GOLDEN_FILES": None}), redirect_stderr(output):
                    with self.assertRaisesRegex(AssertionError, message):
                        self._run_golden_test(path.name)
                self.assertIn("=== BEGIN GOLDEN DIFF ===\n", output.getvalue())
                self.assertIn("=== END GOLDEN DIFF ===\n", output.getvalue())
                self.assertEqual(path.read_bytes(), old_content)
                self.assertEqual((path.parent / "executions").read_text().splitlines(), ["0", "1"])

    def test_regeneration_prints_diff_and_writes_passing_golden(self):
        with tempfile.TemporaryDirectory() as root:
            path = self._golden_file(root, [None])
            output = io.StringIO()
            with self.temp_env({"SPARK_GENERATE_GOLDEN_FILES": "1"}), redirect_stderr(output):
                self._run_golden_test(path.name)
            self.assertIn("=== BEGIN GOLDEN DIFF ===\n", output.getvalue())
            self.assertIn("--! expected_result\n", path.read_text())
            self.assertEqual((path.parent / "executions").read_text().splitlines(), ["0"])
            output = io.StringIO()
            with self.temp_env({"SPARK_GENERATE_GOLDEN_FILES": None}), redirect_stderr(output):
                self._run_golden_test(path.name)
            self.assertNotIn("=== BEGIN GOLDEN DIFF ===", output.getvalue())
            self.assertEqual((path.parent / "executions").read_text().splitlines(), ["0", "0"])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
