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
``pyspark.testing.df_golden`` for the file format.

Running the tests::

    python/run-tests --testnames pyspark.sql.tests.df_golden.test_df_golden

Regenerating golden files
-------------------------
Set ``SPARK_GENERATE_GOLDEN_FILES=1`` before running the tests, or use the
wrapper script::

    python/pyspark/sql/tests/df_golden/regenerate.sh [--verify]

With ``--verify`` the wrapper re-runs the tests afterwards against the
regenerated files.

Adding a new test file
----------------------
Drop a new ``<name>.test`` file in this directory and its case scripts under
``scripts/<name>/`` -- the ``.test`` file is auto-discovered (a
``test_<name>`` method is registered here), no code changes needed.  Then
regenerate the golden files with the steps above.
"""

import os

from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.df_golden import run_golden_test


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
        test_files = sorted(
            f for f in os.listdir(cls.test_file_dir) if f.endswith(".test")
        )

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


if __name__ == "__main__":
    from pyspark.testing import main

    main()
