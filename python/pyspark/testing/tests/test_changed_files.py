#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import tempfile
import unittest

from pyspark.testing.utils import (
    PySparkBaseTestCase,
    have_grimp,
    grimp_requirement_message,
)

# A test module and modules it does / does not import (transitively). These relationships are
# stable parts of the pyspark import graph.
_TEST_MODULE = "pyspark.sql.tests.test_functions"
_RELEVANT_FILE = "python/pyspark/sql/functions/builtin.py"  # imported by _TEST_MODULE
_IRRELEVANT_FILE = "python/pyspark/ml/classification.py"  # not imported by _TEST_MODULE


@unittest.skipIf(not have_grimp, grimp_requirement_message)
class ChangedFilesSelectionTests(unittest.TestCase):
    """Tests for the "smart" test selection driven by PYSPARK_CHANGED_FILES.

    ``PySparkBaseTestCase.skip_if_changed_files_irrelevant`` skips a test class when none of the
    changed files' modules are reachable from the test's own module in the pyspark import graph.
    These exercise that logic directly against real pyspark modules with known relationships.
    """

    def setUp(self):
        # The relevance check is memoized with functools.cache; clear it so each case starts fresh.
        PySparkBaseTestCase._is_module_relevant_to_changed_files.cache_clear()

    def _is_relevant(self, module, files):
        with tempfile.NamedTemporaryFile("w") as f:
            f.write("\n".join(files))
            f.flush()
            return PySparkBaseTestCase._is_module_relevant_to_changed_files(module, f.name)

    def test_relevance(self):
        own_file = "python/" + _TEST_MODULE.replace(".", os.path.sep) + ".py"
        cases = [
            ("imported module is relevant", [_RELEVANT_FILE], True),
            ("unimported module is irrelevant", [_IRRELEVANT_FILE], False),
            ("relevant among irrelevant is relevant", [_IRRELEVANT_FILE, _RELEVANT_FILE], True),
            ("the test's own file is relevant via the self-module short circuit", [own_file], True),
            (
                "non-pyspark files are conservatively relevant",
                ["sql/core/src/main/scala/Foo.scala"],
                True,
            ),
            (
                "package __init__ with no graph node is conservatively relevant",
                ["python/pyspark/sql/__init__.py"],
                True,
            ),
        ]
        for desc, files, expected in cases:
            with self.subTest(desc):
                self.assertEqual(self._is_relevant(_TEST_MODULE, files), expected)

    def test_skip_if_changed_files_irrelevant(self):
        class _Dummy(PySparkBaseTestCase):
            pass

        _Dummy.__module__ = _TEST_MODULE

        # Irrelevant changes raise SkipTest.
        with tempfile.NamedTemporaryFile("w") as f:
            f.write(_IRRELEVANT_FILE)
            f.flush()
            with self.assertRaises(unittest.SkipTest):
                _Dummy.skip_if_changed_files_irrelevant(f.name)

        # Relevant changes do not.
        with tempfile.NamedTemporaryFile("w") as f:
            f.write(_RELEVANT_FILE)
            f.flush()
            _Dummy.skip_if_changed_files_irrelevant(f.name)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
