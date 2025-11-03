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
import unittest
import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.pandas.typedef.typehints import extension_object_dtypes_available


class BitwiseMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_bitwise(self):
        pser1 = pd.Series([True, False, True, False, np.nan, np.nan, True, False, np.nan])
        pser2 = pd.Series([True, False, False, True, True, False, np.nan, np.nan, np.nan])
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        self.assert_eq(pser1 | pser2, (psser1 | psser2).sort_index())
        self.assert_eq(pser1 & pser2, (psser1 & psser2).sort_index())

        pser1 = pd.Series([True, False, np.nan], index=list("ABC"))
        pser2 = pd.Series([False, True, np.nan], index=list("DEF"))
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        self.assert_eq(pser1 | pser2, (psser1 | psser2).sort_index())
        self.assert_eq(pser1 & pser2, (psser1 & psser2).sort_index())

    @unittest.skipIf(
        not extension_object_dtypes_available, "pandas extension object dtypes are not available"
    )
    def test_bitwise_extension_dtype(self):
        pser1 = pd.Series(
            [True, False, True, False, np.nan, np.nan, True, False, np.nan], dtype="boolean"
        )
        pser2 = pd.Series(
            [True, False, False, True, True, False, np.nan, np.nan, np.nan], dtype="boolean"
        )
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        self.assert_eq((psser1 | psser2).sort_index(), pser1 | pser2)
        self.assert_eq((psser1 & psser2).sort_index(), pser1 & pser2)

        pser1 = pd.Series([True, False, np.nan], index=list("ABC"), dtype="boolean")
        pser2 = pd.Series([False, True, np.nan], index=list("DEF"), dtype="boolean")
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        # a pandas bug?
        # assert_eq((psser1 | psser2).sort_index(), pser1 | pser2)
        # assert_eq((psser1 & psser2).sort_index(), pser1 & pser2)
        self.assert_eq(
            (psser1 | psser2).sort_index(),
            pd.Series([True, None, None, None, True, None], index=list("ABCDEF"), dtype="boolean"),
        )
        self.assert_eq(
            (psser1 & psser2).sort_index(),
            pd.Series(
                [None, False, None, False, None, None], index=list("ABCDEF"), dtype="boolean"
            ),
        )


class BitwiseTests(
    BitwiseMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_bitwise import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
