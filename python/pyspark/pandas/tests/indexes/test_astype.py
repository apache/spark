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

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class IndexesAsTypeMixin:
    def test_astype(self):
        pidx = pd.Index([10, 20, 15, 30, 45], name="x")
        psidx = ps.Index(pidx)

        self.assert_eq(psidx.astype(int), pidx.astype(int))
        self.assert_eq(psidx.astype(np.int8), pidx.astype(np.int8))
        self.assert_eq(psidx.astype(np.int16), pidx.astype(np.int16))
        self.assert_eq(psidx.astype(np.int32), pidx.astype(np.int32))
        self.assert_eq(psidx.astype(np.int64), pidx.astype(np.int64))
        self.assert_eq(psidx.astype(np.byte), pidx.astype(np.byte))
        self.assert_eq(psidx.astype("int"), pidx.astype("int"))
        self.assert_eq(psidx.astype("int8"), pidx.astype("int8"))
        self.assert_eq(psidx.astype("int16"), pidx.astype("int16"))
        self.assert_eq(psidx.astype("int32"), pidx.astype("int32"))
        self.assert_eq(psidx.astype("int64"), pidx.astype("int64"))
        self.assert_eq(psidx.astype("b"), pidx.astype("b"))
        self.assert_eq(psidx.astype("byte"), pidx.astype("byte"))
        self.assert_eq(psidx.astype("i"), pidx.astype("i"))
        self.assert_eq(psidx.astype("long"), pidx.astype("long"))
        self.assert_eq(psidx.astype("short"), pidx.astype("short"))
        self.assert_eq(psidx.astype(np.float32), pidx.astype(np.float32))
        self.assert_eq(psidx.astype(np.float64), pidx.astype(np.float64))
        self.assert_eq(psidx.astype("float"), pidx.astype("float"))
        self.assert_eq(psidx.astype("float32"), pidx.astype("float32"))
        self.assert_eq(psidx.astype("float64"), pidx.astype("float64"))
        self.assert_eq(psidx.astype("double"), pidx.astype("double"))
        self.assert_eq(psidx.astype("f"), pidx.astype("f"))
        self.assert_eq(psidx.astype(bool), pidx.astype(bool))
        self.assert_eq(psidx.astype("bool"), pidx.astype("bool"))
        self.assert_eq(psidx.astype("?"), pidx.astype("?"))
        self.assert_eq(psidx.astype(np.str_), pidx.astype(np.str_))
        self.assert_eq(psidx.astype("str"), pidx.astype("str"))
        self.assert_eq(psidx.astype("U"), pidx.astype("U"))

        pidx = pd.Index([10, 20, 15, 30, 45, None], name="x")
        psidx = ps.Index(pidx)
        self.assert_eq(psidx.astype(bool), pidx.astype(bool))
        self.assert_eq(psidx.astype(str), pidx.astype(str))

        pidx = pd.Index(["hi", "hi ", " ", " \t", "", None], name="x")
        psidx = ps.Index(pidx)

        self.assert_eq(psidx.astype(bool), pidx.astype(bool))
        self.assert_eq(psidx.astype(str), pidx.astype(str))

        pidx = pd.Index([True, False, None], name="x")
        psidx = ps.Index(pidx)

        self.assert_eq(psidx.astype(bool), pidx.astype(bool))

        pidx = pd.Index(["2020-10-27"], name="x")
        psidx = ps.Index(pidx)

        self.assert_eq(psidx.astype("datetime64[ns]"), pidx.astype("datetime64[ns]"))

        with self.assertRaisesRegex(TypeError, "not understood"):
            psidx.astype("int63")


class IndexesAsTypeTests(
    IndexesAsTypeMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_astype import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
