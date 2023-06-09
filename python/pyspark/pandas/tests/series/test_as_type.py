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
from distutils.version import LooseVersion

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import ComparisonTestBase
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.pandas.typedef.typehints import (
    extension_dtypes,
    extension_dtypes_available,
    extension_float_dtypes_available,
    extension_object_dtypes_available,
)


class SeriesAsTypeMixin:
    @property
    def pser(self):
        return pd.Series([1, 2, 3, 4, 5, 6, 7], name="x")

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @unittest.skipIf(
        LooseVersion(pd.__version__) >= LooseVersion("2.0.0"),
        "TODO(SPARK-43466): Enable SeriesTests.test_astype for pandas 2.0.0.",
    )
    def test_astype(self):
        psers = [pd.Series([10, 20, 15, 30, 45], name="x")]

        if extension_dtypes_available:
            psers.append(pd.Series([10, 20, 15, 30, 45], name="x", dtype="Int64"))
        if extension_float_dtypes_available:
            psers.append(pd.Series([10, 20, 15, 30, 45], name="x", dtype="Float64"))

        for pser in psers:
            self._test_numeric_astype(pser)

        pser = pd.Series([10, 20, 15, 30, 45, None, np.nan], name="x")
        psser = ps.Series(pser)

        self.assert_eq(psser.astype(bool), pser.astype(bool))
        self.assert_eq(psser.astype(str), pser.astype(str))

        pser = pd.Series(["hi", "hi ", " ", " \t", "", None], name="x")
        psser = ps.Series(pser)

        self.assert_eq(psser.astype(bool), pser.astype(bool))
        if LooseVersion("1.1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.1.4"):
            # a pandas bug: https://github.com/databricks/koalas/pull/1818#issuecomment-703961980
            self.assert_eq(psser.astype(str).tolist(), ["hi", "hi ", " ", " \t", "", "None"])
        else:
            self.assert_eq(psser.astype(str), pser.astype(str))
        self.assert_eq(psser.str.strip().astype(bool), pser.str.strip().astype(bool))

        if extension_object_dtypes_available:
            from pandas import StringDtype

            self._check_extension(psser.astype("string"), pser.astype("string"))
            self._check_extension(psser.astype(StringDtype()), pser.astype(StringDtype()))

        pser = pd.Series([True, False, None], name="x")
        psser = ps.Series(pser)

        self.assert_eq(psser.astype(bool), pser.astype(bool))
        self.assert_eq(psser.astype(str), pser.astype(str))

        if extension_object_dtypes_available:
            from pandas import BooleanDtype, StringDtype

            self._check_extension(psser.astype("boolean"), pser.astype("boolean"))
            self._check_extension(psser.astype(BooleanDtype()), pser.astype(BooleanDtype()))

            if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
                self._check_extension(psser.astype("string"), pser.astype("string"))
                self._check_extension(psser.astype(StringDtype()), pser.astype(StringDtype()))
            else:
                self._check_extension(
                    psser.astype("string"),
                    pd.Series(["True", "False", None], name="x", dtype="string"),
                )
                self._check_extension(
                    psser.astype(StringDtype()),
                    pd.Series(["True", "False", None], name="x", dtype=StringDtype()),
                )

        pser = pd.Series(["2020-10-27 00:00:01", None], name="x")
        psser = ps.Series(pser)

        self.assert_eq(psser.astype(np.datetime64), pser.astype(np.datetime64))
        self.assert_eq(psser.astype("datetime64[ns]"), pser.astype("datetime64[ns]"))
        self.assert_eq(psser.astype("M"), pser.astype("M"))
        self.assert_eq(psser.astype("M").astype(str), pser.astype("M").astype(str))
        # Comment out the below test cause because pandas returns `NaT` or `nan` randomly
        # self.assert_eq(
        #     psser.astype("M").dt.date.astype(str), pser.astype("M").dt.date.astype(str)
        # )

        if extension_object_dtypes_available:
            from pandas import StringDtype

            # The behavior of casting datetime to nullable string is changed from pandas 1.3.
            if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
                self._check_extension(
                    psser.astype("M").astype("string"), pser.astype("M").astype("string")
                )
                self._check_extension(
                    psser.astype("M").astype(StringDtype()), pser.astype("M").astype(StringDtype())
                )
            else:
                expected = ps.Series(["2020-10-27 00:00:01", None], name="x", dtype="string")
                self._check_extension(psser.astype("M").astype("string"), expected)
                self._check_extension(psser.astype("M").astype(StringDtype()), expected)

        with self.assertRaisesRegex(TypeError, "not understood"):
            psser.astype("int63")

    def _test_numeric_astype(self, pser):
        psser = ps.Series(pser)

        self.assert_eq(psser.astype(int), pser.astype(int))
        self.assert_eq(psser.astype(np.int8), pser.astype(np.int8))
        self.assert_eq(psser.astype(np.int16), pser.astype(np.int16))
        self.assert_eq(psser.astype(np.int32), pser.astype(np.int32))
        self.assert_eq(psser.astype(np.int64), pser.astype(np.int64))
        self.assert_eq(psser.astype(np.byte), pser.astype(np.byte))
        self.assert_eq(psser.astype("int"), pser.astype("int"))
        self.assert_eq(psser.astype("int8"), pser.astype("int8"))
        self.assert_eq(psser.astype("int16"), pser.astype("int16"))
        self.assert_eq(psser.astype("int32"), pser.astype("int32"))
        self.assert_eq(psser.astype("int64"), pser.astype("int64"))
        self.assert_eq(psser.astype("b"), pser.astype("b"))
        self.assert_eq(psser.astype("byte"), pser.astype("byte"))
        self.assert_eq(psser.astype("i"), pser.astype("i"))
        self.assert_eq(psser.astype("long"), pser.astype("long"))
        self.assert_eq(psser.astype("short"), pser.astype("short"))
        self.assert_eq(psser.astype(np.float32), pser.astype(np.float32))
        self.assert_eq(psser.astype(np.float64), pser.astype(np.float64))
        self.assert_eq(psser.astype("float"), pser.astype("float"))
        self.assert_eq(psser.astype("float32"), pser.astype("float32"))
        self.assert_eq(psser.astype("float64"), pser.astype("float64"))
        self.assert_eq(psser.astype("double"), pser.astype("double"))
        self.assert_eq(psser.astype("f"), pser.astype("f"))
        self.assert_eq(psser.astype(bool), pser.astype(bool))
        self.assert_eq(psser.astype("bool"), pser.astype("bool"))
        self.assert_eq(psser.astype("?"), pser.astype("?"))
        self.assert_eq(psser.astype(np.unicode_), pser.astype(np.unicode_))
        self.assert_eq(psser.astype("str"), pser.astype("str"))
        self.assert_eq(psser.astype("U"), pser.astype("U"))

        if extension_dtypes_available:
            from pandas import Int8Dtype, Int16Dtype, Int32Dtype, Int64Dtype

            self._check_extension(psser.astype("Int8"), pser.astype("Int8"))
            self._check_extension(psser.astype("Int16"), pser.astype("Int16"))
            self._check_extension(psser.astype("Int32"), pser.astype("Int32"))
            self._check_extension(psser.astype("Int64"), pser.astype("Int64"))
            self._check_extension(psser.astype(Int8Dtype()), pser.astype(Int8Dtype()))
            self._check_extension(psser.astype(Int16Dtype()), pser.astype(Int16Dtype()))
            self._check_extension(psser.astype(Int32Dtype()), pser.astype(Int32Dtype()))
            self._check_extension(psser.astype(Int64Dtype()), pser.astype(Int64Dtype()))

        if extension_object_dtypes_available:
            from pandas import StringDtype

            if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
                self._check_extension(psser.astype("string"), pser.astype("string"))
                self._check_extension(psser.astype(StringDtype()), pser.astype(StringDtype()))
            else:
                self._check_extension(
                    psser.astype("string"),
                    pd.Series(["10", "20", "15", "30", "45"], name="x", dtype="string"),
                )
                self._check_extension(
                    psser.astype(StringDtype()),
                    pd.Series(["10", "20", "15", "30", "45"], name="x", dtype=StringDtype()),
                )

        if extension_float_dtypes_available:
            from pandas import Float32Dtype, Float64Dtype

            self._check_extension(psser.astype("Float32"), pser.astype("Float32"))
            self._check_extension(psser.astype("Float64"), pser.astype("Float64"))
            self._check_extension(psser.astype(Float32Dtype()), pser.astype(Float32Dtype()))
            self._check_extension(psser.astype(Float64Dtype()), pser.astype(Float64Dtype()))

    def _check_extension(self, psser, pser):
        if LooseVersion("1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.2.2"):
            self.assert_eq(psser, pser, check_exact=False)
            self.assertTrue(isinstance(psser.dtype, extension_dtypes))
        else:
            self.assert_eq(psser, pser)


class SeriesAsTypeTests(SeriesAsTypeMixin, ComparisonTestBase, SQLTestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.series.test_as_type import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
