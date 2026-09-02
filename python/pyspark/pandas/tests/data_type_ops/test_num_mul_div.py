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


import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.utils import is_ansi_mode_test


class NumMulDivTestsMixin:
    @property
    def float_pser(self):
        return pd.Series([1, 2, 3], dtype=float)

    @property
    def float_psser(self):
        return ps.from_pandas(self.float_pser)

    def test_mul(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            ignore_null = self.ignore_null(col)
            self.assert_eq(pser * pser, psser * psser, check_exact=False, ignore_null=ignore_null)
            self.assert_eq(
                pser * pser.astype(bool),
                psser * psser.astype(bool),
                check_exact=False,
                ignore_null=ignore_null,
            )
            self.assert_eq(pser * True, psser * True, check_exact=False, ignore_null=ignore_null)
            self.assert_eq(pser * False, psser * False, check_exact=False, ignore_null=ignore_null)

            if psser.dtype in [int, np.int32]:
                self.assert_eq(pser * pdf["string"], psser * psdf["string"])
            else:
                self.assertRaises(TypeError, lambda: psser * psdf["string"])

            self.assert_eq(
                pser * pdf["bool"], psser * psdf["bool"], check_exact=False, ignore_null=ignore_null
            )

            self.assertRaises(TypeError, lambda: psser * psdf["datetime"])
            self.assertRaises(TypeError, lambda: psser * psdf["date"])
            self.assertRaises(TypeError, lambda: psser * psdf["categorical"])

        if is_ansi_mode_test:
            self.assertRaises(TypeError, lambda: psdf["decimal"] * psdf["float"])
            self.assertRaises(TypeError, lambda: psdf["float"] * psdf["decimal"])
            self.assertRaises(TypeError, lambda: psdf["decimal"] * psdf["float32"])
            self.assertRaises(TypeError, lambda: psdf["float32"] * psdf["decimal"])
            self.assertRaises(TypeError, lambda: psdf["decimal"] * 0.1)
            self.assertRaises(TypeError, lambda: 0.1 * psdf["decimal"])

    def test_truediv(self):
        pdf, psdf = self.pdf, self.psdf
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]
            if psser.dtype in [float, int, np.int32]:
                self.assert_eq(pser / pser, psser / psser)
                self.assert_eq(pser / pser.astype(bool), psser / psser.astype(bool))
                self.assert_eq(pser / True, psser / True)
                self.assert_eq(pser / False, psser / False)

            for n_col in self.non_numeric_df_cols:
                if n_col == "bool":
                    self.assert_eq(pdf["float"] / pdf[n_col], psdf["float"] / psdf[n_col])
                else:
                    self.assertRaises(TypeError, lambda: psser / psdf[n_col])

        if is_ansi_mode_test:
            self.assertRaises(TypeError, lambda: psdf["decimal"] / psdf["float"])
            self.assertRaises(TypeError, lambda: psdf["float"] / psdf["decimal"])
            self.assertRaises(TypeError, lambda: psdf["decimal"] / psdf["float32"])
            self.assertRaises(TypeError, lambda: psdf["float32"] / psdf["decimal"])
            self.assertRaises(TypeError, lambda: psdf["decimal"] / 0.1)
            self.assertRaises(TypeError, lambda: 0.1 / psdf["decimal"])

    def test_floordiv(self):
        pdf, psdf = self.pdf, self.psdf
        pser, psser = pdf["float"], psdf["float"]
        self.assert_eq(pser // pser, psser // psser)
        self.assert_eq(pser // pser.astype(bool), psser // psser.astype(bool))
        self.assert_eq(pser // True, psser // True)
        self.assert_eq(pser // False, psser // False)

        for n_col in self.non_numeric_df_cols:
            if n_col == "bool":
                self.assert_eq(pdf["float"] // pdf["bool"], psdf["float"] // psdf["bool"])
            else:
                for col in self.numeric_df_cols:
                    psser = psdf[col]
                    self.assertRaises(TypeError, lambda: psser // psdf[n_col])

        if is_ansi_mode_test:
            self.assertRaises(TypeError, lambda: psdf["decimal"] // psdf["float"])
            self.assertRaises(TypeError, lambda: psdf["float"] // psdf["decimal"])
            self.assertRaises(TypeError, lambda: psdf["decimal"] // psdf["float32"])
            self.assertRaises(TypeError, lambda: psdf["float32"] // psdf["decimal"])
            self.assertRaises(TypeError, lambda: psdf["decimal"] // 0.1)
            self.assertRaises(TypeError, lambda: 0.1 // psdf["decimal"])

        # A divisor that is not exactly representable: 1.0 / 0.1 rounds up to exactly 10.0,
        # so flooring the quotient gives 10 where pandas returns 9.
        pser = pd.Series([1.0, 10.0, -1.0, 2.5])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser // 0.1, psser // 0.1)

        # An integral operand above 2**53 loses its low bits when divided as double. pandas
        # returns int64 here, which Spark's division cannot, so compare the values.
        pser = pd.Series([9007199254740993, -9007199254740993])
        psser = ps.from_pandas(pser)
        self.assert_eq((pser // 2).astype(float), psser // 2)
        self.assert_eq((pser // 3).astype(float), psser // 3)

        # An infinite divisor leaves a quotient of 0 or -1, and an infinite dividend has no
        # finite floor, which pandas reports as nan.
        pser = pd.Series([1.0, -1.0, np.inf, -np.inf])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser // np.inf, psser // np.inf)
        self.assert_eq(pser // -np.inf, psser // -np.inf)
        pser = pd.Series([np.inf, -np.inf, np.nan, 1.0])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser // 2.0, psser // 2.0)

        # Finite operands whose quotient overflows to an infinity, which is its own floor.
        edge_pdf = pd.DataFrame({"a": [1e300, -1e300], "b": [1e-300, 1e-300]})
        edge_psdf = ps.from_pandas(edge_pdf)
        self.assert_eq(edge_pdf.a // edge_pdf.b, edge_psdf.a // edge_psdf.b)

        # A negative zero divisor negates the result, and a zero dividend keeps its own sign.
        pser = pd.Series([1.0, -1.0, 2.5])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser // -0.0, psser // -0.0)
        pser = pd.Series([-0.0, 0.0])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser // 3.0, psser // 3.0)
        # An equality check cannot see the sign of a zero, so compare it directly.
        self.assertEqual(
            np.signbit(pser // 3.0).tolist(), np.signbit((psser // 3.0).to_pandas()).tolist()
        )

        # The only quotient that does not fit in a long, where pandas wraps around.
        pser = pd.Series([-(2**63)])
        psser = ps.from_pandas(pser)
        self.assert_eq((pser // -1).astype(float), psser // -1)

    def test_mod(self):
        pdf, psdf = self.pdf, self.psdf

        # element-wise modulo for numeric columns
        for col in self.numeric_df_cols:
            pser, psser = pdf[col], psdf[col]

            if psser.dtype in [float, int, np.int32]:
                self.assert_eq(pser % pser, psser % psser)
                self.assert_eq(pser % pser.astype(bool), psser % psser.astype(bool))
                self.assert_eq(pser % True, psser % True)
                # TODO: decide if to follow pser % False
                self.assert_eq(pser % 0, psser % False)

            # modulo with non-numeric columns
            for n_col in self.non_numeric_df_cols:
                if n_col == "bool":
                    self.assert_eq(pdf["float"] % pdf["bool"], psdf["float"] % psdf["bool"])
                else:
                    self.assertRaises(TypeError, lambda: psser % psdf[n_col])

        if is_ansi_mode_test:
            self.assertRaises(TypeError, lambda: psdf["decimal"] % psdf["float"])
            self.assertRaises(TypeError, lambda: psdf["float"] % psdf["decimal"])
            self.assertRaises(TypeError, lambda: psdf["decimal"] % psdf["float32"])
            self.assertRaises(TypeError, lambda: psdf["float32"] % psdf["decimal"])
            self.assertRaises(TypeError, lambda: psdf["decimal"] % 0.1)
            self.assertRaises(TypeError, lambda: 0.1 % psdf["decimal"])


class NumMulDivTests(
    NumMulDivTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
