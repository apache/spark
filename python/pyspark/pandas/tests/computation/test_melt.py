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
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.pandas.utils import name_like_string


class FrameMeltMixin:
    def test_melt(self):
        pdf = pd.DataFrame(
            {"A": [1, 3, 5], "B": [2, 4, 6], "C": [7, 8, 9]}, index=np.random.rand(3)
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.melt().sort_values(["variable", "value"]).reset_index(drop=True),
            pdf.melt().sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars="A").sort_values(["variable", "value"]).reset_index(drop=True),
            pdf.melt(id_vars="A").sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=["A", "B"]).sort_values(["variable", "value"]).reset_index(drop=True),
            pdf.melt(id_vars=["A", "B"]).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=("A", "B")).sort_values(["variable", "value"]).reset_index(drop=True),
            pdf.melt(id_vars=("A", "B")).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=["A"], value_vars=["C"])
            .sort_values(["variable", "value"])
            .reset_index(drop=True),
            pdf.melt(id_vars=["A"], value_vars=["C"]).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=["A"], value_vars=["B"], var_name="myVarname", value_name="myValname")
            .sort_values(["myVarname", "myValname"])
            .reset_index(drop=True),
            pdf.melt(
                id_vars=["A"], value_vars=["B"], var_name="myVarname", value_name="myValname"
            ).sort_values(["myVarname", "myValname"]),
        )
        self.assert_eq(
            psdf.melt(value_vars=("A", "B"))
            .sort_values(["variable", "value"])
            .reset_index(drop=True),
            pdf.melt(value_vars=("A", "B")).sort_values(["variable", "value"]),
        )

        self.assertRaises(KeyError, lambda: psdf.melt(id_vars="Z"))
        self.assertRaises(KeyError, lambda: psdf.melt(value_vars="Z"))

        # multi-index columns
        TEN = 10.0
        TWELVE = 20.0

        columns = pd.MultiIndex.from_tuples([(TEN, "A"), (TEN, "B"), (TWELVE, "C")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.melt().sort_values(["variable_0", "variable_1", "value"]).reset_index(drop=True),
            pdf.melt().sort_values(["variable_0", "variable_1", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=[(TEN, "A")])
            .sort_values(["variable_0", "variable_1", "value"])
            .reset_index(drop=True),
            pdf.melt(id_vars=[(TEN, "A")])
            .sort_values(["variable_0", "variable_1", "value"])
            .rename(columns=name_like_string),
        )
        self.assert_eq(
            psdf.melt(id_vars=[(TEN, "A")], value_vars=[(TWELVE, "C")])
            .sort_values(["variable_0", "variable_1", "value"])
            .reset_index(drop=True),
            pdf.melt(id_vars=[(TEN, "A")], value_vars=[(TWELVE, "C")])
            .sort_values(["variable_0", "variable_1", "value"])
            .rename(columns=name_like_string),
        )
        self.assertRaises(
            ValueError,
            lambda: psdf.melt(
                id_vars=[(TEN, "A")],
                value_vars=[(TEN, "B")],
                var_name=["myV1", "myV2"],
                value_name="myValname",
            )
            .sort_values(["myV1", "myV2", "myValname"])
            .reset_index(drop=True),
        )

        columns.names = ["v0", "v1"]
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.melt().sort_values(["v0", "v1", "value"]).reset_index(drop=True),
            pdf.melt().sort_values(["v0", "v1", "value"]),
        )

        self.assertRaises(ValueError, lambda: psdf.melt(id_vars=(TEN, "A")))
        self.assertRaises(ValueError, lambda: psdf.melt(value_vars=(TEN, "A")))
        self.assertRaises(KeyError, lambda: psdf.melt(id_vars=[TEN]))
        self.assertRaises(KeyError, lambda: psdf.melt(id_vars=[(TWELVE, "A")]))
        self.assertRaises(KeyError, lambda: psdf.melt(value_vars=[TWELVE]))
        self.assertRaises(KeyError, lambda: psdf.melt(value_vars=[(TWELVE, "A")]))

        # non-string names
        pdf.columns = [10.0, 20.0, 30.0]
        psdf.columns = [10.0, 20.0, 30.0]

        self.assert_eq(
            psdf.melt().sort_values(["variable", "value"]).reset_index(drop=True),
            pdf.melt().sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=10.0).sort_values(["variable", "value"]).reset_index(drop=True),
            pdf.melt(id_vars=10.0).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=[10.0, 20.0])
            .sort_values(["variable", "value"])
            .reset_index(drop=True),
            pdf.melt(id_vars=[10.0, 20.0]).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=(10.0, 20.0))
            .sort_values(["variable", "value"])
            .reset_index(drop=True),
            pdf.melt(id_vars=(10.0, 20.0)).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(id_vars=[10.0], value_vars=[30.0])
            .sort_values(["variable", "value"])
            .reset_index(drop=True),
            pdf.melt(id_vars=[10.0], value_vars=[30.0]).sort_values(["variable", "value"]),
        )
        self.assert_eq(
            psdf.melt(value_vars=(10.0, 20.0))
            .sort_values(["variable", "value"])
            .reset_index(drop=True),
            pdf.melt(value_vars=(10.0, 20.0)).sort_values(["variable", "value"]),
        )


class FrameMeltTests(
    FrameMeltMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_melt import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
