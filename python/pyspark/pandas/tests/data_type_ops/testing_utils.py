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

import datetime
import decimal
from distutils.version import LooseVersion

import numpy as np
import pandas as pd

import pyspark.pandas as ps
from pyspark.pandas.typedef import extension_dtypes


class TestCasesUtils(object):
    """A utility holding common test cases for arithmetic operations of different data types."""

    @property
    def numeric_psers(self):
        dtypes = [np.float32, float, int, np.int32]
        sers = [pd.Series([1, 2, 3], dtype=dtype) for dtype in dtypes]
        sers.append(pd.Series([decimal.Decimal(1), decimal.Decimal(2), decimal.Decimal(3)]))
        return sers

    @property
    def numeric_pssers(self):
        return [ps.from_pandas(pser) for pser in self.numeric_psers]

    @property
    def numeric_pser_psser_pairs(self):
        return zip(self.numeric_psers, self.numeric_pssers)

    @property
    def non_numeric_psers(self):
        psers = {
            "string": pd.Series(["x", "y", "z"]),
            "datetime": pd.to_datetime(pd.Series([1, 2, 3])),
            "bool": pd.Series([True, True, False]),
            "date": pd.Series(
                [datetime.date(1994, 1, 1), datetime.date(1994, 1, 2), datetime.date(1994, 1, 3)]
            ),
            "categorical": pd.Series(["a", "b", "a"], dtype="category"),
        }
        return psers

    @property
    def non_numeric_pssers(self):
        pssers = {}

        for k, v in self.non_numeric_psers.items():
            pssers[k] = ps.from_pandas(v)
        return pssers

    @property
    def non_numeric_pser_psser_pairs(self):
        return zip(self.non_numeric_psers.values(), self.non_numeric_pssers.values())

    @property
    def pssers(self):
        return self.numeric_pssers + list(self.non_numeric_pssers.values())

    @property
    def psers(self):
        return self.numeric_psers + list(self.non_numeric_psers.values())

    @property
    def pser_psser_pairs(self):
        return zip(self.psers, self.pssers)

    def check_extension(self, psser, pser):
        if LooseVersion("1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.2.2"):
            self.assert_eq(psser, pser, check_exact=False)
            self.assertTrue(isinstance(psser.dtype, extension_dtypes))
        else:
            self.assert_eq(psser, pser)
