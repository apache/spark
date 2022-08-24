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

import inspect

import pyspark.pandas as ps
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.scalars import _MissingPandasLikeScalars
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class ScalarTest(PandasOnSparkTestCase):
    def test_missing(self):
        missing_scalars = inspect.getmembers(_MissingPandasLikeScalars)

        missing_scalars = [
            name
            for (name, type_) in missing_scalars
            if isinstance(type_, PandasNotImplementedError)
        ]

        for scalar_name in missing_scalars:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "The scalar `ps.{0}` is not reimplemented in pyspark.pandas;"
                " use `pd.{0}`.".format(scalar_name),
            ):
                getattr(ps, scalar_name)


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_scalars import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
