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
from pyspark.errors import PySparkValueError
from pyspark.sql import Row
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_plotly,
    plotly_requirement_message,
    have_pandas,
    pandas_requirement_message,
)

if have_plotly and have_pandas:
    from pyspark.sql.plot import PySparkSampledPlotBase, PySparkTopNPlotBase


@unittest.skipIf(
    not have_plotly or not have_pandas, plotly_requirement_message or pandas_requirement_message
)
class DataFramePlotTestsMixin:
    def test_backend(self):
        accessor = self.spark.range(2).plot
        backend = accessor._get_plot_backend()
        self.assertEqual(backend.__name__, "pyspark.sql.plot.plotly")

        with self.assertRaises(PySparkValueError) as pe:
            accessor._get_plot_backend("matplotlib")

        self.check_error(
            exception=pe.exception,
            errorClass="UNSUPPORTED_PLOT_BACKEND",
            messageParameters={"backend": "matplotlib", "supported_backends": "plotly"},
        )

    def test_topn_max_rows(self):
        with self.sql_conf({"spark.sql.pyspark.plotting.max_rows": "1000"}):
            self.spark.conf.set("spark.sql.pyspark.plotting.max_rows", "1000")
            sdf = self.spark.range(2500)
            pdf = PySparkTopNPlotBase().get_top_n(sdf)
            self.assertEqual(len(pdf), 1000)

    def test_sampled_plot_with_max_rows(self):
        data = [Row(a=i, b=i + 1, c=i + 2, d=i + 3) for i in range(2000)]
        sdf = self.spark.createDataFrame(data)
        pdf = PySparkSampledPlotBase().get_sampled(sdf)
        self.assertEqual(round(len(pdf) / 2000, 1), 0.5)


class DataFramePlotTests(DataFramePlotTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.plot.test_frame_plot import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
