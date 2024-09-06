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
import pyspark.sql.plot  # noqa: F401
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_plotly, plotly_requirement_message


@unittest.skipIf(not have_plotly, plotly_requirement_message)
class DataFramePlotPlotlyTestsMixin:
    @property
    def sdf(self):
        data = [("A", 10, 1.5), ("B", 30, 2.5), ("C", 20, 3.5)]
        columns = ["category", "int_val", "float_val"]
        return self.spark.createDataFrame(data, columns)

    def _check_fig_data(self, fig_data, expected_x, expected_y, expected_name=""):
        self.assertEqual(fig_data["mode"], "lines")
        self.assertEqual(fig_data["type"], "scatter")
        self.assertEqual(fig_data["xaxis"], "x")
        self.assertEqual(list(fig_data["x"]), expected_x)
        self.assertEqual(fig_data["yaxis"], "y")
        self.assertEqual(list(fig_data["y"]), expected_y)
        self.assertEqual(fig_data["name"], expected_name)

    def test_line_plot(self):
        # single column as vertical axis
        fig = self.sdf.plot(kind="line", x="category", y="int_val")
        self._check_fig_data(fig["data"][0], ["A", "B", "C"], [10, 30, 20])

        # multiple columns as vertical axis
        fig = self.sdf.plot(kind="line", x="category", y=["int_val", "float_val"])
        self._check_fig_data(fig["data"][0], ["A", "B", "C"], [10, 30, 20], "int_val")
        self._check_fig_data(fig["data"][1], ["A", "B", "C"], [1.5, 2.5, 3.5], "float_val")


class DataFramePlotPlotlyTests(DataFramePlotPlotlyTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.plot.test_frame_plot_plotly import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
