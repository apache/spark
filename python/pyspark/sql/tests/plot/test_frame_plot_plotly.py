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
from datetime import datetime

import pyspark.sql.plot  # noqa: F401
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_plotly, plotly_requirement_message


@unittest.skipIf(not have_plotly, plotly_requirement_message)
class DataFramePlotPlotlyTestsMixin:
    @property
    def sdf(self):
        data = [("A", 10, 1.5), ("B", 30, 2.5), ("C", 20, 3.5)]
        columns = ["category", "int_val", "float_val"]
        return self.spark.createDataFrame(data, columns)

    @property
    def sdf2(self):
        data = [(5.1, 3.5, 0), (4.9, 3.0, 0), (7.0, 3.2, 1), (6.4, 3.2, 1), (5.9, 3.0, 2)]
        columns = ["length", "width", "species"]
        return self.spark.createDataFrame(data, columns)

    @property
    def sdf3(self):
        data = [
            (3, 5, 20, datetime(2018, 1, 31)),
            (2, 5, 42, datetime(2018, 2, 28)),
            (3, 6, 28, datetime(2018, 3, 31)),
            (9, 12, 62, datetime(2018, 4, 30)),
        ]
        columns = ["sales", "signups", "visits", "date"]
        return self.spark.createDataFrame(data, columns)

    def _check_fig_data(self, kind, fig_data, expected_x, expected_y, expected_name=""):
        if kind == "line":
            self.assertEqual(fig_data["mode"], "lines")
            self.assertEqual(fig_data["type"], "scatter")
        elif kind == "bar":
            self.assertEqual(fig_data["type"], "bar")
        elif kind == "barh":
            self.assertEqual(fig_data["type"], "bar")
            self.assertEqual(fig_data["orientation"], "h")
        elif kind == "scatter":
            self.assertEqual(fig_data["type"], "scatter")
            self.assertEqual(fig_data["orientation"], "v")
            self.assertEqual(fig_data["mode"], "markers")
        elif kind == "area":
            self.assertEqual(fig_data["type"], "scatter")
            self.assertEqual(fig_data["orientation"], "v")
            self.assertEqual(fig_data["mode"], "lines")
        elif kind == "pie":
            self.assertEqual(fig_data["type"], "pie")
            self.assertEqual(list(fig_data["labels"]), expected_x)
            self.assertEqual(list(fig_data["values"]), expected_y)
            return

        self.assertEqual(fig_data["xaxis"], "x")
        self.assertEqual(list(fig_data["x"]), expected_x)
        self.assertEqual(fig_data["yaxis"], "y")
        self.assertEqual(list(fig_data["y"]), expected_y)
        self.assertEqual(fig_data["name"], expected_name)

    def test_line_plot(self):
        # single column as vertical axis
        fig = self.sdf.plot(kind="line", x="category", y="int_val")
        self._check_fig_data("line", fig["data"][0], ["A", "B", "C"], [10, 30, 20])

        # multiple columns as vertical axis
        fig = self.sdf.plot.line(x="category", y=["int_val", "float_val"])
        self._check_fig_data("line", fig["data"][0], ["A", "B", "C"], [10, 30, 20], "int_val")
        self._check_fig_data("line", fig["data"][1], ["A", "B", "C"], [1.5, 2.5, 3.5], "float_val")

    def test_bar_plot(self):
        # single column as vertical axis
        fig = self.sdf.plot(kind="bar", x="category", y="int_val")
        self._check_fig_data("bar", fig["data"][0], ["A", "B", "C"], [10, 30, 20])

        # multiple columns as vertical axis
        fig = self.sdf.plot.bar(x="category", y=["int_val", "float_val"])
        self._check_fig_data("bar", fig["data"][0], ["A", "B", "C"], [10, 30, 20], "int_val")
        self._check_fig_data("bar", fig["data"][1], ["A", "B", "C"], [1.5, 2.5, 3.5], "float_val")

    def test_barh_plot(self):
        # single column as vertical axis
        fig = self.sdf.plot(kind="barh", x="category", y="int_val")
        self._check_fig_data("barh", fig["data"][0], ["A", "B", "C"], [10, 30, 20])

        # multiple columns as vertical axis
        fig = self.sdf.plot.barh(x="category", y=["int_val", "float_val"])
        self._check_fig_data("barh", fig["data"][0], ["A", "B", "C"], [10, 30, 20], "int_val")
        self._check_fig_data("barh", fig["data"][1], ["A", "B", "C"], [1.5, 2.5, 3.5], "float_val")

        # multiple columns as horizontal axis
        fig = self.sdf.plot.barh(x=["int_val", "float_val"], y="category")
        self._check_fig_data("barh", fig["data"][0], [10, 30, 20], ["A", "B", "C"], "int_val")
        self._check_fig_data("barh", fig["data"][1], [1.5, 2.5, 3.5], ["A", "B", "C"], "float_val")

    def test_scatter_plot(self):
        fig = self.sdf2.plot(kind="scatter", x="length", y="width")
        self._check_fig_data(
            "scatter", fig["data"][0], [5.1, 4.9, 7.0, 6.4, 5.9], [3.5, 3.0, 3.2, 3.2, 3.0]
        )
        fig = self.sdf2.plot.scatter(x="width", y="length")
        self._check_fig_data(
            "scatter", fig["data"][0], [3.5, 3.0, 3.2, 3.2, 3.0], [5.1, 4.9, 7.0, 6.4, 5.9]
        )

    def test_area_plot(self):
        # single column as vertical axis
        fig = self.sdf3.plot(kind="area", x="date", y="sales")
        expected_x = [
            datetime(2018, 1, 31, 0, 0),
            datetime(2018, 2, 28, 0, 0),
            datetime(2018, 3, 31, 0, 0),
            datetime(2018, 4, 30, 0, 0),
        ]
        self._check_fig_data("area", fig["data"][0], expected_x, [3, 2, 3, 9])

        # multiple columns as vertical axis
        fig = self.sdf3.plot.area(x="date", y=["sales", "signups", "visits"])
        self._check_fig_data("area", fig["data"][0], expected_x, [3, 2, 3, 9], "sales")
        self._check_fig_data("area", fig["data"][1], expected_x, [5, 5, 6, 12], "signups")
        self._check_fig_data("area", fig["data"][2], expected_x, [20, 42, 28, 62], "visits")

    def test_pie_plot(self):
        fig = self.sdf3.plot(kind="pie", x="date", y="sales")
        expected_x = [
            datetime(2018, 1, 31, 0, 0),
            datetime(2018, 2, 28, 0, 0),
            datetime(2018, 3, 31, 0, 0),
            datetime(2018, 4, 30, 0, 0),
        ]
        self._check_fig_data("pie", fig["data"][0], expected_x, [3, 2, 3, 9])


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
