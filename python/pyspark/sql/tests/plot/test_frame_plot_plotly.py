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
from pyspark.errors import PySparkTypeError, PySparkValueError
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_plotly,
    have_numpy,
    plotly_requirement_message,
    numpy_requirement_message,
)


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

    @property
    def sdf4(self):
        data = [
            ("A", 50, 55),
            ("B", 55, 60),
            ("C", 60, 65),
            ("D", 65, 70),
            ("E", 70, 75),
            # outliers
            ("F", 10, 15),
            ("G", 85, 90),
            ("H", 5, 150),
        ]
        columns = ["student", "math_score", "english_score"]
        return self.spark.createDataFrame(data, columns)

    def _check_fig_data(self, fig_data, **kwargs):
        for key, expected_value in kwargs.items():
            if key in ["x", "y", "labels", "values"]:
                converted_values = [v.item() if hasattr(v, "item") else v for v in fig_data[key]]
                self.assertEqual(converted_values, expected_value)
            else:
                self.assertEqual(fig_data[key], expected_value)

    def test_line_plot(self):
        # single column as vertical axis
        fig = self.sdf.plot(kind="line", x="category", y="int_val")
        expected_fig_data = {
            "mode": "lines",
            "name": "",
            "orientation": "v",
            "x": ["A", "B", "C"],
            "xaxis": "x",
            "y": [10, 30, 20],
            "yaxis": "y",
            "type": "scatter",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)

        # multiple columns as vertical axis
        fig = self.sdf.plot.line(x="category", y=["int_val", "float_val"])
        expected_fig_data = {
            "mode": "lines",
            "name": "int_val",
            "orientation": "v",
            "x": ["A", "B", "C"],
            "xaxis": "x",
            "y": [10, 30, 20],
            "yaxis": "y",
            "type": "scatter",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)
        expected_fig_data = {
            "mode": "lines",
            "name": "float_val",
            "orientation": "v",
            "x": ["A", "B", "C"],
            "xaxis": "x",
            "y": [1.5, 2.5, 3.5],
            "yaxis": "y",
            "type": "scatter",
        }
        self._check_fig_data(fig["data"][1], **expected_fig_data)

    def test_bar_plot(self):
        # single column as vertical axis
        fig = self.sdf.plot(kind="bar", x="category", y="int_val")
        expected_fig_data = {
            "name": "",
            "orientation": "v",
            "x": ["A", "B", "C"],
            "xaxis": "x",
            "y": [10, 30, 20],
            "yaxis": "y",
            "type": "bar",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)

        # multiple columns as vertical axis
        fig = self.sdf.plot.bar(x="category", y=["int_val", "float_val"])
        expected_fig_data = {
            "name": "int_val",
            "orientation": "v",
            "x": ["A", "B", "C"],
            "xaxis": "x",
            "y": [10, 30, 20],
            "yaxis": "y",
            "type": "bar",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)
        expected_fig_data = {
            "name": "float_val",
            "orientation": "v",
            "x": ["A", "B", "C"],
            "xaxis": "x",
            "y": [1.5, 2.5, 3.5],
            "yaxis": "y",
            "type": "bar",
        }
        self._check_fig_data(fig["data"][1], **expected_fig_data)

    def test_barh_plot(self):
        # single column as vertical axis
        fig = self.sdf.plot(kind="barh", x="category", y="int_val")
        expected_fig_data = {
            "name": "",
            "orientation": "h",
            "x": ["A", "B", "C"],
            "xaxis": "x",
            "y": [10, 30, 20],
            "yaxis": "y",
            "type": "bar",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)

        # multiple columns as vertical axis
        fig = self.sdf.plot.barh(x="category", y=["int_val", "float_val"])
        expected_fig_data = {
            "name": "int_val",
            "orientation": "h",
            "x": ["A", "B", "C"],
            "xaxis": "x",
            "y": [10, 30, 20],
            "yaxis": "y",
            "type": "bar",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)
        expected_fig_data = {
            "name": "float_val",
            "orientation": "h",
            "x": ["A", "B", "C"],
            "xaxis": "x",
            "y": [1.5, 2.5, 3.5],
            "yaxis": "y",
            "type": "bar",
        }
        self._check_fig_data(fig["data"][1], **expected_fig_data)

        # multiple columns as horizontal axis
        fig = self.sdf.plot.barh(x=["int_val", "float_val"], y="category")
        expected_fig_data = {
            "name": "int_val",
            "orientation": "h",
            "y": ["A", "B", "C"],
            "xaxis": "x",
            "x": [10, 30, 20],
            "yaxis": "y",
            "type": "bar",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)
        expected_fig_data = {
            "name": "float_val",
            "orientation": "h",
            "y": ["A", "B", "C"],
            "xaxis": "x",
            "x": [1.5, 2.5, 3.5],
            "yaxis": "y",
            "type": "bar",
        }
        self._check_fig_data(fig["data"][1], **expected_fig_data)

    def test_scatter_plot(self):
        fig = self.sdf2.plot(kind="scatter", x="length", y="width")
        expected_fig_data = {
            "name": "",
            "orientation": "v",
            "x": [5.1, 4.9, 7.0, 6.4, 5.9],
            "xaxis": "x",
            "y": [3.5, 3.0, 3.2, 3.2, 3.0],
            "yaxis": "y",
            "type": "scatter",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)
        expected_fig_data = {
            "name": "",
            "orientation": "v",
            "y": [5.1, 4.9, 7.0, 6.4, 5.9],
            "xaxis": "x",
            "x": [3.5, 3.0, 3.2, 3.2, 3.0],
            "yaxis": "y",
            "type": "scatter",
        }
        fig = self.sdf2.plot.scatter(x="width", y="length")
        self._check_fig_data(fig["data"][0], **expected_fig_data)

    def test_area_plot(self):
        # single column as vertical axis
        fig = self.sdf3.plot(kind="area", x="date", y="sales")
        expected_x = [
            datetime(2018, 1, 31, 0, 0),
            datetime(2018, 2, 28, 0, 0),
            datetime(2018, 3, 31, 0, 0),
            datetime(2018, 4, 30, 0, 0),
        ]
        expected_fig_data = {
            "name": "",
            "orientation": "v",
            "x": expected_x,
            "xaxis": "x",
            "y": [3, 2, 3, 9],
            "yaxis": "y",
            "mode": "lines",
            "type": "scatter",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)

        # multiple columns as vertical axis
        fig = self.sdf3.plot.area(x="date", y=["sales", "signups", "visits"])
        expected_fig_data = {
            "name": "sales",
            "orientation": "v",
            "x": expected_x,
            "xaxis": "x",
            "y": [3, 2, 3, 9],
            "yaxis": "y",
            "mode": "lines",
            "type": "scatter",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)
        expected_fig_data = {
            "name": "signups",
            "orientation": "v",
            "x": expected_x,
            "xaxis": "x",
            "y": [5, 5, 6, 12],
            "yaxis": "y",
            "mode": "lines",
            "type": "scatter",
        }
        self._check_fig_data(fig["data"][1], **expected_fig_data)
        expected_fig_data = {
            "name": "visits",
            "orientation": "v",
            "x": expected_x,
            "xaxis": "x",
            "y": [20, 42, 28, 62],
            "yaxis": "y",
            "mode": "lines",
            "type": "scatter",
        }
        self._check_fig_data(fig["data"][2], **expected_fig_data)

    def test_pie_plot(self):
        fig = self.sdf3.plot(kind="pie", x="date", y="sales")
        expected_x = [
            datetime(2018, 1, 31, 0, 0),
            datetime(2018, 2, 28, 0, 0),
            datetime(2018, 3, 31, 0, 0),
            datetime(2018, 4, 30, 0, 0),
        ]
        expected_fig_data = {
            "name": "",
            "labels": expected_x,
            "values": [3, 2, 3, 9],
            "type": "pie",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)

        # y is not a numerical column
        with self.assertRaises(PySparkTypeError) as pe:
            self.sdf.plot.pie(x="int_val", y="category")

        self.check_error(
            exception=pe.exception,
            errorClass="PLOT_NOT_NUMERIC_COLUMN",
            messageParameters={"arg_name": "y", "arg_type": "StringType()"},
        )

    def test_box_plot(self):
        fig = self.sdf4.plot.box(column="math_score")
        expected_fig_data = {
            "boxpoints": "suspectedoutliers",
            "lowerfence": (5,),
            "mean": (50.0,),
            "median": (55,),
            "name": "math_score",
            "notched": False,
            "q1": (10,),
            "q3": (65,),
            "upperfence": (85,),
            "x": [0],
            "type": "box",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)

        fig = self.sdf4.plot(kind="box", column=["math_score", "english_score"])
        self._check_fig_data(fig["data"][0], **expected_fig_data)
        expected_fig_data = {
            "boxpoints": "suspectedoutliers",
            "lowerfence": (55,),
            "mean": (72.5,),
            "median": (65,),
            "name": "english_score",
            "notched": False,
            "q1": (55,),
            "q3": (75,),
            "upperfence": (90,),
            "x": [1],
            "y": [[150, 15]],
            "type": "box",
        }
        self._check_fig_data(fig["data"][1], **expected_fig_data)
        with self.assertRaises(PySparkValueError) as pe:
            self.sdf4.plot.box(column="math_score", boxpoints=True)
        self.check_error(
            exception=pe.exception,
            errorClass="UNSUPPORTED_PLOT_BACKEND_PARAM",
            messageParameters={
                "backend": "plotly",
                "param": "boxpoints",
                "value": "True",
                "supported_values": ", ".join(["suspectedoutliers", "False"]),
            },
        )
        with self.assertRaises(PySparkValueError) as pe:
            self.sdf4.plot.box(column="math_score", notched=True)
        self.check_error(
            exception=pe.exception,
            errorClass="UNSUPPORTED_PLOT_BACKEND_PARAM",
            messageParameters={
                "backend": "plotly",
                "param": "notched",
                "value": "True",
                "supported_values": ", ".join(["False"]),
            },
        )

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_kde_plot(self):
        fig = self.sdf4.plot.kde(column="math_score", bw_method=0.3, ind=5)
        expected_fig_data = {
            "mode": "lines",
            "name": "math_score",
            "orientation": "v",
            "xaxis": "x",
            "yaxis": "y",
            "type": "scatter",
        }
        self._check_fig_data(fig["data"][0], **expected_fig_data)

        fig = self.sdf4.plot.kde(column=["math_score", "english_score"], bw_method=0.3, ind=5)
        self._check_fig_data(fig["data"][0], **expected_fig_data)
        expected_fig_data = {
            "mode": "lines",
            "name": "english_score",
            "orientation": "v",
            "xaxis": "x",
            "yaxis": "y",
            "type": "scatter",
        }
        self._check_fig_data(fig["data"][1], **expected_fig_data)
        self.assertEqual(fig["data"][0]["x"], fig["data"][1]["x"])


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
