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
import pprint

import pandas as pd
import numpy as np

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import (
    have_plotly,
    plotly_requirement_message,
    PandasOnSparkTestCase,
    TestUtils,
)
from pyspark.pandas.utils import name_like_string

if have_plotly:
    from plotly import express
    import plotly.graph_objs as go


@unittest.skipIf(not have_plotly, plotly_requirement_message)
class DataFramePlotPlotlyTestsMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        pd.set_option("plotting.backend", "plotly")
        set_option("plotting.backend", "plotly")
        set_option("plotting.max_rows", 2000)
        set_option("plotting.sample_ratio", None)

    @classmethod
    def tearDownClass(cls):
        pd.reset_option("plotting.backend")
        reset_option("plotting.backend")
        reset_option("plotting.max_rows")
        reset_option("plotting.sample_ratio")
        super().tearDownClass()

    @property
    def pdf1(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 50], "b": [2, 3, 4, 5, 7, 9, 10, 15, 34, 45, 49]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9, 10, 10],
        )

    @property
    def psdf1(self):
        return ps.from_pandas(self.pdf1)

    def test_line_plot(self):
        def check_line_plot(pdf, psdf):
            self.assertEqual(pdf.plot(kind="line"), psdf.plot(kind="line"))
            self.assertEqual(pdf.plot.line(), psdf.plot.line())

        pdf1 = self.pdf1
        psdf1 = self.psdf1
        check_line_plot(pdf1, psdf1)

    def test_area_plot(self):
        def check_area_plot(pdf, psdf):
            self.assertEqual(pdf.plot(kind="area"), psdf.plot(kind="area"))
            self.assertEqual(pdf.plot.area(), psdf.plot.area())

        pdf = self.pdf1
        psdf = self.psdf1
        check_area_plot(pdf, psdf)

    def test_area_plot_y(self):
        def check_area_plot_y(pdf, psdf, y):
            self.assertEqual(pdf.plot.area(y=y), psdf.plot.area(y=y))

        # test if frame area plot is correct when y is specified
        pdf = pd.DataFrame(
            {
                "sales": [3, 2, 3, 9, 10, 6],
                "signups": [5, 5, 6, 12, 14, 13],
                "visits": [20, 42, 28, 62, 81, 50],
            },
            index=pd.date_range(start="2018/01/01", end="2018/07/01", freq="M"),
        )
        psdf = ps.from_pandas(pdf)
        check_area_plot_y(pdf, psdf, y="sales")

    def test_barh_plot_with_x_y(self):
        def check_barh_plot_with_x_y(pdf, psdf, x, y):
            self.assertEqual(pdf.plot(kind="barh", x=x, y=y), psdf.plot(kind="barh", x=x, y=y))
            self.assertEqual(pdf.plot.barh(x=x, y=y), psdf.plot.barh(x=x, y=y))

        # this is testing plot with specified x and y
        pdf1 = pd.DataFrame({"lab": ["A", "B", "C"], "val": [10, 30, 20], "val2": [1.1, 2.2, 3.3]})
        psdf1 = ps.from_pandas(pdf1)
        check_barh_plot_with_x_y(pdf1, psdf1, x="val", y="lab")
        check_barh_plot_with_x_y(pdf1, psdf1, x=["val", "val2"], y="lab")

    def test_barh_plot(self):
        def check_barh_plot(pdf, psdf):
            self.assertEqual(pdf.plot(kind="barh"), psdf.plot(kind="barh"))
            self.assertEqual(pdf.plot.barh(), psdf.plot.barh())

        # this is testing when x or y is not assigned
        pdf1 = pd.DataFrame({"lab": [20.1, 40.5, 60.6], "val": [10, 30, 20]})
        psdf1 = ps.from_pandas(pdf1)
        check_barh_plot(pdf1, psdf1)

    def test_bar_plot(self):
        def check_bar_plot(pdf, psdf):
            self.assertEqual(pdf.plot(kind="bar"), psdf.plot(kind="bar"))
            self.assertEqual(pdf.plot.bar(), psdf.plot.bar())

        pdf1 = self.pdf1
        psdf1 = self.psdf1
        check_bar_plot(pdf1, psdf1)

    def test_bar_with_x_y(self):
        # this is testing plot with specified x and y
        pdf = pd.DataFrame({"lab": ["A", "B", "C"], "val": [10, 30, 20]})
        psdf = ps.from_pandas(pdf)

        self.assertEqual(
            pdf.plot(kind="bar", x="lab", y="val"), psdf.plot(kind="bar", x="lab", y="val")
        )
        self.assertEqual(pdf.plot.bar(x="lab", y="val"), psdf.plot.bar(x="lab", y="val"))

    def test_scatter_plot(self):
        def check_scatter_plot(pdf, psdf, x, y, c):
            self.assertEqual(pdf.plot.scatter(x=x, y=y), psdf.plot.scatter(x=x, y=y))
            self.assertEqual(
                pdf.plot(kind="scatter", x=x, y=y), psdf.plot(kind="scatter", x=x, y=y)
            )

            # check when keyword c is given as name of a column
            self.assertEqual(
                pdf.plot.scatter(x=x, y=y, c=c, s=50), psdf.plot.scatter(x=x, y=y, c=c, s=50)
            )

        # Use pandas scatter plot example
        pdf1 = pd.DataFrame(np.random.rand(50, 4), columns=["a", "b", "c", "d"])
        psdf1 = ps.from_pandas(pdf1)
        check_scatter_plot(pdf1, psdf1, x="a", y="b", c="c")

    def test_pie_plot(self):
        def check_pie_plot(psdf):
            pdf = psdf._to_pandas()
            self.assertEqual(
                psdf.plot(kind="pie", y=psdf.columns[0]),
                express.pie(pdf, values="a", names=pdf.index),
            )

            self.assertEqual(
                psdf.plot(kind="pie", values="a"),
                express.pie(pdf, values="a"),
            )

        psdf1 = self.psdf1
        check_pie_plot(psdf1)

        # TODO: support multi-index columns
        # columns = pd.MultiIndex.from_tuples([("x", "y"), ("y", "z")])
        # psdf1.columns = columns
        # check_pie_plot(psdf1)

        # TODO: support multi-index
        # psdf1 = ps.DataFrame(
        #     {
        #         "a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 50],
        #         "b": [2, 3, 4, 5, 7, 9, 10, 15, 34, 45, 49]
        #     },
        #     index=pd.MultiIndex.from_tuples([("x", "y")] * 11),
        # )
        # check_pie_plot(psdf1)

    def test_hist_layout_kwargs(self):
        s = ps.Series([1, 3, 2])
        plt = s.plot.hist(title="Title", foo="xxx")
        self.assertEqual(plt.layout.barmode, "stack")
        self.assertEqual(plt.layout.title.text, "Title")
        self.assertFalse(hasattr(plt.layout, "foo"))

    def test_hist_plot(self):
        def check_hist_plot(psdf):
            bins = np.array([1.0, 5.9, 10.8, 15.7, 20.6, 25.5, 30.4, 35.3, 40.2, 45.1, 50.0])
            data = [
                np.array([5.0, 4.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0]),
                np.array([4.0, 3.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0]),
            ]
            prev = bins[0]
            text_bins = []
            for b in bins[1:]:
                text_bins.append("[%s, %s)" % (prev, b))
                prev = b
            text_bins[-1] = text_bins[-1][:-1] + "]"
            bins = 0.5 * (bins[:-1] + bins[1:])
            name_a = name_like_string(psdf.columns[0])
            name_b = name_like_string(psdf.columns[1])
            bars = [
                go.Bar(
                    x=bins,
                    y=data[0],
                    name=name_a,
                    text=text_bins,
                    hovertemplate=("variable=" + name_a + "<br>value=%{text}<br>count=%{y}"),
                ),
                go.Bar(
                    x=bins,
                    y=data[1],
                    name=name_b,
                    text=text_bins,
                    hovertemplate=("variable=" + name_b + "<br>value=%{text}<br>count=%{y}"),
                ),
            ]
            fig = go.Figure(data=bars, layout=go.Layout(barmode="stack"))
            fig["layout"]["xaxis"]["title"] = "value"
            fig["layout"]["yaxis"]["title"] = "count"

            self.assertEqual(
                pprint.pformat(psdf.plot(kind="hist").to_dict()), pprint.pformat(fig.to_dict())
            )

        psdf1 = self.psdf1
        check_hist_plot(psdf1)

        columns = pd.MultiIndex.from_tuples([("x", "y"), ("y", "z")])
        psdf1.columns = columns
        check_hist_plot(psdf1)

    def test_kde_plot(self):
        psdf = ps.DataFrame({"a": [1, 2, 3, 4, 5], "b": [1, 3, 5, 7, 9], "c": [2, 4, 6, 8, 10]})

        pdf = pd.DataFrame(
            {
                "Density": [
                    0.03515491,
                    0.06834979,
                    0.00663503,
                    0.02372059,
                    0.06834979,
                    0.01806934,
                    0.01806934,
                    0.06834979,
                    0.02372059,
                ],
                "names": ["a", "a", "a", "b", "b", "b", "c", "c", "c"],
                "index": [-3.5, 5.5, 14.5, -3.5, 5.5, 14.5, -3.5, 5.5, 14.5],
            }
        )

        actual = psdf.plot.kde(bw_method=5, ind=3)

        expected = express.line(pdf, x="index", y="Density", color="names")
        expected["layout"]["xaxis"]["title"] = None

        self.assertEqual(pprint.pformat(actual.to_dict()), pprint.pformat(expected.to_dict()))


class DataFramePlotPlotlyTests(DataFramePlotPlotlyTestsMixin, PandasOnSparkTestCase, TestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.plot.test_frame_plot_plotly import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
