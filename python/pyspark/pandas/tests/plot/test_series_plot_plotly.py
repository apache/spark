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
from pyspark.pandas.utils import name_like_string
from pyspark.testing.pandasutils import (
    have_plotly,
    plotly_requirement_message,
    PandasOnSparkTestCase,
    TestUtils,
)

if have_plotly:
    from plotly import express
    import plotly.graph_objs as go


@unittest.skipIf(not have_plotly, plotly_requirement_message)
class SeriesPlotPlotlyTest(PandasOnSparkTestCase, TestUtils):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        pd.set_option("plotting.backend", "plotly")
        set_option("plotting.backend", "plotly")
        set_option("plotting.max_rows", 1000)
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
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 50]}, index=[0, 1, 3, 5, 6, 8, 9, 9, 9, 10, 10]
        )

    @property
    def psdf1(self):
        return ps.from_pandas(self.pdf1)

    @property
    def psdf2(self):
        return ps.range(1002)

    @property
    def pdf2(self):
        return self.psdf2._to_pandas()

    def test_bar_plot(self):
        pdf = self.pdf1
        psdf = self.psdf1

        self.assertEqual(pdf["a"].plot(kind="bar"), psdf["a"].plot(kind="bar"))
        self.assertEqual(pdf["a"].plot.bar(), psdf["a"].plot.bar())

    def test_line_plot(self):
        pdf = self.pdf1
        psdf = self.psdf1

        self.assertEqual(pdf["a"].plot(kind="line"), psdf["a"].plot(kind="line"))
        self.assertEqual(pdf["a"].plot.line(), psdf["a"].plot.line())

    def test_barh_plot(self):
        pdf = self.pdf1
        psdf = self.psdf1

        self.assertEqual(pdf["a"].plot(kind="barh"), psdf["a"].plot(kind="barh"))

    def test_area_plot(self):
        pdf = pd.DataFrame(
            {
                "sales": [3, 2, 3, 9, 10, 6],
                "signups": [5, 5, 6, 12, 14, 13],
                "visits": [20, 42, 28, 62, 81, 50],
            },
            index=pd.date_range(start="2018/01/01", end="2018/07/01", freq="M"),
        )
        psdf = ps.from_pandas(pdf)

        self.assertEqual(pdf["sales"].plot(kind="area"), psdf["sales"].plot(kind="area"))
        self.assertEqual(pdf["sales"].plot.area(), psdf["sales"].plot.area())

        # just a sanity check for df.col type
        self.assertEqual(pdf.sales.plot(kind="area"), psdf.sales.plot(kind="area"))

    def test_pie_plot(self):
        psdf = self.psdf1
        pdf = psdf._to_pandas()
        self.assertEqual(
            psdf["a"].plot(kind="pie"),
            express.pie(pdf, values=pdf.columns[0], names=pdf.index),
        )

        # TODO: support multi-index columns
        # columns = pd.MultiIndex.from_tuples([("x", "y")])
        # psdf.columns = columns
        # pdf.columns = columns
        # self.assertEqual(
        #     psdf[("x", "y")].plot(kind="pie"),
        #     express.pie(pdf, values=pdf.iloc[:, 0].to_numpy(), names=pdf.index.to_numpy()),
        # )

        # TODO: support multi-index
        # psdf = ps.DataFrame(
        #     {
        #         "a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 50],
        #         "b": [2, 3, 4, 5, 7, 9, 10, 15, 34, 45, 49]
        #     },
        #     index=pd.MultiIndex.from_tuples([("x", "y")] * 11),
        # )
        # pdf = psdf._to_pandas()
        # self.assertEqual(
        #     psdf["a"].plot(kind="pie"), express.pie(pdf, values=pdf.columns[0], names=pdf.index),
        # )

    def test_hist_plot(self):
        def check_hist_plot(psser):
            bins = np.array([1.0, 5.9, 10.8, 15.7, 20.6, 25.5, 30.4, 35.3, 40.2, 45.1, 50.0])
            data = np.array([5.0, 4.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0])
            prev = bins[0]
            text_bins = []
            for b in bins[1:]:
                text_bins.append("[%s, %s)" % (prev, b))
                prev = b
            text_bins[-1] = text_bins[-1][:-1] + "]"
            bins = 0.5 * (bins[:-1] + bins[1:])
            name_a = name_like_string(psser.name)
            bars = [
                go.Bar(
                    x=bins,
                    y=data,
                    name=name_a,
                    text=text_bins,
                    hovertemplate=("variable=" + name_a + "<br>value=%{text}<br>count=%{y}"),
                ),
            ]
            fig = go.Figure(data=bars, layout=go.Layout(barmode="stack"))
            fig["layout"]["xaxis"]["title"] = "value"
            fig["layout"]["yaxis"]["title"] = "count"

            self.assertEqual(
                pprint.pformat(psser.plot(kind="hist").to_dict()), pprint.pformat(fig.to_dict())
            )

        psdf1 = self.psdf1
        check_hist_plot(psdf1["a"])

        columns = pd.MultiIndex.from_tuples([("x", "y")])
        psdf1.columns = columns
        check_hist_plot(psdf1[("x", "y")])

    def test_pox_plot(self):
        def check_pox_plot(psser):
            fig = go.Figure()
            fig.add_trace(
                go.Box(
                    name=name_like_string(psser.name),
                    q1=[3],
                    median=[6],
                    q3=[9],
                    mean=[10.0],
                    lowerfence=[1],
                    upperfence=[15],
                    y=[[50]],
                    boxpoints="suspectedoutliers",
                    notched=False,
                )
            )
            fig["layout"]["xaxis"]["title"] = name_like_string(psser.name)
            fig["layout"]["yaxis"]["title"] = "value"

            self.assertEqual(
                pprint.pformat(psser.plot(kind="box").to_dict()), pprint.pformat(fig.to_dict())
            )

        psdf1 = self.psdf1
        check_pox_plot(psdf1["a"])

        columns = pd.MultiIndex.from_tuples([("x", "y")])
        psdf1.columns = columns
        check_pox_plot(psdf1[("x", "y")])

    def test_pox_plot_arguments(self):
        with self.assertRaisesRegex(ValueError, "does not support"):
            self.psdf1.a.plot.box(boxpoints="all")
        with self.assertRaisesRegex(ValueError, "does not support"):
            self.psdf1.a.plot.box(notched=True)
        self.psdf1.a.plot.box(hovertext="abc")  # other arguments should not throw an exception

    def test_kde_plot(self):
        psdf = ps.DataFrame({"a": [1, 2, 3, 4, 5]})
        pdf = pd.DataFrame(
            {
                "Density": [0.05709372, 0.07670272, 0.05709372],
                "names": ["a", "a", "a"],
                "index": [-1.0, 3.0, 7.0],
            }
        )

        actual = psdf.a.plot.kde(bw_method=5, ind=3)

        expected = express.line(pdf, x="index", y="Density")
        expected["layout"]["xaxis"]["title"] = None

        self.assertEqual(pprint.pformat(actual.to_dict()), pprint.pformat(expected.to_dict()))


if __name__ == "__main__":
    from pyspark.pandas.tests.plot.test_series_plot_plotly import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
