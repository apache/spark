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

import base64
from io import BytesIO
import unittest

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import (
    have_matplotlib,
    matplotlib_requirement_message,
    PandasOnSparkTestCase,
    TestUtils,
)

if have_matplotlib:
    import matplotlib
    from matplotlib import pyplot as plt

    matplotlib.use("agg")


@unittest.skipIf(not have_matplotlib, matplotlib_requirement_message)
class SeriesPlotMatplotlibTest(PandasOnSparkTestCase, TestUtils):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        pd.set_option("plotting.backend", "matplotlib")
        set_option("plotting.backend", "matplotlib")
        set_option("plotting.max_rows", 1000)

    @classmethod
    def tearDownClass(cls):
        pd.reset_option("plotting.backend")
        reset_option("plotting.backend")
        reset_option("plotting.max_rows")
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
        return self.psdf2.to_pandas()

    @staticmethod
    def plot_to_base64(ax):
        bytes_data = BytesIO()
        ax.figure.savefig(bytes_data, format="png")
        bytes_data.seek(0)
        b64_data = base64.b64encode(bytes_data.read())
        plt.close(ax.figure)
        return b64_data

    def test_bar_plot(self):
        pdf = self.pdf1
        psdf = self.psdf1

        ax1 = pdf["a"].plot(kind="bar", colormap="Paired")
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf["a"].plot(kind="bar", colormap="Paired")
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

        ax1 = pdf["a"].plot(kind="bar", colormap="Paired")
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf["a"].plot(kind="bar", colormap="Paired")
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

    def test_bar_plot_limited(self):
        pdf = self.pdf2
        psdf = self.psdf2

        _, ax1 = plt.subplots(1, 1)
        ax1 = pdf["id"][:1000].plot.bar(colormap="Paired")
        ax1.text(
            1,
            1,
            "showing top 1000 elements only",
            size=6,
            ha="right",
            va="bottom",
            transform=ax1.transAxes,
        )
        bin1 = self.plot_to_base64(ax1)

        _, ax2 = plt.subplots(1, 1)
        ax2 = psdf["id"].plot.bar(colormap="Paired")
        bin2 = self.plot_to_base64(ax2)

        self.assertEqual(bin1, bin2)

    def test_pie_plot(self):
        pdf = self.pdf1
        psdf = self.psdf1

        ax1 = pdf["a"].plot.pie(colormap="Paired")
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf["a"].plot.pie(colormap="Paired")
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

        ax1 = pdf["a"].plot(kind="pie", colormap="Paired")
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf["a"].plot(kind="pie", colormap="Paired")
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

    def test_pie_plot_limited(self):
        pdf = self.pdf2
        psdf = self.psdf2

        _, ax1 = plt.subplots(1, 1)
        ax1 = pdf["id"][:1000].plot.pie(colormap="Paired")
        ax1.text(
            1,
            1,
            "showing top 1000 elements only",
            size=6,
            ha="right",
            va="bottom",
            transform=ax1.transAxes,
        )
        bin1 = self.plot_to_base64(ax1)

        _, ax2 = plt.subplots(1, 1)
        ax2 = psdf["id"].plot.pie(colormap="Paired")
        bin2 = self.plot_to_base64(ax2)

        self.assertEqual(bin1, bin2)

    def test_line_plot(self):
        pdf = self.pdf1
        psdf = self.psdf1

        ax1 = pdf["a"].plot(kind="line", colormap="Paired")
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf["a"].plot(kind="line", colormap="Paired")
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

        ax1 = pdf["a"].plot.line(colormap="Paired")
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf["a"].plot.line(colormap="Paired")
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

    def test_barh_plot(self):
        pdf = self.pdf1
        psdf = self.psdf1

        ax1 = pdf["a"].plot(kind="barh", colormap="Paired")
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf["a"].plot(kind="barh", colormap="Paired")
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

    def test_barh_plot_limited(self):
        pdf = self.pdf2
        psdf = self.psdf2

        _, ax1 = plt.subplots(1, 1)
        ax1 = pdf["id"][:1000].plot.barh(colormap="Paired")
        ax1.text(
            1,
            1,
            "showing top 1000 elements only",
            size=6,
            ha="right",
            va="bottom",
            transform=ax1.transAxes,
        )
        bin1 = self.plot_to_base64(ax1)

        _, ax2 = plt.subplots(1, 1)
        ax2 = psdf["id"].plot.barh(colormap="Paired")
        bin2 = self.plot_to_base64(ax2)

        self.assertEqual(bin1, bin2)

    def test_hist(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 50]}, index=[0, 1, 3, 5, 6, 8, 9, 9, 9, 10, 10]
        )

        psdf = ps.from_pandas(pdf)

        def plot_to_base64(ax):
            bytes_data = BytesIO()
            ax.figure.savefig(bytes_data, format="png")
            bytes_data.seek(0)
            b64_data = base64.b64encode(bytes_data.read())
            plt.close(ax.figure)
            return b64_data

        _, ax1 = plt.subplots(1, 1)
        # Using plot.hist() because pandas changes ticks props when called hist()
        ax1 = pdf["a"].plot.hist()
        _, ax2 = plt.subplots(1, 1)
        ax2 = psdf["a"].hist()
        self.assert_eq(plot_to_base64(ax1), plot_to_base64(ax2))

    def test_hist_plot(self):
        pdf = self.pdf1
        psdf = self.psdf1

        _, ax1 = plt.subplots(1, 1)
        ax1 = pdf["a"].plot.hist()
        bin1 = self.plot_to_base64(ax1)
        _, ax2 = plt.subplots(1, 1)
        ax2 = psdf["a"].plot.hist()
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

        ax1 = pdf["a"].plot.hist(bins=15)
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf["a"].plot.hist(bins=15)
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

        ax1 = pdf["a"].plot(kind="hist", bins=15)
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf["a"].plot(kind="hist", bins=15)
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

        ax1 = pdf["a"].plot.hist(bins=3, bottom=[2, 1, 3])
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf["a"].plot.hist(bins=3, bottom=[2, 1, 3])
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

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

        ax1 = pdf["sales"].plot(kind="area", colormap="Paired")
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf["sales"].plot(kind="area", colormap="Paired")
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

        ax1 = pdf["sales"].plot.area(colormap="Paired")
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf["sales"].plot.area(colormap="Paired")
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

        # just a sanity check for df.col type
        ax1 = pdf.sales.plot(kind="area", colormap="Paired")
        bin1 = self.plot_to_base64(ax1)
        ax2 = psdf.sales.plot(kind="area", colormap="Paired")
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)

    def test_box_plot(self):
        def check_box_plot(pser, psser, *args, **kwargs):
            _, ax1 = plt.subplots(1, 1)
            ax1 = pser.plot.box(*args, **kwargs)
            _, ax2 = plt.subplots(1, 1)
            ax2 = psser.plot.box(*args, **kwargs)

            diffs = [
                np.array([0, 0.5, 0, 0.5, 0, -0.5, 0, -0.5, 0, 0.5]),
                np.array([0, 0.5, 0, 0]),
                np.array([0, -0.5, 0, 0]),
            ]

            try:
                for i, (line1, line2) in enumerate(zip(ax1.get_lines(), ax2.get_lines())):
                    expected = line1.get_xydata().ravel()
                    actual = line2.get_xydata().ravel()
                    if i < 3:
                        actual += diffs[i]
                    self.assert_eq(pd.Series(expected), pd.Series(actual))
            finally:
                ax1.cla()
                ax2.cla()

        # Non-named Series
        pser = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 50], [0, 1, 3, 5, 6, 8, 9, 9, 9, 10, 10])
        psser = ps.from_pandas(pser)

        spec = [(self.pdf1.a, self.psdf1.a), (pser, psser)]

        for p, k in spec:
            check_box_plot(p, k)
            check_box_plot(p, k, showfliers=True)
            check_box_plot(p, k, sym="")
            check_box_plot(p, k, sym=".", color="r")
            check_box_plot(p, k, use_index=False, labels=["Test"])
            check_box_plot(p, k, usermedians=[2.0])
            check_box_plot(p, k, conf_intervals=[(1.0, 3.0)])

        val = (1, 3)
        self.assertRaises(
            ValueError, lambda: check_box_plot(self.pdf1, self.psdf1, usermedians=[2.0, 3.0])
        )
        self.assertRaises(
            ValueError, lambda: check_box_plot(self.pdf1, self.psdf1, conf_intervals=[val, val])
        )
        self.assertRaises(
            ValueError, lambda: check_box_plot(self.pdf1, self.psdf1, conf_intervals=[(1,)])
        )

    def test_kde_plot(self):
        def moving_average(a, n=10):
            ret = np.cumsum(a, dtype=float)
            ret[n:] = ret[n:] - ret[:-n]
            return ret[n - 1 :] / n

        def check_kde_plot(pdf, psdf, *args, **kwargs):
            _, ax1 = plt.subplots(1, 1)
            ax1 = pdf["a"].plot.kde(*args, **kwargs)
            _, ax2 = plt.subplots(1, 1)
            ax2 = psdf["a"].plot.kde(*args, **kwargs)

            try:
                for i, (line1, line2) in enumerate(zip(ax1.get_lines(), ax2.get_lines())):
                    expected = line1.get_xydata().ravel()
                    actual = line2.get_xydata().ravel()
                    # TODO: Due to implementation difference, the output is different comparing
                    # to pandas'. We should identify the root cause of difference, and reduce
                    # the diff.

                    # Note: Data is from 1 to 50. So, it smooths them by moving average and compares
                    # both.
                    self.assertTrue(
                        np.allclose(moving_average(actual), moving_average(expected), rtol=3)
                    )
            finally:
                ax1.cla()
                ax2.cla()

        check_kde_plot(self.pdf1, self.psdf1, bw_method=0.3)
        check_kde_plot(self.pdf1, self.psdf1, ind=[1, 2, 3, 4, 5], bw_method=3.0)

    def test_empty_hist(self):
        pdf = self.pdf1.assign(categorical="A")
        psdf = ps.from_pandas(pdf)
        psser = psdf["categorical"]

        with self.assertRaisesRegex(TypeError, "Empty 'DataFrame': no numeric data to plot"):
            psser.plot.hist()

    def test_single_value_hist(self):
        pdf = self.pdf1.assign(single=2)
        psdf = ps.from_pandas(pdf)

        _, ax1 = plt.subplots(1, 1)
        ax1 = pdf["single"].plot.hist()
        bin1 = self.plot_to_base64(ax1)
        _, ax2 = plt.subplots(1, 1)
        ax2 = psdf["single"].plot.hist()
        bin2 = self.plot_to_base64(ax2)
        self.assertEqual(bin1, bin2)


if __name__ == "__main__":
    from pyspark.pandas.tests.plot.test_series_plot_matplotlib import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
