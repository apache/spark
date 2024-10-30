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

import pandas as pd
import numpy as np

from pyspark import pandas as ps
from pyspark.pandas.plot import PandasOnSparkPlotAccessor, BoxPlotBase
from pyspark.testing.pandasutils import have_plotly, plotly_requirement_message


class SeriesPlotTestsMixin:
    @property
    def pdf1(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 50]}, index=[0, 1, 3, 5, 6, 8, 9, 9, 9, 10, 10]
        )

    @property
    def psdf1(self):
        return ps.from_pandas(self.pdf1)

    @unittest.skipIf(not have_plotly, plotly_requirement_message)
    def test_plot_backends(self):
        plot_backend = "plotly"

        with ps.option_context("plotting.backend", plot_backend):
            self.assertEqual(ps.options.plotting.backend, plot_backend)

            module = PandasOnSparkPlotAccessor._get_plot_backend(plot_backend)
            self.assertEqual(module.__name__, "pyspark.pandas.plot.plotly")

    def test_plot_backends_incorrect(self):
        fake_plot_backend = "none_plotting_module"

        with ps.option_context("plotting.backend", fake_plot_backend):
            self.assertEqual(ps.options.plotting.backend, fake_plot_backend)

            with self.assertRaises(ValueError):
                PandasOnSparkPlotAccessor._get_plot_backend(fake_plot_backend)

    def test_box_summary(self):
        psdf = self.psdf1
        pdf = self.pdf1

        k = 1.5

        results = BoxPlotBase.compute_box(
            sdf=psdf._internal.resolved_copy.spark_frame,
            colnames=["a"],
            whis=k,
            precision=0.01,
            showfliers=True,
        )
        self.assertEqual(len(results), 1)
        result = results[0]

        expected_mean = pdf["a"].mean()
        expected_median = pdf["a"].median()
        expected_q1 = np.percentile(pdf["a"], 25)
        expected_q3 = np.percentile(pdf["a"], 75)

        self.assertEqual(expected_mean, result["mean"])
        self.assertEqual(expected_median, result["med"])
        self.assertEqual(expected_q1, result["q1"] + 0.5)
        self.assertEqual(expected_q3, result["q3"] - 0.5)
        self.assertEqual([50], result["fliers"])


class SeriesPlotTests(SeriesPlotTestsMixin, unittest.TestCase):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.plot.test_series_plot import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
