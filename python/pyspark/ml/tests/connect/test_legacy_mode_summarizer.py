# -*- coding: utf-8 -*-
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
import numpy as np

from pyspark.ml.connect.summarizer import summarize_dataframe
from pyspark.sql import SparkSession


class SummarizerTestsMixin:
    def test_summarize_dataframe(self):
        df1 = self.spark.createDataFrame(
            [
                ([2.0, -1.5],),
                ([-3.0, 0.5],),
                ([1.0, 3.5],),
            ],
            schema=["features"],
        )

        df1_local = df1.toPandas()

        result = summarize_dataframe(df1, "features", ["min", "max", "sum", "mean", "std"])
        result_local = summarize_dataframe(
            df1_local, "features", ["min", "max", "sum", "mean", "std"]
        )
        expected_result = {
            "min": [-3.0, -1.5],
            "max": [2.0, 3.5],
            "sum": [0.0, 2.5],
            "mean": [0.0, 0.83333333],
            "std": [2.64575131, 2.51661148],
        }

        def assert_dict_allclose(dict1, dict2):
            assert set(dict1.keys()) == set(dict2.keys())

            for key in dict1:
                np.testing.assert_allclose(dict1[key], dict2[key])

        assert_dict_allclose(result, expected_result)
        assert_dict_allclose(result_local, expected_result)


class SummarizerTests(SummarizerTestsMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[2]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_legacy_mode_summarizer import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
