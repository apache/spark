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

import tempfile
import unittest

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.ml.fpm import (
    FPGrowth,
    FPGrowthModel,
)


class FPMTestsMixin:
    def test_fp_growth(self):
        df = self.spark.createDataFrame(
            [
                ["r z h k p"],
                ["z y x w v u t s"],
                ["s x o n r"],
                ["x z y m t s q e"],
                ["z"],
                ["x z y r q t p"],
            ],
            ["items"],
        ).select(sf.split("items", " ").alias("items"))

        fp = FPGrowth(minSupport=0.2, minConfidence=0.7)
        fp.setNumPartitions(1)
        self.assertEqual(fp.getMinSupport(), 0.2)
        self.assertEqual(fp.getMinConfidence(), 0.7)
        self.assertEqual(fp.getNumPartitions(), 1)

        # Estimator save & load
        with tempfile.TemporaryDirectory(prefix="fp_growth") as d:
            fp.write().overwrite().save(d)
            fp2 = FPGrowth.load(d)
            self.assertEqual(str(fp), str(fp2))

        model = fp.fit(df)

        self.assertEqual(model.freqItemsets.columns, ["items", "freq"])
        self.assertEqual(model.freqItemsets.count(), 54)

        self.assertEqual(
            model.associationRules.columns,
            ["antecedent", "consequent", "confidence", "lift", "support"],
        )
        self.assertEqual(model.associationRules.count(), 89)

        output = model.transform(df)
        self.assertEqual(output.columns, ["items", "prediction"])
        self.assertEqual(output.count(), 6)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="fp_growth_model") as d:
            model.write().overwrite().save(d)
            model2 = FPGrowthModel.load(d)
            self.assertEqual(str(model), str(model2))


class FPMTests(FPMTestsMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[4]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == "__main__":
    from pyspark.ml.tests.test_fpm import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
