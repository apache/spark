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

import numpy as np

from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import (
    LinearSVC,
    LinearSVCModel,
    OneVsRest,
    OneVsRestModel,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase


class OneVsRestTestsMixin:
    def test_one_vs_rest(self):
        spark = self.spark
        df = (
            spark.createDataFrame(
                [
                    (0, 1.0, Vectors.dense(0.0, 5.0)),
                    (1, 0.0, Vectors.dense(1.0, 2.0)),
                    (2, 1.0, Vectors.dense(2.0, 1.0)),
                    (3, 2.0, Vectors.dense(3.0, 3.0)),
                ],
                ["index", "label", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("index")
            .select("label", "features")
        )

        svc = LinearSVC(maxIter=1, regParam=1.0)
        self.assertEqual(svc.getMaxIter(), 1)
        self.assertEqual(svc.getRegParam(), 1.0)

        ovr = OneVsRest(classifier=svc, parallelism=1)
        self.assertEqual(ovr.getParallelism(), 1)

        model = ovr.fit(df)
        self.assertIsInstance(model, OneVsRestModel)
        self.assertEqual(len(model.models), 3)
        for submodel in model.models:
            self.assertIsInstance(submodel, LinearSVCModel)

        self.assertTrue(
            np.allclose(model.models[0].intercept, 0.06279247869226989, atol=1e-4),
            model.models[0].intercept,
        )
        self.assertTrue(
            np.allclose(
                model.models[0].coefficients.toArray(),
                [-0.1198765502306968, -0.1027513287691687],
                atol=1e-4,
            ),
            model.models[0].coefficients,
        )

        self.assertTrue(
            np.allclose(model.models[1].intercept, 0.025877458475338313, atol=1e-4),
            model.models[1].intercept,
        )
        self.assertTrue(
            np.allclose(
                model.models[1].coefficients.toArray(),
                [-0.0362284418654736, 0.010350983390135305],
                atol=1e-4,
            ),
            model.models[1].coefficients,
        )

        self.assertTrue(
            np.allclose(model.models[2].intercept, -0.37024065419409624, atol=1e-4),
            model.models[2].intercept,
        )
        self.assertTrue(
            np.allclose(
                model.models[2].coefficients.toArray(),
                [0.12886829400126, 0.012273170857262873],
                atol=1e-4,
            ),
            model.models[2].coefficients,
        )

        output = model.transform(df)
        expected_cols = ["label", "features", "rawPrediction", "prediction"]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 4)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="linear_svc") as d:
            ovr.write().overwrite().save(d)
            ovr2 = OneVsRest.load(d)
            self.assertEqual(str(ovr), str(ovr2))

            model.write().overwrite().save(d)
            model2 = OneVsRestModel.load(d)
            self.assertEqual(str(model), str(model2))


class OneVsRestTests(OneVsRestTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.ml.tests.test_ovr import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
