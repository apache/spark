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

import numpy as np

from pyspark.ml.classification import (
    LinearSVC,
    LinearSVCModel,
    OneVsRest,
    OneVsRestModel,
)
from pyspark.ml.linalg import Vectors
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

        self.assertEqual(OneVsRest().getIntermediateStorageLevel(), "MEMORY_AND_DISK")
        ovr = OneVsRest(classifier=svc, parallelism=1, intermediateStorageLevel="DISK_ONLY")
        self.assertEqual(ovr.getIntermediateStorageLevel(), "DISK_ONLY")
        ovr.setParams(intermediateStorageLevel="MEMORY_ONLY")
        self.assertEqual(ovr.getIntermediateStorageLevel(), "MEMORY_ONLY")
        ovr.setIntermediateStorageLevel("DISK_ONLY")
        self.assertEqual(ovr.getIntermediateStorageLevel(), "DISK_ONLY")
        self.assertEqual(ovr.getParallelism(), 1)

        model = ovr.fit(df)
        self.assertIsInstance(model, OneVsRestModel)
        self.assertEqual(model.getIntermediateStorageLevel(), "DISK_ONLY")
        model.setIntermediateStorageLevel("MEMORY_ONLY")
        self.assertEqual(model.getIntermediateStorageLevel(), "MEMORY_ONLY")
        model.setIntermediateStorageLevel("DISK_ONLY")
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
            self.assertEqual(ovr2.getIntermediateStorageLevel(), "DISK_ONLY")
            self.assertEqual(str(ovr), str(ovr2))

            model.write().overwrite().save(d)
            model2 = OneVsRestModel.load(d)
            self.assertEqual(model2.getIntermediateStorageLevel(), "DISK_ONLY")
            self.assertEqual(str(model), str(model2))


class OneVsRestTests(OneVsRestTestsMixin, ReusedSQLTestCase):
    def test_intermediate_storage_level_conversion(self):
        from pyspark import StorageLevel

        ovr = OneVsRest()
        for entry in self.sc._jvm.org.apache.spark.storage.StorageLevelMapper.values():
            name = entry.name()
            if name == "NONE":
                continue
            with self.subTest(storageLevel=name):
                expected = self.sc._jvm.org.apache.spark.storage.StorageLevel.fromString(name)
                actual = ovr.setIntermediateStorageLevel(name)._getIntermediateStorageLevel()
                self.assertEqual(
                    actual,
                    StorageLevel(
                        expected.useDisk(),
                        expected.useMemory(),
                        expected.useOffHeap(),
                        expected.deserialized(),
                        expected.replication(),
                    ),
                )

        for name in ["NONE", "INVALID", "MEMORY_AND_DISK_DESER"]:
            with self.subTest(storageLevel=name), self.assertRaises(ValueError):
                ovr.setIntermediateStorageLevel(name)._getIntermediateStorageLevel()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
