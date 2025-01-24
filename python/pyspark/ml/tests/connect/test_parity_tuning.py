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

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.connect.readwrite import RemoteCrossValidatorModelWriter
from pyspark.ml.linalg import Vectors
from pyspark.ml.tests.test_tuning import TuningTestsMixin
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.testing.connectutils import ReusedConnectTestCase


class TuningParityTests(TuningTestsMixin, ReusedConnectTestCase):
    def test_remote_cross_validator_model_writer(self):
        df = self.spark.createDataFrame(
            [
                (1.0, 1.0, Vectors.dense(0.0, 5.0)),
                (0.0, 2.0, Vectors.dense(1.0, 2.0)),
                (1.0, 3.0, Vectors.dense(2.0, 1.0)),
                (0.0, 4.0, Vectors.dense(3.0, 3.0)),
            ],
            ["label", "weight", "features"],
        )

        lor = LogisticRegression()
        lor_model = lor.fit(df)
        cv_model = CrossValidatorModel(lor_model)
        writer = RemoteCrossValidatorModelWriter(cv_model, {"a": "b"}, self.spark)
        self.assertEqual(writer.optionMap["a"], "b")


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_parity_tuning import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
