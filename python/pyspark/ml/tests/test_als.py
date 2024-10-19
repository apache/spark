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

import pyspark.sql.functions as sf
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.testing.sqlutils import ReusedSQLTestCase


class ALSTest(ReusedSQLTestCase):
    def test_ambiguous_column(self):
        data = self.spark.createDataFrame(
            [[1, 15, 1], [1, 2, 2], [2, 3, 4], [2, 2, 5]],
            ["user", "item", "rating"],
        )
        model = ALS(
            userCol="user",
            itemCol="item",
            ratingCol="rating",
            numUserBlocks=10,
            numItemBlocks=10,
            maxIter=1,
            seed=42,
        ).fit(data)

        with tempfile.TemporaryDirectory(prefix="test_ambiguous_column") as d:
            model.write().overwrite().save(d)
            loaded_model = ALSModel().load(d)

            with self.sql_conf({"spark.sql.analyzer.failAmbiguousSelfJoin": False}):
                users = loaded_model.userFactors.select(sf.col("id").alias("user"))
                items = loaded_model.itemFactors.select(sf.col("id").alias("item"))
                predictions = loaded_model.transform(users.crossJoin(items))
                self.assertTrue(predictions.count() > 0)

            with self.sql_conf({"spark.sql.analyzer.failAmbiguousSelfJoin": True}):
                users = loaded_model.userFactors.select(sf.col("id").alias("user"))
                items = loaded_model.itemFactors.select(sf.col("id").alias("item"))
                predictions = loaded_model.transform(users.crossJoin(items))
                self.assertTrue(predictions.count() > 0)


if __name__ == "__main__":
    from pyspark.ml.tests.test_als import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
