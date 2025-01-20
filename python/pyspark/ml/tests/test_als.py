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


class ALSTestsMixin:
    def test_als(self):
        df = (
            self.spark.createDataFrame(
                [[1, 15, 1], [1, 2, 2], [2, 3, 4], [2, 2, 5]],
                ["user", "item", "rating"],
            )
            .coalesce(1)
            .sortWithinPartitions("user", "item")
        )

        als = ALS(
            userCol="user",
            itemCol="item",
            rank=10,
            seed=1,
        )
        als.setMaxIter(2)

        self.assertEqual(als.getUserCol(), "user")
        self.assertEqual(als.getItemCol(), "item")
        self.assertEqual(als.getRank(), 10)
        self.assertEqual(als.getSeed(), 1)
        self.assertEqual(als.getMaxIter(), 2)

        # Estimator save & load
        with tempfile.TemporaryDirectory(prefix="ALS") as d:
            als.write().overwrite().save(d)
            als2 = ALS.load(d)
            self.assertEqual(str(als), str(als2))

        model = als.fit(df)
        self.assertEqual(model.rank, 10)

        self.assertEqual(model.itemFactors.columns, ["id", "features"])
        self.assertEqual(model.itemFactors.count(), 3)

        self.assertEqual(model.userFactors.columns, ["id", "features"])
        self.assertEqual(model.userFactors.count(), 2)

        # Transform
        output = model.transform(df)
        self.assertEqual(output.columns, ["user", "item", "rating", "prediction"])
        self.assertEqual(output.count(), 4)

        output1 = model.recommendForAllUsers(3)
        self.assertEqual(output1.columns, ["user", "recommendations"])
        self.assertEqual(output1.count(), 2)

        output2 = model.recommendForAllItems(3)
        self.assertEqual(output2.columns, ["item", "recommendations"])
        self.assertEqual(output2.count(), 3)

        output3 = model.recommendForUserSubset(df, 3)
        self.assertEqual(output3.columns, ["user", "recommendations"])
        self.assertEqual(output3.count(), 2)

        output4 = model.recommendForItemSubset(df, 3)
        self.assertEqual(output4.columns, ["item", "recommendations"])
        self.assertEqual(output4.count(), 3)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="als_model") as d:
            model.write().overwrite().save(d)
            model2 = ALSModel.load(d)
            self.assertEqual(str(model), str(model2))

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


class ALSTests(ALSTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.ml.tests.test_als import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
