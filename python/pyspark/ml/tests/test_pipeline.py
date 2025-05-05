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

from pyspark.sql import Row
from pyspark.ml.pipeline import Pipeline, PipelineModel
from pyspark.ml.feature import (
    VectorAssembler,
    MaxAbsScaler,
    MaxAbsScalerModel,
    MinMaxScaler,
    MinMaxScalerModel,
)
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.clustering import KMeans, KMeansModel, GaussianMixture
from pyspark.testing.mlutils import MockDataset, MockEstimator, MockTransformer
from pyspark.testing.sqlutils import ReusedSQLTestCase


class PipelineTestsMixin:
    def test_pipeline(self):
        dataset = MockDataset()
        estimator0 = MockEstimator()
        transformer1 = MockTransformer()
        estimator2 = MockEstimator()
        transformer3 = MockTransformer()
        pipeline = Pipeline(stages=[estimator0, transformer1, estimator2, transformer3])
        pipeline_model = pipeline.fit(dataset, {estimator0.fake: 0, transformer1.fake: 1})
        model0, transformer1, model2, transformer3 = pipeline_model.stages
        self.assertEqual(0, model0.dataset_index)
        self.assertEqual(0, model0.getFake())
        self.assertEqual(1, transformer1.dataset_index)
        self.assertEqual(1, transformer1.getFake())
        self.assertEqual(2, dataset.index)
        self.assertIsNone(model2.dataset_index, "The last model shouldn't be called in fit.")
        self.assertIsNone(
            transformer3.dataset_index, "The last transformer shouldn't be called in fit."
        )
        dataset = pipeline_model.transform(dataset)
        self.assertEqual(2, model0.dataset_index)
        self.assertEqual(3, transformer1.dataset_index)
        self.assertEqual(4, model2.dataset_index)
        self.assertEqual(5, transformer3.dataset_index)
        self.assertEqual(6, dataset.index)

    def test_identity_pipeline(self):
        dataset = MockDataset()

        def doTransform(pipeline):
            pipeline_model = pipeline.fit(dataset)
            return pipeline_model.transform(dataset)

        # check that empty pipeline did not perform any transformation
        self.assertEqual(dataset.index, doTransform(Pipeline(stages=[])).index)
        # check that failure to set stages param will raise KeyError for missing param
        self.assertRaises(KeyError, lambda: doTransform(Pipeline()))

    def test_classification_pipeline(self):
        df = self.spark.createDataFrame([(1, 1, 0, 3), (0, 2, 0, 1)], ["y", "a", "b", "c"])

        assembler = VectorAssembler(outputCol="features")
        assembler.setInputCols(["a", "b", "c"])

        scaler = MaxAbsScaler(inputCol="features", outputCol="scaled_features")

        lr = LogisticRegression(featuresCol="scaled_features", labelCol="y")
        lr.setMaxIter(1)

        pipeline = Pipeline(stages=[assembler, scaler, lr])
        self.assertEqual(len(pipeline.getStages()), 3)

        model = pipeline.fit(df)
        self.assertEqual(len(model.stages), 3)
        self.assertIsInstance(model.stages[0], VectorAssembler)
        self.assertIsInstance(model.stages[1], MaxAbsScalerModel)
        self.assertIsInstance(model.stages[2], LogisticRegressionModel)

        output = model.transform(df)
        self.assertEqual(
            output.columns,
            [
                "y",
                "a",
                "b",
                "c",
                "features",
                "scaled_features",
                "rawPrediction",
                "probability",
                "prediction",
            ],
        )
        self.assertEqual(output.count(), 2)

        # save & load
        with tempfile.TemporaryDirectory(prefix="classification_pipeline") as d:
            pipeline.write().overwrite().save(d)
            pipeline2 = Pipeline.load(d)
            self.assertEqual(str(pipeline), str(pipeline2))
            self.assertEqual(str(pipeline.getStages()), str(pipeline2.getStages()))

            model.write().overwrite().save(d)
            model2 = PipelineModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(str(model.stages), str(model2.stages))

    def test_clustering_pipeline(self):
        df = self.spark.createDataFrame([(1, 1, 0, 3), (0, 2, 0, 1)], ["y", "a", "b", "c"])

        assembler = VectorAssembler(outputCol="features")
        assembler.setInputCols(["a", "b", "c"])

        scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")

        km = KMeans(k=2, maxIter=2)

        pipeline = Pipeline(stages=[assembler, scaler, km])
        self.assertEqual(len(pipeline.getStages()), 3)

        # Pipeline save & load
        with tempfile.TemporaryDirectory(prefix="clustering_pipeline") as d:
            pipeline.write().overwrite().save(d)
            pipeline2 = Pipeline.load(d)
            self.assertEqual(str(pipeline), str(pipeline2))
            self.assertEqual(str(pipeline.getStages()), str(pipeline2.getStages()))

        model = pipeline.fit(df)
        self.assertEqual(len(model.stages), 3)
        self.assertIsInstance(model.stages[0], VectorAssembler)
        self.assertIsInstance(model.stages[1], MinMaxScalerModel)
        self.assertIsInstance(model.stages[2], KMeansModel)

        output = model.transform(df)
        self.assertEqual(
            output.columns,
            [
                "y",
                "a",
                "b",
                "c",
                "features",
                "scaled_features",
                "prediction",
            ],
        )
        self.assertEqual(output.count(), 2)

        # PipelineModel save & load
        with tempfile.TemporaryDirectory(prefix="clustering_pipeline") as d:
            pipeline.write().overwrite().save(d)
            pipeline2 = Pipeline.load(d)
            self.assertEqual(str(pipeline), str(pipeline2))
            self.assertEqual(str(pipeline.getStages()), str(pipeline2.getStages()))

            model.write().overwrite().save(d)
            model2 = PipelineModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(str(model.stages), str(model2.stages))

    def test_model_gc(self):
        spark = self.spark
        df1 = spark.createDataFrame(
            [
                Row(label=0.0, weight=0.1, features=Vectors.dense([0.0, 0.0])),
                Row(label=0.0, weight=0.5, features=Vectors.dense([0.0, 1.0])),
                Row(label=1.0, weight=1.0, features=Vectors.dense([1.0, 0.0])),
            ]
        )

        def fit_transform(df):
            lr = LogisticRegression(maxIter=1, regParam=0.01, weightCol="weight")
            model = lr.fit(df)
            return model.transform(df)

        output1 = fit_transform(df1)
        self.assertEqual(output1.count(), 3)

        df2 = spark.range(10)

        def fit_transform_and_union(df1, df2):
            output1 = fit_transform(df1)
            return output1.unionByName(df2, True)

        output2 = fit_transform_and_union(df1, df2)
        self.assertEqual(output2.count(), 13)

    def test_model_training_summary_gc(self):
        spark = self.spark
        df1 = spark.createDataFrame(
            [
                Row(label=0.0, weight=0.1, features=Vectors.dense([0.0, 0.0])),
                Row(label=0.0, weight=0.5, features=Vectors.dense([0.0, 1.0])),
                Row(label=1.0, weight=1.0, features=Vectors.dense([1.0, 0.0])),
            ]
        )

        def fit_predictions(df):
            lr = LogisticRegression(maxIter=1, regParam=0.01, weightCol="weight")
            model = lr.fit(df)
            return model.summary.predictions

        output1 = fit_predictions(df1)
        self.assertEqual(output1.count(), 3)

        df2 = spark.range(10)

        def fit_predictions_and_union(df1, df2):
            output1 = fit_predictions(df1)
            return output1.unionByName(df2, True)

        output2 = fit_predictions_and_union(df1, df2)
        self.assertEqual(output2.count(), 13)

    def test_model_testing_summary_gc(self):
        spark = self.spark
        df1 = spark.createDataFrame(
            [
                Row(label=0.0, weight=0.1, features=Vectors.dense([0.0, 0.0])),
                Row(label=0.0, weight=0.5, features=Vectors.dense([0.0, 1.0])),
                Row(label=1.0, weight=1.0, features=Vectors.dense([1.0, 0.0])),
            ]
        )

        def fit_predictions(df):
            lr = LogisticRegression(maxIter=1, regParam=0.01, weightCol="weight")
            model = lr.fit(df)
            return model.evaluate(df).predictions

        output1 = fit_predictions(df1)
        self.assertEqual(output1.count(), 3)

        df2 = spark.range(10)

        def fit_predictions_and_union(df1, df2):
            output1 = fit_predictions(df1)
            return output1.unionByName(df2, True)

        output2 = fit_predictions_and_union(df1, df2)
        self.assertEqual(output2.count(), 13)

    def test_model_attr_df_gc(self):
        spark = self.spark
        df1 = (
            spark.createDataFrame(
                [
                    (1, 1.0, Vectors.dense([-0.1, -0.05])),
                    (2, 2.0, Vectors.dense([-0.01, -0.1])),
                    (3, 3.0, Vectors.dense([0.9, 0.8])),
                    (4, 1.0, Vectors.dense([0.75, 0.935])),
                    (5, 1.0, Vectors.dense([-0.83, -0.68])),
                    (6, 1.0, Vectors.dense([-0.91, -0.76])),
                ],
                ["index", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("index")
            .select("weight", "features")
        )

        def fit_attr_df(df):
            gmm = GaussianMixture(k=2, maxIter=2, weightCol="weight", seed=1)
            model = gmm.fit(df)
            return model.gaussiansDF

        output1 = fit_attr_df(df1)
        self.assertEqual(output1.count(), 2)

        df2 = spark.range(10)

        def fit_attr_df_and_union(df1, df2):
            output1 = fit_attr_df(df1)
            return output1.unionByName(df2, True)

        output2 = fit_attr_df_and_union(df1, df2)
        self.assertEqual(output2.count(), 12)


class PipelineTests(PipelineTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.ml.tests.test_pipeline import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
