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

import json
from shutil import rmtree
import tempfile
import unittest

from pyspark.ml import Transformer
from pyspark.ml.classification import DecisionTreeClassifier, FMClassifier, \
    FMClassificationModel, LogisticRegression, MultilayerPerceptronClassifier, \
    MultilayerPerceptronClassificationModel, OneVsRest, OneVsRestModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import Binarizer, HashingTF, PCA
from pyspark.ml.linalg import Vectors
from pyspark.ml.param import Params
from pyspark.ml.pipeline import Pipeline, PipelineModel
from pyspark.ml.regression import DecisionTreeRegressor, GeneralizedLinearRegression, \
    GeneralizedLinearRegressionModel, \
    LinearRegression
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWriter
from pyspark.ml.wrapper import JavaParams
from pyspark.testing.mlutils import MockUnaryTransformer, SparkSessionTestCase


class TestDefaultSolver(SparkSessionTestCase):

    def test_multilayer_load(self):
        df = self.spark.createDataFrame([(0.0, Vectors.dense([0.0, 0.0])),
                                         (1.0, Vectors.dense([0.0, 1.0])),
                                         (1.0, Vectors.dense([1.0, 0.0])),
                                         (0.0, Vectors.dense([1.0, 1.0]))],
                                        ["label",  "features"])

        mlp = MultilayerPerceptronClassifier(layers=[2, 2, 2], seed=123)
        model = mlp.fit(df)
        self.assertEqual(model.getSolver(), "l-bfgs")
        transformed1 = model.transform(df)
        path = tempfile.mkdtemp()
        model_path = path + "/mlp"
        model.save(model_path)
        model2 = MultilayerPerceptronClassificationModel.load(model_path)
        self.assertEqual(model2.getSolver(), "l-bfgs")
        transformed2 = model2.transform(df)
        self.assertEqual(transformed1.take(4), transformed2.take(4))

    def test_fm_load(self):
        df = self.spark.createDataFrame([(1.0, Vectors.dense(1.0)),
                                         (0.0, Vectors.sparse(1, [], []))],
                                        ["label",  "features"])
        fm = FMClassifier(factorSize=2, maxIter=50, stepSize=2.0)
        model = fm.fit(df)
        self.assertEqual(model.getSolver(), "adamW")
        transformed1 = model.transform(df)
        path = tempfile.mkdtemp()
        model_path = path + "/fm"
        model.save(model_path)
        model2 = FMClassificationModel.load(model_path)
        self.assertEqual(model2.getSolver(), "adamW")
        transformed2 = model2.transform(df)
        self.assertEqual(transformed1.take(2), transformed2.take(2))

    def test_glr_load(self):
        df = self.spark.createDataFrame([(1.0, Vectors.dense(0.0, 0.0)),
                                         (1.0, Vectors.dense(1.0, 2.0)),
                                         (2.0, Vectors.dense(0.0, 0.0)),
                                         (2.0, Vectors.dense(1.0, 1.0))],
                                        ["label",  "features"])
        glr = GeneralizedLinearRegression(family="gaussian", link="identity", linkPredictionCol="p")
        model = glr.fit(df)
        self.assertEqual(model.getSolver(), "irls")
        transformed1 = model.transform(df)
        path = tempfile.mkdtemp()
        model_path = path + "/glr"
        model.save(model_path)
        model2 = GeneralizedLinearRegressionModel.load(model_path)
        self.assertEqual(model2.getSolver(), "irls")
        transformed2 = model2.transform(df)
        self.assertEqual(transformed1.take(4), transformed2.take(4))


class PersistenceTest(SparkSessionTestCase):

    def test_linear_regression(self):
        lr = LinearRegression(maxIter=1)
        path = tempfile.mkdtemp()
        lr_path = path + "/lr"
        lr.save(lr_path)
        lr2 = LinearRegression.load(lr_path)
        self.assertEqual(lr.uid, lr2.uid)
        self.assertEqual(type(lr.uid), type(lr2.uid))
        self.assertEqual(lr2.uid, lr2.maxIter.parent,
                         "Loaded LinearRegression instance uid (%s) did not match Param's uid (%s)"
                         % (lr2.uid, lr2.maxIter.parent))
        self.assertEqual(lr._defaultParamMap[lr.maxIter], lr2._defaultParamMap[lr2.maxIter],
                         "Loaded LinearRegression instance default params did not match " +
                         "original defaults")
        try:
            rmtree(path)
        except OSError:
            pass

    def test_linear_regression_pmml_basic(self):
        # Most of the validation is done in the Scala side, here we just check
        # that we output text rather than parquet (e.g. that the format flag
        # was respected).
        df = self.spark.createDataFrame([(1.0, 2.0, Vectors.dense(1.0)),
                                         (0.0, 2.0, Vectors.sparse(1, [], []))],
                                        ["label", "weight", "features"])
        lr = LinearRegression(maxIter=1)
        model = lr.fit(df)
        path = tempfile.mkdtemp()
        lr_path = path + "/lr-pmml"
        model.write().format("pmml").save(lr_path)
        pmml_text_list = self.sc.textFile(lr_path).collect()
        pmml_text = "\n".join(pmml_text_list)
        self.assertIn("Apache Spark", pmml_text)
        self.assertIn("PMML", pmml_text)

    def test_logistic_regression(self):
        lr = LogisticRegression(maxIter=1)
        path = tempfile.mkdtemp()
        lr_path = path + "/logreg"
        lr.save(lr_path)
        lr2 = LogisticRegression.load(lr_path)
        self.assertEqual(lr2.uid, lr2.maxIter.parent,
                         "Loaded LogisticRegression instance uid (%s) "
                         "did not match Param's uid (%s)"
                         % (lr2.uid, lr2.maxIter.parent))
        self.assertEqual(lr._defaultParamMap[lr.maxIter], lr2._defaultParamMap[lr2.maxIter],
                         "Loaded LogisticRegression instance default params did not match " +
                         "original defaults")
        try:
            rmtree(path)
        except OSError:
            pass

    def test_kmeans(self):
        kmeans = KMeans(k=2, seed=1)
        path = tempfile.mkdtemp()
        km_path = path + "/km"
        kmeans.save(km_path)
        kmeans2 = KMeans.load(km_path)
        self.assertEqual(kmeans.uid, kmeans2.uid)
        self.assertEqual(type(kmeans.uid), type(kmeans2.uid))
        self.assertEqual(kmeans2.uid, kmeans2.k.parent,
                         "Loaded KMeans instance uid (%s) did not match Param's uid (%s)"
                         % (kmeans2.uid, kmeans2.k.parent))
        self.assertEqual(kmeans._defaultParamMap[kmeans.k], kmeans2._defaultParamMap[kmeans2.k],
                         "Loaded KMeans instance default params did not match " +
                         "original defaults")
        try:
            rmtree(path)
        except OSError:
            pass

    def test_kmean_pmml_basic(self):
        # Most of the validation is done in the Scala side, here we just check
        # that we output text rather than parquet (e.g. that the format flag
        # was respected).
        data = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),
                (Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
        df = self.spark.createDataFrame(data, ["features"])
        kmeans = KMeans(k=2, seed=1)
        model = kmeans.fit(df)
        path = tempfile.mkdtemp()
        km_path = path + "/km-pmml"
        model.write().format("pmml").save(km_path)
        pmml_text_list = self.sc.textFile(km_path).collect()
        pmml_text = "\n".join(pmml_text_list)
        self.assertIn("Apache Spark", pmml_text)
        self.assertIn("PMML", pmml_text)

    def _compare_params(self, m1, m2, param):
        """
        Compare 2 ML Params instances for the given param, and assert both have the same param value
        and parent. The param must be a parameter of m1.
        """
        # Prevent key not found error in case of some param in neither paramMap nor defaultParamMap.
        if m1.isDefined(param):
            paramValue1 = m1.getOrDefault(param)
            paramValue2 = m2.getOrDefault(m2.getParam(param.name))
            if isinstance(paramValue1, Params):
                self._compare_pipelines(paramValue1, paramValue2)
            else:
                self.assertEqual(paramValue1, paramValue2)  # for general types param
            # Assert parents are equal
            self.assertEqual(param.parent, m2.getParam(param.name).parent)
        else:
            # If m1 is not defined param, then m2 should not, too. See SPARK-14931.
            self.assertFalse(m2.isDefined(m2.getParam(param.name)))

    def _compare_pipelines(self, m1, m2):
        """
        Compare 2 ML types, asserting that they are equivalent.
        This currently supports:
         - basic types
         - Pipeline, PipelineModel
         - OneVsRest, OneVsRestModel
        This checks:
         - uid
         - type
         - Param values and parents
        """
        self.assertEqual(m1.uid, m2.uid)
        self.assertEqual(type(m1), type(m2))
        if isinstance(m1, JavaParams) or isinstance(m1, Transformer):
            self.assertEqual(len(m1.params), len(m2.params))
            for p in m1.params:
                self._compare_params(m1, m2, p)
        elif isinstance(m1, Pipeline):
            self.assertEqual(len(m1.getStages()), len(m2.getStages()))
            for s1, s2 in zip(m1.getStages(), m2.getStages()):
                self._compare_pipelines(s1, s2)
        elif isinstance(m1, PipelineModel):
            self.assertEqual(len(m1.stages), len(m2.stages))
            for s1, s2 in zip(m1.stages, m2.stages):
                self._compare_pipelines(s1, s2)
        elif isinstance(m1, OneVsRest) or isinstance(m1, OneVsRestModel):
            for p in m1.params:
                self._compare_params(m1, m2, p)
            if isinstance(m1, OneVsRestModel):
                self.assertEqual(len(m1.models), len(m2.models))
                for x, y in zip(m1.models, m2.models):
                    self._compare_pipelines(x, y)
        elif isinstance(m1, Params):
            # Test on python backend Estimator/Transformer/Model/Evaluator
            self.assertEqual(len(m1.params), len(m2.params))
            for p in m1.params:
                self._compare_params(m1, m2, p)
        else:
            raise RuntimeError("_compare_pipelines does not yet support type: %s" % type(m1))

    def test_pipeline_persistence(self):
        """
        Pipeline[HashingTF, PCA]
        """
        temp_path = tempfile.mkdtemp()

        try:
            df = self.spark.createDataFrame([(["a", "b", "c"],), (["c", "d", "e"],)], ["words"])
            tf = HashingTF(numFeatures=10, inputCol="words", outputCol="features")
            pca = PCA(k=2, inputCol="features", outputCol="pca_features")
            pl = Pipeline(stages=[tf, pca])
            model = pl.fit(df)

            pipeline_path = temp_path + "/pipeline"
            pl.save(pipeline_path)
            loaded_pipeline = Pipeline.load(pipeline_path)
            self._compare_pipelines(pl, loaded_pipeline)

            model_path = temp_path + "/pipeline-model"
            model.save(model_path)
            loaded_model = PipelineModel.load(model_path)
            self._compare_pipelines(model, loaded_model)
        finally:
            try:
                rmtree(temp_path)
            except OSError:
                pass

    def test_nested_pipeline_persistence(self):
        """
        Pipeline[HashingTF, Pipeline[PCA]]
        """
        temp_path = tempfile.mkdtemp()

        try:
            df = self.spark.createDataFrame([(["a", "b", "c"],), (["c", "d", "e"],)], ["words"])
            tf = HashingTF(numFeatures=10, inputCol="words", outputCol="features")
            pca = PCA(k=2, inputCol="features", outputCol="pca_features")
            p0 = Pipeline(stages=[pca])
            pl = Pipeline(stages=[tf, p0])
            model = pl.fit(df)

            pipeline_path = temp_path + "/pipeline"
            pl.save(pipeline_path)
            loaded_pipeline = Pipeline.load(pipeline_path)
            self._compare_pipelines(pl, loaded_pipeline)

            model_path = temp_path + "/pipeline-model"
            model.save(model_path)
            loaded_model = PipelineModel.load(model_path)
            self._compare_pipelines(model, loaded_model)
        finally:
            try:
                rmtree(temp_path)
            except OSError:
                pass

    def test_python_transformer_pipeline_persistence(self):
        """
        Pipeline[MockUnaryTransformer, Binarizer]
        """
        temp_path = tempfile.mkdtemp()

        try:
            df = self.spark.range(0, 10).toDF('input')
            tf = MockUnaryTransformer(shiftVal=2)\
                .setInputCol("input").setOutputCol("shiftedInput")
            tf2 = Binarizer(threshold=6, inputCol="shiftedInput", outputCol="binarized")
            pl = Pipeline(stages=[tf, tf2])
            model = pl.fit(df)

            pipeline_path = temp_path + "/pipeline"
            pl.save(pipeline_path)
            loaded_pipeline = Pipeline.load(pipeline_path)
            self._compare_pipelines(pl, loaded_pipeline)

            model_path = temp_path + "/pipeline-model"
            model.save(model_path)
            loaded_model = PipelineModel.load(model_path)
            self._compare_pipelines(model, loaded_model)
        finally:
            try:
                rmtree(temp_path)
            except OSError:
                pass

    def _run_test_onevsrest(self, LogisticRegressionCls):
        temp_path = tempfile.mkdtemp()
        df = self.spark.createDataFrame([(0.0, 0.5, Vectors.dense(1.0, 0.8)),
                                         (1.0, 0.5, Vectors.sparse(2, [], [])),
                                         (2.0, 1.0, Vectors.dense(0.5, 0.5))] * 10,
                                        ["label", "wt", "features"])

        lr = LogisticRegressionCls(maxIter=5, regParam=0.01)
        ovr = OneVsRest(classifier=lr)

        def reload_and_compare(ovr, suffix):
            model = ovr.fit(df)
            ovrPath = temp_path + "/{}".format(suffix)
            ovr.save(ovrPath)
            loadedOvr = OneVsRest.load(ovrPath)
            self._compare_pipelines(ovr, loadedOvr)
            modelPath = temp_path + "/{}Model".format(suffix)
            model.save(modelPath)
            loadedModel = OneVsRestModel.load(modelPath)
            self._compare_pipelines(model, loadedModel)

        reload_and_compare(OneVsRest(classifier=lr), "ovr")
        reload_and_compare(OneVsRest(classifier=lr).setWeightCol("wt"), "ovrw")

    def test_onevsrest(self):
        from pyspark.testing.mlutils import DummyLogisticRegression
        self._run_test_onevsrest(LogisticRegression)
        self._run_test_onevsrest(DummyLogisticRegression)

    def test_decisiontree_classifier(self):
        dt = DecisionTreeClassifier(maxDepth=1)
        path = tempfile.mkdtemp()
        dtc_path = path + "/dtc"
        dt.save(dtc_path)
        dt2 = DecisionTreeClassifier.load(dtc_path)
        self.assertEqual(dt2.uid, dt2.maxDepth.parent,
                         "Loaded DecisionTreeClassifier instance uid (%s) "
                         "did not match Param's uid (%s)"
                         % (dt2.uid, dt2.maxDepth.parent))
        self.assertEqual(dt._defaultParamMap[dt.maxDepth], dt2._defaultParamMap[dt2.maxDepth],
                         "Loaded DecisionTreeClassifier instance default params did not match " +
                         "original defaults")
        try:
            rmtree(path)
        except OSError:
            pass

    def test_decisiontree_regressor(self):
        dt = DecisionTreeRegressor(maxDepth=1)
        path = tempfile.mkdtemp()
        dtr_path = path + "/dtr"
        dt.save(dtr_path)
        dt2 = DecisionTreeClassifier.load(dtr_path)
        self.assertEqual(dt2.uid, dt2.maxDepth.parent,
                         "Loaded DecisionTreeRegressor instance uid (%s) "
                         "did not match Param's uid (%s)"
                         % (dt2.uid, dt2.maxDepth.parent))
        self.assertEqual(dt._defaultParamMap[dt.maxDepth], dt2._defaultParamMap[dt2.maxDepth],
                         "Loaded DecisionTreeRegressor instance default params did not match " +
                         "original defaults")
        try:
            rmtree(path)
        except OSError:
            pass

    def test_default_read_write(self):
        temp_path = tempfile.mkdtemp()

        lr = LogisticRegression()
        lr.setMaxIter(50)
        lr.setThreshold(.75)
        writer = DefaultParamsWriter(lr)

        savePath = temp_path + "/lr"
        writer.save(savePath)

        reader = DefaultParamsReadable.read()
        lr2 = reader.load(savePath)

        self.assertEqual(lr.uid, lr2.uid)
        self.assertEqual(lr.extractParamMap(), lr2.extractParamMap())

        # test overwrite
        lr.setThreshold(.8)
        writer.overwrite().save(savePath)

        reader = DefaultParamsReadable.read()
        lr3 = reader.load(savePath)

        self.assertEqual(lr.uid, lr3.uid)
        self.assertEqual(lr.extractParamMap(), lr3.extractParamMap())

    def test_default_read_write_default_params(self):
        lr = LogisticRegression()
        self.assertFalse(lr.isSet(lr.getParam("threshold")))

        lr.setMaxIter(50)
        lr.setThreshold(.75)

        # `threshold` is set by user, default param `predictionCol` is not set by user.
        self.assertTrue(lr.isSet(lr.getParam("threshold")))
        self.assertFalse(lr.isSet(lr.getParam("predictionCol")))
        self.assertTrue(lr.hasDefault(lr.getParam("predictionCol")))

        writer = DefaultParamsWriter(lr)
        metadata = json.loads(writer._get_metadata_to_save(lr, self.sc))
        self.assertTrue("defaultParamMap" in metadata)

        reader = DefaultParamsReadable.read()
        metadataStr = json.dumps(metadata, separators=[',',  ':'])
        loadedMetadata = reader._parseMetaData(metadataStr, )
        reader.getAndSetParams(lr, loadedMetadata)

        self.assertTrue(lr.isSet(lr.getParam("threshold")))
        self.assertFalse(lr.isSet(lr.getParam("predictionCol")))
        self.assertTrue(lr.hasDefault(lr.getParam("predictionCol")))

        # manually create metadata without `defaultParamMap` section.
        del metadata['defaultParamMap']
        metadataStr = json.dumps(metadata, separators=[',',  ':'])
        loadedMetadata = reader._parseMetaData(metadataStr, )
        with self.assertRaisesRegex(AssertionError, "`defaultParamMap` section not found"):
            reader.getAndSetParams(lr, loadedMetadata)

        # Prior to 2.4.0, metadata doesn't have `defaultParamMap`.
        metadata['sparkVersion'] = '2.3.0'
        metadataStr = json.dumps(metadata, separators=[',',  ':'])
        loadedMetadata = reader._parseMetaData(metadataStr, )
        reader.getAndSetParams(lr, loadedMetadata)


if __name__ == "__main__":
    from pyspark.ml.tests.test_persistence import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
