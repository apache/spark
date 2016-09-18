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

"""
Unit tests for MLlib Python DataFrame-based APIs.
"""
import sys
if sys.version > '3':
    xrange = range
    basestring = str

try:
    import xmlrunner
except ImportError:
    xmlrunner = None

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from shutil import rmtree
import tempfile
import array as pyarray
import numpy as np
from numpy import (
    array, array_equal, zeros, inf, random, exp, dot, all, mean, abs, arange, tile, ones)
from numpy import sum as array_sum
import inspect

from pyspark import keyword_only, SparkContext
from pyspark.ml import Estimator, Model, Pipeline, PipelineModel, Transformer
from pyspark.ml.classification import *
from pyspark.ml.clustering import *
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import *
from pyspark.ml.linalg import Vector, SparseVector, DenseVector, VectorUDT,\
    DenseMatrix, SparseMatrix, Vectors, Matrices, MatrixUDT, _convert_to_vector
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasMaxIter, HasInputCol, HasSeed
from pyspark.ml.recommendation import ALS
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, \
    GeneralizedLinearRegression
from pyspark.ml.tuning import *
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.common import _java2py
from pyspark.serializers import PickleSerializer
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import rand
from pyspark.sql.utils import IllegalArgumentException
from pyspark.storagelevel import *
from pyspark.tests import ReusedPySparkTestCase as PySparkTestCase

ser = PickleSerializer()


class MLlibTestCase(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext('local[4]', "MLlib tests")
        self.spark = SparkSession(self.sc)

    def tearDown(self):
        self.spark.stop()


class SparkSessionTestCase(PySparkTestCase):
    @classmethod
    def setUpClass(cls):
        PySparkTestCase.setUpClass()
        cls.spark = SparkSession(cls.sc)

    @classmethod
    def tearDownClass(cls):
        PySparkTestCase.tearDownClass()
        cls.spark.stop()


class MockDataset(DataFrame):

    def __init__(self):
        self.index = 0


class HasFake(Params):

    def __init__(self):
        super(HasFake, self).__init__()
        self.fake = Param(self, "fake", "fake param")

    def getFake(self):
        return self.getOrDefault(self.fake)


class MockTransformer(Transformer, HasFake):

    def __init__(self):
        super(MockTransformer, self).__init__()
        self.dataset_index = None

    def _transform(self, dataset):
        self.dataset_index = dataset.index
        dataset.index += 1
        return dataset


class MockEstimator(Estimator, HasFake):

    def __init__(self):
        super(MockEstimator, self).__init__()
        self.dataset_index = None

    def _fit(self, dataset):
        self.dataset_index = dataset.index
        model = MockModel()
        self._copyValues(model)
        return model


class MockModel(MockTransformer, Model, HasFake):
    pass


class ParamTypeConversionTests(PySparkTestCase):
    """
    Test that param type conversion happens.
    """

    def test_int(self):
        lr = LogisticRegression(maxIter=5.0)
        self.assertEqual(lr.getMaxIter(), 5)
        self.assertTrue(type(lr.getMaxIter()) == int)
        self.assertRaises(TypeError, lambda: LogisticRegression(maxIter="notAnInt"))
        self.assertRaises(TypeError, lambda: LogisticRegression(maxIter=5.1))

    def test_float(self):
        lr = LogisticRegression(tol=1)
        self.assertEqual(lr.getTol(), 1.0)
        self.assertTrue(type(lr.getTol()) == float)
        self.assertRaises(TypeError, lambda: LogisticRegression(tol="notAFloat"))

    def test_vector(self):
        ewp = ElementwiseProduct(scalingVec=[1, 3])
        self.assertEqual(ewp.getScalingVec(), DenseVector([1.0, 3.0]))
        ewp = ElementwiseProduct(scalingVec=np.array([1.2, 3.4]))
        self.assertEqual(ewp.getScalingVec(), DenseVector([1.2, 3.4]))
        self.assertRaises(TypeError, lambda: ElementwiseProduct(scalingVec=["a", "b"]))

    def test_list(self):
        l = [0, 1]
        for lst_like in [l, np.array(l), DenseVector(l), SparseVector(len(l),
                         range(len(l)), l), pyarray.array('l', l), xrange(2), tuple(l)]:
            converted = TypeConverters.toList(lst_like)
            self.assertEqual(type(converted), list)
            self.assertListEqual(converted, l)

    def test_list_int(self):
        for indices in [[1.0, 2.0], np.array([1.0, 2.0]), DenseVector([1.0, 2.0]),
                        SparseVector(2, {0: 1.0, 1: 2.0}), xrange(1, 3), (1.0, 2.0),
                        pyarray.array('d', [1.0, 2.0])]:
            vs = VectorSlicer(indices=indices)
            self.assertListEqual(vs.getIndices(), [1, 2])
            self.assertTrue(all([type(v) == int for v in vs.getIndices()]))
        self.assertRaises(TypeError, lambda: VectorSlicer(indices=["a", "b"]))

    def test_list_float(self):
        b = Bucketizer(splits=[1, 4])
        self.assertEqual(b.getSplits(), [1.0, 4.0])
        self.assertTrue(all([type(v) == float for v in b.getSplits()]))
        self.assertRaises(TypeError, lambda: Bucketizer(splits=["a", 1.0]))

    def test_list_string(self):
        for labels in [np.array(['a', u'b']), ['a', u'b'], np.array(['a', 'b'])]:
            idx_to_string = IndexToString(labels=labels)
            self.assertListEqual(idx_to_string.getLabels(), ['a', 'b'])
        self.assertRaises(TypeError, lambda: IndexToString(labels=['a', 2]))

    def test_string(self):
        lr = LogisticRegression()
        for col in ['features', u'features', np.str_('features')]:
            lr.setFeaturesCol(col)
            self.assertEqual(lr.getFeaturesCol(), 'features')
        self.assertRaises(TypeError, lambda: LogisticRegression(featuresCol=2.3))

    def test_bool(self):
        self.assertRaises(TypeError, lambda: LogisticRegression(fitIntercept=1))
        self.assertRaises(TypeError, lambda: LogisticRegression(fitIntercept="false"))


class PipelineTests(PySparkTestCase):

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
        self.assertIsNone(transformer3.dataset_index,
                          "The last transformer shouldn't be called in fit.")
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


class TestParams(HasMaxIter, HasInputCol, HasSeed):
    """
    A subclass of Params mixed with HasMaxIter, HasInputCol and HasSeed.
    """
    @keyword_only
    def __init__(self, seed=None):
        super(TestParams, self).__init__()
        self._setDefault(maxIter=10)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, seed=None):
        """
        setParams(self, seed=None)
        Sets params for this test.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


class OtherTestParams(HasMaxIter, HasInputCol, HasSeed):
    """
    A subclass of Params mixed with HasMaxIter, HasInputCol and HasSeed.
    """
    @keyword_only
    def __init__(self, seed=None):
        super(OtherTestParams, self).__init__()
        self._setDefault(maxIter=10)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, seed=None):
        """
        setParams(self, seed=None)
        Sets params for this test.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


class HasThrowableProperty(Params):

    def __init__(self):
        super(HasThrowableProperty, self).__init__()
        self.p = Param(self, "none", "empty param")

    @property
    def test_property(self):
        raise RuntimeError("Test property to raise error when invoked")


class ParamTests(PySparkTestCase):

    def test_copy_new_parent(self):
        testParams = TestParams()
        # Copying an instantiated param should fail
        with self.assertRaises(ValueError):
            testParams.maxIter._copy_new_parent(testParams)
        # Copying a dummy param should succeed
        TestParams.maxIter._copy_new_parent(testParams)
        maxIter = testParams.maxIter
        self.assertEqual(maxIter.name, "maxIter")
        self.assertEqual(maxIter.doc, "max number of iterations (>= 0).")
        self.assertTrue(maxIter.parent == testParams.uid)

    def test_param(self):
        testParams = TestParams()
        maxIter = testParams.maxIter
        self.assertEqual(maxIter.name, "maxIter")
        self.assertEqual(maxIter.doc, "max number of iterations (>= 0).")
        self.assertTrue(maxIter.parent == testParams.uid)

    def test_hasparam(self):
        testParams = TestParams()
        self.assertTrue(all([testParams.hasParam(p.name) for p in testParams.params]))
        self.assertFalse(testParams.hasParam("notAParameter"))

    def test_params(self):
        testParams = TestParams()
        maxIter = testParams.maxIter
        inputCol = testParams.inputCol
        seed = testParams.seed

        params = testParams.params
        self.assertEqual(params, [inputCol, maxIter, seed])

        self.assertTrue(testParams.hasParam(maxIter.name))
        self.assertTrue(testParams.hasDefault(maxIter))
        self.assertFalse(testParams.isSet(maxIter))
        self.assertTrue(testParams.isDefined(maxIter))
        self.assertEqual(testParams.getMaxIter(), 10)
        testParams.setMaxIter(100)
        self.assertTrue(testParams.isSet(maxIter))
        self.assertEqual(testParams.getMaxIter(), 100)

        self.assertTrue(testParams.hasParam(inputCol.name))
        self.assertFalse(testParams.hasDefault(inputCol))
        self.assertFalse(testParams.isSet(inputCol))
        self.assertFalse(testParams.isDefined(inputCol))
        with self.assertRaises(KeyError):
            testParams.getInputCol()

        # Since the default is normally random, set it to a known number for debug str
        testParams._setDefault(seed=41)
        testParams.setSeed(43)

        self.assertEqual(
            testParams.explainParams(),
            "\n".join(["inputCol: input column name. (undefined)",
                       "maxIter: max number of iterations (>= 0). (default: 10, current: 100)",
                       "seed: random seed. (default: 41, current: 43)"]))

    def test_kmeans_param(self):
        algo = KMeans()
        self.assertEqual(algo.getInitMode(), "k-means||")
        algo.setK(10)
        self.assertEqual(algo.getK(), 10)
        algo.setInitSteps(10)
        self.assertEqual(algo.getInitSteps(), 10)

    def test_hasseed(self):
        noSeedSpecd = TestParams()
        withSeedSpecd = TestParams(seed=42)
        other = OtherTestParams()
        # Check that we no longer use 42 as the magic number
        self.assertNotEqual(noSeedSpecd.getSeed(), 42)
        origSeed = noSeedSpecd.getSeed()
        # Check that we only compute the seed once
        self.assertEqual(noSeedSpecd.getSeed(), origSeed)
        # Check that a specified seed is honored
        self.assertEqual(withSeedSpecd.getSeed(), 42)
        # Check that a different class has a different seed
        self.assertNotEqual(other.getSeed(), noSeedSpecd.getSeed())

    def test_param_property_error(self):
        param_store = HasThrowableProperty()
        self.assertRaises(RuntimeError, lambda: param_store.test_property)
        params = param_store.params  # should not invoke the property 'test_property'
        self.assertEqual(len(params), 1)

    def test_word2vec_param(self):
        model = Word2Vec().setWindowSize(6)
        # Check windowSize is set properly
        self.assertEqual(model.getWindowSize(), 6)


class FeatureTests(SparkSessionTestCase):

    def test_binarizer(self):
        b0 = Binarizer()
        self.assertListEqual(b0.params, [b0.inputCol, b0.outputCol, b0.threshold])
        self.assertTrue(all([~b0.isSet(p) for p in b0.params]))
        self.assertTrue(b0.hasDefault(b0.threshold))
        self.assertEqual(b0.getThreshold(), 0.0)
        b0.setParams(inputCol="input", outputCol="output").setThreshold(1.0)
        self.assertTrue(all([b0.isSet(p) for p in b0.params]))
        self.assertEqual(b0.getThreshold(), 1.0)
        self.assertEqual(b0.getInputCol(), "input")
        self.assertEqual(b0.getOutputCol(), "output")

        b0c = b0.copy({b0.threshold: 2.0})
        self.assertEqual(b0c.uid, b0.uid)
        self.assertListEqual(b0c.params, b0.params)
        self.assertEqual(b0c.getThreshold(), 2.0)

        b1 = Binarizer(threshold=2.0, inputCol="input", outputCol="output")
        self.assertNotEqual(b1.uid, b0.uid)
        self.assertEqual(b1.getThreshold(), 2.0)
        self.assertEqual(b1.getInputCol(), "input")
        self.assertEqual(b1.getOutputCol(), "output")

    def test_idf(self):
        dataset = self.spark.createDataFrame([
            (DenseVector([1.0, 2.0]),),
            (DenseVector([0.0, 1.0]),),
            (DenseVector([3.0, 0.2]),)], ["tf"])
        idf0 = IDF(inputCol="tf")
        self.assertListEqual(idf0.params, [idf0.inputCol, idf0.minDocFreq, idf0.outputCol])
        idf0m = idf0.fit(dataset, {idf0.outputCol: "idf"})
        self.assertEqual(idf0m.uid, idf0.uid,
                         "Model should inherit the UID from its parent estimator.")
        output = idf0m.transform(dataset)
        self.assertIsNotNone(output.head().idf)

    def test_ngram(self):
        dataset = self.spark.createDataFrame([
            Row(input=["a", "b", "c", "d", "e"])])
        ngram0 = NGram(n=4, inputCol="input", outputCol="output")
        self.assertEqual(ngram0.getN(), 4)
        self.assertEqual(ngram0.getInputCol(), "input")
        self.assertEqual(ngram0.getOutputCol(), "output")
        transformedDF = ngram0.transform(dataset)
        self.assertEqual(transformedDF.head().output, ["a b c d", "b c d e"])

    def test_stopwordsremover(self):
        dataset = self.spark.createDataFrame([Row(input=["a", "panda"])])
        stopWordRemover = StopWordsRemover(inputCol="input", outputCol="output")
        # Default
        self.assertEqual(stopWordRemover.getInputCol(), "input")
        transformedDF = stopWordRemover.transform(dataset)
        self.assertEqual(transformedDF.head().output, ["panda"])
        self.assertEqual(type(stopWordRemover.getStopWords()), list)
        self.assertTrue(isinstance(stopWordRemover.getStopWords()[0], basestring))
        # Custom
        stopwords = ["panda"]
        stopWordRemover.setStopWords(stopwords)
        self.assertEqual(stopWordRemover.getInputCol(), "input")
        self.assertEqual(stopWordRemover.getStopWords(), stopwords)
        transformedDF = stopWordRemover.transform(dataset)
        self.assertEqual(transformedDF.head().output, ["a"])
        # with language selection
        stopwords = StopWordsRemover.loadDefaultStopWords("turkish")
        dataset = self.spark.createDataFrame([Row(input=["acaba", "ama", "biri"])])
        stopWordRemover.setStopWords(stopwords)
        self.assertEqual(stopWordRemover.getStopWords(), stopwords)
        transformedDF = stopWordRemover.transform(dataset)
        self.assertEqual(transformedDF.head().output, [])

    def test_count_vectorizer_with_binary(self):
        dataset = self.spark.createDataFrame([
            (0, "a a a b b c".split(' '), SparseVector(3, {0: 1.0, 1: 1.0, 2: 1.0}),),
            (1, "a a".split(' '), SparseVector(3, {0: 1.0}),),
            (2, "a b".split(' '), SparseVector(3, {0: 1.0, 1: 1.0}),),
            (3, "c".split(' '), SparseVector(3, {2: 1.0}),)], ["id", "words", "expected"])
        cv = CountVectorizer(binary=True, inputCol="words", outputCol="features")
        model = cv.fit(dataset)

        transformedList = model.transform(dataset).select("features", "expected").collect()

        for r in transformedList:
            feature, expected = r
            self.assertEqual(feature, expected)


class HasInducedError(Params):

    def __init__(self):
        super(HasInducedError, self).__init__()
        self.inducedError = Param(self, "inducedError",
                                  "Uniformly-distributed error added to feature")

    def getInducedError(self):
        return self.getOrDefault(self.inducedError)


class InducedErrorModel(Model, HasInducedError):

    def __init__(self):
        super(InducedErrorModel, self).__init__()

    def _transform(self, dataset):
        return dataset.withColumn("prediction",
                                  dataset.feature + (rand(0) * self.getInducedError()))


class InducedErrorEstimator(Estimator, HasInducedError):

    def __init__(self, inducedError=1.0):
        super(InducedErrorEstimator, self).__init__()
        self._set(inducedError=inducedError)

    def _fit(self, dataset):
        model = InducedErrorModel()
        self._copyValues(model)
        return model


class CrossValidatorTests(SparkSessionTestCase):

    def test_copy(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="rmse")

        grid = (ParamGridBuilder()
                .addGrid(iee.inducedError, [100.0, 0.0, 10000.0])
                .build())
        cv = CrossValidator(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        cvCopied = cv.copy()
        self.assertEqual(cv.getEstimator().uid, cvCopied.getEstimator().uid)

        cvModel = cv.fit(dataset)
        cvModelCopied = cvModel.copy()
        for index in range(len(cvModel.avgMetrics)):
            self.assertTrue(abs(cvModel.avgMetrics[index] - cvModelCopied.avgMetrics[index])
                            < 0.0001)

    def test_fit_minimize_metric(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="rmse")

        grid = (ParamGridBuilder()
                .addGrid(iee.inducedError, [100.0, 0.0, 10000.0])
                .build())
        cv = CrossValidator(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        bestModel = cvModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))

        self.assertEqual(0.0, bestModel.getOrDefault('inducedError'),
                         "Best model should have zero induced error")
        self.assertEqual(0.0, bestModelMetric, "Best model has RMSE of 0")

    def test_fit_maximize_metric(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="r2")

        grid = (ParamGridBuilder()
                .addGrid(iee.inducedError, [100.0, 0.0, 10000.0])
                .build())
        cv = CrossValidator(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        bestModel = cvModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))

        self.assertEqual(0.0, bestModel.getOrDefault('inducedError'),
                         "Best model should have zero induced error")
        self.assertEqual(1.0, bestModelMetric, "Best model has R-squared of 1")

    def test_save_load(self):
        # This tests saving and loading the trained model only.
        # Save/load for CrossValidator will be added later: SPARK-13786
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])
        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()
        cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        lrModel = cvModel.bestModel

        cvModelPath = temp_path + "/cvModel"
        lrModel.save(cvModelPath)
        loadedLrModel = LogisticRegressionModel.load(cvModelPath)
        self.assertEqual(loadedLrModel.uid, lrModel.uid)
        self.assertEqual(loadedLrModel.intercept, lrModel.intercept)


class TrainValidationSplitTests(SparkSessionTestCase):

    def test_fit_minimize_metric(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="rmse")

        grid = ParamGridBuilder() \
            .addGrid(iee.inducedError, [100.0, 0.0, 10000.0]) \
            .build()
        tvs = TrainValidationSplit(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        bestModel = tvsModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))
        validationMetrics = tvsModel.validationMetrics

        self.assertEqual(0.0, bestModel.getOrDefault('inducedError'),
                         "Best model should have zero induced error")
        self.assertEqual(0.0, bestModelMetric, "Best model has RMSE of 0")
        self.assertEqual(len(grid), len(validationMetrics),
                         "validationMetrics has the same size of grid parameter")
        self.assertEqual(0.0, min(validationMetrics))

    def test_fit_maximize_metric(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="r2")

        grid = ParamGridBuilder() \
            .addGrid(iee.inducedError, [100.0, 0.0, 10000.0]) \
            .build()
        tvs = TrainValidationSplit(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        bestModel = tvsModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))
        validationMetrics = tvsModel.validationMetrics

        self.assertEqual(0.0, bestModel.getOrDefault('inducedError'),
                         "Best model should have zero induced error")
        self.assertEqual(1.0, bestModelMetric, "Best model has R-squared of 1")
        self.assertEqual(len(grid), len(validationMetrics),
                         "validationMetrics has the same size of grid parameter")
        self.assertEqual(1.0, max(validationMetrics))

    def test_save_load(self):
        # This tests saving and loading the trained model only.
        # Save/load for TrainValidationSplit will be added later: SPARK-13786
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])
        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()
        tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        lrModel = tvsModel.bestModel

        tvsModelPath = temp_path + "/tvsModel"
        lrModel.save(tvsModelPath)
        loadedLrModel = LogisticRegressionModel.load(tvsModelPath)
        self.assertEqual(loadedLrModel.uid, lrModel.uid)
        self.assertEqual(loadedLrModel.intercept, lrModel.intercept)

    def test_copy(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="r2")

        grid = ParamGridBuilder() \
            .addGrid(iee.inducedError, [100.0, 0.0, 10000.0]) \
            .build()
        tvs = TrainValidationSplit(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        tvsCopied = tvs.copy()
        tvsModelCopied = tvsModel.copy()

        self.assertEqual(tvs.getEstimator().uid, tvsCopied.getEstimator().uid,
                         "Copied TrainValidationSplit has the same uid of Estimator")

        self.assertEqual(tvsModel.bestModel.uid, tvsModelCopied.bestModel.uid)
        self.assertEqual(len(tvsModel.validationMetrics),
                         len(tvsModelCopied.validationMetrics),
                         "Copied validationMetrics has the same size of the original")
        for index in range(len(tvsModel.validationMetrics)):
            self.assertEqual(tvsModel.validationMetrics[index],
                             tvsModelCopied.validationMetrics[index])


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
        if isinstance(m1, JavaParams):
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

    def test_onevsrest(self):
        temp_path = tempfile.mkdtemp()
        df = self.spark.createDataFrame([(0.0, Vectors.dense(1.0, 0.8)),
                                         (1.0, Vectors.sparse(2, [], [])),
                                         (2.0, Vectors.dense(0.5, 0.5))] * 10,
                                        ["label", "features"])
        lr = LogisticRegression(maxIter=5, regParam=0.01)
        ovr = OneVsRest(classifier=lr)
        model = ovr.fit(df)
        ovrPath = temp_path + "/ovr"
        ovr.save(ovrPath)
        loadedOvr = OneVsRest.load(ovrPath)
        self._compare_pipelines(ovr, loadedOvr)
        modelPath = temp_path + "/ovrModel"
        model.save(modelPath)
        loadedModel = OneVsRestModel.load(modelPath)
        self._compare_pipelines(model, loadedModel)

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


class LDATest(SparkSessionTestCase):

    def _compare(self, m1, m2):
        """
        Temp method for comparing instances.
        TODO: Replace with generic implementation once SPARK-14706 is merged.
        """
        self.assertEqual(m1.uid, m2.uid)
        self.assertEqual(type(m1), type(m2))
        self.assertEqual(len(m1.params), len(m2.params))
        for p in m1.params:
            if m1.isDefined(p):
                self.assertEqual(m1.getOrDefault(p), m2.getOrDefault(p))
                self.assertEqual(p.parent, m2.getParam(p.name).parent)
        if isinstance(m1, LDAModel):
            self.assertEqual(m1.vocabSize(), m2.vocabSize())
            self.assertEqual(m1.topicsMatrix(), m2.topicsMatrix())

    def test_persistence(self):
        # Test save/load for LDA, LocalLDAModel, DistributedLDAModel.
        df = self.spark.createDataFrame([
            [1, Vectors.dense([0.0, 1.0])],
            [2, Vectors.sparse(2, {0: 1.0})],
        ], ["id", "features"])
        # Fit model
        lda = LDA(k=2, seed=1, optimizer="em")
        distributedModel = lda.fit(df)
        self.assertTrue(distributedModel.isDistributed())
        localModel = distributedModel.toLocal()
        self.assertFalse(localModel.isDistributed())
        # Define paths
        path = tempfile.mkdtemp()
        lda_path = path + "/lda"
        dist_model_path = path + "/distLDAModel"
        local_model_path = path + "/localLDAModel"
        # Test LDA
        lda.save(lda_path)
        lda2 = LDA.load(lda_path)
        self._compare(lda, lda2)
        # Test DistributedLDAModel
        distributedModel.save(dist_model_path)
        distributedModel2 = DistributedLDAModel.load(dist_model_path)
        self._compare(distributedModel, distributedModel2)
        # Test LocalLDAModel
        localModel.save(local_model_path)
        localModel2 = LocalLDAModel.load(local_model_path)
        self._compare(localModel, localModel2)
        # Clean up
        try:
            rmtree(path)
        except OSError:
            pass


class TrainingSummaryTest(SparkSessionTestCase):

    def test_linear_regression_summary(self):
        df = self.spark.createDataFrame([(1.0, 2.0, Vectors.dense(1.0)),
                                         (0.0, 2.0, Vectors.sparse(1, [], []))],
                                        ["label", "weight", "features"])
        lr = LinearRegression(maxIter=5, regParam=0.0, solver="normal", weightCol="weight",
                              fitIntercept=False)
        model = lr.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        # test that api is callable and returns expected types
        self.assertGreater(s.totalIterations, 0)
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.predictionCol, "prediction")
        self.assertEqual(s.labelCol, "label")
        self.assertEqual(s.featuresCol, "features")
        objHist = s.objectiveHistory
        self.assertTrue(isinstance(objHist, list) and isinstance(objHist[0], float))
        self.assertAlmostEqual(s.explainedVariance, 0.25, 2)
        self.assertAlmostEqual(s.meanAbsoluteError, 0.0)
        self.assertAlmostEqual(s.meanSquaredError, 0.0)
        self.assertAlmostEqual(s.rootMeanSquaredError, 0.0)
        self.assertAlmostEqual(s.r2, 1.0, 2)
        self.assertTrue(isinstance(s.residuals, DataFrame))
        self.assertEqual(s.numInstances, 2)
        devResiduals = s.devianceResiduals
        self.assertTrue(isinstance(devResiduals, list) and isinstance(devResiduals[0], float))
        coefStdErr = s.coefficientStandardErrors
        self.assertTrue(isinstance(coefStdErr, list) and isinstance(coefStdErr[0], float))
        tValues = s.tValues
        self.assertTrue(isinstance(tValues, list) and isinstance(tValues[0], float))
        pValues = s.pValues
        self.assertTrue(isinstance(pValues, list) and isinstance(pValues[0], float))
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertAlmostEqual(sameSummary.explainedVariance, s.explainedVariance)

    def test_glr_summary(self):
        from pyspark.ml.linalg import Vectors
        df = self.spark.createDataFrame([(1.0, 2.0, Vectors.dense(1.0)),
                                         (0.0, 2.0, Vectors.sparse(1, [], []))],
                                        ["label", "weight", "features"])
        glr = GeneralizedLinearRegression(family="gaussian", link="identity", weightCol="weight",
                                          fitIntercept=False)
        model = glr.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        # test that api is callable and returns expected types
        self.assertEqual(s.numIterations, 1)  # this should default to a single iteration of WLS
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.predictionCol, "prediction")
        self.assertTrue(isinstance(s.residuals(), DataFrame))
        self.assertTrue(isinstance(s.residuals("pearson"), DataFrame))
        coefStdErr = s.coefficientStandardErrors
        self.assertTrue(isinstance(coefStdErr, list) and isinstance(coefStdErr[0], float))
        tValues = s.tValues
        self.assertTrue(isinstance(tValues, list) and isinstance(tValues[0], float))
        pValues = s.pValues
        self.assertTrue(isinstance(pValues, list) and isinstance(pValues[0], float))
        self.assertEqual(s.degreesOfFreedom, 1)
        self.assertEqual(s.residualDegreeOfFreedom, 1)
        self.assertEqual(s.residualDegreeOfFreedomNull, 2)
        self.assertEqual(s.rank, 1)
        self.assertTrue(isinstance(s.solver, basestring))
        self.assertTrue(isinstance(s.aic, float))
        self.assertTrue(isinstance(s.deviance, float))
        self.assertTrue(isinstance(s.nullDeviance, float))
        self.assertTrue(isinstance(s.dispersion, float))
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertAlmostEqual(sameSummary.deviance, s.deviance)

    def test_logistic_regression_summary(self):
        df = self.spark.createDataFrame([(1.0, 2.0, Vectors.dense(1.0)),
                                         (0.0, 2.0, Vectors.sparse(1, [], []))],
                                        ["label", "weight", "features"])
        lr = LogisticRegression(maxIter=5, regParam=0.01, weightCol="weight", fitIntercept=False)
        model = lr.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        # test that api is callable and returns expected types
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.probabilityCol, "probability")
        self.assertEqual(s.labelCol, "label")
        self.assertEqual(s.featuresCol, "features")
        objHist = s.objectiveHistory
        self.assertTrue(isinstance(objHist, list) and isinstance(objHist[0], float))
        self.assertGreater(s.totalIterations, 0)
        self.assertTrue(isinstance(s.roc, DataFrame))
        self.assertAlmostEqual(s.areaUnderROC, 1.0, 2)
        self.assertTrue(isinstance(s.pr, DataFrame))
        self.assertTrue(isinstance(s.fMeasureByThreshold, DataFrame))
        self.assertTrue(isinstance(s.precisionByThreshold, DataFrame))
        self.assertTrue(isinstance(s.recallByThreshold, DataFrame))
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertAlmostEqual(sameSummary.areaUnderROC, s.areaUnderROC)


class OneVsRestTests(SparkSessionTestCase):

    def test_copy(self):
        df = self.spark.createDataFrame([(0.0, Vectors.dense(1.0, 0.8)),
                                         (1.0, Vectors.sparse(2, [], [])),
                                         (2.0, Vectors.dense(0.5, 0.5))],
                                        ["label", "features"])
        lr = LogisticRegression(maxIter=5, regParam=0.01)
        ovr = OneVsRest(classifier=lr)
        ovr1 = ovr.copy({lr.maxIter: 10})
        self.assertEqual(ovr.getClassifier().getMaxIter(), 5)
        self.assertEqual(ovr1.getClassifier().getMaxIter(), 10)
        model = ovr.fit(df)
        model1 = model.copy({model.predictionCol: "indexed"})
        self.assertEqual(model1.getPredictionCol(), "indexed")

    def test_output_columns(self):
        df = self.spark.createDataFrame([(0.0, Vectors.dense(1.0, 0.8)),
                                         (1.0, Vectors.sparse(2, [], [])),
                                         (2.0, Vectors.dense(0.5, 0.5))],
                                        ["label", "features"])
        lr = LogisticRegression(maxIter=5, regParam=0.01)
        ovr = OneVsRest(classifier=lr)
        model = ovr.fit(df)
        output = model.transform(df)
        self.assertEqual(output.columns, ["label", "features", "prediction"])


class HashingTFTest(SparkSessionTestCase):

    def test_apply_binary_term_freqs(self):

        df = self.spark.createDataFrame([(0, ["a", "a", "b", "c", "c", "c"])], ["id", "words"])
        n = 10
        hashingTF = HashingTF()
        hashingTF.setInputCol("words").setOutputCol("features").setNumFeatures(n).setBinary(True)
        output = hashingTF.transform(df)
        features = output.select("features").first().features.toArray()
        expected = Vectors.dense([1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]).toArray()
        for i in range(0, n):
            self.assertAlmostEqual(features[i], expected[i], 14, "Error at " + str(i) +
                                   ": expected " + str(expected[i]) + ", got " + str(features[i]))


class ALSTest(SparkSessionTestCase):

    def test_storage_levels(self):
        df = self.spark.createDataFrame(
            [(0, 0, 4.0), (0, 1, 2.0), (1, 1, 3.0), (1, 2, 4.0), (2, 1, 1.0), (2, 2, 5.0)],
            ["user", "item", "rating"])
        als = ALS().setMaxIter(1).setRank(1)
        # test default params
        als.fit(df)
        self.assertEqual(als.getIntermediateStorageLevel(), "MEMORY_AND_DISK")
        self.assertEqual(als._java_obj.getIntermediateStorageLevel(), "MEMORY_AND_DISK")
        self.assertEqual(als.getFinalStorageLevel(), "MEMORY_AND_DISK")
        self.assertEqual(als._java_obj.getFinalStorageLevel(), "MEMORY_AND_DISK")
        # test non-default params
        als.setIntermediateStorageLevel("MEMORY_ONLY_2")
        als.setFinalStorageLevel("DISK_ONLY")
        als.fit(df)
        self.assertEqual(als.getIntermediateStorageLevel(), "MEMORY_ONLY_2")
        self.assertEqual(als._java_obj.getIntermediateStorageLevel(), "MEMORY_ONLY_2")
        self.assertEqual(als.getFinalStorageLevel(), "DISK_ONLY")
        self.assertEqual(als._java_obj.getFinalStorageLevel(), "DISK_ONLY")


class DefaultValuesTests(PySparkTestCase):
    """
    Test :py:class:`JavaParams` classes to see if their default Param values match
    those in their Scala counterparts.
    """

    def check_params(self, py_stage):
        if not hasattr(py_stage, "_to_java"):
            return
        java_stage = py_stage._to_java()
        if java_stage is None:
            return
        for p in py_stage.params:
            java_param = java_stage.getParam(p.name)
            py_has_default = py_stage.hasDefault(p)
            java_has_default = java_stage.hasDefault(java_param)
            self.assertEqual(py_has_default, java_has_default,
                             "Default value mismatch of param %s for Params %s"
                             % (p.name, str(py_stage)))
            if py_has_default:
                if p.name == "seed":
                    return  # Random seeds between Spark and PySpark are different
                java_default =\
                    _java2py(self.sc, java_stage.clear(java_param).getOrDefault(java_param))
                py_stage._clear(p)
                py_default = py_stage.getOrDefault(p)
                self.assertEqual(java_default, py_default,
                                 "Java default %s != python default %s of param %s for Params %s"
                                 % (str(java_default), str(py_default), p.name, str(py_stage)))

    def test_java_params(self):
        import pyspark.ml.feature
        import pyspark.ml.classification
        import pyspark.ml.clustering
        import pyspark.ml.pipeline
        import pyspark.ml.recommendation
        import pyspark.ml.regression
        modules = [pyspark.ml.feature, pyspark.ml.classification, pyspark.ml.clustering,
                   pyspark.ml.pipeline, pyspark.ml.recommendation, pyspark.ml.regression]
        for module in modules:
            for name, cls in inspect.getmembers(module, inspect.isclass):
                if not name.endswith('Model') and issubclass(cls, JavaParams)\
                        and not inspect.isabstract(cls):
                    self.check_params(cls())


def _squared_distance(a, b):
    if isinstance(a, Vector):
        return a.squared_distance(b)
    else:
        return b.squared_distance(a)


class VectorTests(MLlibTestCase):

    def _test_serialize(self, v):
        self.assertEqual(v, ser.loads(ser.dumps(v)))
        jvec = self.sc._jvm.org.apache.spark.ml.python.MLSerDe.loads(bytearray(ser.dumps(v)))
        nv = ser.loads(bytes(self.sc._jvm.org.apache.spark.ml.python.MLSerDe.dumps(jvec)))
        self.assertEqual(v, nv)
        vs = [v] * 100
        jvecs = self.sc._jvm.org.apache.spark.ml.python.MLSerDe.loads(bytearray(ser.dumps(vs)))
        nvs = ser.loads(bytes(self.sc._jvm.org.apache.spark.ml.python.MLSerDe.dumps(jvecs)))
        self.assertEqual(vs, nvs)

    def test_serialize(self):
        self._test_serialize(DenseVector(range(10)))
        self._test_serialize(DenseVector(array([1., 2., 3., 4.])))
        self._test_serialize(DenseVector(pyarray.array('d', range(10))))
        self._test_serialize(SparseVector(4, {1: 1, 3: 2}))
        self._test_serialize(SparseVector(3, {}))
        self._test_serialize(DenseMatrix(2, 3, range(6)))
        sm1 = SparseMatrix(
            3, 4, [0, 2, 2, 4, 4], [1, 2, 1, 2], [1.0, 2.0, 4.0, 5.0])
        self._test_serialize(sm1)

    def test_dot(self):
        sv = SparseVector(4, {1: 1, 3: 2})
        dv = DenseVector(array([1., 2., 3., 4.]))
        lst = DenseVector([1, 2, 3, 4])
        mat = array([[1., 2., 3., 4.],
                     [1., 2., 3., 4.],
                     [1., 2., 3., 4.],
                     [1., 2., 3., 4.]])
        arr = pyarray.array('d', [0, 1, 2, 3])
        self.assertEqual(10.0, sv.dot(dv))
        self.assertTrue(array_equal(array([3., 6., 9., 12.]), sv.dot(mat)))
        self.assertEqual(30.0, dv.dot(dv))
        self.assertTrue(array_equal(array([10., 20., 30., 40.]), dv.dot(mat)))
        self.assertEqual(30.0, lst.dot(dv))
        self.assertTrue(array_equal(array([10., 20., 30., 40.]), lst.dot(mat)))
        self.assertEqual(7.0, sv.dot(arr))

    def test_squared_distance(self):
        sv = SparseVector(4, {1: 1, 3: 2})
        dv = DenseVector(array([1., 2., 3., 4.]))
        lst = DenseVector([4, 3, 2, 1])
        lst1 = [4, 3, 2, 1]
        arr = pyarray.array('d', [0, 2, 1, 3])
        narr = array([0, 2, 1, 3])
        self.assertEqual(15.0, _squared_distance(sv, dv))
        self.assertEqual(25.0, _squared_distance(sv, lst))
        self.assertEqual(20.0, _squared_distance(dv, lst))
        self.assertEqual(15.0, _squared_distance(dv, sv))
        self.assertEqual(25.0, _squared_distance(lst, sv))
        self.assertEqual(20.0, _squared_distance(lst, dv))
        self.assertEqual(0.0, _squared_distance(sv, sv))
        self.assertEqual(0.0, _squared_distance(dv, dv))
        self.assertEqual(0.0, _squared_distance(lst, lst))
        self.assertEqual(25.0, _squared_distance(sv, lst1))
        self.assertEqual(3.0, _squared_distance(sv, arr))
        self.assertEqual(3.0, _squared_distance(sv, narr))

    def test_hash(self):
        v1 = DenseVector([0.0, 1.0, 0.0, 5.5])
        v2 = SparseVector(4, [(1, 1.0), (3, 5.5)])
        v3 = DenseVector([0.0, 1.0, 0.0, 5.5])
        v4 = SparseVector(4, [(1, 1.0), (3, 2.5)])
        self.assertEqual(hash(v1), hash(v2))
        self.assertEqual(hash(v1), hash(v3))
        self.assertEqual(hash(v2), hash(v3))
        self.assertFalse(hash(v1) == hash(v4))
        self.assertFalse(hash(v2) == hash(v4))

    def test_eq(self):
        v1 = DenseVector([0.0, 1.0, 0.0, 5.5])
        v2 = SparseVector(4, [(1, 1.0), (3, 5.5)])
        v3 = DenseVector([0.0, 1.0, 0.0, 5.5])
        v4 = SparseVector(6, [(1, 1.0), (3, 5.5)])
        v5 = DenseVector([0.0, 1.0, 0.0, 2.5])
        v6 = SparseVector(4, [(1, 1.0), (3, 2.5)])
        self.assertEqual(v1, v2)
        self.assertEqual(v1, v3)
        self.assertFalse(v2 == v4)
        self.assertFalse(v1 == v5)
        self.assertFalse(v1 == v6)

    def test_equals(self):
        indices = [1, 2, 4]
        values = [1., 3., 2.]
        self.assertTrue(Vectors._equals(indices, values, list(range(5)), [0., 1., 3., 0., 2.]))
        self.assertFalse(Vectors._equals(indices, values, list(range(5)), [0., 3., 1., 0., 2.]))
        self.assertFalse(Vectors._equals(indices, values, list(range(5)), [0., 3., 0., 2.]))
        self.assertFalse(Vectors._equals(indices, values, list(range(5)), [0., 1., 3., 2., 2.]))

    def test_conversion(self):
        # numpy arrays should be automatically upcast to float64
        # tests for fix of [SPARK-5089]
        v = array([1, 2, 3, 4], dtype='float64')
        dv = DenseVector(v)
        self.assertTrue(dv.array.dtype == 'float64')
        v = array([1, 2, 3, 4], dtype='float32')
        dv = DenseVector(v)
        self.assertTrue(dv.array.dtype == 'float64')

    def test_sparse_vector_indexing(self):
        sv = SparseVector(5, {1: 1, 3: 2})
        self.assertEqual(sv[0], 0.)
        self.assertEqual(sv[3], 2.)
        self.assertEqual(sv[1], 1.)
        self.assertEqual(sv[2], 0.)
        self.assertEqual(sv[4], 0.)
        self.assertEqual(sv[-1], 0.)
        self.assertEqual(sv[-2], 2.)
        self.assertEqual(sv[-3], 0.)
        self.assertEqual(sv[-5], 0.)
        for ind in [5, -6]:
            self.assertRaises(IndexError, sv.__getitem__, ind)
        for ind in [7.8, '1']:
            self.assertRaises(TypeError, sv.__getitem__, ind)

        zeros = SparseVector(4, {})
        self.assertEqual(zeros[0], 0.0)
        self.assertEqual(zeros[3], 0.0)
        for ind in [4, -5]:
            self.assertRaises(IndexError, zeros.__getitem__, ind)

        empty = SparseVector(0, {})
        for ind in [-1, 0, 1]:
            self.assertRaises(IndexError, empty.__getitem__, ind)

    def test_matrix_indexing(self):
        mat = DenseMatrix(3, 2, [0, 1, 4, 6, 8, 10])
        expected = [[0, 6], [1, 8], [4, 10]]
        for i in range(3):
            for j in range(2):
                self.assertEqual(mat[i, j], expected[i][j])

    def test_repr_dense_matrix(self):
        mat = DenseMatrix(3, 2, [0, 1, 4, 6, 8, 10])
        self.assertTrue(
            repr(mat),
            'DenseMatrix(3, 2, [0.0, 1.0, 4.0, 6.0, 8.0, 10.0], False)')

        mat = DenseMatrix(3, 2, [0, 1, 4, 6, 8, 10], True)
        self.assertTrue(
            repr(mat),
            'DenseMatrix(3, 2, [0.0, 1.0, 4.0, 6.0, 8.0, 10.0], False)')

        mat = DenseMatrix(6, 3, zeros(18))
        self.assertTrue(
            repr(mat),
            'DenseMatrix(6, 3, [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ..., \
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], False)')

    def test_repr_sparse_matrix(self):
        sm1t = SparseMatrix(
            3, 4, [0, 2, 3, 5], [0, 1, 2, 0, 2], [3.0, 2.0, 4.0, 9.0, 8.0],
            isTransposed=True)
        self.assertTrue(
            repr(sm1t),
            'SparseMatrix(3, 4, [0, 2, 3, 5], [0, 1, 2, 0, 2], [3.0, 2.0, 4.0, 9.0, 8.0], True)')

        indices = tile(arange(6), 3)
        values = ones(18)
        sm = SparseMatrix(6, 3, [0, 6, 12, 18], indices, values)
        self.assertTrue(
            repr(sm), "SparseMatrix(6, 3, [0, 6, 12, 18], \
                [0, 1, 2, 3, 4, 5, 0, 1, ..., 4, 5, 0, 1, 2, 3, 4, 5], \
                [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, ..., \
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0], False)")

        self.assertTrue(
            str(sm),
            "6 X 3 CSCMatrix\n\
            (0,0) 1.0\n(1,0) 1.0\n(2,0) 1.0\n(3,0) 1.0\n(4,0) 1.0\n(5,0) 1.0\n\
            (0,1) 1.0\n(1,1) 1.0\n(2,1) 1.0\n(3,1) 1.0\n(4,1) 1.0\n(5,1) 1.0\n\
            (0,2) 1.0\n(1,2) 1.0\n(2,2) 1.0\n(3,2) 1.0\n..\n..")

        sm = SparseMatrix(1, 18, zeros(19), [], [])
        self.assertTrue(
            repr(sm),
            'SparseMatrix(1, 18, \
                [0, 0, 0, 0, 0, 0, 0, 0, ..., 0, 0, 0, 0, 0, 0, 0, 0], [], [], False)')

    def test_sparse_matrix(self):
        # Test sparse matrix creation.
        sm1 = SparseMatrix(
            3, 4, [0, 2, 2, 4, 4], [1, 2, 1, 2], [1.0, 2.0, 4.0, 5.0])
        self.assertEqual(sm1.numRows, 3)
        self.assertEqual(sm1.numCols, 4)
        self.assertEqual(sm1.colPtrs.tolist(), [0, 2, 2, 4, 4])
        self.assertEqual(sm1.rowIndices.tolist(), [1, 2, 1, 2])
        self.assertEqual(sm1.values.tolist(), [1.0, 2.0, 4.0, 5.0])
        self.assertTrue(
            repr(sm1),
            'SparseMatrix(3, 4, [0, 2, 2, 4, 4], [1, 2, 1, 2], [1.0, 2.0, 4.0, 5.0], False)')

        # Test indexing
        expected = [
            [0, 0, 0, 0],
            [1, 0, 4, 0],
            [2, 0, 5, 0]]

        for i in range(3):
            for j in range(4):
                self.assertEqual(expected[i][j], sm1[i, j])
        self.assertTrue(array_equal(sm1.toArray(), expected))

        # Test conversion to dense and sparse.
        smnew = sm1.toDense().toSparse()
        self.assertEqual(sm1.numRows, smnew.numRows)
        self.assertEqual(sm1.numCols, smnew.numCols)
        self.assertTrue(array_equal(sm1.colPtrs, smnew.colPtrs))
        self.assertTrue(array_equal(sm1.rowIndices, smnew.rowIndices))
        self.assertTrue(array_equal(sm1.values, smnew.values))

        sm1t = SparseMatrix(
            3, 4, [0, 2, 3, 5], [0, 1, 2, 0, 2], [3.0, 2.0, 4.0, 9.0, 8.0],
            isTransposed=True)
        self.assertEqual(sm1t.numRows, 3)
        self.assertEqual(sm1t.numCols, 4)
        self.assertEqual(sm1t.colPtrs.tolist(), [0, 2, 3, 5])
        self.assertEqual(sm1t.rowIndices.tolist(), [0, 1, 2, 0, 2])
        self.assertEqual(sm1t.values.tolist(), [3.0, 2.0, 4.0, 9.0, 8.0])

        expected = [
            [3, 2, 0, 0],
            [0, 0, 4, 0],
            [9, 0, 8, 0]]

        for i in range(3):
            for j in range(4):
                self.assertEqual(expected[i][j], sm1t[i, j])
        self.assertTrue(array_equal(sm1t.toArray(), expected))

    def test_dense_matrix_is_transposed(self):
        mat1 = DenseMatrix(3, 2, [0, 4, 1, 6, 3, 9], isTransposed=True)
        mat = DenseMatrix(3, 2, [0, 1, 3, 4, 6, 9])
        self.assertEqual(mat1, mat)

        expected = [[0, 4], [1, 6], [3, 9]]
        for i in range(3):
            for j in range(2):
                self.assertEqual(mat1[i, j], expected[i][j])
        self.assertTrue(array_equal(mat1.toArray(), expected))

        sm = mat1.toSparse()
        self.assertTrue(array_equal(sm.rowIndices, [1, 2, 0, 1, 2]))
        self.assertTrue(array_equal(sm.colPtrs, [0, 2, 5]))
        self.assertTrue(array_equal(sm.values, [1, 3, 4, 6, 9]))

    def test_norms(self):
        a = DenseVector([0, 2, 3, -1])
        self.assertAlmostEqual(a.norm(2), 3.742, 3)
        self.assertTrue(a.norm(1), 6)
        self.assertTrue(a.norm(inf), 3)
        a = SparseVector(4, [0, 2], [3, -4])
        self.assertAlmostEqual(a.norm(2), 5)
        self.assertTrue(a.norm(1), 7)
        self.assertTrue(a.norm(inf), 4)

        tmp = SparseVector(4, [0, 2], [3, 0])
        self.assertEqual(tmp.numNonzeros(), 1)


class VectorUDTTests(MLlibTestCase):

    dv0 = DenseVector([])
    dv1 = DenseVector([1.0, 2.0])
    sv0 = SparseVector(2, [], [])
    sv1 = SparseVector(2, [1], [2.0])
    udt = VectorUDT()

    def test_json_schema(self):
        self.assertEqual(VectorUDT.fromJson(self.udt.jsonValue()), self.udt)

    def test_serialization(self):
        for v in [self.dv0, self.dv1, self.sv0, self.sv1]:
            self.assertEqual(v, self.udt.deserialize(self.udt.serialize(v)))

    def test_infer_schema(self):
        rdd = self.sc.parallelize([Row(label=1.0, features=self.dv1),
                                   Row(label=0.0, features=self.sv1)])
        df = rdd.toDF()
        schema = df.schema
        field = [f for f in schema.fields if f.name == "features"][0]
        self.assertEqual(field.dataType, self.udt)
        vectors = df.rdd.map(lambda p: p.features).collect()
        self.assertEqual(len(vectors), 2)
        for v in vectors:
            if isinstance(v, SparseVector):
                self.assertEqual(v, self.sv1)
            elif isinstance(v, DenseVector):
                self.assertEqual(v, self.dv1)
            else:
                raise TypeError("expecting a vector but got %r of type %r" % (v, type(v)))


class MatrixUDTTests(MLlibTestCase):

    dm1 = DenseMatrix(3, 2, [0, 1, 4, 5, 9, 10])
    dm2 = DenseMatrix(3, 2, [0, 1, 4, 5, 9, 10], isTransposed=True)
    sm1 = SparseMatrix(1, 1, [0, 1], [0], [2.0])
    sm2 = SparseMatrix(2, 1, [0, 0, 1], [0], [5.0], isTransposed=True)
    udt = MatrixUDT()

    def test_json_schema(self):
        self.assertEqual(MatrixUDT.fromJson(self.udt.jsonValue()), self.udt)

    def test_serialization(self):
        for m in [self.dm1, self.dm2, self.sm1, self.sm2]:
            self.assertEqual(m, self.udt.deserialize(self.udt.serialize(m)))

    def test_infer_schema(self):
        rdd = self.sc.parallelize([("dense", self.dm1), ("sparse", self.sm1)])
        df = rdd.toDF()
        schema = df.schema
        self.assertTrue(schema.fields[1].dataType, self.udt)
        matrices = df.rdd.map(lambda x: x._2).collect()
        self.assertEqual(len(matrices), 2)
        for m in matrices:
            if isinstance(m, DenseMatrix):
                self.assertTrue(m, self.dm1)
            elif isinstance(m, SparseMatrix):
                self.assertTrue(m, self.sm1)
            else:
                raise ValueError("Expected a matrix but got type %r" % type(m))


if __name__ == "__main__":
    from pyspark.ml.tests import *
    if xmlrunner:
        unittest.main(testRunner=xmlrunner.XMLTestRunner(output='target/test-reports'))
    else:
        unittest.main()
