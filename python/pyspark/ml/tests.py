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
from numpy import abs, all, arange, array, array_equal, inf, ones, tile, zeros
import inspect
import py4j

from pyspark import keyword_only, SparkContext
from pyspark.ml import Estimator, Model, Pipeline, PipelineModel, Transformer, UnaryTransformer
from pyspark.ml.classification import *
from pyspark.ml.clustering import *
from pyspark.ml.common import _java2py, _py2java
from pyspark.ml.evaluation import BinaryClassificationEvaluator, \
    MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import *
from pyspark.ml.fpm import FPGrowth, FPGrowthModel
from pyspark.ml.image import ImageSchema
from pyspark.ml.linalg import DenseMatrix, DenseMatrix, DenseVector, Matrices, MatrixUDT, \
    SparseMatrix, SparseVector, Vector, VectorUDT, Vectors
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasInputCol, HasMaxIter, HasSeed
from pyspark.ml.recommendation import ALS
from pyspark.ml.regression import DecisionTreeRegressor, GeneralizedLinearRegression, \
    LinearRegression
from pyspark.ml.stat import ChiSquareTest
from pyspark.ml.tuning import *
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaParams, JavaWrapper
from pyspark.serializers import PickleSerializer
from pyspark.sql import DataFrame, Row, SparkSession, HiveContext
from pyspark.sql.functions import rand
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.storagelevel import *
from pyspark.tests import QuietTest, ReusedPySparkTestCase as PySparkTestCase

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


class MockUnaryTransformer(UnaryTransformer, DefaultParamsReadable, DefaultParamsWritable):

    shift = Param(Params._dummy(), "shift", "The amount by which to shift " +
                  "data in a DataFrame",
                  typeConverter=TypeConverters.toFloat)

    def __init__(self, shiftVal=1):
        super(MockUnaryTransformer, self).__init__()
        self._setDefault(shift=1)
        self._set(shift=shiftVal)

    def getShift(self):
        return self.getOrDefault(self.shift)

    def setShift(self, shift):
        self._set(shift=shift)

    def createTransformFunc(self):
        shiftVal = self.getShift()
        return lambda x: x + shiftVal

    def outputDataType(self):
        return DoubleType()

    def validateInputType(self, inputType):
        if inputType != DoubleType():
            raise TypeError("Bad input type: {}. ".format(inputType) +
                            "Requires Double.")


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
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, seed=None):
        """
        setParams(self, seed=None)
        Sets params for this test.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)


class OtherTestParams(HasMaxIter, HasInputCol, HasSeed):
    """
    A subclass of Params mixed with HasMaxIter, HasInputCol and HasSeed.
    """
    @keyword_only
    def __init__(self, seed=None):
        super(OtherTestParams, self).__init__()
        self._setDefault(maxIter=10)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, seed=None):
        """
        setParams(self, seed=None)
        Sets params for this test.
        """
        kwargs = self._input_kwargs
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
        self.assertTrue(testParams.hasParam(u"maxIter"))

    def test_resolveparam(self):
        testParams = TestParams()
        self.assertEqual(testParams._resolveParam(testParams.maxIter), testParams.maxIter)
        self.assertEqual(testParams._resolveParam("maxIter"), testParams.maxIter)

        self.assertEqual(testParams._resolveParam(u"maxIter"), testParams.maxIter)
        if sys.version_info[0] >= 3:
            # In Python 3, it is allowed to get/set attributes with non-ascii characters.
            e_cls = AttributeError
        else:
            e_cls = UnicodeEncodeError
        self.assertRaises(e_cls, lambda: testParams._resolveParam(u"아"))

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

        otherParam = Param(Params._dummy(), "otherParam", "Parameter used to test that " +
                           "set raises an error for a non-member parameter.",
                           typeConverter=TypeConverters.toString)
        with self.assertRaises(ValueError):
            testParams.set(otherParam, "value")

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

    def test_copy_param_extras(self):
        tp = TestParams(seed=42)
        extra = {tp.getParam(TestParams.inputCol.name): "copy_input"}
        tp_copy = tp.copy(extra=extra)
        self.assertEqual(tp.uid, tp_copy.uid)
        self.assertEqual(tp.params, tp_copy.params)
        for k, v in extra.items():
            self.assertTrue(tp_copy.isDefined(k))
            self.assertEqual(tp_copy.getOrDefault(k), v)
        copied_no_extra = {}
        for k, v in tp_copy._paramMap.items():
            if k not in extra:
                copied_no_extra[k] = v
        self.assertEqual(tp._paramMap, copied_no_extra)
        self.assertEqual(tp._defaultParamMap, tp_copy._defaultParamMap)

    def test_logistic_regression_check_thresholds(self):
        self.assertIsInstance(
            LogisticRegression(threshold=0.5, thresholds=[0.5, 0.5]),
            LogisticRegression
        )

        self.assertRaisesRegexp(
            ValueError,
            "Logistic Regression getThreshold found inconsistent.*$",
            LogisticRegression, threshold=0.42, thresholds=[0.5, 0.5]
        )

    @staticmethod
    def check_params(test_self, py_stage, check_params_exist=True):
        """
        Checks common requirements for Params.params:
          - set of params exist in Java and Python and are ordered by names
          - param parent has the same UID as the object's UID
          - default param value from Java matches value in Python
          - optionally check if all params from Java also exist in Python
        """
        py_stage_str = "%s %s" % (type(py_stage), py_stage)
        if not hasattr(py_stage, "_to_java"):
            return
        java_stage = py_stage._to_java()
        if java_stage is None:
            return
        test_self.assertEqual(py_stage.uid, java_stage.uid(), msg=py_stage_str)
        if check_params_exist:
            param_names = [p.name for p in py_stage.params]
            java_params = list(java_stage.params())
            java_param_names = [jp.name() for jp in java_params]
            test_self.assertEqual(
                param_names, sorted(java_param_names),
                "Param list in Python does not match Java for %s:\nJava = %s\nPython = %s"
                % (py_stage_str, java_param_names, param_names))
        for p in py_stage.params:
            test_self.assertEqual(p.parent, py_stage.uid)
            java_param = java_stage.getParam(p.name)
            py_has_default = py_stage.hasDefault(p)
            java_has_default = java_stage.hasDefault(java_param)
            test_self.assertEqual(py_has_default, java_has_default,
                                  "Default value mismatch of param %s for Params %s"
                                  % (p.name, str(py_stage)))
            if py_has_default:
                if p.name == "seed":
                    continue  # Random seeds between Spark and PySpark are different
                java_default = _java2py(test_self.sc,
                                        java_stage.clear(java_param).getOrDefault(java_param))
                py_stage._clear(p)
                py_default = py_stage.getOrDefault(p)
                # equality test for NaN is always False
                if isinstance(java_default, float) and np.isnan(java_default):
                    java_default = "NaN"
                    py_default = "NaN" if np.isnan(py_default) else "not NaN"
                test_self.assertEqual(
                    java_default, py_default,
                    "Java default %s != python default %s of param %s for Params %s"
                    % (str(java_default), str(py_default), p.name, str(py_stage)))


class EvaluatorTests(SparkSessionTestCase):

    def test_java_params(self):
        """
        This tests a bug fixed by SPARK-18274 which causes multiple copies
        of a Params instance in Python to be linked to the same Java instance.
        """
        evaluator = RegressionEvaluator(metricName="r2")
        df = self.spark.createDataFrame([Row(label=1.0, prediction=1.1)])
        evaluator.evaluate(df)
        self.assertEqual(evaluator._java_obj.getMetricName(), "r2")
        evaluatorCopy = evaluator.copy({evaluator.metricName: "mae"})
        evaluator.evaluate(df)
        evaluatorCopy.evaluate(df)
        self.assertEqual(evaluator._java_obj.getMetricName(), "r2")
        self.assertEqual(evaluatorCopy._java_obj.getMetricName(), "mae")


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
        # Test that parameters transferred to Python Model
        ParamTests.check_params(self, idf0m)

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

    def test_rformula_force_index_label(self):
        df = self.spark.createDataFrame([
            (1.0, 1.0, "a"),
            (0.0, 2.0, "b"),
            (1.0, 0.0, "a")], ["y", "x", "s"])
        # Does not index label by default since it's numeric type.
        rf = RFormula(formula="y ~ x + s")
        model = rf.fit(df)
        transformedDF = model.transform(df)
        self.assertEqual(transformedDF.head().label, 1.0)
        # Force to index label.
        rf2 = RFormula(formula="y ~ x + s").setForceIndexLabel(True)
        model2 = rf2.fit(df)
        transformedDF2 = model2.transform(df)
        self.assertEqual(transformedDF2.head().label, 0.0)

    def test_rformula_string_indexer_order_type(self):
        df = self.spark.createDataFrame([
            (1.0, 1.0, "a"),
            (0.0, 2.0, "b"),
            (1.0, 0.0, "a")], ["y", "x", "s"])
        rf = RFormula(formula="y ~ x + s", stringIndexerOrderType="alphabetDesc")
        self.assertEqual(rf.getStringIndexerOrderType(), 'alphabetDesc')
        transformedDF = rf.fit(df).transform(df)
        observed = transformedDF.select("features").collect()
        expected = [[1.0, 0.0], [2.0, 1.0], [0.0, 0.0]]
        for i in range(0, len(expected)):
            self.assertTrue(all(observed[i]["features"].toArray() == expected[i]))

    def test_string_indexer_handle_invalid(self):
        df = self.spark.createDataFrame([
            (0, "a"),
            (1, "d"),
            (2, None)], ["id", "label"])

        si1 = StringIndexer(inputCol="label", outputCol="indexed", handleInvalid="keep",
                            stringOrderType="alphabetAsc")
        model1 = si1.fit(df)
        td1 = model1.transform(df)
        actual1 = td1.select("id", "indexed").collect()
        expected1 = [Row(id=0, indexed=0.0), Row(id=1, indexed=1.0), Row(id=2, indexed=2.0)]
        self.assertEqual(actual1, expected1)

        si2 = si1.setHandleInvalid("skip")
        model2 = si2.fit(df)
        td2 = model2.transform(df)
        actual2 = td2.select("id", "indexed").collect()
        expected2 = [Row(id=0, indexed=0.0), Row(id=1, indexed=1.0)]
        self.assertEqual(actual2, expected2)


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

    def test_save_load_trained_model(self):
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

    def test_save_load_simple_estimator(self):
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

        # test save/load of CrossValidator
        cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        cvPath = temp_path + "/cv"
        cv.save(cvPath)
        loadedCV = CrossValidator.load(cvPath)
        self.assertEqual(loadedCV.getEstimator().uid, cv.getEstimator().uid)
        self.assertEqual(loadedCV.getEvaluator().uid, cv.getEvaluator().uid)
        self.assertEqual(loadedCV.getEstimatorParamMaps(), cv.getEstimatorParamMaps())

        # test save/load of CrossValidatorModel
        cvModelPath = temp_path + "/cvModel"
        cvModel.save(cvModelPath)
        loadedModel = CrossValidatorModel.load(cvModelPath)
        self.assertEqual(loadedModel.bestModel.uid, cvModel.bestModel.uid)

    def test_parallel_evaluation(self):
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])

        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [5, 6]).build()
        evaluator = BinaryClassificationEvaluator()

        # test save/load of CrossValidator
        cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        cv.setParallelism(1)
        cvSerialModel = cv.fit(dataset)
        cv.setParallelism(2)
        cvParallelModel = cv.fit(dataset)
        self.assertEqual(cvSerialModel.avgMetrics, cvParallelModel.avgMetrics)

    def test_save_load_nested_estimator(self):
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])

        ova = OneVsRest(classifier=LogisticRegression())
        lr1 = LogisticRegression().setMaxIter(100)
        lr2 = LogisticRegression().setMaxIter(150)
        grid = ParamGridBuilder().addGrid(ova.classifier, [lr1, lr2]).build()
        evaluator = MulticlassClassificationEvaluator()

        # test save/load of CrossValidator
        cv = CrossValidator(estimator=ova, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        cvPath = temp_path + "/cv"
        cv.save(cvPath)
        loadedCV = CrossValidator.load(cvPath)
        self.assertEqual(loadedCV.getEstimator().uid, cv.getEstimator().uid)
        self.assertEqual(loadedCV.getEvaluator().uid, cv.getEvaluator().uid)

        originalParamMap = cv.getEstimatorParamMaps()
        loadedParamMap = loadedCV.getEstimatorParamMaps()
        for i, param in enumerate(loadedParamMap):
            for p in param:
                if p.name == "classifier":
                    self.assertEqual(param[p].uid, originalParamMap[i][p].uid)
                else:
                    self.assertEqual(param[p], originalParamMap[i][p])

        # test save/load of CrossValidatorModel
        cvModelPath = temp_path + "/cvModel"
        cvModel.save(cvModelPath)
        loadedModel = CrossValidatorModel.load(cvModelPath)
        self.assertEqual(loadedModel.bestModel.uid, cvModel.bestModel.uid)


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

    def test_save_load_trained_model(self):
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

    def test_save_load_simple_estimator(self):
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

        tvsPath = temp_path + "/tvs"
        tvs.save(tvsPath)
        loadedTvs = TrainValidationSplit.load(tvsPath)
        self.assertEqual(loadedTvs.getEstimator().uid, tvs.getEstimator().uid)
        self.assertEqual(loadedTvs.getEvaluator().uid, tvs.getEvaluator().uid)
        self.assertEqual(loadedTvs.getEstimatorParamMaps(), tvs.getEstimatorParamMaps())

        tvsModelPath = temp_path + "/tvsModel"
        tvsModel.save(tvsModelPath)
        loadedModel = TrainValidationSplitModel.load(tvsModelPath)
        self.assertEqual(loadedModel.bestModel.uid, tvsModel.bestModel.uid)

    def test_parallel_evaluation(self):
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])
        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [5, 6]).build()
        evaluator = BinaryClassificationEvaluator()
        tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        tvs.setParallelism(1)
        tvsSerialModel = tvs.fit(dataset)
        tvs.setParallelism(2)
        tvsParallelModel = tvs.fit(dataset)
        self.assertEqual(tvsSerialModel.validationMetrics, tvsParallelModel.validationMetrics)

    def test_save_load_nested_estimator(self):
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
        ova = OneVsRest(classifier=LogisticRegression())
        lr1 = LogisticRegression().setMaxIter(100)
        lr2 = LogisticRegression().setMaxIter(150)
        grid = ParamGridBuilder().addGrid(ova.classifier, [lr1, lr2]).build()
        evaluator = MulticlassClassificationEvaluator()

        tvs = TrainValidationSplit(estimator=ova, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        tvsPath = temp_path + "/tvs"
        tvs.save(tvsPath)
        loadedTvs = TrainValidationSplit.load(tvsPath)
        self.assertEqual(loadedTvs.getEstimator().uid, tvs.getEstimator().uid)
        self.assertEqual(loadedTvs.getEvaluator().uid, tvs.getEvaluator().uid)

        originalParamMap = tvs.getEstimatorParamMaps()
        loadedParamMap = loadedTvs.getEstimatorParamMaps()
        for i, param in enumerate(loadedParamMap):
            for p in param:
                if p.name == "classifier":
                    self.assertEqual(param[p].uid, originalParamMap[i][p].uid)
                else:
                    self.assertEqual(param[p], originalParamMap[i][p])

        tvsModelPath = temp_path + "/tvsModel"
        tvsModel.save(tvsModelPath)
        loadedModel = TrainValidationSplitModel.load(tvsModelPath)
        self.assertEqual(loadedModel.bestModel.uid, tvsModel.bestModel.uid)

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
        self.assertEqual(s.degreesOfFreedom, 1)
        devResiduals = s.devianceResiduals
        self.assertTrue(isinstance(devResiduals, list) and isinstance(devResiduals[0], float))
        coefStdErr = s.coefficientStandardErrors
        self.assertTrue(isinstance(coefStdErr, list) and isinstance(coefStdErr[0], float))
        tValues = s.tValues
        self.assertTrue(isinstance(tValues, list) and isinstance(tValues[0], float))
        pValues = s.pValues
        self.assertTrue(isinstance(pValues, list) and isinstance(pValues[0], float))
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned
        # The child class LinearRegressionTrainingSummary runs full test
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
        self.assertEqual(s.numInstances, 2)
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
        # one check is enough to verify a summary is returned
        # The child class GeneralizedLinearRegressionTrainingSummary runs full test
        sameSummary = model.evaluate(df)
        self.assertAlmostEqual(sameSummary.deviance, s.deviance)

    def test_binary_logistic_regression_summary(self):
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
        self.assertEqual(s.predictionCol, "prediction")
        objHist = s.objectiveHistory
        self.assertTrue(isinstance(objHist, list) and isinstance(objHist[0], float))
        self.assertGreater(s.totalIterations, 0)
        self.assertTrue(isinstance(s.labels, list))
        self.assertTrue(isinstance(s.truePositiveRateByLabel, list))
        self.assertTrue(isinstance(s.falsePositiveRateByLabel, list))
        self.assertTrue(isinstance(s.precisionByLabel, list))
        self.assertTrue(isinstance(s.recallByLabel, list))
        self.assertTrue(isinstance(s.fMeasureByLabel(), list))
        self.assertTrue(isinstance(s.fMeasureByLabel(1.0), list))
        self.assertTrue(isinstance(s.roc, DataFrame))
        self.assertAlmostEqual(s.areaUnderROC, 1.0, 2)
        self.assertTrue(isinstance(s.pr, DataFrame))
        self.assertTrue(isinstance(s.fMeasureByThreshold, DataFrame))
        self.assertTrue(isinstance(s.precisionByThreshold, DataFrame))
        self.assertTrue(isinstance(s.recallByThreshold, DataFrame))
        self.assertAlmostEqual(s.accuracy, 1.0, 2)
        self.assertAlmostEqual(s.weightedTruePositiveRate, 1.0, 2)
        self.assertAlmostEqual(s.weightedFalsePositiveRate, 0.0, 2)
        self.assertAlmostEqual(s.weightedRecall, 1.0, 2)
        self.assertAlmostEqual(s.weightedPrecision, 1.0, 2)
        self.assertAlmostEqual(s.weightedFMeasure(), 1.0, 2)
        self.assertAlmostEqual(s.weightedFMeasure(1.0), 1.0, 2)
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertAlmostEqual(sameSummary.areaUnderROC, s.areaUnderROC)

    def test_multiclass_logistic_regression_summary(self):
        df = self.spark.createDataFrame([(1.0, 2.0, Vectors.dense(1.0)),
                                         (0.0, 2.0, Vectors.sparse(1, [], [])),
                                         (2.0, 2.0, Vectors.dense(2.0)),
                                         (2.0, 2.0, Vectors.dense(1.9))],
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
        self.assertEqual(s.predictionCol, "prediction")
        objHist = s.objectiveHistory
        self.assertTrue(isinstance(objHist, list) and isinstance(objHist[0], float))
        self.assertGreater(s.totalIterations, 0)
        self.assertTrue(isinstance(s.labels, list))
        self.assertTrue(isinstance(s.truePositiveRateByLabel, list))
        self.assertTrue(isinstance(s.falsePositiveRateByLabel, list))
        self.assertTrue(isinstance(s.precisionByLabel, list))
        self.assertTrue(isinstance(s.recallByLabel, list))
        self.assertTrue(isinstance(s.fMeasureByLabel(), list))
        self.assertTrue(isinstance(s.fMeasureByLabel(1.0), list))
        self.assertAlmostEqual(s.accuracy, 0.75, 2)
        self.assertAlmostEqual(s.weightedTruePositiveRate, 0.75, 2)
        self.assertAlmostEqual(s.weightedFalsePositiveRate, 0.25, 2)
        self.assertAlmostEqual(s.weightedRecall, 0.75, 2)
        self.assertAlmostEqual(s.weightedPrecision, 0.583, 2)
        self.assertAlmostEqual(s.weightedFMeasure(), 0.65, 2)
        self.assertAlmostEqual(s.weightedFMeasure(1.0), 0.65, 2)
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertAlmostEqual(sameSummary.accuracy, s.accuracy)

    def test_gaussian_mixture_summary(self):
        data = [(Vectors.dense(1.0),), (Vectors.dense(5.0),), (Vectors.dense(10.0),),
                (Vectors.sparse(1, [], []),)]
        df = self.spark.createDataFrame(data, ["features"])
        gmm = GaussianMixture(k=2)
        model = gmm.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.probabilityCol, "probability")
        self.assertTrue(isinstance(s.probability, DataFrame))
        self.assertEqual(s.featuresCol, "features")
        self.assertEqual(s.predictionCol, "prediction")
        self.assertTrue(isinstance(s.cluster, DataFrame))
        self.assertEqual(len(s.clusterSizes), 2)
        self.assertEqual(s.k, 2)

    def test_bisecting_kmeans_summary(self):
        data = [(Vectors.dense(1.0),), (Vectors.dense(5.0),), (Vectors.dense(10.0),),
                (Vectors.sparse(1, [], []),)]
        df = self.spark.createDataFrame(data, ["features"])
        bkm = BisectingKMeans(k=2)
        model = bkm.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.featuresCol, "features")
        self.assertEqual(s.predictionCol, "prediction")
        self.assertTrue(isinstance(s.cluster, DataFrame))
        self.assertEqual(len(s.clusterSizes), 2)
        self.assertEqual(s.k, 2)

    def test_kmeans_summary(self):
        data = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),
                (Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
        df = self.spark.createDataFrame(data, ["features"])
        kmeans = KMeans(k=2, seed=1)
        model = kmeans.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.featuresCol, "features")
        self.assertEqual(s.predictionCol, "prediction")
        self.assertTrue(isinstance(s.cluster, DataFrame))
        self.assertEqual(len(s.clusterSizes), 2)
        self.assertEqual(s.k, 2)


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
        ovr = OneVsRest(classifier=lr, parallelism=1)
        model = ovr.fit(df)
        output = model.transform(df)
        self.assertEqual(output.columns, ["label", "features", "prediction"])

    def test_parallelism_doesnt_change_output(self):
        df = self.spark.createDataFrame([(0.0, Vectors.dense(1.0, 0.8)),
                                         (1.0, Vectors.sparse(2, [], [])),
                                         (2.0, Vectors.dense(0.5, 0.5))],
                                        ["label", "features"])
        ovrPar1 = OneVsRest(classifier=LogisticRegression(maxIter=5, regParam=.01), parallelism=1)
        modelPar1 = ovrPar1.fit(df)
        ovrPar2 = OneVsRest(classifier=LogisticRegression(maxIter=5, regParam=.01), parallelism=2)
        modelPar2 = ovrPar2.fit(df)
        for i, model in enumerate(modelPar1.models):
            self.assertTrue(np.allclose(model.coefficients.toArray(),
                                        modelPar2.models[i].coefficients.toArray(), atol=1E-4))
            self.assertTrue(np.allclose(model.intercept, modelPar2.models[i].intercept, atol=1E-4))

    def test_support_for_weightCol(self):
        df = self.spark.createDataFrame([(0.0, Vectors.dense(1.0, 0.8), 1.0),
                                         (1.0, Vectors.sparse(2, [], []), 1.0),
                                         (2.0, Vectors.dense(0.5, 0.5), 1.0)],
                                        ["label", "features", "weight"])
        # classifier inherits hasWeightCol
        lr = LogisticRegression(maxIter=5, regParam=0.01)
        ovr = OneVsRest(classifier=lr, weightCol="weight")
        self.assertIsNotNone(ovr.fit(df))
        # classifier doesn't inherit hasWeightCol
        dt = DecisionTreeClassifier()
        ovr2 = OneVsRest(classifier=dt, weightCol="weight")
        self.assertIsNotNone(ovr2.fit(df))


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


class GeneralizedLinearRegressionTest(SparkSessionTestCase):

    def test_tweedie_distribution(self):

        df = self.spark.createDataFrame(
            [(1.0, Vectors.dense(0.0, 0.0)),
             (1.0, Vectors.dense(1.0, 2.0)),
             (2.0, Vectors.dense(0.0, 0.0)),
             (2.0, Vectors.dense(1.0, 1.0)), ], ["label", "features"])

        glr = GeneralizedLinearRegression(family="tweedie", variancePower=1.6)
        model = glr.fit(df)
        self.assertTrue(np.allclose(model.coefficients.toArray(), [-0.4645, 0.3402], atol=1E-4))
        self.assertTrue(np.isclose(model.intercept, 0.7841, atol=1E-4))

        model2 = glr.setLinkPower(-1.0).fit(df)
        self.assertTrue(np.allclose(model2.coefficients.toArray(), [-0.6667, 0.5], atol=1E-4))
        self.assertTrue(np.isclose(model2.intercept, 0.6667, atol=1E-4))

    def test_offset(self):

        df = self.spark.createDataFrame(
            [(0.2, 1.0, 2.0, Vectors.dense(0.0, 5.0)),
             (0.5, 2.1, 0.5, Vectors.dense(1.0, 2.0)),
             (0.9, 0.4, 1.0, Vectors.dense(2.0, 1.0)),
             (0.7, 0.7, 0.0, Vectors.dense(3.0, 3.0))], ["label", "weight", "offset", "features"])

        glr = GeneralizedLinearRegression(family="poisson", weightCol="weight", offsetCol="offset")
        model = glr.fit(df)
        self.assertTrue(np.allclose(model.coefficients.toArray(), [0.664647, -0.3192581],
                                    atol=1E-4))
        self.assertTrue(np.isclose(model.intercept, -1.561613, atol=1E-4))


class LinearRegressionTest(SparkSessionTestCase):

    def test_linear_regression_with_huber_loss(self):

        data_path = "data/mllib/sample_linear_regression_data.txt"
        df = self.spark.read.format("libsvm").load(data_path)

        lir = LinearRegression(loss="huber", epsilon=2.0)
        model = lir.fit(df)

        expectedCoefficients = [0.136, 0.7648, -0.7761, 2.4236, 0.537,
                                1.2612, -0.333, -0.5694, -0.6311, 0.6053]
        expectedIntercept = 0.1607
        expectedScale = 9.758

        self.assertTrue(
            np.allclose(model.coefficients.toArray(), expectedCoefficients, atol=1E-3))
        self.assertTrue(np.isclose(model.intercept, expectedIntercept, atol=1E-3))
        self.assertTrue(np.isclose(model.scale, expectedScale, atol=1E-3))


class LogisticRegressionTest(SparkSessionTestCase):

    def test_binomial_logistic_regression_with_bound(self):

        df = self.spark.createDataFrame(
            [(1.0, 1.0, Vectors.dense(0.0, 5.0)),
             (0.0, 2.0, Vectors.dense(1.0, 2.0)),
             (1.0, 3.0, Vectors.dense(2.0, 1.0)),
             (0.0, 4.0, Vectors.dense(3.0, 3.0)), ], ["label", "weight", "features"])

        lor = LogisticRegression(regParam=0.01, weightCol="weight",
                                 lowerBoundsOnCoefficients=Matrices.dense(1, 2, [-1.0, -1.0]),
                                 upperBoundsOnIntercepts=Vectors.dense(0.0))
        model = lor.fit(df)
        self.assertTrue(
            np.allclose(model.coefficients.toArray(), [-0.2944, -0.0484], atol=1E-4))
        self.assertTrue(np.isclose(model.intercept, 0.0, atol=1E-4))

    def test_multinomial_logistic_regression_with_bound(self):

        data_path = "data/mllib/sample_multiclass_classification_data.txt"
        df = self.spark.read.format("libsvm").load(data_path)

        lor = LogisticRegression(regParam=0.01,
                                 lowerBoundsOnCoefficients=Matrices.dense(3, 4, range(12)),
                                 upperBoundsOnIntercepts=Vectors.dense(0.0, 0.0, 0.0))
        model = lor.fit(df)
        expected = [[4.593, 4.5516, 9.0099, 12.2904],
                    [1.0, 8.1093, 7.0, 10.0],
                    [3.041, 5.0, 8.0, 11.0]]
        for i in range(0, len(expected)):
            self.assertTrue(
                np.allclose(model.coefficientMatrix.toArray()[i], expected[i], atol=1E-4))
        self.assertTrue(
            np.allclose(model.interceptVector.toArray(), [-0.9057, -1.1392, -0.0033], atol=1E-4))


class MultilayerPerceptronClassifierTest(SparkSessionTestCase):

    def test_raw_and_probability_prediction(self):

        data_path = "data/mllib/sample_multiclass_classification_data.txt"
        df = self.spark.read.format("libsvm").load(data_path)

        mlp = MultilayerPerceptronClassifier(maxIter=100, layers=[4, 5, 4, 3],
                                             blockSize=128, seed=123)
        model = mlp.fit(df)
        test = self.sc.parallelize([Row(features=Vectors.dense(0.1, 0.1, 0.25, 0.25))]).toDF()
        result = model.transform(test).head()
        expected_prediction = 2.0
        expected_probability = [0.0, 0.0, 1.0]
        expected_rawPrediction = [57.3955, -124.5462, 67.9943]
        self.assertTrue(result.prediction, expected_prediction)
        self.assertTrue(np.allclose(result.probability, expected_probability, atol=1E-4))
        self.assertTrue(np.allclose(result.rawPrediction, expected_rawPrediction, atol=1E-4))


class FPGrowthTests(SparkSessionTestCase):
    def setUp(self):
        super(FPGrowthTests, self).setUp()
        self.data = self.spark.createDataFrame(
            [([1, 2], ), ([1, 2], ), ([1, 2, 3], ), ([1, 3], )],
            ["items"])

    def test_association_rules(self):
        fp = FPGrowth()
        fpm = fp.fit(self.data)

        expected_association_rules = self.spark.createDataFrame(
            [([3], [1], 1.0), ([2], [1], 1.0)],
            ["antecedent", "consequent", "confidence"]
        )
        actual_association_rules = fpm.associationRules

        self.assertEqual(actual_association_rules.subtract(expected_association_rules).count(), 0)
        self.assertEqual(expected_association_rules.subtract(actual_association_rules).count(), 0)

    def test_freq_itemsets(self):
        fp = FPGrowth()
        fpm = fp.fit(self.data)

        expected_freq_itemsets = self.spark.createDataFrame(
            [([1], 4), ([2], 3), ([2, 1], 3), ([3], 2), ([3, 1], 2)],
            ["items", "freq"]
        )
        actual_freq_itemsets = fpm.freqItemsets

        self.assertEqual(actual_freq_itemsets.subtract(expected_freq_itemsets).count(), 0)
        self.assertEqual(expected_freq_itemsets.subtract(actual_freq_itemsets).count(), 0)

    def tearDown(self):
        del self.data


class ImageReaderTest(SparkSessionTestCase):

    def test_read_images(self):
        data_path = 'data/mllib/images/kittens'
        df = ImageSchema.readImages(data_path, recursive=True, dropImageFailures=True)
        self.assertEqual(df.count(), 4)
        first_row = df.take(1)[0][0]
        array = ImageSchema.toNDArray(first_row)
        self.assertEqual(len(array), first_row[1])
        self.assertEqual(ImageSchema.toImage(array, origin=first_row[0]), first_row)
        self.assertEqual(df.schema, ImageSchema.imageSchema)
        expected = {'CV_8UC3': 16, 'Undefined': -1, 'CV_8U': 0, 'CV_8UC1': 0, 'CV_8UC4': 24}
        self.assertEqual(ImageSchema.ocvTypes, expected)
        expected = ['origin', 'height', 'width', 'nChannels', 'mode', 'data']
        self.assertEqual(ImageSchema.imageFields, expected)
        self.assertEqual(ImageSchema.undefinedImageType, "Undefined")

        with QuietTest(self.sc):
            self.assertRaisesRegexp(
                TypeError,
                "image argument should be pyspark.sql.types.Row; however",
                lambda: ImageSchema.toNDArray("a"))

        with QuietTest(self.sc):
            self.assertRaisesRegexp(
                ValueError,
                "image argument should have attributes specified in",
                lambda: ImageSchema.toNDArray(Row(a=1)))

        with QuietTest(self.sc):
            self.assertRaisesRegexp(
                TypeError,
                "array argument should be numpy.ndarray; however, it got",
                lambda: ImageSchema.toImage("a"))


class ImageReaderTest2(PySparkTestCase):

    @classmethod
    def setUpClass(cls):
        super(ImageReaderTest2, cls).setUpClass()
        # Note that here we enable Hive's support.
        cls.spark = None
        try:
            cls.sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
        except py4j.protocol.Py4JError:
            cls.tearDownClass()
            raise unittest.SkipTest("Hive is not available")
        except TypeError:
            cls.tearDownClass()
            raise unittest.SkipTest("Hive is not available")
        cls.spark = HiveContext._createForTesting(cls.sc)

    @classmethod
    def tearDownClass(cls):
        super(ImageReaderTest2, cls).tearDownClass()
        if cls.spark is not None:
            cls.spark.sparkSession.stop()
            cls.spark = None

    def test_read_images_multiple_times(self):
        # This test case is to check if `ImageSchema.readImages` tries to
        # initiate Hive client multiple times. See SPARK-22651.
        data_path = 'data/mllib/images/kittens'
        ImageSchema.readImages(data_path, recursive=True, dropImageFailures=True)
        ImageSchema.readImages(data_path, recursive=True, dropImageFailures=True)


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
                    # NOTE: disable check_params_exist until there is parity with Scala API
                    ParamTests.check_params(self, cls(), check_params_exist=False)


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

    def test_sparse_vector_iteration(self):
        self.assertListEqual(list(SparseVector(3, [], [])), [0.0, 0.0, 0.0])
        self.assertListEqual(list(SparseVector(5, [0, 3], [1.0, 2.0])), [1.0, 0.0, 0.0, 2.0, 0.0])

    def test_matrix_indexing(self):
        mat = DenseMatrix(3, 2, [0, 1, 4, 6, 8, 10])
        expected = [[0, 6], [1, 8], [4, 10]]
        for i in range(3):
            for j in range(2):
                self.assertEqual(mat[i, j], expected[i][j])

        for i, j in [(-1, 0), (4, 1), (3, 4)]:
            self.assertRaises(IndexError, mat.__getitem__, (i, j))

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

        for i, j in [(-1, 1), (4, 3), (3, 5)]:
            self.assertRaises(IndexError, sm1.__getitem__, (i, j))

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


class WrapperTests(MLlibTestCase):

    def test_new_java_array(self):
        # test array of strings
        str_list = ["a", "b", "c"]
        java_class = self.sc._gateway.jvm.java.lang.String
        java_array = JavaWrapper._new_java_array(str_list, java_class)
        self.assertEqual(_java2py(self.sc, java_array), str_list)
        # test array of integers
        int_list = [1, 2, 3]
        java_class = self.sc._gateway.jvm.java.lang.Integer
        java_array = JavaWrapper._new_java_array(int_list, java_class)
        self.assertEqual(_java2py(self.sc, java_array), int_list)
        # test array of floats
        float_list = [0.1, 0.2, 0.3]
        java_class = self.sc._gateway.jvm.java.lang.Double
        java_array = JavaWrapper._new_java_array(float_list, java_class)
        self.assertEqual(_java2py(self.sc, java_array), float_list)
        # test array of bools
        bool_list = [False, True, True]
        java_class = self.sc._gateway.jvm.java.lang.Boolean
        java_array = JavaWrapper._new_java_array(bool_list, java_class)
        self.assertEqual(_java2py(self.sc, java_array), bool_list)
        # test array of Java DenseVectors
        v1 = DenseVector([0.0, 1.0])
        v2 = DenseVector([1.0, 0.0])
        vec_java_list = [_py2java(self.sc, v1), _py2java(self.sc, v2)]
        java_class = self.sc._gateway.jvm.org.apache.spark.ml.linalg.DenseVector
        java_array = JavaWrapper._new_java_array(vec_java_list, java_class)
        self.assertEqual(_java2py(self.sc, java_array), [v1, v2])
        # test empty array
        java_class = self.sc._gateway.jvm.java.lang.Integer
        java_array = JavaWrapper._new_java_array([], java_class)
        self.assertEqual(_java2py(self.sc, java_array), [])


class ChiSquareTestTests(SparkSessionTestCase):

    def test_chisquaretest(self):
        data = [[0, Vectors.dense([0, 1, 2])],
                [1, Vectors.dense([1, 1, 1])],
                [2, Vectors.dense([2, 1, 0])]]
        df = self.spark.createDataFrame(data, ['label', 'feat'])
        res = ChiSquareTest.test(df, 'feat', 'label')
        # This line is hitting the collect bug described in #17218, commented for now.
        # pValues = res.select("degreesOfFreedom").collect())
        self.assertIsInstance(res, DataFrame)
        fieldNames = set(field.name for field in res.schema.fields)
        expectedFields = ["pValues", "degreesOfFreedom", "statistics"]
        self.assertTrue(all(field in fieldNames for field in expectedFields))


class UnaryTransformerTests(SparkSessionTestCase):

    def test_unary_transformer_validate_input_type(self):
        shiftVal = 3
        transformer = MockUnaryTransformer(shiftVal=shiftVal)\
            .setInputCol("input").setOutputCol("output")

        # should not raise any errors
        transformer.validateInputType(DoubleType())

        with self.assertRaises(TypeError):
            # passing the wrong input type should raise an error
            transformer.validateInputType(IntegerType())

    def test_unary_transformer_transform(self):
        shiftVal = 3
        transformer = MockUnaryTransformer(shiftVal=shiftVal)\
            .setInputCol("input").setOutputCol("output")

        df = self.spark.range(0, 10).toDF('input')
        df = df.withColumn("input", df.input.cast(dataType="double"))

        transformed_df = transformer.transform(df)
        results = transformed_df.select("input", "output").collect()

        for res in results:
            self.assertEqual(res.input + shiftVal, res.output)


class EstimatorTest(unittest.TestCase):

    def testDefaultFitMultiple(self):
        N = 4
        data = MockDataset()
        estimator = MockEstimator()
        params = [{estimator.fake: i} for i in range(N)]
        modelIter = estimator.fitMultiple(data, params)
        indexList = []
        for index, model in modelIter:
            self.assertEqual(model.getFake(), index)
            indexList.append(index)
        self.assertEqual(sorted(indexList), list(range(N)))


if __name__ == "__main__":
    from pyspark.ml.tests import *
    if xmlrunner:
        unittest.main(testRunner=xmlrunner.XMLTestRunner(output='target/test-reports'))
    else:
        unittest.main()
