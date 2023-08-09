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

import inspect
import array as pyarray
import unittest

import numpy as np

from pyspark import keyword_only
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import (
    Binarizer,
    Bucketizer,
    ElementwiseProduct,
    IndexToString,
    MaxAbsScaler,
    VectorSlicer,
    Word2Vec,
)
from pyspark.ml.linalg import DenseVector, SparseVector, Vectors
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasInputCol, HasMaxIter, HasSeed
from pyspark.ml.regression import LinearRegressionModel, GeneralizedLinearRegressionModel
from pyspark.ml.wrapper import JavaParams
from pyspark.testing.mlutils import check_params, PySparkTestCase, SparkSessionTestCase


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
        lst = [0, 1]
        for lst_like in [
            lst,
            np.array(lst),
            DenseVector(lst),
            SparseVector(len(lst), range(len(lst)), lst),
            pyarray.array("l", lst),
            range(2),
            tuple(lst),
        ]:
            converted = TypeConverters.toList(lst_like)
            self.assertEqual(type(converted), list)
            self.assertListEqual(converted, lst)

    def test_list_int(self):
        for indices in [
            [1.0, 2.0],
            np.array([1.0, 2.0]),
            DenseVector([1.0, 2.0]),
            SparseVector(2, {0: 1.0, 1: 2.0}),
            range(1, 3),
            (1.0, 2.0),
            pyarray.array("d", [1.0, 2.0]),
        ]:
            vs = VectorSlicer(indices=indices)
            self.assertListEqual(vs.getIndices(), [1, 2])
            self.assertTrue(all([type(v) == int for v in vs.getIndices()]))
        self.assertRaises(TypeError, lambda: VectorSlicer(indices=["a", "b"]))

    def test_list_float(self):
        b = Bucketizer(splits=[1, 4])
        self.assertEqual(b.getSplits(), [1.0, 4.0])
        self.assertTrue(all([type(v) == float for v in b.getSplits()]))
        self.assertRaises(TypeError, lambda: Bucketizer(splits=["a", 1.0]))

    def test_list_list_float(self):
        b = Bucketizer(splitsArray=[[-0.1, 0.5, 3], [-5, 1.5]])
        self.assertEqual(b.getSplitsArray(), [[-0.1, 0.5, 3.0], [-5.0, 1.5]])
        self.assertTrue(all([type(v) == list for v in b.getSplitsArray()]))
        self.assertTrue(all([type(v) == float for v in b.getSplitsArray()[0]]))
        self.assertTrue(all([type(v) == float for v in b.getSplitsArray()[1]]))
        self.assertRaises(TypeError, lambda: Bucketizer(splitsArray=["a", 1.0]))
        self.assertRaises(TypeError, lambda: Bucketizer(splitsArray=[[-5, 1.5], ["a", 1.0]]))

    def test_list_string(self):
        for labels in [np.array(["a", "b"]), ["a", "b"], np.array(["a", "b"])]:
            idx_to_string = IndexToString(labels=labels)
            self.assertListEqual(idx_to_string.getLabels(), ["a", "b"])
        self.assertRaises(TypeError, lambda: IndexToString(labels=["a", 2]))

    def test_string(self):
        lr = LogisticRegression()
        for col in ["features", "features", np.str_("features")]:
            lr.setFeaturesCol(col)
            self.assertEqual(lr.getFeaturesCol(), "features")
        self.assertRaises(TypeError, lambda: LogisticRegression(featuresCol=2.3))

    def test_bool(self):
        self.assertRaises(TypeError, lambda: LogisticRegression(fitIntercept=1))
        self.assertRaises(TypeError, lambda: LogisticRegression(fitIntercept="false"))


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


class ParamTests(SparkSessionTestCase):
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
        self.assertTrue(testParams.hasParam("maxIter"))

    def test_resolveparam(self):
        testParams = TestParams()
        self.assertEqual(testParams._resolveParam(testParams.maxIter), testParams.maxIter)
        self.assertEqual(testParams._resolveParam("maxIter"), testParams.maxIter)

        self.assertEqual(testParams._resolveParam("maxIter"), testParams.maxIter)
        self.assertRaises(AttributeError, lambda: testParams._resolveParam("아"))

        # Invalid type
        invalid_type = 1
        self.assertRaises(TypeError, testParams._resolveParam, invalid_type)

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

        self.assertTrue(testParams.hasParam(inputCol.name))
        self.assertFalse(testParams.hasDefault(inputCol))
        self.assertFalse(testParams.isSet(inputCol))
        self.assertFalse(testParams.isDefined(inputCol))
        with self.assertRaises(KeyError):
            testParams.getInputCol()

        otherParam = Param(
            Params._dummy(),
            "otherParam",
            "Parameter used to test that " + "set raises an error for a non-member parameter.",
            typeConverter=TypeConverters.toString,
        )
        with self.assertRaises(ValueError):
            testParams.set(otherParam, "value")

        # Since the default is normally random, set it to a known number for debug str
        testParams._setDefault(seed=41)

        self.assertEqual(
            testParams.explainParams(),
            "\n".join(
                [
                    "inputCol: input column name. (undefined)",
                    "maxIter: max number of iterations (>= 0). (default: 10)",
                    "seed: random seed. (default: 41)",
                ]
            ),
        )

    def test_clear_param(self):
        df = self.spark.createDataFrame([(Vectors.dense([1.0]),), (Vectors.dense([2.0]),)], ["a"])
        maScaler = MaxAbsScaler(inputCol="a", outputCol="scaled")
        model = maScaler.fit(df)
        self.assertTrue(model.isSet(model.outputCol))
        self.assertEqual(model.getOutputCol(), "scaled")
        model.clear(model.outputCol)
        self.assertFalse(model.isSet(model.outputCol))
        self.assertEqual(model.getOutputCol()[:12], "MaxAbsScaler")
        output = model.transform(df)
        self.assertEqual(model.getOutputCol(), output.schema.names[1])

    def test_kmeans_param(self):
        algo = KMeans()
        self.assertEqual(algo.getInitMode(), "k-means||")
        algo.setK(10)
        self.assertEqual(algo.getK(), 10)
        algo.setInitSteps(10)
        self.assertEqual(algo.getInitSteps(), 10)
        self.assertEqual(algo.getDistanceMeasure(), "euclidean")
        algo.setDistanceMeasure("cosine")
        self.assertEqual(algo.getDistanceMeasure(), "cosine")

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
        with self.assertRaises(TypeError):
            tp.copy(extra={"unknown_parameter": None})
        with self.assertRaises(TypeError):
            tp.copy(extra=["must be a dict"])

    def test_logistic_regression_check_thresholds(self):
        self.assertIsInstance(
            LogisticRegression(threshold=0.5, thresholds=[0.5, 0.5]), LogisticRegression
        )

        self.assertRaisesRegex(
            ValueError,
            "Logistic Regression getThreshold found inconsistent.*$",
            LogisticRegression,
            threshold=0.42,
            thresholds=[0.5, 0.5],
        )

    def test_preserve_set_state(self):
        dataset = self.spark.createDataFrame([(0.5,)], ["data"])
        binarizer = Binarizer(inputCol="data")
        self.assertFalse(binarizer.isSet("threshold"))
        binarizer.transform(dataset)
        binarizer._transfer_params_from_java()
        self.assertFalse(
            binarizer.isSet("threshold"),
            "Params not explicitly set should remain unset after transform",
        )

    def test_default_params_transferred(self):
        dataset = self.spark.createDataFrame([(0.5,)], ["data"])
        binarizer = Binarizer(inputCol="data")
        # intentionally change the pyspark default, but don't set it
        binarizer._defaultParamMap[binarizer.outputCol] = "my_default"
        result = binarizer.transform(dataset).select("my_default").collect()
        self.assertFalse(binarizer.isSet(binarizer.outputCol))
        self.assertEqual(result[0][0], 1.0)

    def test_lr_evaluate_invaild_type(self):
        lr = LinearRegressionModel()
        invalid_type = ""
        self.assertRaises(TypeError, lr.evaluate, invalid_type)

    def test_glr_evaluate_invaild_type(self):
        glr = GeneralizedLinearRegressionModel()
        invalid_type = ""
        self.assertRaises(TypeError, glr.evaluate, invalid_type)


class DefaultValuesTests(PySparkTestCase):
    """
    Test :py:class:`JavaParams` classes to see if their default Param values match
    those in their Scala counterparts.
    """

    def test_java_params(self):
        import re

        import pyspark.ml.feature
        import pyspark.ml.classification
        import pyspark.ml.clustering
        import pyspark.ml.evaluation
        import pyspark.ml.pipeline
        import pyspark.ml.recommendation
        import pyspark.ml.regression

        modules = [
            pyspark.ml.feature,
            pyspark.ml.classification,
            pyspark.ml.clustering,
            pyspark.ml.evaluation,
            pyspark.ml.pipeline,
            pyspark.ml.recommendation,
            pyspark.ml.regression,
        ]
        for module in modules:
            for name, cls in inspect.getmembers(module, inspect.isclass):
                if (
                    not name.endswith("Model")
                    and not name.endswith("Params")
                    and issubclass(cls, JavaParams)
                    and not inspect.isabstract(cls)
                    and not re.match("_?Java", name)
                    and name != "_LSH"
                    and name != "_Selector"
                ):
                    check_params(self, cls(), check_params_exist=True)

        # Additional classes that need explicit construction
        from pyspark.ml.feature import CountVectorizerModel, StringIndexerModel

        check_params(
            self, CountVectorizerModel.from_vocabulary(["a"], "input"), check_params_exist=True
        )
        check_params(
            self, StringIndexerModel.from_labels(["a", "b"], "input"), check_params_exist=True
        )


if __name__ == "__main__":
    from pyspark.ml.tests.test_param import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
