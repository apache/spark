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
Fuller unit tests for Python MLlib.
"""

import sys
import array as pyarray

from numpy import array, array_equal
from py4j.protocol import Py4JJavaError

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from pyspark.mllib.linalg import Vector, SparseVector, DenseVector, VectorUDT, _convert_to_vector,\
    DenseMatrix, Vectors, Matrices
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.random import RandomRDDs
from pyspark.mllib.stat import Statistics
from pyspark.serializers import PickleSerializer
from pyspark.sql import SQLContext
from pyspark.tests import ReusedPySparkTestCase as PySparkTestCase

_have_scipy = False
try:
    import scipy.sparse
    _have_scipy = True
except:
    # No SciPy, but that's okay, we'll skip those tests
    pass

ser = PickleSerializer()


def _squared_distance(a, b):
    if isinstance(a, Vector):
        return a.squared_distance(b)
    else:
        return b.squared_distance(a)


class VectorTests(PySparkTestCase):

    def _test_serialize(self, v):
        self.assertEqual(v, ser.loads(ser.dumps(v)))
        jvec = self.sc._jvm.SerDe.loads(bytearray(ser.dumps(v)))
        nv = ser.loads(str(self.sc._jvm.SerDe.dumps(jvec)))
        self.assertEqual(v, nv)
        vs = [v] * 100
        jvecs = self.sc._jvm.SerDe.loads(bytearray(ser.dumps(vs)))
        nvs = ser.loads(str(self.sc._jvm.SerDe.dumps(jvecs)))
        self.assertEqual(vs, nvs)

    def test_serialize(self):
        self._test_serialize(DenseVector(range(10)))
        self._test_serialize(DenseVector(array([1., 2., 3., 4.])))
        self._test_serialize(DenseVector(pyarray.array('d', range(10))))
        self._test_serialize(SparseVector(4, {1: 1, 3: 2}))
        self._test_serialize(SparseVector(3, {}))
        self._test_serialize(DenseMatrix(2, 3, range(6)))

    def test_dot(self):
        sv = SparseVector(4, {1: 1, 3: 2})
        dv = DenseVector(array([1., 2., 3., 4.]))
        lst = DenseVector([1, 2, 3, 4])
        mat = array([[1., 2., 3., 4.],
                     [1., 2., 3., 4.],
                     [1., 2., 3., 4.],
                     [1., 2., 3., 4.]])
        self.assertEquals(10.0, sv.dot(dv))
        self.assertTrue(array_equal(array([3., 6., 9., 12.]), sv.dot(mat)))
        self.assertEquals(30.0, dv.dot(dv))
        self.assertTrue(array_equal(array([10., 20., 30., 40.]), dv.dot(mat)))
        self.assertEquals(30.0, lst.dot(dv))
        self.assertTrue(array_equal(array([10., 20., 30., 40.]), lst.dot(mat)))

    def test_squared_distance(self):
        sv = SparseVector(4, {1: 1, 3: 2})
        dv = DenseVector(array([1., 2., 3., 4.]))
        lst = DenseVector([4, 3, 2, 1])
        self.assertEquals(15.0, _squared_distance(sv, dv))
        self.assertEquals(25.0, _squared_distance(sv, lst))
        self.assertEquals(20.0, _squared_distance(dv, lst))
        self.assertEquals(15.0, _squared_distance(dv, sv))
        self.assertEquals(25.0, _squared_distance(lst, sv))
        self.assertEquals(20.0, _squared_distance(lst, dv))
        self.assertEquals(0.0, _squared_distance(sv, sv))
        self.assertEquals(0.0, _squared_distance(dv, dv))
        self.assertEquals(0.0, _squared_distance(lst, lst))

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
        sv = SparseVector(4, {1: 1, 3: 2})
        self.assertEquals(sv[0], 0.)
        self.assertEquals(sv[3], 2.)
        self.assertEquals(sv[1], 1.)
        self.assertEquals(sv[2], 0.)
        self.assertEquals(sv[-1], 2)
        self.assertEquals(sv[-2], 0)
        self.assertEquals(sv[-4], 0)
        for ind in [4, -5, 7.8]:
            self.assertRaises(ValueError, sv.__getitem__, ind)


class ListTests(PySparkTestCase):

    """
    Test MLlib algorithms on plain lists, to make sure they're passed through
    as NumPy arrays.
    """

    def test_kmeans(self):
        from pyspark.mllib.clustering import KMeans
        data = [
            [0, 1.1],
            [0, 1.2],
            [1.1, 0],
            [1.2, 0],
        ]
        clusters = KMeans.train(self.sc.parallelize(data), 2, initializationMode="k-means||")
        self.assertEquals(clusters.predict(data[0]), clusters.predict(data[1]))
        self.assertEquals(clusters.predict(data[2]), clusters.predict(data[3]))

    def test_kmeans_deterministic(self):
        from pyspark.mllib.clustering import KMeans
        X = range(0, 100, 10)
        Y = range(0, 100, 10)
        data = [[x, y] for x, y in zip(X, Y)]
        clusters1 = KMeans.train(self.sc.parallelize(data),
                                 3, initializationMode="k-means||", seed=42)
        clusters2 = KMeans.train(self.sc.parallelize(data),
                                 3, initializationMode="k-means||", seed=42)
        centers1 = clusters1.centers
        centers2 = clusters2.centers
        for c1, c2 in zip(centers1, centers2):
            # TODO: Allow small numeric difference.
            self.assertTrue(array_equal(c1, c2))

    def test_classification(self):
        from pyspark.mllib.classification import LogisticRegressionWithSGD, SVMWithSGD, NaiveBayes
        from pyspark.mllib.tree import DecisionTree, RandomForest, GradientBoostedTrees
        data = [
            LabeledPoint(0.0, [1, 0, 0]),
            LabeledPoint(1.0, [0, 1, 1]),
            LabeledPoint(0.0, [2, 0, 0]),
            LabeledPoint(1.0, [0, 2, 1])
        ]
        rdd = self.sc.parallelize(data)
        features = [p.features.tolist() for p in data]

        lr_model = LogisticRegressionWithSGD.train(rdd)
        self.assertTrue(lr_model.predict(features[0]) <= 0)
        self.assertTrue(lr_model.predict(features[1]) > 0)
        self.assertTrue(lr_model.predict(features[2]) <= 0)
        self.assertTrue(lr_model.predict(features[3]) > 0)

        svm_model = SVMWithSGD.train(rdd)
        self.assertTrue(svm_model.predict(features[0]) <= 0)
        self.assertTrue(svm_model.predict(features[1]) > 0)
        self.assertTrue(svm_model.predict(features[2]) <= 0)
        self.assertTrue(svm_model.predict(features[3]) > 0)

        nb_model = NaiveBayes.train(rdd)
        self.assertTrue(nb_model.predict(features[0]) <= 0)
        self.assertTrue(nb_model.predict(features[1]) > 0)
        self.assertTrue(nb_model.predict(features[2]) <= 0)
        self.assertTrue(nb_model.predict(features[3]) > 0)

        categoricalFeaturesInfo = {0: 3}  # feature 0 has 3 categories
        dt_model = DecisionTree.trainClassifier(
            rdd, numClasses=2, categoricalFeaturesInfo=categoricalFeaturesInfo)
        self.assertTrue(dt_model.predict(features[0]) <= 0)
        self.assertTrue(dt_model.predict(features[1]) > 0)
        self.assertTrue(dt_model.predict(features[2]) <= 0)
        self.assertTrue(dt_model.predict(features[3]) > 0)

        rf_model = RandomForest.trainClassifier(
            rdd, numClasses=2, categoricalFeaturesInfo=categoricalFeaturesInfo, numTrees=100)
        self.assertTrue(rf_model.predict(features[0]) <= 0)
        self.assertTrue(rf_model.predict(features[1]) > 0)
        self.assertTrue(rf_model.predict(features[2]) <= 0)
        self.assertTrue(rf_model.predict(features[3]) > 0)

        gbt_model = GradientBoostedTrees.trainClassifier(
            rdd, categoricalFeaturesInfo=categoricalFeaturesInfo)
        self.assertTrue(gbt_model.predict(features[0]) <= 0)
        self.assertTrue(gbt_model.predict(features[1]) > 0)
        self.assertTrue(gbt_model.predict(features[2]) <= 0)
        self.assertTrue(gbt_model.predict(features[3]) > 0)

    def test_regression(self):
        from pyspark.mllib.regression import LinearRegressionWithSGD, LassoWithSGD, \
            RidgeRegressionWithSGD
        from pyspark.mllib.tree import DecisionTree, RandomForest, GradientBoostedTrees
        data = [
            LabeledPoint(-1.0, [0, -1]),
            LabeledPoint(1.0, [0, 1]),
            LabeledPoint(-1.0, [0, -2]),
            LabeledPoint(1.0, [0, 2])
        ]
        rdd = self.sc.parallelize(data)
        features = [p.features.tolist() for p in data]

        lr_model = LinearRegressionWithSGD.train(rdd)
        self.assertTrue(lr_model.predict(features[0]) <= 0)
        self.assertTrue(lr_model.predict(features[1]) > 0)
        self.assertTrue(lr_model.predict(features[2]) <= 0)
        self.assertTrue(lr_model.predict(features[3]) > 0)

        lasso_model = LassoWithSGD.train(rdd)
        self.assertTrue(lasso_model.predict(features[0]) <= 0)
        self.assertTrue(lasso_model.predict(features[1]) > 0)
        self.assertTrue(lasso_model.predict(features[2]) <= 0)
        self.assertTrue(lasso_model.predict(features[3]) > 0)

        rr_model = RidgeRegressionWithSGD.train(rdd)
        self.assertTrue(rr_model.predict(features[0]) <= 0)
        self.assertTrue(rr_model.predict(features[1]) > 0)
        self.assertTrue(rr_model.predict(features[2]) <= 0)
        self.assertTrue(rr_model.predict(features[3]) > 0)

        categoricalFeaturesInfo = {0: 2}  # feature 0 has 2 categories
        dt_model = DecisionTree.trainRegressor(
            rdd, categoricalFeaturesInfo=categoricalFeaturesInfo)
        self.assertTrue(dt_model.predict(features[0]) <= 0)
        self.assertTrue(dt_model.predict(features[1]) > 0)
        self.assertTrue(dt_model.predict(features[2]) <= 0)
        self.assertTrue(dt_model.predict(features[3]) > 0)

        rf_model = RandomForest.trainRegressor(
            rdd, categoricalFeaturesInfo=categoricalFeaturesInfo, numTrees=100)
        self.assertTrue(rf_model.predict(features[0]) <= 0)
        self.assertTrue(rf_model.predict(features[1]) > 0)
        self.assertTrue(rf_model.predict(features[2]) <= 0)
        self.assertTrue(rf_model.predict(features[3]) > 0)

        gbt_model = GradientBoostedTrees.trainRegressor(
            rdd, categoricalFeaturesInfo=categoricalFeaturesInfo)
        self.assertTrue(gbt_model.predict(features[0]) <= 0)
        self.assertTrue(gbt_model.predict(features[1]) > 0)
        self.assertTrue(gbt_model.predict(features[2]) <= 0)
        self.assertTrue(gbt_model.predict(features[3]) > 0)


class StatTests(PySparkTestCase):
    # SPARK-4023
    def test_col_with_different_rdds(self):
        # numpy
        data = RandomRDDs.normalVectorRDD(self.sc, 1000, 10, 10)
        summary = Statistics.colStats(data)
        self.assertEqual(1000, summary.count())
        # array
        data = self.sc.parallelize([range(10)] * 10)
        summary = Statistics.colStats(data)
        self.assertEqual(10, summary.count())
        # array
        data = self.sc.parallelize([pyarray.array("d", range(10))] * 10)
        summary = Statistics.colStats(data)
        self.assertEqual(10, summary.count())


class VectorUDTTests(PySparkTestCase):

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
        sqlCtx = SQLContext(self.sc)
        rdd = self.sc.parallelize([LabeledPoint(1.0, self.dv1), LabeledPoint(0.0, self.sv1)])
        srdd = sqlCtx.inferSchema(rdd)
        schema = srdd.schema()
        field = [f for f in schema.fields if f.name == "features"][0]
        self.assertEqual(field.dataType, self.udt)
        vectors = srdd.map(lambda p: p.features).collect()
        self.assertEqual(len(vectors), 2)
        for v in vectors:
            if isinstance(v, SparseVector):
                self.assertEqual(v, self.sv1)
            elif isinstance(v, DenseVector):
                self.assertEqual(v, self.dv1)
            else:
                raise ValueError("expecting a vector but got %r of type %r" % (v, type(v)))


@unittest.skipIf(not _have_scipy, "SciPy not installed")
class SciPyTests(PySparkTestCase):

    """
    Test both vector operations and MLlib algorithms with SciPy sparse matrices,
    if SciPy is available.
    """

    def test_serialize(self):
        from scipy.sparse import lil_matrix
        lil = lil_matrix((4, 1))
        lil[1, 0] = 1
        lil[3, 0] = 2
        sv = SparseVector(4, {1: 1, 3: 2})
        self.assertEquals(sv, _convert_to_vector(lil))
        self.assertEquals(sv, _convert_to_vector(lil.tocsc()))
        self.assertEquals(sv, _convert_to_vector(lil.tocoo()))
        self.assertEquals(sv, _convert_to_vector(lil.tocsr()))
        self.assertEquals(sv, _convert_to_vector(lil.todok()))

        def serialize(l):
            return ser.loads(ser.dumps(_convert_to_vector(l)))
        self.assertEquals(sv, serialize(lil))
        self.assertEquals(sv, serialize(lil.tocsc()))
        self.assertEquals(sv, serialize(lil.tocsr()))
        self.assertEquals(sv, serialize(lil.todok()))

    def test_dot(self):
        from scipy.sparse import lil_matrix
        lil = lil_matrix((4, 1))
        lil[1, 0] = 1
        lil[3, 0] = 2
        dv = DenseVector(array([1., 2., 3., 4.]))
        self.assertEquals(10.0, dv.dot(lil))

    def test_squared_distance(self):
        from scipy.sparse import lil_matrix
        lil = lil_matrix((4, 1))
        lil[1, 0] = 3
        lil[3, 0] = 2
        dv = DenseVector(array([1., 2., 3., 4.]))
        sv = SparseVector(4, {0: 1, 1: 2, 2: 3, 3: 4})
        self.assertEquals(15.0, dv.squared_distance(lil))
        self.assertEquals(15.0, sv.squared_distance(lil))

    def scipy_matrix(self, size, values):
        """Create a column SciPy matrix from a dictionary of values"""
        from scipy.sparse import lil_matrix
        lil = lil_matrix((size, 1))
        for key, value in values.items():
            lil[key, 0] = value
        return lil

    def test_clustering(self):
        from pyspark.mllib.clustering import KMeans
        data = [
            self.scipy_matrix(3, {1: 1.0}),
            self.scipy_matrix(3, {1: 1.1}),
            self.scipy_matrix(3, {2: 1.0}),
            self.scipy_matrix(3, {2: 1.1})
        ]
        clusters = KMeans.train(self.sc.parallelize(data), 2, initializationMode="k-means||")
        self.assertEquals(clusters.predict(data[0]), clusters.predict(data[1]))
        self.assertEquals(clusters.predict(data[2]), clusters.predict(data[3]))

    def test_classification(self):
        from pyspark.mllib.classification import LogisticRegressionWithSGD, SVMWithSGD, NaiveBayes
        from pyspark.mllib.tree import DecisionTree
        data = [
            LabeledPoint(0.0, self.scipy_matrix(2, {0: 1.0})),
            LabeledPoint(1.0, self.scipy_matrix(2, {1: 1.0})),
            LabeledPoint(0.0, self.scipy_matrix(2, {0: 2.0})),
            LabeledPoint(1.0, self.scipy_matrix(2, {1: 2.0}))
        ]
        rdd = self.sc.parallelize(data)
        features = [p.features for p in data]

        lr_model = LogisticRegressionWithSGD.train(rdd)
        self.assertTrue(lr_model.predict(features[0]) <= 0)
        self.assertTrue(lr_model.predict(features[1]) > 0)
        self.assertTrue(lr_model.predict(features[2]) <= 0)
        self.assertTrue(lr_model.predict(features[3]) > 0)

        svm_model = SVMWithSGD.train(rdd)
        self.assertTrue(svm_model.predict(features[0]) <= 0)
        self.assertTrue(svm_model.predict(features[1]) > 0)
        self.assertTrue(svm_model.predict(features[2]) <= 0)
        self.assertTrue(svm_model.predict(features[3]) > 0)

        nb_model = NaiveBayes.train(rdd)
        self.assertTrue(nb_model.predict(features[0]) <= 0)
        self.assertTrue(nb_model.predict(features[1]) > 0)
        self.assertTrue(nb_model.predict(features[2]) <= 0)
        self.assertTrue(nb_model.predict(features[3]) > 0)

        categoricalFeaturesInfo = {0: 3}  # feature 0 has 3 categories
        dt_model = DecisionTree.trainClassifier(rdd, numClasses=2,
                                                categoricalFeaturesInfo=categoricalFeaturesInfo)
        self.assertTrue(dt_model.predict(features[0]) <= 0)
        self.assertTrue(dt_model.predict(features[1]) > 0)
        self.assertTrue(dt_model.predict(features[2]) <= 0)
        self.assertTrue(dt_model.predict(features[3]) > 0)

    def test_regression(self):
        from pyspark.mllib.regression import LinearRegressionWithSGD, LassoWithSGD, \
            RidgeRegressionWithSGD
        from pyspark.mllib.tree import DecisionTree
        data = [
            LabeledPoint(-1.0, self.scipy_matrix(2, {1: -1.0})),
            LabeledPoint(1.0, self.scipy_matrix(2, {1: 1.0})),
            LabeledPoint(-1.0, self.scipy_matrix(2, {1: -2.0})),
            LabeledPoint(1.0, self.scipy_matrix(2, {1: 2.0}))
        ]
        rdd = self.sc.parallelize(data)
        features = [p.features for p in data]

        lr_model = LinearRegressionWithSGD.train(rdd)
        self.assertTrue(lr_model.predict(features[0]) <= 0)
        self.assertTrue(lr_model.predict(features[1]) > 0)
        self.assertTrue(lr_model.predict(features[2]) <= 0)
        self.assertTrue(lr_model.predict(features[3]) > 0)

        lasso_model = LassoWithSGD.train(rdd)
        self.assertTrue(lasso_model.predict(features[0]) <= 0)
        self.assertTrue(lasso_model.predict(features[1]) > 0)
        self.assertTrue(lasso_model.predict(features[2]) <= 0)
        self.assertTrue(lasso_model.predict(features[3]) > 0)

        rr_model = RidgeRegressionWithSGD.train(rdd)
        self.assertTrue(rr_model.predict(features[0]) <= 0)
        self.assertTrue(rr_model.predict(features[1]) > 0)
        self.assertTrue(rr_model.predict(features[2]) <= 0)
        self.assertTrue(rr_model.predict(features[3]) > 0)

        categoricalFeaturesInfo = {0: 2}  # feature 0 has 2 categories
        dt_model = DecisionTree.trainRegressor(rdd, categoricalFeaturesInfo=categoricalFeaturesInfo)
        self.assertTrue(dt_model.predict(features[0]) <= 0)
        self.assertTrue(dt_model.predict(features[1]) > 0)
        self.assertTrue(dt_model.predict(features[2]) <= 0)
        self.assertTrue(dt_model.predict(features[3]) > 0)


class ChiSqTestTests(PySparkTestCase):
    def test_goodness_of_fit(self):
        from numpy import inf

        observed = Vectors.dense([4, 6, 5])
        pearson = Statistics.chiSqTest(observed)

        # Validated against the R command `chisq.test(c(4, 6, 5), p=c(1/3, 1/3, 1/3))`
        self.assertEqual(pearson.statistic, 0.4)
        self.assertEqual(pearson.degreesOfFreedom, 2)
        self.assertAlmostEqual(pearson.pValue, 0.8187, 4)

        # Different expected and observed sum
        observed1 = Vectors.dense([21, 38, 43, 80])
        expected1 = Vectors.dense([3, 5, 7, 20])
        pearson1 = Statistics.chiSqTest(observed1, expected1)

        # Results validated against the R command
        # `chisq.test(c(21, 38, 43, 80), p=c(3/35, 1/7, 1/5, 4/7))`
        self.assertAlmostEqual(pearson1.statistic, 14.1429, 4)
        self.assertEqual(pearson1.degreesOfFreedom, 3)
        self.assertAlmostEqual(pearson1.pValue, 0.002717, 4)

        # Vectors with different sizes
        observed3 = Vectors.dense([1.0, 2.0, 3.0])
        expected3 = Vectors.dense([1.0, 2.0, 3.0, 4.0])
        self.assertRaises(ValueError, Statistics.chiSqTest, observed3, expected3)

        # Negative counts in observed
        neg_obs = Vectors.dense([1.0, 2.0, 3.0, -4.0])
        self.assertRaises(Py4JJavaError, Statistics.chiSqTest, neg_obs, expected1)

        # Count = 0.0 in expected but not observed
        zero_expected = Vectors.dense([1.0, 0.0, 3.0])
        pearson_inf = Statistics.chiSqTest(observed, zero_expected)
        self.assertEqual(pearson_inf.statistic, inf)
        self.assertEqual(pearson_inf.degreesOfFreedom, 2)
        self.assertEqual(pearson_inf.pValue, 0.0)

        # 0.0 in expected and observed simultaneously
        zero_observed = Vectors.dense([2.0, 0.0, 1.0])
        self.assertRaises(Py4JJavaError, Statistics.chiSqTest, zero_observed, zero_expected)

    def test_matrix_independence(self):
        data = [40.0, 24.0, 29.0, 56.0, 32.0, 42.0, 31.0, 10.0, 0.0, 30.0, 15.0, 12.0]
        chi = Statistics.chiSqTest(Matrices.dense(3, 4, data))

        # Results validated against R command
        # `chisq.test(rbind(c(40, 56, 31, 30),c(24, 32, 10, 15), c(29, 42, 0, 12)))`
        self.assertAlmostEqual(chi.statistic, 21.9958, 4)
        self.assertEqual(chi.degreesOfFreedom, 6)
        self.assertAlmostEqual(chi.pValue, 0.001213, 4)

        # Negative counts
        neg_counts = Matrices.dense(2, 2, [4.0, 5.0, 3.0, -3.0])
        self.assertRaises(Py4JJavaError, Statistics.chiSqTest, neg_counts)

        # Row sum = 0.0
        row_zero = Matrices.dense(2, 2, [0.0, 1.0, 0.0, 2.0])
        self.assertRaises(Py4JJavaError, Statistics.chiSqTest, row_zero)

        # Column sum = 0.0
        col_zero = Matrices.dense(2, 2, [0.0, 0.0, 2.0, 2.0])
        self.assertRaises(Py4JJavaError, Statistics.chiSqTest, col_zero)

    def test_chi_sq_pearson(self):
        data = [
            LabeledPoint(0.0, Vectors.dense([0.5, 10.0])),
            LabeledPoint(0.0, Vectors.dense([1.5, 20.0])),
            LabeledPoint(1.0, Vectors.dense([1.5, 30.0])),
            LabeledPoint(0.0, Vectors.dense([3.5, 30.0])),
            LabeledPoint(0.0, Vectors.dense([3.5, 40.0])),
            LabeledPoint(1.0, Vectors.dense([3.5, 40.0]))
        ]

        for numParts in [2, 4, 6, 8]:
            chi = Statistics.chiSqTest(self.sc.parallelize(data, numParts))
            feature1 = chi[0]
            self.assertEqual(feature1.statistic, 0.75)
            self.assertEqual(feature1.degreesOfFreedom, 2)
            self.assertAlmostEqual(feature1.pValue, 0.6873, 4)

            feature2 = chi[1]
            self.assertEqual(feature2.statistic, 1.5)
            self.assertEqual(feature2.degreesOfFreedom, 3)
            self.assertAlmostEqual(feature2.pValue, 0.6823, 4)

    def test_right_number_of_results(self):
        num_cols = 1001
        sparse_data = [
            LabeledPoint(0.0, Vectors.sparse(num_cols, [(100, 2.0)])),
            LabeledPoint(0.1, Vectors.sparse(num_cols, [(200, 1.0)]))
        ]
        chi = Statistics.chiSqTest(self.sc.parallelize(sparse_data))
        self.assertEqual(len(chi), num_cols)
        self.assertIsNotNone(chi[1000])

if __name__ == "__main__":
    if not _have_scipy:
        print "NOTE: Skipping SciPy tests as it does not seem to be installed"
    unittest.main()
    if not _have_scipy:
        print "NOTE: SciPy tests were skipped as it does not seem to be installed"
