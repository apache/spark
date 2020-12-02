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

from math import sqrt
import unittest

from numpy import array, abs, tile

from pyspark.mllib.linalg import SparseVector, DenseVector, Vectors
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.feature import HashingTF, IDF, StandardScaler, ElementwiseProduct, Word2Vec
from pyspark.testing.mllibutils import MLlibTestCase


class FeatureTest(MLlibTestCase):
    def test_idf_model(self):
        data = [
            Vectors.dense([1, 2, 6, 0, 2, 3, 1, 1, 0, 0, 3]),
            Vectors.dense([1, 3, 0, 1, 3, 0, 0, 2, 0, 0, 1]),
            Vectors.dense([1, 4, 1, 0, 0, 4, 9, 0, 1, 2, 0]),
            Vectors.dense([2, 1, 0, 3, 0, 0, 5, 0, 2, 3, 9])
        ]
        model = IDF().fit(self.sc.parallelize(data, 2))
        idf = model.idf()
        self.assertEqual(len(idf), 11)


class Word2VecTests(MLlibTestCase):
    def test_word2vec_setters(self):
        model = Word2Vec() \
            .setVectorSize(2) \
            .setLearningRate(0.01) \
            .setNumPartitions(2) \
            .setNumIterations(10) \
            .setSeed(1024) \
            .setMinCount(3) \
            .setWindowSize(6)
        self.assertEqual(model.vectorSize, 2)
        self.assertTrue(model.learningRate < 0.02)
        self.assertEqual(model.numPartitions, 2)
        self.assertEqual(model.numIterations, 10)
        self.assertEqual(model.seed, 1024)
        self.assertEqual(model.minCount, 3)
        self.assertEqual(model.windowSize, 6)

    def test_word2vec_get_vectors(self):
        data = [
            ["a", "b", "c", "d", "e", "f", "g"],
            ["a", "b", "c", "d", "e", "f"],
            ["a", "b", "c", "d", "e"],
            ["a", "b", "c", "d"],
            ["a", "b", "c"],
            ["a", "b"],
            ["a"]
        ]
        model = Word2Vec().fit(self.sc.parallelize(data))
        self.assertEqual(len(model.getVectors()), 3)


class StandardScalerTests(MLlibTestCase):
    def test_model_setters(self):
        data = [
            [1.0, 2.0, 3.0],
            [2.0, 3.0, 4.0],
            [3.0, 4.0, 5.0]
        ]
        model = StandardScaler().fit(self.sc.parallelize(data))
        self.assertIsNotNone(model.setWithMean(True))
        self.assertIsNotNone(model.setWithStd(True))
        self.assertEqual(model.transform([1.0, 2.0, 3.0]), DenseVector([-1.0, -1.0, -1.0]))

    def test_model_transform(self):
        data = [
            [1.0, 2.0, 3.0],
            [2.0, 3.0, 4.0],
            [3.0, 4.0, 5.0]
        ]
        model = StandardScaler().fit(self.sc.parallelize(data))
        self.assertEqual(model.transform([1.0, 2.0, 3.0]), DenseVector([1.0, 2.0, 3.0]))


class ElementwiseProductTests(MLlibTestCase):
    def test_model_transform(self):
        weight = Vectors.dense([3, 2, 1])

        densevec = Vectors.dense([4, 5, 6])
        sparsevec = Vectors.sparse(3, [0], [1])
        eprod = ElementwiseProduct(weight)
        self.assertEqual(eprod.transform(densevec), DenseVector([12, 10, 6]))
        self.assertEqual(
            eprod.transform(sparsevec), SparseVector(3, [0], [3]))


class HashingTFTest(MLlibTestCase):

    def test_binary_term_freqs(self):
        hashingTF = HashingTF(100).setBinary(True)
        doc = "a a b c c c".split(" ")
        n = hashingTF.numFeatures
        output = hashingTF.transform(doc).toArray()
        expected = Vectors.sparse(n, {hashingTF.indexOf("a"): 1.0,
                                      hashingTF.indexOf("b"): 1.0,
                                      hashingTF.indexOf("c"): 1.0}).toArray()
        for i in range(0, n):
            self.assertAlmostEqual(output[i], expected[i], 14, "Error at " + str(i) +
                                   ": expected " + str(expected[i]) + ", got " + str(output[i]))


class DimensionalityReductionTests(MLlibTestCase):

    denseData = [
        Vectors.dense([0.0, 1.0, 2.0]),
        Vectors.dense([3.0, 4.0, 5.0]),
        Vectors.dense([6.0, 7.0, 8.0]),
        Vectors.dense([9.0, 0.0, 1.0])
    ]
    sparseData = [
        Vectors.sparse(3, [(1, 1.0), (2, 2.0)]),
        Vectors.sparse(3, [(0, 3.0), (1, 4.0), (2, 5.0)]),
        Vectors.sparse(3, [(0, 6.0), (1, 7.0), (2, 8.0)]),
        Vectors.sparse(3, [(0, 9.0), (2, 1.0)])
    ]

    def assertEqualUpToSign(self, vecA, vecB):
        eq1 = vecA - vecB
        eq2 = vecA + vecB
        self.assertTrue(sum(abs(eq1)) < 1e-6 or sum(abs(eq2)) < 1e-6)

    def test_svd(self):
        denseMat = RowMatrix(self.sc.parallelize(self.denseData))
        sparseMat = RowMatrix(self.sc.parallelize(self.sparseData))
        m = 4
        n = 3
        for mat in [denseMat, sparseMat]:
            for k in range(1, 4):
                rm = mat.computeSVD(k, computeU=True)
                self.assertEqual(rm.s.size, k)
                self.assertEqual(rm.U.numRows(), m)
                self.assertEqual(rm.U.numCols(), k)
                self.assertEqual(rm.V.numRows, n)
                self.assertEqual(rm.V.numCols, k)

        # Test that U returned is None if computeU is set to False.
        self.assertEqual(mat.computeSVD(1).U, None)

        # Test that low rank matrices cannot have number of singular values
        # greater than a limit.
        rm = RowMatrix(self.sc.parallelize(tile([1, 2, 3], (3, 1))))
        self.assertEqual(rm.computeSVD(3, False, 1e-6).s.size, 1)

    def test_pca(self):
        expected_pcs = array([
            [0.0, 1.0, 0.0],
            [sqrt(2.0) / 2.0, 0.0, sqrt(2.0) / 2.0],
            [sqrt(2.0) / 2.0, 0.0, -sqrt(2.0) / 2.0]
        ])
        n = 3
        denseMat = RowMatrix(self.sc.parallelize(self.denseData))
        sparseMat = RowMatrix(self.sc.parallelize(self.sparseData))
        for mat in [denseMat, sparseMat]:
            for k in range(1, 4):
                pcs = mat.computePrincipalComponents(k)
                self.assertEqual(pcs.numRows, n)
                self.assertEqual(pcs.numCols, k)

                # We can just test the updated principal component for equality.
                self.assertEqualUpToSign(pcs.toArray()[:, k - 1], expected_pcs[:, k - 1])


if __name__ == "__main__":
    from pyspark.mllib.tests.test_feature import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
