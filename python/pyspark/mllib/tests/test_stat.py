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

import array as pyarray
import unittest

from numpy import array

from pyspark.mllib.linalg import Vector, SparseVector, DenseVector, VectorUDT, _convert_to_vector, \
    DenseMatrix, SparseMatrix, Vectors, Matrices, MatrixUDT
from pyspark.mllib.random import RandomRDDs
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat import Statistics
from pyspark.sql.utils import IllegalArgumentException
from pyspark.testing.mllibutils import MLlibTestCase


class StatTests(MLlibTestCase):
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

    def test_col_norms(self):
        data = RandomRDDs.normalVectorRDD(self.sc, 1000, 10, 10)
        summary = Statistics.colStats(data)
        self.assertEqual(10, len(summary.normL1()))
        self.assertEqual(10, len(summary.normL2()))

        data2 = self.sc.parallelize(range(10)).map(lambda x: Vectors.dense(x))
        summary2 = Statistics.colStats(data2)
        self.assertEqual(array([45.0]), summary2.normL1())
        import math
        expectedNormL2 = math.sqrt(sum(map(lambda x: x*x, range(10))))
        self.assertTrue(math.fabs(summary2.normL2()[0] - expectedNormL2) < 1e-14)


class ChiSqTestTests(MLlibTestCase):
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
        self.assertRaises(IllegalArgumentException, Statistics.chiSqTest, neg_obs, expected1)

        # Count = 0.0 in expected but not observed
        zero_expected = Vectors.dense([1.0, 0.0, 3.0])
        pearson_inf = Statistics.chiSqTest(observed, zero_expected)
        self.assertEqual(pearson_inf.statistic, inf)
        self.assertEqual(pearson_inf.degreesOfFreedom, 2)
        self.assertEqual(pearson_inf.pValue, 0.0)

        # 0.0 in expected and observed simultaneously
        zero_observed = Vectors.dense([2.0, 0.0, 1.0])
        self.assertRaises(
            IllegalArgumentException, Statistics.chiSqTest, zero_observed, zero_expected)

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
        self.assertRaises(IllegalArgumentException, Statistics.chiSqTest, neg_counts)

        # Row sum = 0.0
        row_zero = Matrices.dense(2, 2, [0.0, 1.0, 0.0, 2.0])
        self.assertRaises(IllegalArgumentException, Statistics.chiSqTest, row_zero)

        # Column sum = 0.0
        col_zero = Matrices.dense(2, 2, [0.0, 0.0, 2.0, 2.0])
        self.assertRaises(IllegalArgumentException, Statistics.chiSqTest, col_zero)

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


class KolmogorovSmirnovTest(MLlibTestCase):

    def test_R_implementation_equivalence(self):
        data = self.sc.parallelize([
            1.1626852897838, -0.585924465893051, 1.78546500331661, -1.33259371048501,
            -0.446566766553219, 0.569606122374976, -2.88971761441412, -0.869018343326555,
            -0.461702683149641, -0.555540910137444, -0.0201353678515895, -0.150382224136063,
            -0.628126755843964, 1.32322085193283, -1.52135057001199, -0.437427868856691,
            0.970577579543399, 0.0282226444247749, -0.0857821886527593, 0.389214404984942
        ])
        model = Statistics.kolmogorovSmirnovTest(data, "norm")
        self.assertAlmostEqual(model.statistic, 0.189, 3)
        self.assertAlmostEqual(model.pValue, 0.422, 3)

        model = Statistics.kolmogorovSmirnovTest(data, "norm", 0, 1)
        self.assertAlmostEqual(model.statistic, 0.189, 3)
        self.assertAlmostEqual(model.pValue, 0.422, 3)


if __name__ == "__main__":
    from pyspark.mllib.tests.test_stat import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
