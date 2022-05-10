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

import unittest
import array as pyarray

from numpy import arange, array, array_equal, inf, ones, tile, zeros

from pyspark.serializers import CPickleSerializer
from pyspark.ml.linalg import (
    DenseMatrix,
    DenseVector,
    MatrixUDT,
    SparseMatrix,
    SparseVector,
    Vector,
    VectorUDT,
    Vectors,
)
from pyspark.testing.mllibutils import MLlibTestCase
from pyspark.sql import Row
from pyspark.sql.functions import unwrap_udt


class VectorTests(MLlibTestCase):
    def _test_serialize(self, v):
        ser = CPickleSerializer()
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
        self._test_serialize(DenseVector(array([1.0, 2.0, 3.0, 4.0])))
        self._test_serialize(DenseVector(pyarray.array("d", range(10))))
        self._test_serialize(SparseVector(4, {1: 1, 3: 2}))
        self._test_serialize(SparseVector(3, {}))
        self._test_serialize(DenseMatrix(2, 3, range(6)))
        sm1 = SparseMatrix(3, 4, [0, 2, 2, 4, 4], [1, 2, 1, 2], [1.0, 2.0, 4.0, 5.0])
        self._test_serialize(sm1)

    def test_dot(self):
        sv = SparseVector(4, {1: 1, 3: 2})
        dv = DenseVector(array([1.0, 2.0, 3.0, 4.0]))
        lst = DenseVector([1, 2, 3, 4])
        mat = array(
            [[1.0, 2.0, 3.0, 4.0], [1.0, 2.0, 3.0, 4.0], [1.0, 2.0, 3.0, 4.0], [1.0, 2.0, 3.0, 4.0]]
        )
        arr = pyarray.array("d", [0, 1, 2, 3])
        self.assertEqual(10.0, sv.dot(dv))
        self.assertTrue(array_equal(array([3.0, 6.0, 9.0, 12.0]), sv.dot(mat)))
        self.assertEqual(30.0, dv.dot(dv))
        self.assertTrue(array_equal(array([10.0, 20.0, 30.0, 40.0]), dv.dot(mat)))
        self.assertEqual(30.0, lst.dot(dv))
        self.assertTrue(array_equal(array([10.0, 20.0, 30.0, 40.0]), lst.dot(mat)))
        self.assertEqual(7.0, sv.dot(arr))

    def test_squared_distance(self):
        def squared_distance(a, b):
            if isinstance(a, Vector):
                return a.squared_distance(b)
            else:
                return b.squared_distance(a)

        sv = SparseVector(4, {1: 1, 3: 2})
        dv = DenseVector(array([1.0, 2.0, 3.0, 4.0]))
        lst = DenseVector([4, 3, 2, 1])
        lst1 = [4, 3, 2, 1]
        arr = pyarray.array("d", [0, 2, 1, 3])
        narr = array([0, 2, 1, 3])
        self.assertEqual(15.0, squared_distance(sv, dv))
        self.assertEqual(25.0, squared_distance(sv, lst))
        self.assertEqual(20.0, squared_distance(dv, lst))
        self.assertEqual(15.0, squared_distance(dv, sv))
        self.assertEqual(25.0, squared_distance(lst, sv))
        self.assertEqual(20.0, squared_distance(lst, dv))
        self.assertEqual(0.0, squared_distance(sv, sv))
        self.assertEqual(0.0, squared_distance(dv, dv))
        self.assertEqual(0.0, squared_distance(lst, lst))
        self.assertEqual(25.0, squared_distance(sv, lst1))
        self.assertEqual(3.0, squared_distance(sv, arr))
        self.assertEqual(3.0, squared_distance(sv, narr))

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
        dm1 = DenseMatrix(2, 2, [2, 0, 0, 0])
        sm1 = SparseMatrix(2, 2, [0, 2, 3], [0], [2])
        self.assertEqual(v1, v2)
        self.assertEqual(v1, v3)
        self.assertFalse(v2 == v4)
        self.assertFalse(v1 == v5)
        self.assertFalse(v1 == v6)
        # this is done as Dense and Sparse matrices can be semantically
        # equal while still implementing a different __eq__ method
        self.assertEqual(dm1, sm1)
        self.assertEqual(sm1, dm1)

    def test_equals(self):
        indices = [1, 2, 4]
        values = [1.0, 3.0, 2.0]
        self.assertTrue(Vectors._equals(indices, values, list(range(5)), [0.0, 1.0, 3.0, 0.0, 2.0]))
        self.assertFalse(
            Vectors._equals(indices, values, list(range(5)), [0.0, 3.0, 1.0, 0.0, 2.0])
        )
        self.assertFalse(Vectors._equals(indices, values, list(range(5)), [0.0, 3.0, 0.0, 2.0]))
        self.assertFalse(
            Vectors._equals(indices, values, list(range(5)), [0.0, 1.0, 3.0, 2.0, 2.0])
        )

    def test_conversion(self):
        # numpy arrays should be automatically upcast to float64
        # tests for fix of [SPARK-5089]
        v = array([1, 2, 3, 4], dtype="float64")
        dv = DenseVector(v)
        self.assertTrue(dv.array.dtype == "float64")
        v = array([1, 2, 3, 4], dtype="float32")
        dv = DenseVector(v)
        self.assertTrue(dv.array.dtype == "float64")

    def test_sparse_vector_indexing(self):
        sv = SparseVector(5, {1: 1, 3: 2})
        self.assertEqual(sv[0], 0.0)
        self.assertEqual(sv[3], 2.0)
        self.assertEqual(sv[1], 1.0)
        self.assertEqual(sv[2], 0.0)
        self.assertEqual(sv[4], 0.0)
        self.assertEqual(sv[-1], 0.0)
        self.assertEqual(sv[-2], 2.0)
        self.assertEqual(sv[-3], 0.0)
        self.assertEqual(sv[-5], 0.0)
        for ind in [5, -6]:
            self.assertRaises(IndexError, sv.__getitem__, ind)
        for ind in [7.8, "1"]:
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
        self.assertTrue(repr(mat), "DenseMatrix(3, 2, [0.0, 1.0, 4.0, 6.0, 8.0, 10.0], False)")

        mat = DenseMatrix(3, 2, [0, 1, 4, 6, 8, 10], True)
        self.assertTrue(repr(mat), "DenseMatrix(3, 2, [0.0, 1.0, 4.0, 6.0, 8.0, 10.0], False)")

        mat = DenseMatrix(6, 3, zeros(18))
        self.assertTrue(
            repr(mat),
            "DenseMatrix(6, 3, [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ..., \
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], False)",
        )

    def test_repr_sparse_matrix(self):
        sm1t = SparseMatrix(
            3, 4, [0, 2, 3, 5], [0, 1, 2, 0, 2], [3.0, 2.0, 4.0, 9.0, 8.0], isTransposed=True
        )
        self.assertTrue(
            repr(sm1t),
            "SparseMatrix(3, 4, [0, 2, 3, 5], [0, 1, 2, 0, 2], [3.0, 2.0, 4.0, 9.0, 8.0], True)",
        )

        indices = tile(arange(6), 3)
        values = ones(18)
        sm = SparseMatrix(6, 3, [0, 6, 12, 18], indices, values)
        self.assertTrue(
            repr(sm),
            "SparseMatrix(6, 3, [0, 6, 12, 18], \
                [0, 1, 2, 3, 4, 5, 0, 1, ..., 4, 5, 0, 1, 2, 3, 4, 5], \
                [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, ..., \
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0], False)",
        )

        self.assertTrue(
            str(sm),
            "6 X 3 CSCMatrix\n\
            (0,0) 1.0\n(1,0) 1.0\n(2,0) 1.0\n(3,0) 1.0\n(4,0) 1.0\n(5,0) 1.0\n\
            (0,1) 1.0\n(1,1) 1.0\n(2,1) 1.0\n(3,1) 1.0\n(4,1) 1.0\n(5,1) 1.0\n\
            (0,2) 1.0\n(1,2) 1.0\n(2,2) 1.0\n(3,2) 1.0\n..\n..",
        )

        sm = SparseMatrix(1, 18, zeros(19), [], [])
        self.assertTrue(
            repr(sm),
            "SparseMatrix(1, 18, \
                [0, 0, 0, 0, 0, 0, 0, 0, ..., 0, 0, 0, 0, 0, 0, 0, 0], [], [], False)",
        )

    def test_sparse_matrix(self):
        # Test sparse matrix creation.
        sm1 = SparseMatrix(3, 4, [0, 2, 2, 4, 4], [1, 2, 1, 2], [1.0, 2.0, 4.0, 5.0])
        self.assertEqual(sm1.numRows, 3)
        self.assertEqual(sm1.numCols, 4)
        self.assertEqual(sm1.colPtrs.tolist(), [0, 2, 2, 4, 4])
        self.assertEqual(sm1.rowIndices.tolist(), [1, 2, 1, 2])
        self.assertEqual(sm1.values.tolist(), [1.0, 2.0, 4.0, 5.0])
        self.assertTrue(
            repr(sm1),
            "SparseMatrix(3, 4, [0, 2, 2, 4, 4], [1, 2, 1, 2], [1.0, 2.0, 4.0, 5.0], False)",
        )

        # Test indexing
        expected = [[0, 0, 0, 0], [1, 0, 4, 0], [2, 0, 5, 0]]

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
            3, 4, [0, 2, 3, 5], [0, 1, 2, 0, 2], [3.0, 2.0, 4.0, 9.0, 8.0], isTransposed=True
        )
        self.assertEqual(sm1t.numRows, 3)
        self.assertEqual(sm1t.numCols, 4)
        self.assertEqual(sm1t.colPtrs.tolist(), [0, 2, 3, 5])
        self.assertEqual(sm1t.rowIndices.tolist(), [0, 1, 2, 0, 2])
        self.assertEqual(sm1t.values.tolist(), [3.0, 2.0, 4.0, 9.0, 8.0])

        expected = [[3, 2, 0, 0], [0, 0, 4, 0], [9, 0, 8, 0]]

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
        rdd = self.sc.parallelize(
            [Row(label=1.0, features=self.dv1), Row(label=0.0, features=self.sv1)]
        )
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

    def test_unwrap_udt(self):
        df = self.spark.createDataFrame(
            [(Vectors.dense(1.0, 2.0, 3.0),), (Vectors.sparse(3, {1: 1.0, 2: 5.5}),)],
            ["vec"],
        )
        results = df.select(unwrap_udt("vec").alias("v2")).collect()
        unwrapped_vec = Row("type", "size", "indices", "values")
        expected = [
            Row(v2=unwrapped_vec(1, None, None, [1.0, 2.0, 3.0])),
            Row(v2=unwrapped_vec(0, 3, [1, 2], [1.0, 5.5])),
        ]
        self.assertEquals(results, expected)


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
    from pyspark.ml.tests.test_linalg import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
