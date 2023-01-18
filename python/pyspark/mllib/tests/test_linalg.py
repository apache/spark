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

from numpy import array, array_equal, zeros, arange, tile, ones, inf

import pyspark.ml.linalg as newlinalg
from pyspark.serializers import CPickleSerializer
from pyspark.mllib.linalg import (
    Vector,
    SparseVector,
    DenseVector,
    VectorUDT,
    _convert_to_vector,
    DenseMatrix,
    SparseMatrix,
    Vectors,
    Matrices,
    MatrixUDT,
)
from pyspark.mllib.linalg.distributed import RowMatrix, IndexedRowMatrix, IndexedRow
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import Row
from pyspark.testing.mllibutils import MLlibTestCase
from pyspark.testing.utils import have_scipy


class VectorTests(MLlibTestCase):
    def _test_serialize(self, v):
        ser = CPickleSerializer()
        self.assertEqual(v, ser.loads(ser.dumps(v)))
        jvec = self.sc._jvm.org.apache.spark.mllib.api.python.SerDe.loads(bytearray(ser.dumps(v)))
        nv = ser.loads(bytes(self.sc._jvm.org.apache.spark.mllib.api.python.SerDe.dumps(jvec)))
        self.assertEqual(v, nv)
        vs = [v] * 100
        jvecs = self.sc._jvm.org.apache.spark.mllib.api.python.SerDe.loads(bytearray(ser.dumps(vs)))
        nvs = ser.loads(bytes(self.sc._jvm.org.apache.spark.mllib.api.python.SerDe.dumps(jvecs)))
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

    def test_parse_vector(self):
        a = DenseVector([])
        self.assertEqual(str(a), "[]")
        self.assertEqual(Vectors.parse(str(a)), a)
        a = DenseVector([3, 4, 6, 7])
        self.assertEqual(str(a), "[3.0,4.0,6.0,7.0]")
        self.assertEqual(Vectors.parse(str(a)), a)
        a = SparseVector(4, [], [])
        self.assertEqual(str(a), "(4,[],[])")
        self.assertEqual(SparseVector.parse(str(a)), a)
        a = SparseVector(4, [0, 2], [3, 4])
        self.assertEqual(str(a), "(4,[0,2],[3.0,4.0])")
        self.assertEqual(Vectors.parse(str(a)), a)
        a = SparseVector(10, [0, 1], [4, 5])
        self.assertEqual(SparseVector.parse(" (10, [0,1 ],[ 4.0,5.0] )"), a)

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

    def test_ml_mllib_vector_conversion(self):
        # to ml
        # dense
        mllibDV = Vectors.dense([1, 2, 3])
        mlDV1 = newlinalg.Vectors.dense([1, 2, 3])
        mlDV2 = mllibDV.asML()
        self.assertEqual(mlDV2, mlDV1)
        # sparse
        mllibSV = Vectors.sparse(4, {1: 1.0, 3: 5.5})
        mlSV1 = newlinalg.Vectors.sparse(4, {1: 1.0, 3: 5.5})
        mlSV2 = mllibSV.asML()
        self.assertEqual(mlSV2, mlSV1)
        # from ml
        # dense
        mllibDV1 = Vectors.dense([1, 2, 3])
        mlDV = newlinalg.Vectors.dense([1, 2, 3])
        mllibDV2 = Vectors.fromML(mlDV)
        self.assertEqual(mllibDV1, mllibDV2)
        # sparse
        mllibSV1 = Vectors.sparse(4, {1: 1.0, 3: 5.5})
        mlSV = newlinalg.Vectors.sparse(4, {1: 1.0, 3: 5.5})
        mllibSV2 = Vectors.fromML(mlSV)
        self.assertEqual(mllibSV1, mllibSV2)

    def test_ml_mllib_matrix_conversion(self):
        # to ml
        # dense
        mllibDM = Matrices.dense(2, 2, [0, 1, 2, 3])
        mlDM1 = newlinalg.Matrices.dense(2, 2, [0, 1, 2, 3])
        mlDM2 = mllibDM.asML()
        self.assertEqual(mlDM2, mlDM1)
        # transposed
        mllibDMt = DenseMatrix(2, 2, [0, 1, 2, 3], True)
        mlDMt1 = newlinalg.DenseMatrix(2, 2, [0, 1, 2, 3], True)
        mlDMt2 = mllibDMt.asML()
        self.assertEqual(mlDMt2, mlDMt1)
        # sparse
        mllibSM = Matrices.sparse(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4])
        mlSM1 = newlinalg.Matrices.sparse(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4])
        mlSM2 = mllibSM.asML()
        self.assertEqual(mlSM2, mlSM1)
        # transposed
        mllibSMt = SparseMatrix(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4], True)
        mlSMt1 = newlinalg.SparseMatrix(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4], True)
        mlSMt2 = mllibSMt.asML()
        self.assertEqual(mlSMt2, mlSMt1)
        # from ml
        # dense
        mllibDM1 = Matrices.dense(2, 2, [1, 2, 3, 4])
        mlDM = newlinalg.Matrices.dense(2, 2, [1, 2, 3, 4])
        mllibDM2 = Matrices.fromML(mlDM)
        self.assertEqual(mllibDM1, mllibDM2)
        # transposed
        mllibDMt1 = DenseMatrix(2, 2, [1, 2, 3, 4], True)
        mlDMt = newlinalg.DenseMatrix(2, 2, [1, 2, 3, 4], True)
        mllibDMt2 = Matrices.fromML(mlDMt)
        self.assertEqual(mllibDMt1, mllibDMt2)
        # sparse
        mllibSM1 = Matrices.sparse(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4])
        mlSM = newlinalg.Matrices.sparse(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4])
        mllibSM2 = Matrices.fromML(mlSM)
        self.assertEqual(mllibSM1, mllibSM2)
        # transposed
        mllibSMt1 = SparseMatrix(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4], True)
        mlSMt = newlinalg.SparseMatrix(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4], True)
        mllibSMt2 = Matrices.fromML(mlSMt)
        self.assertEqual(mllibSMt1, mllibSMt2)


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
        rdd = self.sc.parallelize([LabeledPoint(1.0, self.dv1), LabeledPoint(0.0, self.sv1)])
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

    def test_row_matrix_from_dataframe(self):
        from pyspark.errors import IllegalArgumentException

        df = self.spark.createDataFrame([Row(Vectors.dense(1))])
        row_matrix = RowMatrix(df)
        self.assertEqual(row_matrix.numRows(), 1)
        self.assertEqual(row_matrix.numCols(), 1)
        with self.assertRaises(IllegalArgumentException):
            RowMatrix(df.selectExpr("'monkey'"))

    def test_indexed_row_matrix_from_dataframe(self):
        from pyspark.errors import IllegalArgumentException

        df = self.spark.createDataFrame([Row(int(0), Vectors.dense(1))])
        matrix = IndexedRowMatrix(df)
        self.assertEqual(matrix.numRows(), 1)
        self.assertEqual(matrix.numCols(), 1)
        with self.assertRaises(IllegalArgumentException):
            IndexedRowMatrix(df.drop("_1"))

    def test_row_matrix_invalid_type(self):
        rows = self.sc.parallelize([[1, 2, 3], [4, 5, 6]])
        invalid_type = ""
        matrix = RowMatrix(rows)
        self.assertRaises(TypeError, matrix.multiply, invalid_type)

        irows = self.sc.parallelize([IndexedRow(0, [1, 2, 3]), IndexedRow(1, [4, 5, 6])])
        imatrix = IndexedRowMatrix(irows)
        self.assertRaises(TypeError, imatrix.multiply, invalid_type)


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


@unittest.skipIf(not have_scipy, "SciPy not installed")
class SciPyTests(MLlibTestCase):

    """
    Test both vector operations and MLlib algorithms with SciPy sparse matrices,
    if SciPy is available.
    """

    def test_serialize(self):
        from scipy.sparse import lil_matrix

        ser = CPickleSerializer()
        lil = lil_matrix((4, 1))
        lil[1, 0] = 1
        lil[3, 0] = 2
        sv = SparseVector(4, {1: 1, 3: 2})
        self.assertEqual(sv, _convert_to_vector(lil))
        self.assertEqual(sv, _convert_to_vector(lil.tocsc()))
        self.assertEqual(sv, _convert_to_vector(lil.tocoo()))
        self.assertEqual(sv, _convert_to_vector(lil.tocsr()))
        self.assertEqual(sv, _convert_to_vector(lil.todok()))

        def serialize(d):
            return ser.loads(ser.dumps(_convert_to_vector(d)))

        self.assertEqual(sv, serialize(lil))
        self.assertEqual(sv, serialize(lil.tocsc()))
        self.assertEqual(sv, serialize(lil.tocsr()))
        self.assertEqual(sv, serialize(lil.todok()))

    def test_convert_to_vector(self):
        from scipy.sparse import csc_matrix

        # Create a CSC matrix with non-sorted indices
        indptr = array([0, 2])
        indices = array([3, 1])
        data = array([2.0, 1.0])
        csc = csc_matrix((data, indices, indptr))
        self.assertFalse(csc.has_sorted_indices)
        sv = SparseVector(4, {1: 1, 3: 2})
        self.assertEqual(sv, _convert_to_vector(csc))

    def test_dot(self):
        from scipy.sparse import lil_matrix

        lil = lil_matrix((4, 1))
        lil[1, 0] = 1
        lil[3, 0] = 2
        dv = DenseVector(array([1.0, 2.0, 3.0, 4.0]))
        self.assertEqual(10.0, dv.dot(lil))

    def test_squared_distance(self):
        from scipy.sparse import lil_matrix

        lil = lil_matrix((4, 1))
        lil[1, 0] = 3
        lil[3, 0] = 2
        dv = DenseVector(array([1.0, 2.0, 3.0, 4.0]))
        sv = SparseVector(4, {0: 1, 1: 2, 2: 3, 3: 4})
        self.assertEqual(15.0, dv.squared_distance(lil))
        self.assertEqual(15.0, sv.squared_distance(lil))

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
            self.scipy_matrix(3, {2: 1.1}),
        ]
        clusters = KMeans.train(self.sc.parallelize(data), 2, initializationMode="k-means||")
        self.assertEqual(clusters.predict(data[0]), clusters.predict(data[1]))
        self.assertEqual(clusters.predict(data[2]), clusters.predict(data[3]))

    def test_classification(self):
        from pyspark.mllib.classification import LogisticRegressionWithSGD, SVMWithSGD, NaiveBayes
        from pyspark.mllib.tree import DecisionTree

        data = [
            LabeledPoint(0.0, self.scipy_matrix(2, {0: 1.0})),
            LabeledPoint(1.0, self.scipy_matrix(2, {1: 1.0})),
            LabeledPoint(0.0, self.scipy_matrix(2, {0: 2.0})),
            LabeledPoint(1.0, self.scipy_matrix(2, {1: 2.0})),
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
        dt_model = DecisionTree.trainClassifier(
            rdd, numClasses=2, categoricalFeaturesInfo=categoricalFeaturesInfo
        )
        self.assertTrue(dt_model.predict(features[0]) <= 0)
        self.assertTrue(dt_model.predict(features[1]) > 0)
        self.assertTrue(dt_model.predict(features[2]) <= 0)
        self.assertTrue(dt_model.predict(features[3]) > 0)

    def test_regression(self):
        from pyspark.mllib.regression import (
            LinearRegressionWithSGD,
            LassoWithSGD,
            RidgeRegressionWithSGD,
        )
        from pyspark.mllib.tree import DecisionTree

        data = [
            LabeledPoint(-1.0, self.scipy_matrix(2, {1: -1.0})),
            LabeledPoint(1.0, self.scipy_matrix(2, {1: 1.0})),
            LabeledPoint(-1.0, self.scipy_matrix(2, {1: -2.0})),
            LabeledPoint(1.0, self.scipy_matrix(2, {1: 2.0})),
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


if __name__ == "__main__":
    from pyspark.mllib.tests.test_linalg import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
