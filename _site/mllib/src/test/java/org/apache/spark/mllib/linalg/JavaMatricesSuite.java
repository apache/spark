/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.linalg;

import static org.junit.Assert.*;
import org.junit.Test;

import java.io.Serializable;
import java.util.Random;

public class JavaMatricesSuite implements Serializable {

    @Test
    public void randMatrixConstruction() {
        Random rng = new Random(24);
        Matrix r = Matrices.rand(3, 4, rng);
        rng.setSeed(24);
        DenseMatrix dr = DenseMatrix.rand(3, 4, rng);
        assertArrayEquals(r.toArray(), dr.toArray(), 0.0);

        rng.setSeed(24);
        Matrix rn = Matrices.randn(3, 4, rng);
        rng.setSeed(24);
        DenseMatrix drn = DenseMatrix.randn(3, 4, rng);
        assertArrayEquals(rn.toArray(), drn.toArray(), 0.0);

        rng.setSeed(24);
        Matrix s = Matrices.sprand(3, 4, 0.5, rng);
        rng.setSeed(24);
        SparseMatrix sr = SparseMatrix.sprand(3, 4, 0.5, rng);
        assertArrayEquals(s.toArray(), sr.toArray(), 0.0);

        rng.setSeed(24);
        Matrix sn = Matrices.sprandn(3, 4, 0.5, rng);
        rng.setSeed(24);
        SparseMatrix srn = SparseMatrix.sprandn(3, 4, 0.5, rng);
        assertArrayEquals(sn.toArray(), srn.toArray(), 0.0);
    }

    @Test
    public void identityMatrixConstruction() {
        Matrix r = Matrices.eye(2);
        DenseMatrix dr = DenseMatrix.eye(2);
        SparseMatrix sr = SparseMatrix.speye(2);
        assertArrayEquals(r.toArray(), dr.toArray(), 0.0);
        assertArrayEquals(sr.toArray(), dr.toArray(), 0.0);
        assertArrayEquals(r.toArray(), new double[]{1.0, 0.0, 0.0, 1.0}, 0.0);
    }

    @Test
    public void diagonalMatrixConstruction() {
        Vector v = Vectors.dense(1.0, 0.0, 2.0);
        Vector sv = Vectors.sparse(3, new int[]{0, 2}, new double[]{1.0, 2.0});

        Matrix m = Matrices.diag(v);
        Matrix sm = Matrices.diag(sv);
        DenseMatrix d = DenseMatrix.diag(v);
        DenseMatrix sd = DenseMatrix.diag(sv);
        SparseMatrix s = SparseMatrix.spdiag(v);
        SparseMatrix ss = SparseMatrix.spdiag(sv);

        assertArrayEquals(m.toArray(), sm.toArray(), 0.0);
        assertArrayEquals(d.toArray(), sm.toArray(), 0.0);
        assertArrayEquals(d.toArray(), sd.toArray(), 0.0);
        assertArrayEquals(sd.toArray(), s.toArray(), 0.0);
        assertArrayEquals(s.toArray(), ss.toArray(), 0.0);
        assertArrayEquals(s.values(), ss.values(), 0.0);
        assertEquals(2, s.values().length);
        assertEquals(2, ss.values().length);
        assertEquals(4, s.colPtrs().length);
        assertEquals(4, ss.colPtrs().length);
    }

    @Test
    public void zerosMatrixConstruction() {
        Matrix z = Matrices.zeros(2, 2);
        Matrix one = Matrices.ones(2, 2);
        DenseMatrix dz = DenseMatrix.zeros(2, 2);
        DenseMatrix done = DenseMatrix.ones(2, 2);

        assertArrayEquals(z.toArray(), new double[]{0.0, 0.0, 0.0, 0.0}, 0.0);
        assertArrayEquals(dz.toArray(), new double[]{0.0, 0.0, 0.0, 0.0}, 0.0);
        assertArrayEquals(one.toArray(), new double[]{1.0, 1.0, 1.0, 1.0}, 0.0);
        assertArrayEquals(done.toArray(), new double[]{1.0, 1.0, 1.0, 1.0}, 0.0);
    }

    @Test
    public void sparseDenseConversion() {
        int m = 3;
        int n = 2;
        double[] values = new double[]{1.0, 2.0, 4.0, 5.0};
        double[] allValues = new double[]{1.0, 2.0, 0.0, 0.0, 4.0, 5.0};
        int[] colPtrs = new int[]{0, 2, 4};
        int[] rowIndices = new int[]{0, 1, 1, 2};

        SparseMatrix spMat1 = new SparseMatrix(m, n, colPtrs, rowIndices, values);
        DenseMatrix deMat1 = new DenseMatrix(m, n, allValues);

        SparseMatrix spMat2 = deMat1.toSparse();
        DenseMatrix deMat2 = spMat1.toDense();

        assertArrayEquals(spMat1.toArray(), spMat2.toArray(), 0.0);
        assertArrayEquals(deMat1.toArray(), deMat2.toArray(), 0.0);
    }

    @Test
    public void concatenateMatrices() {
        int m = 3;
        int n = 2;

        Random rng = new Random(42);
        SparseMatrix spMat1 = SparseMatrix.sprand(m, n, 0.5, rng);
        rng.setSeed(42);
        DenseMatrix deMat1 = DenseMatrix.rand(m, n, rng);
        Matrix deMat2 = Matrices.eye(3);
        Matrix spMat2 = Matrices.speye(3);
        Matrix deMat3 = Matrices.eye(2);
        Matrix spMat3 = Matrices.speye(2);

        Matrix spHorz = Matrices.horzcat(new Matrix[]{spMat1, spMat2});
        Matrix deHorz1 = Matrices.horzcat(new Matrix[]{deMat1, deMat2});
        Matrix deHorz2 = Matrices.horzcat(new Matrix[]{spMat1, deMat2});
        Matrix deHorz3 = Matrices.horzcat(new Matrix[]{deMat1, spMat2});

        assertEquals(3, deHorz1.numRows());
        assertEquals(3, deHorz2.numRows());
        assertEquals(3, deHorz3.numRows());
        assertEquals(3, spHorz.numRows());
        assertEquals(5, deHorz1.numCols());
        assertEquals(5, deHorz2.numCols());
        assertEquals(5, deHorz3.numCols());
        assertEquals(5, spHorz.numCols());

        Matrix spVert = Matrices.vertcat(new Matrix[]{spMat1, spMat3});
        Matrix deVert1 = Matrices.vertcat(new Matrix[]{deMat1, deMat3});
        Matrix deVert2 = Matrices.vertcat(new Matrix[]{spMat1, deMat3});
        Matrix deVert3 = Matrices.vertcat(new Matrix[]{deMat1, spMat3});

        assertEquals(5, deVert1.numRows());
        assertEquals(5, deVert2.numRows());
        assertEquals(5, deVert3.numRows());
        assertEquals(5, spVert.numRows());
        assertEquals(2, deVert1.numCols());
        assertEquals(2, deVert2.numCols());
        assertEquals(2, deVert3.numCols());
        assertEquals(2, spVert.numCols());
    }
}
