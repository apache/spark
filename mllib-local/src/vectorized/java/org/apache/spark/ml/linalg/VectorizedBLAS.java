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

package org.apache.spark.ml.linalg;

import com.github.fommil.netlib.F2jBLAS;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class VectorizedBLAS extends F2jBLAS {

  private final static VectorSpecies<Float>  FMAX = FloatVector.SPECIES_MAX;
  private final static VectorSpecies<Double> DMAX = DoubleVector.SPECIES_MAX;

  // y += alpha * x
  @Override
  public void daxpy(int n, double alpha, double[] x, int incx, double[] y, int incy) {
    if (incx == 1 && incy == 1 && n <= x.length && n <= y.length) {
      if (alpha != 0.) {
        int i = 0;
        for (; i < DMAX.loopBound(n); i += DMAX.length()) {
          DoubleVector vx = DoubleVector.fromArray(DMAX, x, i);
          DoubleVector vy = DoubleVector.fromArray(DMAX, y, i);
          vx.lanewise(VectorOperators.MUL, alpha).add(vy)
            .intoArray(y, i);
        }
        for (; i < n; i += 1) {
          y[i] += alpha * x[i];
        }
      }
    } else {
      super.daxpy(n, alpha, x, incx, y, incy);
    }
  }

  // sum(x * y)
  @Override
  public float sdot(int n, float[] x, int incx, float[] y, int incy) {
    if (incx == 1 && incy == 1) {
      float sum = 0.0f;
      int i = 0;
      for (; i < FMAX.loopBound(n); i += FMAX.length()) {
        FloatVector vx = FloatVector.fromArray(FMAX, x, i);
        FloatVector vy = FloatVector.fromArray(FMAX, y, i);
        sum += vx.mul(vy).reduceLanes(VectorOperators.ADD);
      }
      for (; i < n; i += 1) {
        sum += x[i] * y[i];
      }
      return sum;
    } else {
      return super.sdot(n, x, incx, y, incy);
    }
  }

  // sum(x * y)
  @Override
  public double ddot(int n, double[] x, int incx, double[] y, int incy) {
    if (incx == 1 && incy == 1) {
      double sum = 0.;
      int i = 0;
      for (; i < DMAX.loopBound(n); i += DMAX.length()) {
        DoubleVector vx = DoubleVector.fromArray(DMAX, x, i);
        DoubleVector vy = DoubleVector.fromArray(DMAX, y, i);
        sum += vx.mul(vy).reduceLanes(VectorOperators.ADD);
      }
      for (; i < n; i += 1) {
        sum += x[i] * y[i];
      }
      return sum;
    } else {
      return super.ddot(n, x, incx, y, incy);
    }
  }

  // x = alpha * x
  @Override
  public void dscal(int n, double alpha, double[] x, int incx) {
    if (incx == 1) {
      if (alpha != 1.) {
        int i = 0;
        for (; i < DMAX.loopBound(n); i += DMAX.length()) {
          DoubleVector vx = DoubleVector.fromArray(DMAX, x, i);
          vx.lanewise(VectorOperators.MUL, alpha)
            .intoArray(x, i);
        }
        for (; i < n; i += 1) {
          x[i] *= alpha;
        }
      }
    } else {
      super.dscal(n, alpha, x, incx);
    }
  }

  // y = alpha * A * x + beta * y
  @Override
  public void dgemv(String trans, int m, int n, double alpha, double[] a, int lda, double[] x, int incx, double beta, double[] y, int incy) {
    if (trans == "T" && incx == 1 && incy == 1 && lda == m) {
      // y = beta * y
      dscal(n, beta, y, 1);
      // y += alpha * A * x
      if (alpha != 0.) {
        for (int col = 0; col < n; col += 1) {
          int row = 0;
          for (; row < DMAX.loopBound(m); row += DMAX.length()) {
            DoubleVector vx = DoubleVector.fromArray(DMAX, x, row);
            DoubleVector va = DoubleVector.fromArray(DMAX, a, row + col * m);
            y[col] += alpha * vx.mul(va).reduceLanes(VectorOperators.ADD);
          }
          for (; row < m; row += 1) {
            y[col] += alpha * x[row] * a[row + col * m];
          }
        }
      }
    } else {
      super.dgemv(trans, m, n, alpha, a, lda, x, incx, beta, y, incy);
    }
  }

  @Override
  public void dgemm(String transa, String transb, int m, int n, int k, double alpha, double[] a, int lda, double[] b, int ldb, double beta, double[] c, int ldc) {
    if (transa == "T" && transb == "N" && lda == k && ldb == k && ldc == m) {
      // C = beta * C
      dscal(m * n, beta, c, 1);
      // C += alpha * A * B
      if (alpha != 0.) {
        for (int col = 0; col < n; col += 1) {
          for (int row = 0; row < m; row += 1) {
            int i = 0;
            for (; i < DMAX.loopBound(k); i += DMAX.length()) {
              DoubleVector va = DoubleVector.fromArray(DMAX, a, i + row * k);
              DoubleVector vb = DoubleVector.fromArray(DMAX, b, i + col * k);
              c[row + col * m] += alpha * va.mul(vb).reduceLanes(VectorOperators.ADD);
            }
            for (; i < k; i += 1) {
              c[row + col * m] += alpha * a[i + row * k] * b[i + col * k];
            }
          }
        }
      }
    } else {
      super.dgemm(transa, transb, m, n, k, alpha, a, lda, b, ldb, beta, c, ldc);
    }
  }
}
