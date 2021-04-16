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

  private static final VectorSpecies<Float>  FMAX = FloatVector.SPECIES_MAX;
  private static final VectorSpecies<Double> DMAX = DoubleVector.SPECIES_MAX;

  // y += alpha * x
  @Override
  public void daxpy(int n, double alpha, double[] x, int incx, double[] y, int incy) {
    if (n >= 0
        && x != null && x.length >= n && incx == 1
        && y != null && y.length >= n && incy == 1) {
      if (alpha != 0.) {
        DoubleVector valpha = DoubleVector.broadcast(DMAX, alpha);
        int i = 0;
        for (; i < DMAX.loopBound(n); i += DMAX.length()) {
          DoubleVector vx = DoubleVector.fromArray(DMAX, x, i);
          DoubleVector vy = DoubleVector.fromArray(DMAX, y, i);
          vx.fma(valpha, vy).intoArray(y, i);
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
    if (n >= 0
        && x != null && x.length >= n && incx == 1
        && y != null && y.length >= n && incy == 1) {
      float sum = 0.0f;
      int i = 0;
      FloatVector vsum = FloatVector.zero(FMAX);
      for (; i < FMAX.loopBound(n); i += FMAX.length()) {
        FloatVector vx = FloatVector.fromArray(FMAX, x, i);
        FloatVector vy = FloatVector.fromArray(FMAX, y, i);
        vsum = vx.fma(vy, vsum);
      }
      sum += vsum.reduceLanes(VectorOperators.ADD);
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
    if (n >= 0
        && x != null && x.length >= n && incx == 1
        && y != null && y.length >= n && incy == 1) {
      double sum = 0.;
      int i = 0;
      DoubleVector vsum = DoubleVector.zero(DMAX);
      for (; i < DMAX.loopBound(n); i += DMAX.length()) {
        DoubleVector vx = DoubleVector.fromArray(DMAX, x, i);
        DoubleVector vy = DoubleVector.fromArray(DMAX, y, i);
        vsum = vx.fma(vy, vsum);
      }
      sum += vsum.reduceLanes(VectorOperators.ADD);
      for (; i < n; i += 1) {
        sum += x[i] * y[i];
      }
      return sum;
    } else {
      return super.ddot(n, x, incx, y, incy);
    }
  }

  @Override
  public void dscal(int n, double alpha, double[] x, int incx) {
    dscal(n, alpha, x, 0, incx);
  }

  // x = alpha * x
  @Override
  public void dscal(int n, double alpha, double[] x, int offsetx, int incx) {
    if (n >= 0 && x != null && x.length >= offsetx + n && incx == 1) {
      if (alpha != 1.) {
        DoubleVector valpha = DoubleVector.broadcast(DMAX, alpha);
        int i = 0;
        for (; i < DMAX.loopBound(n); i += DMAX.length()) {
          DoubleVector vx = DoubleVector.fromArray(DMAX, x, offsetx + i);
          vx.mul(valpha).intoArray(x, offsetx + i);
        }
        for (; i < n; i += 1) {
          x[offsetx + i] *= alpha;
        }
      }
    } else {
      super.dscal(n, alpha, x, offsetx, incx);
    }
  }

  @Override
  public void sscal(int n, float alpha, float[] x, int incx) {
    sscal(n, alpha, x, 0, incx);
  }

  // x = alpha * x
  @Override
  public void sscal(int n, float alpha, float[] x, int offsetx, int incx) {
    if (n >= 0 && x != null && x.length >= offsetx + n && incx == 1) {
      if (alpha != 1.) {
        FloatVector valpha = FloatVector.broadcast(FMAX, alpha);
        int i = 0;
        for (; i < FMAX.loopBound(n); i += FMAX.length()) {
          FloatVector vx = FloatVector.fromArray(FMAX, x, offsetx + i);
          vx.mul(valpha).intoArray(x, offsetx + i);
        }
        for (; i < n; i += 1) {
          x[offsetx + i] *= alpha;
        }
      }
    } else {
      super.sscal(n, alpha, x, offsetx, incx);
    }
  }

  // y = alpha * a * x + beta * y
  @Override
  public void dspmv(String uplo, int n, double alpha, double[] a,
      double[] x, int incx, double beta, double[] y, int incy) {
    if ("U".equals(uplo)
        && n >= 0
        && a != null && a.length >= n * (n + 1) / 2
        && x != null && x.length >= n && incx == 1
        && y != null && y.length >= n && incy == 1) {
      // y = beta * y
      dscal(n, beta, y, 1);
      // y += alpha * A * x
      if (alpha != 0.) {
        DoubleVector valpha = DoubleVector.broadcast(DMAX, alpha);
        for (int row = 0; row < n; row += 1) {
          int col = 0;
          DoubleVector vyrowsum = DoubleVector.zero(DMAX);
          DoubleVector valphaxrow = DoubleVector.broadcast(DMAX, alpha * x[row]);
          for (; col < DMAX.loopBound(row); col += DMAX.length()) {
            DoubleVector vx = DoubleVector.fromArray(DMAX, x, col);
            DoubleVector vy = DoubleVector.fromArray(DMAX, y, col);
            DoubleVector va = DoubleVector.fromArray(DMAX, a, col + row * (row + 1) / 2);
            vyrowsum = valpha.mul(vx).fma(va, vyrowsum);
            valphaxrow.fma(va, vy).intoArray(y, col);
          }
          y[row] += vyrowsum.reduceLanes(VectorOperators.ADD);
          for (; col < row; col += 1) {
            y[row] += alpha * x[col] * a[col + row * (row + 1) / 2];
            y[col] += alpha * x[row] * a[col + row * (row + 1) / 2];
          }
          y[row] += alpha * x[col] * a[col + row * (row + 1) / 2];
        }
      }
    } else {
      super.dspmv(uplo, n, alpha, a, x, incx, beta, y, incy);
    }
  }

  // a += alpha * x * x.t
  @Override
  public void dspr(String uplo, int n, double alpha, double[] x, int incx, double[] a) {
    if ("U".equals(uplo)
        && n >= 0
        && x != null && x.length >= n && incx == 1
        && a != null && a.length >= n * (n + 1) / 2) {
      if (alpha != 0.) {
        for (int row = 0; row < n; row += 1) {
          int col = 0;
          DoubleVector valphaxrow = DoubleVector.broadcast(DMAX, alpha * x[row]);
          for (; col < DMAX.loopBound(row + 1); col += DMAX.length()) {
            DoubleVector vx = DoubleVector.fromArray(DMAX, x, col);
            DoubleVector va = DoubleVector.fromArray(DMAX, a, col + row * (row + 1) / 2);
            vx.fma(valphaxrow, va).intoArray(a, col + row * (row + 1) / 2);
          }
          for (; col < row + 1; col += 1) {
            a[col + row * (row + 1) / 2] += alpha * x[row] * x[col];
          }
        }
      }
    } else {
      super.dspr(uplo, n, alpha, x, incx, a);
    }
  }

  // a += alpha * x * x.t
  @Override
  public void dsyr(String uplo, int n, double alpha, double[] x, int incx, double[] a, int lda) {
    if ("U".equals(uplo)
        && n >= 0
        && x != null && x.length >= n && incx == 1
        && a != null && a.length >= n * n && lda == n) {
      if (alpha != 0.) {
        for (int row = 0; row < n; row += 1) {
          int col = 0;
          DoubleVector valphaxrow = DoubleVector.broadcast(DMAX, alpha * x[row]);
          for (; col < DMAX.loopBound(row + 1); col += DMAX.length()) {
            DoubleVector vx = DoubleVector.fromArray(DMAX, x, col);
            DoubleVector va = DoubleVector.fromArray(DMAX, a, col + row * n);
            vx.fma(valphaxrow, va).intoArray(a, col + row * n);
          }
          for (; col < row + 1; col += 1) {
            a[col + row * n] += alpha * x[row] * x[col];
          }
        }
      }
    } else {
      super.dsyr(uplo, n, alpha, x, incx, a, lda);
    }
  }

  @Override
  public void dgemv(String trans, int m, int n,
      double alpha, double[] a, int lda, double[] x, int incx,
      double beta, double[] y, int incy) {
    dgemv(trans, m, n, alpha, a, 0, lda, x, 0, incx, beta, y, 0, incy);
  }

  // y = alpha * A * x + beta * y
  @Override
  public void dgemv(String trans, int m, int n,
      double alpha, double[] a, int offseta, int lda, double[] x, int offsetx, int incx,
      double beta, double[] y, int offsety, int incy) {
    if ("N".equals(trans)
        && m >= 0 && n >= 0
        && a != null && a.length >= offseta + m * n && lda == m
        && x != null && x.length >= offsetx + n && incx == 1
        && y != null && y.length >= offsety + m && incy == 1) {
      // y = beta * y
      dscal(m, beta, y, offsety, 1);
      // y += alpha * A * x
      if (alpha != 0.) {
        DoubleVector valpha = DoubleVector.broadcast(DMAX, alpha);
        for (int col = 0; col < n; col += 1) {
          int row = 0;
          for (; row < DMAX.loopBound(m); row += DMAX.length()) {
            DoubleVector va = DoubleVector.fromArray(DMAX, a, offseta + row + col * m);
            DoubleVector vy = DoubleVector.fromArray(DMAX, y, offsety + row);
            valpha.mul(x[offsetx + col]).fma(va, vy)
                  .intoArray(y, offsety + row);
          }
          for (; row < m; row += 1) {
            y[offsety + row] += alpha * x[offsetx + col] * a[offseta + row + col * m];
          }
        }
      }
    } else if ("T".equals(trans)
        && m >= 0 && n >= 0
        && a != null && a.length >= offseta + m * n && lda == m
        && x != null && x.length >= offsetx + m && incx == 1
        && y != null && y.length >= offsety + n && incy == 1) {
      if (alpha != 0. || beta != 1.) {
        for (int col = 0; col < n; col += 1) {
          double sum = 0.;
          int row = 0;
          DoubleVector vsum = DoubleVector.zero(DMAX);
          for (; row < DMAX.loopBound(m); row += DMAX.length()) {
            DoubleVector va = DoubleVector.fromArray(DMAX, a, offseta + row + col * m);
            DoubleVector vx = DoubleVector.fromArray(DMAX, x, offsetx + row);
            vsum = va.fma(vx, vsum);
          }
          sum += vsum.reduceLanes(VectorOperators.ADD);
          for (; row < m; row += 1) {
            sum += x[offsetx + row] * a[offseta + row + col * m];
          }
          y[offsety + col] = alpha * sum + beta * y[offsety + col];
        }
      }
    } else {
      super.dgemv(trans, m, n, alpha, a, offseta, lda, x, offsetx, incx, beta, y, offsety, incy);
    }
  }

  @Override
  public void sgemv(String trans, int m, int n,
      float alpha, float[] a, int lda, float[] x, int incx,
      float beta, float[] y, int incy) {
    sgemv(trans, m, n, alpha, a, 0, lda, x, 0, incx, beta, y, 0, incy);
  }

  // y = alpha * A * x + beta * y
  @Override
  public void sgemv(String trans, int m, int n,
      float alpha, float[] a, int offseta, int lda, float[] x, int offsetx, int incx,
      float beta, float[] y, int offsety, int incy) {
    if ("N".equals(trans)
        && m >= 0 && n >= 0
        && a != null && a.length >= offseta + m * n && lda == m
        && x != null && x.length >= offsetx + n && incx == 1
        && y != null && y.length >= offsety + m && incy == 1) {
      // y = beta * y
      sscal(m, beta, y, offsety, 1);
      // y += alpha * A * x
      if (alpha != 0.f) {
        FloatVector valpha = FloatVector.broadcast(FMAX, alpha);
        for (int col = 0; col < n; col += 1) {
          int row = 0;
          for (; row < FMAX.loopBound(m); row += FMAX.length()) {
            FloatVector va = FloatVector.fromArray(FMAX, a, offseta + row + col * m);
            FloatVector vy = FloatVector.fromArray(FMAX, y, offsety + row);
            valpha.mul(x[offsetx + col]).fma(va, vy)
                  .intoArray(y, offsety + row);
          }
          for (; row < m; row += 1) {
            y[offsety + row] += alpha * x[offsetx + col] * a[offseta + row + col * m];
          }
        }
      }
    } else if ("T".equals(trans)
        && m >= 0 && n >= 0
        && a != null && a.length >= offseta + m * n && lda == m
        && x != null && x.length >= offsetx + m && incx == 1
        && y != null && y.length >= offsety + n && incy == 1) {
      if (alpha != 0. || beta != 1.) {
        for (int col = 0; col < n; col += 1) {
          float sum = 0.f;
          int row = 0;
          FloatVector vsum = FloatVector.zero(FMAX);
          for (; row < FMAX.loopBound(m); row += FMAX.length()) {
            FloatVector va = FloatVector.fromArray(FMAX, a, offseta + row + col * m);
            FloatVector vx = FloatVector.fromArray(FMAX, x, offsetx + row);
            vsum = va.fma(vx, vsum);
          }
          sum += vsum.reduceLanes(VectorOperators.ADD);
          for (; row < m; row += 1) {
            sum += x[offsetx + row] * a[offseta + row + col * m];
          }
          y[offsety + col] = alpha * sum + beta * y[offsety + col];
        }
      }
    } else {
      super.sgemv(trans, m, n, alpha, a, offseta, lda, x, offsetx, incx, beta, y, offsety, incy);
    }
  }

  @Override
  public void dgemm(String transa, String transb, int m, int n, int k,
      double alpha, double[] a, int lda, double[] b, int ldb,
      double beta, double[] c, int ldc) {
    dgemm(transa, transb, m, n, k, alpha, a, 0, lda, b, 0, ldb, beta, c, 0, ldc);
  }

  // c = alpha * a * b + beta * c
  @Override
  public void dgemm(String transa, String transb, int m, int n, int k,
      double alpha, double[] a, int offseta, int lda, double[] b, int offsetb, int ldb,
      double beta, double[] c, int offsetc, int ldc) {
    if ("N".equals(transa) && "N".equals(transb)
        && m >= 0 && n >= 0 && k >= 0
        && a != null && a.length >= offseta + m * k && lda == m
        && b != null && b.length >= offsetb + k * n && ldb == k
        && c != null && c.length >= offsetc + m * n && ldc == m) {
      // C = beta * C
      dscal(m * n, beta, c, offsetc, 1);
      // C += alpha * A * B
      if (alpha != 0.) {
        DoubleVector valpha = DoubleVector.broadcast(DMAX, alpha);
        for (int col = 0; col < n; col += 1) {
          for (int i = 0; i < k; i += 1) {
            int row = 0;
            for (; row < DMAX.loopBound(m); row += DMAX.length()) {
              DoubleVector va = DoubleVector.fromArray(DMAX, a, offseta + i * m + row);
              DoubleVector vc = DoubleVector.fromArray(DMAX, c, offsetc + col * m + row);
              valpha.mul(b[offsetb + col * k + i]).fma(va, vc)
                    .intoArray(c, offsetc + col * m + row);
            }
            for (; row < m; row += 1) {
              c[offsetc + col * m + row] += alpha * a[offseta + i * m + row] * b[offsetb + col * k + i];
            }
          }
        }
      }
    } else if ("N".equals(transa) && "T".equals(transb)
        && m >= 0 && n >= 0 && k >= 0
        && a != null && a.length >= offseta + m * k && lda == m
        && b != null && b.length >= offsetb + k * n && ldb == n
        && c != null && c.length >= offsetc + m * n && ldc == m) {
      // C = beta * C
      dscal(m * n, beta, c, offsetc, 1);
      // C += alpha * A * B
      if (alpha != 0.) {
        DoubleVector valpha = DoubleVector.broadcast(DMAX, alpha);
        for (int i = 0; i < k; i += 1) {
          for (int col = 0; col < n; col += 1) {
            int row = 0;
            for (; row < DMAX.loopBound(m); row += DMAX.length()) {
              DoubleVector va = DoubleVector.fromArray(DMAX, a, offseta + i * m + row);
              DoubleVector vc = DoubleVector.fromArray(DMAX, c, offsetc + col * m + row);
              valpha.mul(b[offsetb + col + i * n]).fma(va, vc)
                    .intoArray(c, offsetc + col * m + row);
            }
            for (; row < m; row += 1) {
              c[offsetc + col * m + row] += alpha * a[offseta + i * m + row] * b[offsetb + col + i * n];
            }
          }
        }
      }
    } else if ("T".equals(transa) && "N".equals(transb)
        && m >= 0 && n >= 0 && k >= 0
        && a != null && a.length >= offseta + m * k && lda == k
        && b != null && b.length >= offsetb + k * n && ldb == k
        && c != null && c.length >= offsetc + m * n && ldc == m) {
      if (alpha != 0. || beta != 1.) {
        for (int col = 0; col < n; col += 1) {
          for (int row = 0; row < m; row += 1) {
            double sum = 0.;
            int i = 0;
            DoubleVector vsum = DoubleVector.zero(DMAX);
            for (; i < DMAX.loopBound(k); i += DMAX.length()) {
              DoubleVector va = DoubleVector.fromArray(DMAX, a, offseta + i + row * k);
              DoubleVector vb = DoubleVector.fromArray(DMAX, b, offsetb + col * k + i);
              vsum = va.fma(vb, vsum);
            }
            sum += vsum.reduceLanes(VectorOperators.ADD);
            for (; i < k; i += 1) {
              sum += a[offseta + i + row * k] * b[offsetb + col * k + i];
            }
            if (beta != 0.) {
              c[offsetc + col * m + row] = alpha * sum + beta * c[offsetc + col * m + row];
            } else {
              c[offsetc + col * m + row] = alpha * sum;
            }
          }
        }
      }
    } else if ("T".equals(transa) && "T".equals(transb)
        && m >= 0 && n >= 0 && k >= 0
        && a != null && a.length >= offseta + m * k && lda == k
        && b != null && b.length >= offsetb + k * n && ldb == n
        && c != null && c.length >= offsetc + m * n && ldc == m) {
      if (alpha != 0. || beta != 1.) {
        // FIXME: do block by block
        for (int col = 0; col < n; col += 1) {
          for (int row = 0; row < m; row += 1) {
            double sum = 0.;
            for (int i = 0; i < k; i += 1) {
              sum += a[offseta + i + row * k] * b[offsetb + col + i * n];
            }
            if (beta != 0.) {
              c[offsetc + col * m + row] = alpha * sum + beta * c[offsetc + col * m + row];
            } else {
              c[offsetc + col * m + row] = alpha * sum;
            }
          }
        }
      }
    } else {
      super.dgemm(transa, transb, m, n, k,
                  alpha, a, offseta, lda, b, offsetb, ldb,
                  beta, c, offsetc, ldc);
    }
  }
}
