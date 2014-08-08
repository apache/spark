package org.apache.spark.mllib.linalg

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.linalg.MLlibBLAS._

class MLlibBLASSuite extends FunSuite {

  test("dcopy") {
    val sx = Vectors.sparse(4, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0, 0.0)
    val sy = Vectors.sparse(4, Array(0, 1, 3), Array(2.0, 1.0, 1.0))
    val dy = Array(2.0, 1.0, 0.0, 1.0)

    val dy1 = Vectors.dense(dy.clone())
    dcopy(sx, dy1)
    assert(dy1 ~== dx absTol 1e-15)

    val dy2 = Vectors.dense(dy.clone())
    dcopy(dx, dy2)
    assert(dy2 ~== dx absTol 1e-15)

    intercept[IllegalArgumentException] {
      dcopy(sx, sy)
    }

    intercept[IllegalArgumentException] {
      dcopy(dx, sy)
    }

    withClue("vector sizes must match") {
      intercept[Exception] {
        dcopy(sx, Vectors.dense(0.0, 1.0, 2.0))
      }
    }
  }

  test("dscal") {
    val a = 0.1
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)

    dscal(a, sx)
    assert(sx ~== Vectors.sparse(3, Array(0, 2), Array(0.1, -0.2)) absTol 1e-15)

    dscal(a, dx)
    assert(dx ~== Vectors.dense(0.1, 0.0, -0.2) absTol 1e-15)
  }

  test("daxpy") {
    val alpha = 0.1
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)
    val dy = Array(2.0, 1.0, 0.0)
    val expected = Vectors.dense(2.1, 1.0, -0.2)

    val dy1 = Vectors.dense(dy.clone())
    daxpy(alpha, sx, dy1)
    assert(dy1 ~== expected absTol 1e-15)

    val dy2 = Vectors.dense(dy.clone())
    daxpy(alpha, dx, dy2)
    assert(dy2 ~== expected absTol 1e-15)

    val sy = Vectors.sparse(4, Array(0, 1), Array(2.0, 1.0))

    intercept[IllegalArgumentException] {
      daxpy(alpha, sx, sy)
    }

    intercept[IllegalArgumentException] {
      daxpy(alpha, dx, sy)
    }

    withClue("vector sizes must match") {
      intercept[Exception] {
        daxpy(alpha, sx, Vectors.dense(1.0, 2.0))
      }
    }
  }

  test("ddot") {
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)
    val sy = Vectors.sparse(3, Array(0, 1), Array(2.0, 1.0))
    val dy = Vectors.dense(2.0, 1.0, 0.0)

    assert(ddot(sx, sy) ~== 2.0 absTol 1e-15)
    assert(ddot(sy, sx) ~== 2.0 absTol 1e-15)
    assert(ddot(sx, dy) ~== 2.0 absTol 1e-15)
    assert(ddot(dy, sx) ~== 2.0 absTol 1e-15)
    assert(ddot(dx, dy) ~== 2.0 absTol 1e-15)
    assert(ddot(dy, dx) ~== 2.0 absTol 1e-15)

    assert(ddot(sx, sx) ~== 5.0 absTol 1e-15)
    assert(ddot(dx, dx) ~== 5.0 absTol 1e-15)
    assert(ddot(sx, dx) ~== 5.0 absTol 1e-15)
    assert(ddot(dx, dx) ~== 5.0 absTol 1e-15)
  }
}
