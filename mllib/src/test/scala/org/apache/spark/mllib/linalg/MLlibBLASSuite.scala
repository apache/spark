package org.apache.spark.mllib.linalg

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.linalg.MLlibBLAS._

class MLlibBLASSuite extends FunSuite {

  test("copy") {
    val sx = Vectors.sparse(4, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0, 0.0)
    val sy = Vectors.sparse(4, Array(0, 1, 3), Array(2.0, 1.0, 1.0))
    val dy = Array(2.0, 1.0, 0.0, 1.0)

    val dy1 = Vectors.dense(dy.clone())
    copy(sx, dy1)
    assert(dy1 ~== dx absTol 1e-15)

    val dy2 = Vectors.dense(dy.clone())
    copy(dx, dy2)
    assert(dy2 ~== dx absTol 1e-15)

    intercept[IllegalArgumentException] {
      copy(sx, sy)
    }

    intercept[IllegalArgumentException] {
      copy(dx, sy)
    }

    withClue("vector sizes must match") {
      intercept[Exception] {
        copy(sx, Vectors.dense(0.0, 1.0, 2.0))
      }
    }
  }

  test("scal") {
    val a = 0.1
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)

    scal(a, sx)
    assert(sx ~== Vectors.sparse(3, Array(0, 2), Array(0.1, -0.2)) absTol 1e-15)

    scal(a, dx)
    assert(dx ~== Vectors.dense(0.1, 0.0, -0.2) absTol 1e-15)
  }

  test("axpy") {
    val alpha = 0.1
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)
    val dy = Array(2.0, 1.0, 0.0)
    val expected = Vectors.dense(2.1, 1.0, -0.2)

    val dy1 = Vectors.dense(dy.clone())
    axpy(alpha, sx, dy1)
    assert(dy1 ~== expected absTol 1e-15)

    val dy2 = Vectors.dense(dy.clone())
    axpy(alpha, dx, dy2)
    assert(dy2 ~== expected absTol 1e-15)

    val sy = Vectors.sparse(4, Array(0, 1), Array(2.0, 1.0))

    intercept[IllegalArgumentException] {
      axpy(alpha, sx, sy)
    }

    intercept[IllegalArgumentException] {
      axpy(alpha, dx, sy)
    }

    withClue("vector sizes must match") {
      intercept[Exception] {
        axpy(alpha, sx, Vectors.dense(1.0, 2.0))
      }
    }
  }

  test("dot") {
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)
    val sy = Vectors.sparse(3, Array(0, 1), Array(2.0, 1.0))
    val dy = Vectors.dense(2.0, 1.0, 0.0)

    assert(dot(sx, sy) ~== 2.0 absTol 1e-15)
    assert(dot(sy, sx) ~== 2.0 absTol 1e-15)
    assert(dot(sx, dy) ~== 2.0 absTol 1e-15)
    assert(dot(dy, sx) ~== 2.0 absTol 1e-15)
    assert(dot(dx, dy) ~== 2.0 absTol 1e-15)
    assert(dot(dy, dx) ~== 2.0 absTol 1e-15)

    assert(dot(sx, sx) ~== 5.0 absTol 1e-15)
    assert(dot(dx, dx) ~== 5.0 absTol 1e-15)
    assert(dot(sx, dx) ~== 5.0 absTol 1e-15)
    assert(dot(dx, dx) ~== 5.0 absTol 1e-15)

    withClue("vector sizes must match") {
      intercept[Exception] {
        dot(sx, Vectors.dense(2.0, 1.0))
      }
    }
  }
}
