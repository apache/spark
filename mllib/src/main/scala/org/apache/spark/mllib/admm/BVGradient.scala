package org.apache.spark.mllib.admm


abstract class BVGradient {
  def apply(data: BV, label: Double, weights: BV): BV
}

class PegasosBVGradient(val lambda: Double) extends BVGradient {
  def apply(x: BV, label: Double, weights: BV): BV = {
    val y: Double = if (label <= 0.0) -1.0 else 1.0

    val prod = y * x.dot(weights)

    val ret: BV = weights * y
    if(prod < 1.0) {
      ret -= x * y
    }

    ret
  }
}