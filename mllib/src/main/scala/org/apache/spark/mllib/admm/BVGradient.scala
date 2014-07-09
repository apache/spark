package org.apache.spark.mllib.admm


abstract class BVGradient {
  def apply(data: BV, label: Double, weights: BV): BV
}

class PegasosBVGradient(val lambda: Double) extends BVGradient {
  def apply(data: BV, label: Double, weights: BV): BV = {
    val prod = label * data.dot(weights)

    val ret: BV = weights * label
    if(prod < 1) {
      ret -= data * label
    }

    ret
  }
}