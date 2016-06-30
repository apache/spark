package org.apache.spark.mllib.fuzzylogic

object risingFuzzyZone {

  def apply(n: String, min: Double, max: Double) = new risingFuzzyZone(n, min, max)

}

class RisingFuzzyZone private (n: String, min: Double, max: Double) extends fuzzyZone(n, min, max) {

  def fuzzify(v: Double): fuzzy = {
    if (v <= from) fuzzy.MIN
    else if (v >= to) fuzzy.MAX
    else fuzzy((v - from) / (to - from))
  }
  
  def defuzzify(f: fuzzy): Double = to * f.doubleValue()
}