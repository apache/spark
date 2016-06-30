package org.apache.spark.mllib.fuzzylogic

object fallingFuzzyZone {

  def apply(n: String, min: Double, max: Double) = new fallingFuzzyZone(n, min, max)

}

class fallingFuzzyZone private (n: String, min: Double, max: Double) extends fuzzyZone(n, min, max) {

  def fuzzify(v: Double): fuzzy = {
    if (v <= from) fuzzy.MAX
    else if (v >= to) fuzzy.MIN
    else fuzzy((to - v) / (to - from))
  }
  
  def defuzzify(f: fuzzy): Double = from * f.doubleValue()

}