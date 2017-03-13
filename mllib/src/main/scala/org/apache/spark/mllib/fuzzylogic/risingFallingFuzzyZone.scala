package org.apache.spark.mllib.fuzzylogic

object risingFallingFuzzyZone {

  def apply(n: String, min: Double, max: Double) = new risingFallingFuzzyZone(n, min, max)

  def apply(n: String, min: Double, max: Double, pk: Double) = new risingFallingFuzzyZone(n, min, max, pk)
}

class risingFallingFuzzyZone private (n: String, min: Double, max: Double, pk: Double) extends fuzzyZone(n, min, max) {

  require(pk >= from && pk <= to)

  val peak: Double = pk

  def this(n: String, min: Double, max: Double) = this(n, min, max, fuzzyZone.mid(min, max))

  override def toString(): String = { name + "(" + from + ".." + peak + ".." + to + ")" }

  def fuzzify(v: Double): fuzzy = {
    if (v <= from) fuzzy.MIN
    else if (v >= to) fuzzy.MIN
    else if (v == peak) fuzzy.MAX
    else if (v < peak) fuzzy((v - from) / (peak - from))
    else fuzzy((v - peak) / (to - peak)).not()
  }

  def defuzzify(f: fuzzy): Double = peak * f.doubleValue()
}

