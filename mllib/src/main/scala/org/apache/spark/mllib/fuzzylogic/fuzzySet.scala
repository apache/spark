package org.apache.spark.mllib.fuzzylogic

import scala.collection.immutable.Map

object fuzzySet {

  def apply(n: String, zl: List[fuzzyZone]) = new fuzzySet(n, zl)

}

class fuzzySet private (n: String, zl: List[fuzzyZone]) {

  val name = n

  val zones = zl

  def fuzzify(v: Double): Map[String, fuzzy] = {
    val fuzzies = scala.collection.mutable.Map[String, fuzzy]()
    for (z <- zones) {
      fuzzies(z.name) = z.fuzzify(v)
    }
    Map(fuzzies.toSeq: _*)
  }

  def fuzzies(): scala.collection.mutable.Map[String, fuzzy] = {
    val fuzzies = scala.collection.mutable.Map[String, fuzzy]()
    for (z <- zones) {
      fuzzies(z.name) = fuzzy.MIN
    }
    fuzzies
  }

  def defuzzify(fv: Map[String, fuzzy]): Double = {
    var dividend = 0.0
    var divisor = 0.0
    for (z <- zones) {
      val fuzzy = fv(z.name)
      dividend += z.defuzzify(fuzzy)
      divisor += fuzzy.doubleValue()
    }
    if (divisor != 0.0) dividend / divisor else 0.0
  }

}