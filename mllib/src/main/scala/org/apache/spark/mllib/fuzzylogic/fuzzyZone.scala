package org.apache.spark.mllib.fuzzylogic

object fuzzyZone {
  def mid(to: Double, from: Double) = {
    (to - from) / 2.0 + from
  }
}

abstract class fuzzyZone protected (n: String, min: Double, max: Double) {

  require(max >= min)

  val name = n
  val from = min
  val to = max

  def mid() = fuzzyZone.mid(from, to)

  def contains(v: Double): Boolean = if (v < from || v > to) false else true

  def fuzzify(v: Double): fuzzy

  def defuzzify(f: fuzzy): Double

  override def toString(): String = { name + "(" + from + ".." + to + ")" }

}