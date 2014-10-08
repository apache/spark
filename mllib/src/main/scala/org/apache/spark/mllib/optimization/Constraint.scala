package org.apache.spark.mllib.optimization

object Constraint extends Enumeration {
  type Constraint = Value
  val SMOOTH, POSITIVE, BOUNDS, SPARSE, EQUALITY = Value
}