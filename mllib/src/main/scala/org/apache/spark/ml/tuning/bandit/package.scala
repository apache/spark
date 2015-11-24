package org.apache.spark.ml.tuning

import org.apache.spark.ml.Model

package object bandit {
  type A
  type M = A <: org.apache.spark.ml.Model[A]
}
