package org.apache.spark.mllib.linalg

/** Represents SVD factors */
case class SingularValueDecomposition[UType, VType](U: UType, s: Vector, V: VType)
