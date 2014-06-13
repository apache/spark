package org.apache.spark.mllib.optimization

object QpAlgo extends Enumeration {
	type Algo = Value
	val LeastSquares, LeastSquaresBounded, LeastSquaresL1, LeastSquaresBoundedL1, LeastSquaresOrtho, LeastSquaresBoundedOrtho = Value
}