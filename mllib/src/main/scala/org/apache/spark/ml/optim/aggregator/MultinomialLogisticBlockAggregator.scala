/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.ml.optim.aggregator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.InstanceBlock
import org.apache.spark.ml.impl.Utils
import org.apache.spark.ml.linalg._

/**
 * MultinomialLogisticBlockAggregator computes the gradient and loss used in multinomial logistic
 * classification for blocks in sparse or dense matrix in an online fashion.
 *
 * Two MultinomialLogisticBlockAggregator can be merged together to have a summary of loss and
 * gradient of the corresponding joint dataset.
 *
 * NOTE: The feature values are expected to already have be scaled (multiplied by bcInverseStd,
 * but NOT centered) before computation.
 *
 * @param bcCoefficients The coefficients corresponding to the features.
 * @param fitIntercept   Whether to fit an intercept term.
 * @param fitWithMean    Whether to center the data with mean before training, in a virtual way.
 *                       If true, we MUST adjust the intercept of both initial coefficients and
 *                       final solution in the caller.
 * @note In order to avoid unnecessary computation during calculation of the gradient updates
 * we lay out the coefficients in column major order during training. We convert back to row
 * major order when we create the model, since this form is optimal for the matrix operations
 * used for prediction.
 */
private[ml] class MultinomialLogisticBlockAggregator(
    bcInverseStd: Broadcast[Array[Double]],
    bcScaledMean: Broadcast[Array[Double]],
    fitIntercept: Boolean,
    fitWithMean: Boolean)(bcCoefficients: Broadcast[Vector])
  extends DifferentiableLossAggregator[InstanceBlock, MultinomialLogisticBlockAggregator]
    with Logging {

  if (fitWithMean) {
    require(fitIntercept, s"for training without intercept, should not center the vectors")
    require(bcScaledMean != null && bcScaledMean.value.length == bcInverseStd.value.length,
      "scaled means is required when center the vectors")
  }

  private val numFeatures = bcInverseStd.value.length
  protected override val dim: Int = bcCoefficients.value.size
  private val numFeaturesPlusIntercept = if (fitIntercept) numFeatures + 1 else numFeatures
  private val numClasses = dim / numFeaturesPlusIntercept
  require(dim == numClasses * numFeaturesPlusIntercept)

  @transient private lazy val coefficientsArray = bcCoefficients.value match {
    case DenseVector(values) => values
    case _ => throw new IllegalArgumentException(s"coefficients only supports dense vector but " +
      s"got type ${bcCoefficients.value.getClass}.)")
  }

  @transient private lazy val linear = if (fitIntercept) {
    new DenseMatrix(numClasses, numFeatures, coefficientsArray.take(numClasses * numFeatures))
  } else {
    new DenseMatrix(numClasses, numFeatures, coefficientsArray)
  }

  @transient private lazy val intercept = if (fitIntercept) {
    new DenseVector(coefficientsArray.takeRight(numClasses))
  } else {
    null
  }

  // pre-computed margin of an empty vector.
  // with this variable as an offset, for a sparse vector, we only need to
  // deal with non-zero values in prediction.
  @transient private lazy val marginOffset = if (fitWithMean) {
    val offset = new DenseVector(coefficientsArray.takeRight(numClasses)) // intercept
    BLAS.gemv(-1.0, linear, Vectors.dense(bcScaledMean.value), 1.0, offset)
    offset
  } else {
    null
  }

  @transient private var buffer: Array[Double] = _

  /**
   * Add a new training instance block to this BinaryLogisticBlockAggregator, and update the loss
   * and gradient of the objective function.
   *
   * @param block The instance block of data point to be added.
   * @return This BinaryLogisticBlockAggregator object.
   */
  def add(block: InstanceBlock): this.type = {
    require(block.matrix.isTransposed)
    require(numFeatures == block.numFeatures, s"Dimensions mismatch when adding new " +
      s"instance. Expecting $numFeatures but got ${block.numFeatures}.")
    require(block.weightIter.forall(_ >= 0),
      s"instance weights ${block.weightIter.mkString("[", ",", "]")} has to be >= 0.0")

    if (block.weightIter.forall(_ == 0)) return this
    val size = block.size

    if (buffer == null || buffer.length < size * numClasses) {
      buffer = Array.ofDim[Double](size * numClasses)
    }

    // arr here represents margins, shape: S X C
    val arr = buffer
    if (fitIntercept) {
      val offset = if (fitWithMean) marginOffset else intercept
      var j = 0
      while (j < numClasses) {
        if (offset(j) != 0) java.util.Arrays.fill(arr, j * size, (j + 1) * size, offset(j))
        j += 1
      }
      BLAS.gemm(1.0, block.matrix, linear.transpose, 1.0, arr)
    } else {
      BLAS.gemm(1.0, block.matrix, linear.transpose, 0.0, arr)
    }

    // in-place convert margins to multipliers
    // then, mat/arr represents multipliers
    var localLossSum = 0.0
    var localWeightSum = 0.0
    var i = 0
    while (i < size) {
      val weight = block.getWeight(i)
      localWeightSum += weight
      if (weight > 0) {
        val labelIndex = i + block.getLabel(i).toInt * size
        Utils.softmax(arr, numClasses, i, size, arr) // prob
        localLossSum -= weight * math.log(arr(labelIndex))
        if (weight != 1) BLAS.javaBLAS.dscal(numClasses, weight, arr, i, size)
        arr(labelIndex) -= weight
      } else {
        BLAS.javaBLAS.dscal(numClasses, 0, arr, i, size)
      }
      i += 1
    }
    lossSum += localLossSum
    weightSum += localWeightSum

    // mat (multipliers):             S X C, dense                                N
    // mat.transpose (multipliers):   C X S, dense                                T
    // block.matrix (data):           S X F, unknown type                         T
    // gradSumMat (gradientSumArray): C X FPI (numFeaturesPlusIntercept), dense   N
    block.matrix match {
      case dm: DenseMatrix =>
        // gradientSumArray[0 : F X C] += mat.T X dm
        BLAS.nativeBLAS.dgemm("T", "T", numClasses, numFeatures, size, 1.0,
          arr, size, dm.values, numFeatures, 1.0, gradientSumArray, numClasses)

      case sm: SparseMatrix =>
        // dedicated sparse GEMM implementation for transposed C: C += A * B, where:
        // A.isTransposed=false, B.isTransposed=false, C.isTransposed=true
        // alpha = 1.0, beta = 1.0
        val A = sm.transpose
        val kA = A.numCols
        val Avals = A.values
        val ArowIndices = A.rowIndices
        val AcolPtrs = A.colPtrs

        val Bvals = arr
        val nB = numClasses
        val kB = size

        var colCounterForB = 0
        while (colCounterForB < nB) {
          var colCounterForA = 0 // The column of A to multiply with the row of B
          val Bstart = colCounterForB * kB
          while (colCounterForA < kA) {
            var i = AcolPtrs(colCounterForA)
            val indEnd = AcolPtrs(colCounterForA + 1)
            val Bval = Bvals(Bstart + colCounterForA)
            while (i < indEnd) {
              // different from BLAS.gemm, here gradientSumArray is NOT Transposed
              gradientSumArray(colCounterForB + nB * ArowIndices(i)) += Avals(i) * Bval
              i += 1
            }
            colCounterForA += 1
          }
          colCounterForB += 1
        }
    }

    if (fitIntercept) {
      val multiplierSum = Array.ofDim[Double](numClasses)
      var j = 0
      while (j < numClasses) {
        var i = j * size
        val end = i + size
        while (i < end) { multiplierSum(j) += arr(i); i += 1 }
        j += 1
      }

      if (fitWithMean) {
        // above update of the linear part of gradientSumArray does NOT take the centering
        // into account, here we need to adjust this part.
        // following BLAS.dger operation equals to: gradientSumArray[0 : F X C] -= mat.T X _mm_,
        // where _mm_ is a matrix of size S X F with each row equals to array ScaledMean.
        BLAS.nativeBLAS.dger(numClasses, numFeatures, -1.0, multiplierSum, 1,
          bcScaledMean.value, 1, gradientSumArray, numClasses)
      }

      BLAS.javaBLAS.daxpy(numClasses, 1.0, multiplierSum, 0, 1,
        gradientSumArray, numClasses * numFeatures, 1)
    }

    this
  }
}
