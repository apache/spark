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

package org.apache.spark.ml.stat

import java.io._

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.ExpressionUtils.{column, expression}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * A builder object that provides summary statistics about a given column.
 *
 * Users should not directly create such builders, but instead use one of the methods in
 * [[Summarizer]].
 */
@Since("2.3.0")
sealed abstract class SummaryBuilder {
  /**
   * Returns an aggregate object that contains the summary of the column with the requested metrics.
   * @param featuresCol a column that contains features Vector object.
   * @param weightCol a column that contains weight value. Default weight is 1.0.
   * @return an aggregate column that contains the statistics. The exact content of this
   *         structure is determined during the creation of the builder.
   */
  @Since("2.3.0")
  def summary(featuresCol: Column, weightCol: Column): Column

  @Since("2.3.0")
  def summary(featuresCol: Column): Column = summary(featuresCol, lit(1.0))

}

/**
 * Tools for vectorized statistics on MLlib Vectors.
 *
 * The methods in this package provide various statistics for Vectors contained inside DataFrames.
 *
 * This class lets users pick the statistics they would like to extract for a given column. Here is
 * an example in Scala:
 * {{{
 *   import org.apache.spark.ml.linalg._
 *   import org.apache.spark.sql.Row
 *   val dataframe = ... // Some dataframe containing a feature column and a weight column
 *   val multiStatsDF = dataframe.select(
 *       Summarizer.metrics("min", "max", "count").summary($"features", $"weight")
 *   val Row(minVec, maxVec, count) = multiStatsDF.first()
 * }}}
 *
 * If one wants to get a single metric, shortcuts are also available:
 * {{{
 *   val meanDF = dataframe.select(Summarizer.mean($"features"))
 *   val Row(meanVec) = meanDF.first()
 * }}}
 *
 * Note: Currently, the performance of this interface is about 2x~3x slower than using the RDD
 * interface.
 */
@Since("2.3.0")
object Summarizer extends Logging {

  import SummaryBuilderImpl._

  /**
   * Given a list of metrics, provides a builder that it turns computes metrics from a column.
   *
   * See the documentation of [[Summarizer]] for an example.
   *
   * The following metrics are accepted (case sensitive):
   *  - mean: a vector that contains the coefficient-wise mean.
   *  - sum: a vector that contains the coefficient-wise sum.
   *  - variance: a vector that contains the coefficient-wise variance.
   *  - std: a vector that contains the coefficient-wise standard deviation.
   *  - count: the count of all vectors seen.
   *  - numNonzeros: a vector with the number of non-zeros for each coefficients
   *  - max: the maximum for each coefficient.
   *  - min: the minimum for each coefficient.
   *  - normL2: the Euclidean norm for each coefficient.
   *  - normL1: the L1 norm of each coefficient (sum of the absolute values).
   * @param metrics metrics that can be provided.
   * @return a builder.
   * @throws IllegalArgumentException if one of the metric names is not understood.
   *
   * Note: Currently, the performance of this interface is about 2x~3x slower than using the RDD
   * interface.
   */
  @Since("2.3.0")
  @scala.annotation.varargs
  def metrics(metrics: String*): SummaryBuilder = {
    require(metrics.nonEmpty, "Should include at least one metric")
    val (typedMetrics, computeMetrics) = getRelevantMetrics(metrics)
    new SummaryBuilderImpl(typedMetrics, computeMetrics)
  }

  @Since("2.3.0")
  def mean(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "mean")
  }

  @Since("2.3.0")
  def mean(col: Column): Column = mean(col, lit(1.0))

  @Since("3.0.0")
  def sum(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "sum")
  }

  @Since("3.0.0")
  def sum(col: Column): Column = sum(col, lit(1.0))

  @Since("2.3.0")
  def variance(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "variance")
  }

  @Since("2.3.0")
  def variance(col: Column): Column = variance(col, lit(1.0))

  @Since("3.0.0")
  def std(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "std")
  }

  @Since("3.0.0")
  def std(col: Column): Column = std(col, lit(1.0))

  @Since("2.3.0")
  def count(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "count")
  }

  @Since("2.3.0")
  def count(col: Column): Column = count(col, lit(1.0))

  @Since("2.3.0")
  def numNonZeros(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "numNonZeros")
  }

  @Since("2.3.0")
  def numNonZeros(col: Column): Column = numNonZeros(col, lit(1.0))

  @Since("2.3.0")
  def max(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "max")
  }

  @Since("2.3.0")
  def max(col: Column): Column = max(col, lit(1.0))

  @Since("2.3.0")
  def min(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "min")
  }

  @Since("2.3.0")
  def min(col: Column): Column = min(col, lit(1.0))

  @Since("2.3.0")
  def normL1(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "normL1")
  }

  @Since("2.3.0")
  def normL1(col: Column): Column = normL1(col, lit(1.0))

  @Since("2.3.0")
  def normL2(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "normL2")
  }

  @Since("2.3.0")
  def normL2(col: Column): Column = normL2(col, lit(1.0))

  private def getSingleMetric(col: Column, weightCol: Column, metric: String): Column = {
    val c1 = metrics(metric).summary(col, weightCol)
    c1.getField(metric).as(s"$metric($col)")
  }

  private[spark] def createSummarizerBuffer(requested: String*): SummarizerBuffer = {
    val (metrics, computeMetrics) = getRelevantMetrics(requested)
    new SummarizerBuffer(metrics, computeMetrics)
  }

  /** Get regression feature and label summarizers for provided data. */
  private[ml] def getRegressionSummarizers(
      instances: RDD[Instance],
      aggregationDepth: Int = 2,
      requested: Seq[String] = Seq("mean", "std", "count")) = {
    instances.treeAggregate(
      (Summarizer.createSummarizerBuffer(requested: _*),
        Summarizer.createSummarizerBuffer("mean", "std", "count")))(
      seqOp = (c: (SummarizerBuffer, SummarizerBuffer), instance: Instance) =>
        (c._1.add(instance.features, instance.weight),
          c._2.add(Vectors.dense(instance.label), instance.weight)),
      combOp = (c1: (SummarizerBuffer, SummarizerBuffer),
                c2: (SummarizerBuffer, SummarizerBuffer)) =>
        (c1._1.merge(c2._1), c1._2.merge(c2._2)),
      depth = aggregationDepth
    )
  }

  /** Get classification feature and label summarizers for provided data. */
  private[spark] def getClassificationSummarizers(
      instances: RDD[Instance],
      aggregationDepth: Int = 2,
      requested: Seq[String] = Seq("mean", "std", "count")) = {
    instances.treeAggregate(
      (Summarizer.createSummarizerBuffer(requested: _*), new MultiClassSummarizer))(
      seqOp = (c: (SummarizerBuffer, MultiClassSummarizer), instance: Instance) =>
        (c._1.add(instance.features, instance.weight), c._2.add(instance.label, instance.weight)),
      combOp = (c1: (SummarizerBuffer, MultiClassSummarizer),
                c2: (SummarizerBuffer, MultiClassSummarizer)) =>
        (c1._1.merge(c2._1), c1._2.merge(c2._2)),
      depth = aggregationDepth
    )
  }
}

private[ml] class SummaryBuilderImpl(
    requestedMetrics: Seq[SummaryBuilderImpl.Metric],
    requestedCompMetrics: Seq[SummaryBuilderImpl.ComputeMetric]
  ) extends SummaryBuilder {

  override def summary(featuresCol: Column, weightCol: Column): Column = {
    SummaryBuilderImpl.MetricsAggregate(
      requestedMetrics,
      requestedCompMetrics,
      featuresCol,
      weightCol,
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }
}

private[spark] object SummaryBuilderImpl extends Logging {

  def implementedMetrics: Seq[String] = allMetrics.map(_._1).sorted

  @throws[IllegalArgumentException]("When the list is empty or not a subset of known metrics")
  def getRelevantMetrics(requested: Seq[String]): (Seq[Metric], Seq[ComputeMetric]) = {
    val all = requested.map { req =>
      val (_, metric, _, deps) = allMetrics.find(_._1 == req).getOrElse {
        throw new IllegalArgumentException(s"Metric $req cannot be found." +
          s" Valid metrics are $implementedMetrics")
      }
      metric -> deps
    }
    // Do not sort, otherwise the user has to look the schema to see the order that it
    // is going to be given in.
    val metrics = all.map(_._1)
    val computeMetrics = all.flatMap(_._2).distinct.sortBy(_.toString)
    metrics -> computeMetrics
  }

  def structureForMetrics(metrics: Seq[Metric]): StructType = {
    val dict = allMetrics.map { case (name, metric, dataType, _) =>
      (metric, (name, dataType))
    }.toMap
    val fields = metrics.map(dict.apply).map { case (name, dataType) =>
      StructField(name, dataType, nullable = false)
    }
    StructType(fields)
  }

  private val vectorUDT = new VectorUDT

  /**
   * All the metrics that can be currently computed by Spark for vectors.
   *
   * This list associates the user name, the internal (typed) name, and the list of computation
   * metrics that need to de computed internally to get the final result.
   */
  private val allMetrics: Seq[(String, Metric, DataType, Seq[ComputeMetric])] = Seq(
    ("mean", Mean, vectorUDT, Seq(ComputeMean, ComputeWeightSum)),
    ("sum", Sum, vectorUDT, Seq(ComputeMean, ComputeWeightSum)),
    ("variance", Variance, vectorUDT, Seq(ComputeWeightSum, ComputeMean, ComputeM2n)),
    ("std", Std, vectorUDT, Seq(ComputeWeightSum, ComputeMean, ComputeM2n)),
    ("count", Count, LongType, Seq()),
    ("numNonZeros", NumNonZeros, vectorUDT, Seq(ComputeNNZ)),
    ("max", Max, vectorUDT, Seq(ComputeMax, ComputeNNZ)),
    ("min", Min, vectorUDT, Seq(ComputeMin, ComputeNNZ)),
    ("normL2", NormL2, vectorUDT, Seq(ComputeM2)),
    ("normL1", NormL1, vectorUDT, Seq(ComputeL1))
  )

  /**
   * The metrics that are currently implemented.
   */
  sealed trait Metric extends Serializable
  private[stat] case object Mean extends Metric
  private[stat] case object Sum extends Metric
  private[stat] case object Variance extends Metric
  private[stat] case object Std extends Metric
  private[stat] case object Count extends Metric
  private[stat] case object NumNonZeros extends Metric
  private[stat] case object Max extends Metric
  private[stat] case object Min extends Metric
  private[stat] case object NormL2 extends Metric
  private[stat] case object NormL1 extends Metric

  /**
   * The running metrics that are going to be computed.
   *
   * There is a bipartite graph between the metrics and the computed metrics.
   */
  sealed trait ComputeMetric extends Serializable
  private[stat] case object ComputeMean extends ComputeMetric
  private[stat] case object ComputeM2n extends ComputeMetric
  private[stat] case object ComputeM2 extends ComputeMetric
  private[stat] case object ComputeL1 extends ComputeMetric
  private[stat] case object ComputeWeightSum extends ComputeMetric
  private[stat] case object ComputeNNZ extends ComputeMetric
  private[stat] case object ComputeMax extends ComputeMetric
  private[stat] case object ComputeMin extends ComputeMetric


  private case class MetricsAggregate(
      requestedMetrics: Seq[Metric],
      requestedComputeMetrics: Seq[ComputeMetric],
      featuresExpr: Expression,
      weightExpr: Expression,
      mutableAggBufferOffset: Int,
      inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[SummarizerBuffer]
    with ImplicitCastInputTypes
    with BinaryLike[Expression] {

    override def eval(state: SummarizerBuffer): Any = {
      val metrics = requestedMetrics.map {
        case Mean => vectorUDT.serialize(state.mean)
        case Sum => vectorUDT.serialize(state.sum)
        case Variance => vectorUDT.serialize(state.variance)
        case Std => vectorUDT.serialize(state.std)
        case Count => state.count
        case NumNonZeros => vectorUDT.serialize(state.numNonzeros)
        case Max => vectorUDT.serialize(state.max)
        case Min => vectorUDT.serialize(state.min)
        case NormL2 => vectorUDT.serialize(state.normL2)
        case NormL1 => vectorUDT.serialize(state.normL1)
      }
      InternalRow.apply(metrics: _*)
    }

    override def inputTypes: Seq[DataType] = vectorUDT :: DoubleType :: Nil

    override def left: Expression = featuresExpr
    override def right: Expression = weightExpr

    override protected def withNewChildrenInternal(
        newLeft: Expression, newRight: Expression): MetricsAggregate =
      copy(featuresExpr = newLeft, weightExpr = newRight)

    override def update(state: SummarizerBuffer, row: InternalRow): SummarizerBuffer = {
      val features = vectorUDT.deserialize(featuresExpr.eval(row))
      val weight = weightExpr.eval(row).asInstanceOf[Double]
      state.add(features, weight)
      state
    }

    override def merge(state: SummarizerBuffer,
      other: SummarizerBuffer): SummarizerBuffer = {
      state.merge(other)
    }

    override def nullable: Boolean = false

    override def createAggregationBuffer(): SummarizerBuffer
      = new SummarizerBuffer(requestedMetrics, requestedComputeMetrics)

    override def serialize(state: SummarizerBuffer): Array[Byte] = {
      // TODO: Use ByteBuffer to optimize
      Utils.serialize(state)
    }

    override def deserialize(bytes: Array[Byte]): SummarizerBuffer = {
      // TODO: Use ByteBuffer to optimize
      Utils.deserialize(bytes)
    }

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): MetricsAggregate = {
      copy(mutableAggBufferOffset = newMutableAggBufferOffset)
    }

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): MetricsAggregate = {
      copy(inputAggBufferOffset = newInputAggBufferOffset)
    }

    override lazy val dataType: DataType = structureForMetrics(requestedMetrics)

    override def prettyName: String = "aggregate_metrics"

  }
}

private[spark] class SummarizerBuffer(
    requestedMetrics: Seq[SummaryBuilderImpl.Metric],
    requestedCompMetrics: Seq[SummaryBuilderImpl.ComputeMetric]) extends Serializable {
  import SummaryBuilderImpl._

  private var n = 0
  private var currMean: Array[Double] = null
  private var currM2n: Array[Double] = null
  private var currM2: Array[Double] = null
  private var currL1: Array[Double] = null
  private var totalCnt: Long = 0
  private var totalWeightSum: Double = 0.0
  private var weightSquareSum: Double = 0.0
  private var currWeightSum: Array[Double] = null
  private var nnz: Array[Long] = null
  private var currMax: Array[Double] = null
  private var currMin: Array[Double] = null

  def this() = {
    this(
      Seq(
        SummaryBuilderImpl.Mean,
        SummaryBuilderImpl.Sum,
        SummaryBuilderImpl.Variance,
        SummaryBuilderImpl.Std,
        SummaryBuilderImpl.Count,
        SummaryBuilderImpl.NumNonZeros,
        SummaryBuilderImpl.Max,
        SummaryBuilderImpl.Min,
        SummaryBuilderImpl.NormL2,
        SummaryBuilderImpl.NormL1),
      Seq(
        SummaryBuilderImpl.ComputeMean,
        SummaryBuilderImpl.ComputeM2n,
        SummaryBuilderImpl.ComputeM2,
        SummaryBuilderImpl.ComputeL1,
        SummaryBuilderImpl.ComputeWeightSum,
        SummaryBuilderImpl.ComputeNNZ,
        SummaryBuilderImpl.ComputeMax,
        SummaryBuilderImpl.ComputeMin)
    )
  }

  def add(nonZeroIterator: Iterator[(Int, Double)], size: Int, weight: Double): this.type = {
    require(weight >= 0.0, s"sample weight, $weight has to be >= 0.0")
    if (weight == 0.0) return this

    if (n == 0) {
      require(size > 0, s"Vector should have dimension larger than zero.")
      n = size

      if (requestedCompMetrics.contains(ComputeMean)) { currMean = Array.ofDim[Double](n) }
      if (requestedCompMetrics.contains(ComputeM2n)) { currM2n = Array.ofDim[Double](n) }
      if (requestedCompMetrics.contains(ComputeM2)) { currM2 = Array.ofDim[Double](n) }
      if (requestedCompMetrics.contains(ComputeL1)) { currL1 = Array.ofDim[Double](n) }
      if (requestedCompMetrics.contains(ComputeWeightSum)) {
        currWeightSum = Array.ofDim[Double](n)
      }
      if (requestedCompMetrics.contains(ComputeNNZ)) { nnz = Array.ofDim[Long](n) }
      if (requestedCompMetrics.contains(ComputeMax)) {
        currMax = Array.fill[Double](n)(Double.MinValue)
      }
      if (requestedCompMetrics.contains(ComputeMin)) {
        currMin = Array.fill[Double](n)(Double.MaxValue)
      }
    }

    require(n == size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $n but got $size.")

    if (nonZeroIterator.nonEmpty) {
      val localCurrMean = currMean
      val localCurrM2n = currM2n
      val localCurrM2 = currM2
      val localCurrL1 = currL1
      val localCurrWeightSum = currWeightSum
      val localNumNonzeros = nnz
      val localCurrMax = currMax
      val localCurrMin = currMin
      nonZeroIterator.foreach { case (index, value) =>
        if (localCurrMax != null && localCurrMax(index) < value) {
          localCurrMax(index) = value
        }
        if (localCurrMin != null && localCurrMin(index) > value) {
          localCurrMin(index) = value
        }

        if (localCurrWeightSum != null) {
          if (localCurrMean != null) {
            val prevMean = localCurrMean(index)
            val diff = value - prevMean
            localCurrMean(index) = prevMean +
              weight * diff / (localCurrWeightSum(index) + weight)

            if (localCurrM2n != null) {
              localCurrM2n(index) += weight * (value - localCurrMean(index)) * diff
            }
          }
          localCurrWeightSum(index) += weight
        }

        if (localCurrM2 != null) {
          localCurrM2(index) += weight * value * value
        }
        if (localCurrL1 != null) {
          localCurrL1(index) += weight * math.abs(value)
        }

        if (localNumNonzeros != null) {
          localNumNonzeros(index) += 1
        }
      }
    }

    totalWeightSum += weight
    weightSquareSum += weight * weight
    totalCnt += 1
    this
  }

  /**
   * Add a new sample to this summarizer, and update the statistical summary.
   */
  def add(instance: Vector, weight: Double): this.type =
    add(instance.nonZeroIterator, instance.size, weight)

  def add(instance: Vector): this.type = add(instance, 1.0)

  /**
   * Merge another SummarizerBuffer, and update the statistical summary.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other MultivariateOnlineSummarizer to be merged.
   */
  def merge(other: SummarizerBuffer): this.type = {
    if (this.totalWeightSum != 0.0 && other.totalWeightSum != 0.0) {
      require(n == other.n, s"Dimensions mismatch when merging with another summarizer. " +
        s"Expecting $n but got ${other.n}.")
      totalCnt += other.totalCnt
      totalWeightSum += other.totalWeightSum
      weightSquareSum += other.weightSquareSum
      var i = 0
      while (i < n) {
        if (currWeightSum != null) {
          val thisWeightSum = currWeightSum(i)
          val otherWeightSum = other.currWeightSum(i)
          val totalWeightSum = thisWeightSum + otherWeightSum

          if (totalWeightSum != 0.0) {
            if (currMean != null) {
              val deltaMean = other.currMean(i) - currMean(i)
              // merge mean together
              currMean(i) += deltaMean * otherWeightSum / totalWeightSum

              if (currM2n != null) {
                // merge m2n together
                currM2n(i) += other.currM2n(i) +
                  deltaMean * deltaMean * thisWeightSum * otherWeightSum / totalWeightSum
              }
            }
          }
          currWeightSum(i) = totalWeightSum
        }

        // merge m2 together
        if (currM2 != null) { currM2(i) += other.currM2(i) }
        // merge l1 together
        if (currL1 != null) { currL1(i) += other.currL1(i) }
        // merge max and min
        if (currMax != null) { currMax(i) = math.max(currMax(i), other.currMax(i)) }
        if (currMin != null) { currMin(i) = math.min(currMin(i), other.currMin(i)) }
        if (nnz != null) { nnz(i) += other.nnz(i) }
        i += 1
      }
    } else if (totalWeightSum == 0.0 && other.totalWeightSum != 0.0) {
      this.n = other.n
      if (other.currMean != null) { this.currMean = other.currMean.clone() }
      if (other.currM2n != null) { this.currM2n = other.currM2n.clone() }
      if (other.currM2 != null) { this.currM2 = other.currM2.clone() }
      if (other.currL1 != null) { this.currL1 = other.currL1.clone() }
      this.totalCnt = other.totalCnt
      this.totalWeightSum = other.totalWeightSum
      this.weightSquareSum = other.weightSquareSum
      if (other.currWeightSum != null) { this.currWeightSum = other.currWeightSum.clone() }
      if (other.nnz != null) { this.nnz = other.nnz.clone() }
      if (other.currMax != null) { this.currMax = other.currMax.clone() }
      if (other.currMin != null) { this.currMin = other.currMin.clone() }
    }
    this
  }

  /**
   * Sample mean of each dimension.
   */
  def mean: Vector = {
    require(requestedMetrics.contains(Mean))
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realMean = Array.ofDim[Double](n)
    var i = 0
    while (i < n) {
      realMean(i) = currMean(i) * (currWeightSum(i) / totalWeightSum)
      i += 1
    }
    Vectors.dense(realMean)
  }

  /**
   * Sum of each dimension.
   */
  def sum: Vector = {
    require(requestedMetrics.contains(Sum))
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realSum = Array.ofDim[Double](n)
    var i = 0
    while (i < n) {
      realSum(i) = currMean(i) * currWeightSum(i)
      i += 1
    }
    Vectors.dense(realSum)
  }

  /**
   * Unbiased estimate of sample variance of each dimension.
   */
  def variance: Vector = {
    require(requestedMetrics.contains(Variance))
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realVariance = computeVariance
    Vectors.dense(realVariance)
  }

  /**
   * Unbiased estimate of standard deviation of each dimension.
   */
  def std: Vector = {
    require(requestedMetrics.contains(Std))
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realVariance = computeVariance
    Vectors.dense(realVariance.map(math.sqrt))
  }

  private def computeVariance: Array[Double] = {
    val realVariance = Array.ofDim[Double](n)
    val denominator = totalWeightSum - (weightSquareSum / totalWeightSum)

    // Sample variance is computed, if the denominator is less than 0, the variance is just 0.
    if (denominator > 0.0) {
      val deltaMean = currMean
      var i = 0
      val len = currM2n.length
      while (i < len) {
        // We prevent variance from negative value caused by numerical error.
        realVariance(i) = math.max((currM2n(i) + deltaMean(i) * deltaMean(i) * currWeightSum(i) *
          (totalWeightSum - currWeightSum(i)) / totalWeightSum) / denominator, 0.0)
        i += 1
      }
    }
    realVariance
  }

  /**
   * Sample size.
   */
  def count: Long = totalCnt

  /**
   * Sum of weights.
   */
  def weightSum: Double = totalWeightSum

  /**
   * Number of nonzero elements in each dimension.
   *
   */
  def numNonzeros: Vector = {
    require(requestedMetrics.contains(NumNonZeros))
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    Vectors.dense(nnz.map(_.toDouble))
  }

  /**
   * Maximum value of each dimension.
   */
  def max: Vector = {
    require(requestedMetrics.contains(Max))
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMax(i) < 0.0)) currMax(i) = 0.0
      i += 1
    }
    Vectors.dense(currMax)
  }

  /**
   * Minimum value of each dimension.
   */
  def min: Vector = {
    require(requestedMetrics.contains(Min))
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMin(i) > 0.0)) currMin(i) = 0.0
      i += 1
    }
    Vectors.dense(currMin)
  }

  /**
   * L2 (Euclidean) norm of each dimension.
   */
  def normL2: Vector = {
    require(requestedMetrics.contains(NormL2))
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realMagnitude = Array.ofDim[Double](n)

    var i = 0
    val len = currM2.length
    while (i < len) {
      realMagnitude(i) = math.sqrt(currM2(i))
      i += 1
    }
    Vectors.dense(realMagnitude)
  }

  /**
   * L1 norm of each dimension.
   */
  def normL1: Vector = {
    require(requestedMetrics.contains(NormL1))
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    Vectors.dense(currL1)
  }
}
