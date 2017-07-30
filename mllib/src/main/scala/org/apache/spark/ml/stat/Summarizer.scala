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

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors, VectorUDT}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeArrayData, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._

/**
 * A builder object that provides summary statistics about a given column.
 *
 * Users should not directly create such builders, but instead use one of the methods in
 * [[Summarizer]].
 */
@Since("2.2.0")
abstract class SummaryBuilder {
  /**
   * Returns an aggregate object that contains the summary of the column with the requested metrics.
   * @param featuresCol a column that contains features Vector object.
   * @param weightCol a column that contains weight value.
   * @return an aggregate column that contains the statistics. The exact content of this
   *         structure is determined during the creation of the builder.
   */
  @Since("2.2.0")
  def summary(featuresCol: Column, weightCol: Column): Column

  @Since("2.2.0")
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
 *   val dataframe = ... // Some dataframe containing a feature column
 *   val allStats = dataframe.select(Summarizer.metrics("min", "max").summary($"features"))
 *   val Row(min_, max_) = allStats.first()
 * }}}
 *
 * If one wants to get a single metric, shortcuts are also available:
 * {{{
 *   val meanDF = dataframe.select(Summarizer.mean($"features"))
 *   val Row(mean_) = meanDF.first()
 * }}}
 */
@Since("2.2.0")
object Summarizer extends Logging {

  import SummaryBuilderImpl._

  /**
   * Given a list of metrics, provides a builder that it turns computes metrics from a column.
   *
   * See the documentation of [[Summarizer]] for an example.
   *
   * The following metrics are accepted (case sensitive):
   *  - mean: a vector that contains the coefficient-wise mean.
   *  - variance: a vector tha contains the coefficient-wise variance.
   *  - count: the count of all vectors seen.
   *  - numNonzeros: a vector with the number of non-zeros for each coefficients
   *  - max: the maximum for each coefficient.
   *  - min: the minimum for each coefficient.
   *  - normL2: the Euclidian norm for each coefficient.
   *  - normL1: the L1 norm of each coefficient (sum of the absolute values).
   * @param firstMetric the metric being provided
   * @param metrics additional metrics that can be provided.
   * @return a builder.
   * @throws IllegalArgumentException if one of the metric names is not understood.
   */
  @Since("2.2.0")
  def metrics(firstMetric: String, metrics: String*): SummaryBuilder = {
    val (typedMetrics, computeMetrics) = getRelevantMetrics(Seq(firstMetric) ++ metrics)
    new SummaryBuilderImpl(typedMetrics, computeMetrics)
  }

  def mean(col: Column): Column = getSingleMetric(col, "mean")

  def variance(col: Column): Column = getSingleMetric(col, "variance")

  def count(col: Column): Column = getSingleMetric(col, "count")

  def numNonZeros(col: Column): Column = getSingleMetric(col, "numNonZeros")

  def max(col: Column): Column = getSingleMetric(col, "max")

  def min(col: Column): Column = getSingleMetric(col, "min")

  def normL1(col: Column): Column = getSingleMetric(col, "normL1")

  def normL2(col: Column): Column = getSingleMetric(col, "normL2")

  private def getSingleMetric(col: Column, metric: String): Column = {
    val c1 = metrics(metric).summary(col)
    c1.getField(metric).as(s"$metric($col)")
  }
}

private[ml] class SummaryBuilderImpl(
    requestedMetrics: Seq[SummaryBuilderImpl.Metrics],
    requestedCompMetrics: Seq[SummaryBuilderImpl.ComputeMetrics]
  ) extends SummaryBuilder {

  override def summary(featuresCol: Column, weightCol: Column): Column = {

    val agg = SummaryBuilderImpl.MetricsAggregate(
      requestedMetrics,
      featuresCol.expr,
      weightCol.expr,
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)

    new Column(AggregateExpression(agg, mode = Complete, isDistinct = false))
  }
}

private[ml]
object SummaryBuilderImpl extends Logging {

  def implementedMetrics: Seq[String] = allMetrics.map(_._1).sorted

  @throws[IllegalArgumentException]("When the list is empty or not a subset of known metrics")
  def getRelevantMetrics(requested: Seq[String]): (Seq[Metrics], Seq[ComputeMetrics]) = {
    val all = requested.map { req =>
      val (_, metric, _, deps) = allMetrics.find(tup => tup._1 == req).getOrElse {
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

  def structureForMetrics(metrics: Seq[Metrics]): StructType = {
    val dct = allMetrics.map { case (n, m, dt, _) => (m, (n, dt)) }.toMap
    val fields = metrics.map(dct.apply).map { case (n, dt) =>
        StructField(n, dt, nullable = false)
    }
    StructType(fields)
  }

  private val arrayDType = ArrayType(DoubleType, containsNull = false)
  private val arrayLType = ArrayType(LongType, containsNull = false)

  /**
   * All the metrics that can be currently computed by Spark for vectors.
   *
   * This list associates the user name, the internal (typed) name, and the list of computation
   * metrics that need to de computed internally to get the final result.
   */
  private val allMetrics: Seq[(String, Metrics, DataType, Seq[ComputeMetrics])] = Seq(
    ("mean", Mean, arrayDType, Seq(ComputeMean, ComputeWeightSum)),
    ("variance", Variance, arrayDType, Seq(ComputeWeightSum, ComputeMean, ComputeM2n)),
    ("count", Count, LongType, Seq()),
    ("numNonZeros", NumNonZeros, arrayLType, Seq(ComputeNNZ)),
    ("max", Max, arrayDType, Seq(ComputeMax)),
    ("min", Min, arrayDType, Seq(ComputeMin)),
    ("normL2", NormL2, arrayDType, Seq(ComputeM2)),
    ("normL1", NormL1, arrayDType, Seq(ComputeL1))
  )

  /**
   * The metrics that are currently implemented.
   */
  sealed trait Metrics
  case object Mean extends Metrics
  case object Variance extends Metrics
  case object Count extends Metrics
  case object NumNonZeros extends Metrics
  case object Max extends Metrics
  case object Min extends Metrics
  case object NormL2 extends Metrics
  case object NormL1 extends Metrics

  /**
   * The running metrics that are going to be computed.
   *
   * There is a bipartite graph between the metrics and the computed metrics.
   */
  sealed trait ComputeMetrics
  case object ComputeMean extends ComputeMetrics
  case object ComputeM2n extends ComputeMetrics
  case object ComputeM2 extends ComputeMetrics
  case object ComputeL1 extends ComputeMetrics
  case object ComputeWeightSum extends ComputeMetrics
  case object ComputeNNZ extends ComputeMetrics
  case object ComputeMax extends ComputeMetrics
  case object ComputeMin extends ComputeMetrics

  private case class MetricsAggregate(
      requested: Seq[Metrics],
      featuresExpr: Expression,
      weightExpr: Expression,
      mutableAggBufferOffset: Int,
      inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[MultivariateOnlineSummarizer] {

    override def eval(state: MultivariateOnlineSummarizer): InternalRow = {
      val metrics = requested.map({
        case Mean => UnsafeArrayData.fromPrimitiveArray(state.mean.toArray)
        case Variance => UnsafeArrayData.fromPrimitiveArray(state.variance.toArray)
        case Count => state.count
        case NumNonZeros => UnsafeArrayData.fromPrimitiveArray(
          state.numNonzeros.toArray.map(_.toLong))
        case Max => UnsafeArrayData.fromPrimitiveArray(state.max.toArray)
        case Min => UnsafeArrayData.fromPrimitiveArray(state.min.toArray)
        case NormL2 => UnsafeArrayData.fromPrimitiveArray(state.normL2.toArray)
        case NormL1 => UnsafeArrayData.fromPrimitiveArray(state.normL1.toArray)
      })
      InternalRow.apply(metrics: _*)
    }

    override def children: Seq[Expression] = featuresExpr :: weightExpr :: Nil

    override def update(state: MultivariateOnlineSummarizer, row: InternalRow)
        : MultivariateOnlineSummarizer = {

      val features = udt.deserialize(featuresExpr.eval(row))
      val weight = weightExpr.eval(row).asInstanceOf[Double]

      state.add(OldVectors.fromML(features), weight)
      state
    }

    override def merge(state: MultivariateOnlineSummarizer,
                       other: MultivariateOnlineSummarizer): MultivariateOnlineSummarizer = {
      state.merge(other)
    }


    override def nullable: Boolean = false

    override def createAggregationBuffer(): MultivariateOnlineSummarizer
      = new MultivariateOnlineSummarizer

    override def serialize(state: MultivariateOnlineSummarizer): Array[Byte] = {
      val bos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(bos)
      oos.writeObject(state)
      bos.toByteArray
    }

    override def deserialize(bytes: Array[Byte]): MultivariateOnlineSummarizer = {
      val bis = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(bis)
      ois.readObject().asInstanceOf[MultivariateOnlineSummarizer]
    }

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int)
      : MetricsAggregate = {
      copy(mutableAggBufferOffset = newMutableAggBufferOffset)
    }

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): MetricsAggregate = {
      copy(inputAggBufferOffset = newInputAggBufferOffset)
    }

    override lazy val dataType: DataType = structureForMetrics(requested)

    override def prettyName: String = "aggregate_metrics"

  }

  private[this] val udt = new VectorUDT

}
