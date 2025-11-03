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
package org.apache.spark.mllib.regression

import java.io.Serializable
import java.lang.{Double => JDouble}
import java.util.Arrays.binarySearch

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{RangePartitioner, SparkContext}
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.{JavaDoubleRDD, JavaRDD}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.ArrayImplicits._

/**
 * Regression model for isotonic regression.
 *
 * @param boundaries Array of boundaries for which predictions are known.
 *                   Boundaries must be sorted in increasing order.
 * @param predictions Array of predictions associated to the boundaries at the same index.
 *                    Results of isotonic regression and therefore monotone.
 * @param isotonic indicates whether this is isotonic or antitonic.
 *
 */
@Since("1.3.0")
class IsotonicRegressionModel @Since("1.3.0") (
    @Since("1.3.0") val boundaries: Array[Double],
    @Since("1.3.0") val predictions: Array[Double],
    @Since("1.3.0") val isotonic: Boolean) extends Serializable with Saveable {

  private val predictionOrd = if (isotonic) Ordering[Double] else Ordering[Double].reverse

  require(boundaries.length == predictions.length)
  assertOrdered(boundaries)
  assertOrdered(predictions)(predictionOrd)

  /**
   * A Java-friendly constructor that takes two Iterable parameters and one Boolean parameter.
   */
  @Since("1.4.0")
  def this(boundaries: java.lang.Iterable[Double],
      predictions: java.lang.Iterable[Double],
      isotonic: java.lang.Boolean) = {
    this(boundaries.asScala.toArray, predictions.asScala.toArray, isotonic)
  }

  /** Asserts the input array is monotone with the given ordering. */
  private def assertOrdered(xs: Array[Double])(implicit ord: Ordering[Double]): Unit = {
    var i = 1
    val len = xs.length
    while (i < len) {
      require(ord.compare(xs(i - 1), xs(i)) <= 0,
        s"Elements (${xs(i - 1)}, ${xs(i)}) are not ordered.")
      i += 1
    }
  }

  /**
   * Predict labels for provided features.
   * Using a piecewise linear function.
   *
   * @param testData Features to be labeled.
   * @return Predicted labels.
   *
   */
  @Since("1.3.0")
  def predict(testData: RDD[Double]): RDD[Double] = {
    testData.map(predict)
  }

  /**
   * Predict labels for provided features.
   * Using a piecewise linear function.
   *
   * @param testData Features to be labeled.
   * @return Predicted labels.
   *
   */
  @Since("1.3.0")
  def predict(testData: JavaDoubleRDD): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(predict(testData.rdd.retag.asInstanceOf[RDD[Double]]))
  }

  /**
   * Predict a single label.
   * Using a piecewise linear function.
   *
   * @param testData Feature to be labeled.
   * @return Predicted label.
   *         1) If testData exactly matches a boundary then associated prediction is returned.
   *           In case there are multiple predictions with the same boundary then one of them
   *           is returned. Which one is undefined (same as java.util.Arrays.binarySearch).
   *         2) If testData is lower or higher than all boundaries then first or last prediction
   *           is returned respectively. In case there are multiple predictions with the same
   *           boundary then the lowest or highest is returned respectively.
   *         3) If testData falls between two values in boundary array then prediction is treated
   *           as piecewise linear function and interpolated value is returned. In case there are
   *           multiple values with the same boundary then the same rules as in 2) are used.
   *
   */
  @Since("1.3.0")
  def predict(testData: Double): Double = {

    def linearInterpolation(x1: Double, y1: Double, x2: Double, y2: Double, x: Double): Double = {
      y1 + (y2 - y1) * (x - x1) / (x2 - x1)
    }

    val foundIndex = binarySearch(boundaries, testData)
    val insertIndex = -foundIndex - 1

    // Find if the index was lower than all values,
    // higher than all values, in between two values or exact match.
    if (insertIndex == 0) {
      predictions.head
    } else if (insertIndex == boundaries.length) {
      predictions.last
    } else if (foundIndex < 0) {
      linearInterpolation(
        boundaries(insertIndex - 1),
        predictions(insertIndex - 1),
        boundaries(insertIndex),
        predictions(insertIndex),
        testData)
    } else {
      predictions(foundIndex)
    }
  }

  /** A convenient method for boundaries called by the Python API. */
  private[mllib] def boundaryVector: Vector = Vectors.dense(boundaries)

  /** A convenient method for boundaries called by the Python API. */
  private[mllib] def predictionVector: Vector = Vectors.dense(predictions)

  @Since("1.4.0")
  override def save(sc: SparkContext, path: String): Unit = {
    IsotonicRegressionModel.SaveLoadV1_0.save(sc, path, boundaries, predictions, isotonic)
  }
}

@Since("1.4.0")
object IsotonicRegressionModel extends Loader[IsotonicRegressionModel] {

  import org.apache.spark.mllib.util.Loader._

  private object SaveLoadV1_0 {

    def thisFormatVersion: String = "1.0"

    /** Hard-code class name string in case it changes in the future */
    def thisClassName: String = "org.apache.spark.mllib.regression.IsotonicRegressionModel"

    /** Model data for model import/export */
    case class Data(boundary: Double, prediction: Double)

    def save(
        sc: SparkContext,
        path: String,
        boundaries: Array[Double],
        predictions: Array[Double],
        isotonic: Boolean): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
          ("isotonic" -> isotonic)))
      spark.createDataFrame(Seq(Tuple1(metadata))).write.text(metadataPath(path))

      spark.createDataFrame(
        boundaries.toImmutableArraySeq.zip(predictions).map { case (b, p) => Data(b, p) }
      ).write.parquet(dataPath(path))
    }

    def load(sc: SparkContext, path: String): (Array[Double], Array[Double]) = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val dataRDD = spark.read.parquet(dataPath(path))

      checkSchema[Data](dataRDD.schema)
      val dataArray = dataRDD.select("boundary", "prediction").collect()
      val (boundaries, predictions) = dataArray.map { x =>
        (x.getDouble(0), x.getDouble(1))
      }.toList.sortBy(_._1).unzip
      (boundaries.toArray, predictions.toArray)
    }
  }

  @Since("1.4.0")
  override def load(sc: SparkContext, path: String): IsotonicRegressionModel = {
    implicit val formats: Formats = DefaultFormats
    val (loadedClassName, version, metadata) = loadMetadata(sc, path)
    val isotonic = (metadata \ "isotonic").extract[Boolean]
    val classNameV1_0 = SaveLoadV1_0.thisClassName
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val (boundaries, predictions) = SaveLoadV1_0.load(sc, path)
        new IsotonicRegressionModel(boundaries, predictions, isotonic)
      case _ => throw new Exception(
        s"IsotonicRegressionModel.load did not recognize model with (className, format version): " +
        s"($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)"
      )
    }
  }
}

/**
 * Isotonic regression.
 * Currently implemented using parallelized pool adjacent violators algorithm.
 * Only univariate (single feature) algorithm supported.
 *
 * Sequential PAV implementation based on:
 * Grotzinger, S. J., and C. Witzgall.
 *   "Projections onto order simplexes." Applied mathematics and Optimization 12.1 (1984): 247-270.
 *
 * Sequential PAV parallelization based on:
 * Kearsley, Anthony J., Richard A. Tapia, and Michael W. Trosset.
 *   "An approach to parallelizing isotonic regression."
 *   Applied Mathematics and Parallel Computing. Physica-Verlag HD, 1996. 141-147.
 *   Available from <a href="http://softlib.rice.edu/pub/CRPC-TRs/reports/CRPC-TR96640.pdf">here</a>
 *
 * @see <a href="http://en.wikipedia.org/wiki/Isotonic_regression">Isotonic regression
 * (Wikipedia)</a>
 */
@Since("1.3.0")
class IsotonicRegression private (private var isotonic: Boolean) extends Serializable {

  /**
   * Constructs IsotonicRegression instance with default parameter isotonic = true.
   */
  @Since("1.3.0")
  def this() = this(true)

  /**
   * Sets the isotonic parameter.
   *
   * @param isotonic Isotonic (increasing) or antitonic (decreasing) sequence.
   * @return This instance of IsotonicRegression.
   */
  @Since("1.3.0")
  def setIsotonic(isotonic: Boolean): this.type = {
    this.isotonic = isotonic
    this
  }

  /**
   * Run IsotonicRegression algorithm to obtain isotonic regression model.
   *
   * @param input RDD of tuples (label, feature, weight) where label is dependent variable
   *              for which we calculate isotonic regression, feature is independent variable
   *              and weight represents number of measures with default 1.
   *              If multiple labels share the same feature value then they are aggregated using
   *              the weighted average before the algorithm is executed.
   * @return Isotonic regression model.
   */
  @Since("1.3.0")
  def run(input: RDD[(Double, Double, Double)]): IsotonicRegressionModel = {
    val preprocessedInput = if (isotonic) {
      input
    } else {
      input.map(x => (-x._1, x._2, x._3))
    }

    val pooled = parallelPoolAdjacentViolators(preprocessedInput)

    val predictions = if (isotonic) pooled.map(_._1) else pooled.map(-_._1)
    val boundaries = pooled.map(_._2)

    new IsotonicRegressionModel(boundaries, predictions, isotonic)
  }

  /**
   * Run pool adjacent violators algorithm to obtain isotonic regression model.
   *
   * @param input JavaRDD of tuples (label, feature, weight) where label is dependent variable
   *              for which we calculate isotonic regression, feature is independent variable
   *              and weight represents number of measures with default 1.
   *              If multiple labels share the same feature value then they are aggregated using
   *              the weighted average before the algorithm is executed.
   * @return Isotonic regression model.
   */
  @Since("1.3.0")
  def run(input: JavaRDD[(JDouble, JDouble, JDouble)]): IsotonicRegressionModel = {
    run(input.rdd.retag.asInstanceOf[RDD[(Double, Double, Double)]])
  }

  /**
   * Aggregates points of duplicate feature values into a single point using as label the weighted
   * average of the labels of the points with duplicate feature values. All points for a unique
   * feature value are aggregated as:
   *
   *   - Aggregated label is the weighted average of all labels.
   *   - Aggregated feature is the unique feature value.
   *   - Aggregated weight is the sum of all weights.
   *
   * @param input Input data of tuples (label, feature, weight). Weights must be non-negative.
   * @return Points with unique feature values.
   */
  private[regression] def makeUnique(
      input: Array[(Double, Double, Double)]): Array[(Double, Double, Double)] = {

    val cleanInput = input.filter { case (y, x, weight) =>
      require(
        weight >= 0.0,
        s"Negative weight at point ($y, $x, $weight). Weights must be non-negative")
      weight > 0
    }

    if (cleanInput.length <= 1) {
      cleanInput
    } else {
      val pointsAccumulator = new PointsAccumulator

      // Go through input points, merging all points with equal feature values into a single point.
      // Equality of features is defined by shouldAccumulate method. The label of the accumulated
      // points is the weighted average of the labels of all points of equal feature value.

      // Initialize with first point
      pointsAccumulator := cleanInput.head
      // Accumulate the rest
      cleanInput.tail.foreach { case point @ (_, feature, _) =>
        if (pointsAccumulator.shouldAccumulate(feature)) {
          // Still on a duplicate feature, accumulate
          pointsAccumulator += point
        } else {
          // A new unique feature encountered:
          // - append the last accumulated point to unique features output
          pointsAccumulator.appendToOutput()
          // - and reset
          pointsAccumulator := point
        }
      }
      // Append the last accumulated point to unique features output
      pointsAccumulator.appendToOutput()
      pointsAccumulator.getOutput
    }
  }

  /**
   * Performs a pool adjacent violators algorithm (PAV). Implements the algorithm originally
   * described in [1], using the formulation from [2, 3]. Uses an array to keep track of start
   * and end indices of blocks.
   *
   * [1] Grotzinger, S. J., and C. Witzgall. "Projections onto order simplexes." Applied
   * mathematics and Optimization 12.1 (1984): 247-270.
   *
   * [2] Best, Michael J., and Nilotpal Chakravarti. "Active set algorithms for isotonic
   * regression; a unifying framework." Mathematical Programming 47.1-3 (1990): 425-439.
   *
   * [3] Best, Michael J., Nilotpal Chakravarti, and Vasant A. Ubhaya. "Minimizing separable convex
   * functions subject to simple chain constraints." SIAM Journal on Optimization 10.3 (2000):
   * 658-672.
   *
   * @param cleanUniqueInput Input data of tuples(label, feature, weight).Features must be unique
   *                         and weights must be non-negative.
   * @return Result tuples (label, feature, weight) where labels were updated
   *         to form a monotone sequence as per isotonic regression definition.
   */
  private def poolAdjacentViolators(
      cleanUniqueInput: Array[(Double, Double, Double)]): Array[(Double, Double, Double)] = {

    if (cleanUniqueInput.isEmpty) {
      return Array.empty
    }

    // Keeps track of the start and end indices of the blocks. if [i, j] is a valid block from
    // cleanInput(i) to cleanInput(j) (inclusive), then blockBounds(i) = j and blockBounds(j) = i
    // Initially, each data point is its own block.
    val blockBounds = Array.range(0, cleanUniqueInput.length)

    // Keep track of the sum of weights and sum of weight * y for each block. weights(start)
    // gives the values for the block. Entries that are not at the start of a block
    // are meaningless.
    val weights: Array[(Double, Double)] = cleanUniqueInput.map { case (y, _, weight) =>
      (weight, weight * y)
    }

    // a few convenience functions to make the code more readable

    // blockStart and blockEnd have identical implementations. We create two different
    // functions to make the code more expressive
    def blockEnd(start: Int): Int = blockBounds(start)
    def blockStart(end: Int): Int = blockBounds(end)

    // the next block starts at the index after the end of this block
    def nextBlock(start: Int): Int = blockEnd(start) + 1

    // the previous block ends at the index before the start of this block
    // we then use blockStart to find the start
    def prevBlock(start: Int): Int = blockStart(start - 1)

    // Merge two adjacent blocks, updating blockBounds and weights to reflect the merge
    // Return the start index of the merged block
    def merge(block1: Int, block2: Int): Int = {
      assert(
        blockEnd(block1) + 1 == block2,
        s"Attempting to merge non-consecutive blocks [${block1}, ${blockEnd(block1)}]" +
        s" and [${block2}, ${blockEnd(block2)}]. This is likely a bug in the isotonic regression" +
        " implementation. Please file a bug report."
      )
      blockBounds(block1) = blockEnd(block2)
      blockBounds(blockEnd(block2)) = block1
      val w1 = weights(block1)
      val w2 = weights(block2)
      weights(block1) = (w1._1 + w2._1, w1._2 + w2._2)
      block1
    }

    // average value of a block
    def average(start: Int): Double = weights(start)._2 / weights(start)._1

    // Implement Algorithm PAV from [3].
    // Merge on >= instead of > because it eliminates adjacent blocks with the same average, and we
    // want to compress our output as much as possible. Both give correct results.
    var i = 0
    while (nextBlock(i) < cleanUniqueInput.length) {
      if (average(i) >= average(nextBlock(i))) {
        merge(i, nextBlock(i))
        while ((i > 0) && (average(prevBlock(i)) >= average(i))) {
          i = merge(prevBlock(i), i)
        }
      } else {
        i = nextBlock(i)
      }
    }

    // construct the output by walking through the blocks in order
    val output = ArrayBuffer.empty[(Double, Double, Double)]
    i = 0
    while (i < cleanUniqueInput.length) {
      // If block size is > 1, a point at the start and end of the block,
      // each receiving half the weight. Otherwise, a single point with
      // all the weight.
      if (cleanUniqueInput(blockEnd(i))._2 > cleanUniqueInput(i)._2) {
        output += ((average(i), cleanUniqueInput(i)._2, weights(i)._1 / 2))
        output += ((average(i), cleanUniqueInput(blockEnd(i))._2, weights(i)._1 / 2))
      } else {
        output += ((average(i), cleanUniqueInput(i)._2, weights(i)._1))
      }
      i = nextBlock(i)
    }

    output.toArray
  }

  /**
   * Performs parallel pool adjacent violators algorithm.
   * Performs Pool adjacent violators algorithm on each partition and then again on the result.
   *
   * @param input Input data of tuples (label, feature, weight).
   * @return Result tuples (label, feature, weight) where labels were updated
   *         to form a monotone sequence as per isotonic regression definition.
   */
  private def parallelPoolAdjacentViolators(
      input: RDD[(Double, Double, Double)]): Array[(Double, Double, Double)] = {
    val keyedInput = input.keyBy(_._2)
    val parallelStepResult = keyedInput
      // Points with same or adjacent features must collocate within the same partition.
      .partitionBy(new RangePartitioner(keyedInput.getNumPartitions, keyedInput))
      .values
      // Lexicographically sort points by features.
      .mapPartitions(p => Iterator(p.toArray.sortBy(_._2)))
      // Aggregate points with equal features into a single point.
      .map(makeUnique)
      .flatMap(poolAdjacentViolators)
      .collect()
      // Sort again because collect() doesn't promise ordering.
      .sortBy(_._2)
    poolAdjacentViolators(parallelStepResult)
  }

  /**
   * Utility class, holds a buffer of all points with unique features so far, and performs
   * weighted sum accumulation of points. Hides these details for better readability of the
   * main algorithm.
   */
  private class PointsAccumulator {
    private val output = ArrayBuffer[(Double, Double, Double)]()
    private var (currentLabel: Double, currentFeature: Double, currentWeight: Double) =
      (0d, 0d, 0d)

    /** Whether or not this feature exactly equals the current accumulated feature. */
    @inline def shouldAccumulate(feature: Double): Boolean = currentFeature == feature

    /** Resets the current value of the point accumulator using the provided point. */
    @inline def :=(point: (Double, Double, Double)): Unit = {
      val (label, feature, weight) = point
      currentLabel = label * weight
      currentFeature = feature
      currentWeight = weight
    }

    /** Accumulates the provided point into the current value of the point accumulator. */
    @inline def +=(point: (Double, Double, Double)): Unit = {
      val (label, _, weight) = point
      currentLabel += label * weight
      currentWeight += weight
    }

    /** Appends the current value of the point accumulator to the output. */
    @inline def appendToOutput(): Unit =
      output += ((
        currentLabel / currentWeight,
        currentFeature,
        currentWeight))

    /** Returns all accumulated points so far. */
    @inline def getOutput: Array[(Double, Double, Double)] = output.toArray
  }
}
