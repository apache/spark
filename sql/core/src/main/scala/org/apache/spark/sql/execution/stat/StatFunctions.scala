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

package org.apache.spark.sql.execution.stat

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.expressions.{Cast, GenericMutableRow}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[sql] object StatFunctions extends Logging {

  import QuantileSummaries.Stats

  /**
   * Calculates the approximate quantiles of multiple numerical columns of a DataFrame in one pass.
   *
   * The result of this algorithm has the following deterministic bound:
   * If the DataFrame has N elements and if we request the quantile at probability `p` up to error
   * `err`, then the algorithm will return a sample `x` from the DataFrame so that the *exact* rank
   * of `x` is close to (p * N).
   * More precisely,
   *
   *   floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).
   *
   * This method implements a variation of the Greenwald-Khanna algorithm (with some speed
   * optimizations).
   * The algorithm was first present in [[http://dx.doi.org/10.1145/375663.375670 Space-efficient
   * Online Computation of Quantile Summaries]] by Greenwald and Khanna.
   *
   * @param df the dataframe
   * @param cols numerical columns of the dataframe
   * @param probabilities a list of quantile probabilities
   *   Each number must belong to [0, 1].
   *   For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
   * @param relativeError The relative target precision to achieve (>= 0).
   *   If set to zero, the exact quantiles are computed, which could be very expensive.
   *   Note that values greater than 1 are accepted but give the same result as 1.
   *
   * @return for each column, returns the requested approximations
   */
  def multipleApproxQuantiles(
      df: DataFrame,
      cols: Seq[String],
      probabilities: Seq[Double],
      relativeError: Double): Seq[Seq[Double]] = {
    val columns: Seq[Column] = cols.map { colName =>
      val field = df.schema(colName)
      require(field.dataType.isInstanceOf[NumericType],
        s"Quantile calculation for column $colName with data type ${field.dataType}" +
        " is not supported.")
      Column(Cast(Column(colName).expr, DoubleType))
    }
    val emptySummaries = Array.fill(cols.size)(
      new QuantileSummaries(QuantileSummaries.defaultCompressThreshold, relativeError))

    // Note that it works more or less by accident as `rdd.aggregate` is not a pure function:
    // this function returns the same array as given in the input (because `aggregate` reuses
    // the same argument).
    def apply(summaries: Array[QuantileSummaries], row: Row): Array[QuantileSummaries] = {
      var i = 0
      while (i < summaries.length) {
        summaries(i) = summaries(i).insert(row.getDouble(i))
        i += 1
      }
      summaries
    }

    def merge(
        sum1: Array[QuantileSummaries],
        sum2: Array[QuantileSummaries]): Array[QuantileSummaries] = {
      sum1.zip(sum2).map { case (s1, s2) => s1.compress().merge(s2.compress()) }
    }
    val summaries = df.select(columns: _*).rdd.aggregate(emptySummaries)(apply, merge)

    summaries.map { summary => probabilities.map(summary.query) }
  }

  /**
   * Helper class to compute approximate quantile summary.
   * This implementation is based on the algorithm proposed in the paper:
   * "Space-efficient Online Computation of Quantile Summaries" by Greenwald, Michael
   * and Khanna, Sanjeev. (http://dx.doi.org/10.1145/375663.375670)
   *
   * In order to optimize for speed, it maintains an internal buffer of the last seen samples,
   * and only inserts them after crossing a certain size threshold. This guarantees a near-constant
   * runtime complexity compared to the original algorithm.
   *
   * @param compressThreshold the compression threshold.
   *   After the internal buffer of statistics crosses this size, it attempts to compress the
   *   statistics together.
   * @param relativeError the target relative error.
   *   It is uniform across the complete range of values.
   * @param sampled a buffer of quantile statistics.
   *   See the G-K article for more details.
   * @param count the count of all the elements *inserted in the sampled buffer*
   *              (excluding the head buffer)
   * @param headSampled a buffer of latest samples seen so far
   */
  class QuantileSummaries(
      val compressThreshold: Int,
      val relativeError: Double,
      val sampled: ArrayBuffer[Stats] = ArrayBuffer.empty,
      private[stat] var count: Long = 0L,
      val headSampled: ArrayBuffer[Double] = ArrayBuffer.empty) extends Serializable {

    import QuantileSummaries._

    /**
     * Returns a summary with the given observation inserted into the summary.
     * This method may either modify in place the current summary (and return the same summary,
     * modified in place), or it may create a new summary from scratch it necessary.
     * @param x the new observation to insert into the summary
     */
    def insert(x: Double): QuantileSummaries = {
      headSampled.append(x)
      if (headSampled.size >= defaultHeadSize) {
        this.withHeadBufferInserted
      } else {
        this
      }
    }

    /**
     * Inserts an array of (unsorted samples) in a batch, sorting the array first to traverse
     * the summary statistics in a single batch.
     *
     * This method does not modify the current object and returns if necessary a new copy.
     *
     * @return a new quantile summary object.
     */
    private def withHeadBufferInserted: QuantileSummaries = {
      if (headSampled.isEmpty) {
        return this
      }
      var currentCount = count
      val sorted = headSampled.toArray.sorted
      val newSamples: ArrayBuffer[Stats] = new ArrayBuffer[Stats]()
      // The index of the next element to insert
      var sampleIdx = 0
      // The index of the sample currently being inserted.
      var opsIdx: Int = 0
      while(opsIdx < sorted.length) {
        val currentSample = sorted(opsIdx)
        // Add all the samples before the next observation.
        while(sampleIdx < sampled.size && sampled(sampleIdx).value <= currentSample) {
          newSamples.append(sampled(sampleIdx))
          sampleIdx += 1
        }

        // If it is the first one to insert, of if it is the last one
        currentCount += 1
        val delta =
          if (newSamples.isEmpty || (sampleIdx == sampled.size && opsIdx == sorted.length - 1)) {
            0
          } else {
            math.floor(2 * relativeError * currentCount).toInt
          }

        val tuple = Stats(currentSample, 1, delta)
        newSamples.append(tuple)
        opsIdx += 1
      }

      // Add all the remaining existing samples
      while(sampleIdx < sampled.size) {
        newSamples.append(sampled(sampleIdx))
        sampleIdx += 1
      }
      new QuantileSummaries(compressThreshold, relativeError, newSamples, currentCount)
    }

    /**
     * Returns a new summary that compresses the summary statistics and the head buffer.
     *
     * This implements the COMPRESS function of the GK algorithm. It does not modify the object.
     *
     * @return a new summary object with compressed statistics
     */
    def compress(): QuantileSummaries = {
      // Inserts all the elements first
      val inserted = this.withHeadBufferInserted
      assert(inserted.headSampled.isEmpty)
      assert(inserted.count == count + headSampled.size)
      val compressed =
        compressImmut(inserted.sampled, mergeThreshold = 2 * relativeError * inserted.count)
      new QuantileSummaries(compressThreshold, relativeError, compressed, inserted.count)
    }

    private def shallowCopy: QuantileSummaries = {
      new QuantileSummaries(compressThreshold, relativeError, sampled, count, headSampled)
    }

    /**
     * Merges two (compressed) summaries together.
     *
     * Returns a new summary.
     */
    def merge(other: QuantileSummaries): QuantileSummaries = {
      require(headSampled.isEmpty, "Current buffer needs to be compressed before merge")
      require(other.headSampled.isEmpty, "Other buffer needs to be compressed before merge")
      if (other.count == 0) {
        this.shallowCopy
      } else if (count == 0) {
        other.shallowCopy
      } else {
        // Merge the two buffers.
        // The GK algorithm is a bit unclear about it, but it seems there is no need to adjust the
        // statistics during the merging: the invariants are still respected after the merge.
        // TODO: could replace full sort by ordered merge, the two lists are known to be sorted
        // already.
        val res = (sampled ++ other.sampled).sortBy(_.value)
        val comp = compressImmut(res, mergeThreshold = 2 * relativeError * count)
        new QuantileSummaries(
          other.compressThreshold, other.relativeError, comp, other.count + count)
      }
    }

    /**
     * Runs a query for a given quantile.
     * The result follows the approximation guarantees detailed above.
     * The query can only be run on a compressed summary: you need to call compress() before using
     * it.
     *
     * @param quantile the target quantile
     * @return
     */
    def query(quantile: Double): Double = {
      require(quantile >= 0 && quantile <= 1.0, "quantile should be in the range [0.0, 1.0]")
      require(headSampled.isEmpty,
        "Cannot operate on an uncompressed summary, call compress() first")

      if (quantile <= relativeError) {
        return sampled.head.value
      }

      if (quantile >= 1 - relativeError) {
        return sampled.last.value
      }

      // Target rank
      val rank = math.ceil(quantile * count).toInt
      val targetError = math.ceil(relativeError * count)
      // Minimum rank at current sample
      var minRank = 0
      var i = 1
      while (i < sampled.size - 1) {
        val curSample = sampled(i)
        minRank += curSample.g
        val maxRank = minRank + curSample.delta
        if (maxRank - targetError <= rank && rank <= minRank + targetError) {
          return curSample.value
        }
        i += 1
      }
      sampled.last.value
    }
  }

  object QuantileSummaries {
    // TODO(tjhunter) more tuning could be done one the constants here, but for now
    // the main cost of the algorithm is accessing the data in SQL.
    /**
     * The default value for the compression threshold.
     */
    val defaultCompressThreshold: Int = 10000

    /**
     * The size of the head buffer.
     */
    val defaultHeadSize: Int = 50000

    /**
     * The default value for the relative error (1%).
     * With this value, the best extreme percentiles that can be approximated are 1% and 99%.
     */
    val defaultRelativeError: Double = 0.01

    /**
     * Statistics from the Greenwald-Khanna paper.
     * @param value the sampled value
     * @param g the minimum rank jump from the previous value's minimum rank
     * @param delta the maximum span of the rank.
     */
    case class Stats(value: Double, g: Int, delta: Int)

    private def compressImmut(
        currentSamples: IndexedSeq[Stats],
        mergeThreshold: Double): ArrayBuffer[Stats] = {
      val res: ArrayBuffer[Stats] = ArrayBuffer.empty
      if (currentSamples.isEmpty) {
        return res
      }
      // Start for the last element, which is always part of the set.
      // The head contains the current new head, that may be merged with the current element.
      var head = currentSamples.last
      var i = currentSamples.size - 2
      // Do not compress the last element
      while (i >= 1) {
        // The current sample:
        val sample1 = currentSamples(i)
        // Do we need to compress?
        if (sample1.g + head.g + head.delta < mergeThreshold) {
          // Do not insert yet, just merge the current element into the head.
          head = head.copy(g = head.g + sample1.g)
        } else {
          // Prepend the current head, and keep the current sample as target for merging.
          res.prepend(head)
          head = sample1
        }
        i -= 1
      }
      res.prepend(head)
      // If necessary, add the minimum element:
      res.prepend(currentSamples.head)
      res
    }
  }

  /** Calculate the Pearson Correlation Coefficient for the given columns */
  private[sql] def pearsonCorrelation(df: DataFrame, cols: Seq[String]): Double = {
    val counts = collectStatisticalData(df, cols, "correlation")
    counts.Ck / math.sqrt(counts.MkX * counts.MkY)
  }

  /** Helper class to simplify tracking and merging counts. */
  private class CovarianceCounter extends Serializable {
    var xAvg = 0.0 // the mean of all examples seen so far in col1
    var yAvg = 0.0 // the mean of all examples seen so far in col2
    var Ck = 0.0 // the co-moment after k examples
    var MkX = 0.0 // sum of squares of differences from the (current) mean for col1
    var MkY = 0.0 // sum of squares of differences from the (current) mean for col2
    var count = 0L // count of observed examples
    // add an example to the calculation
    def add(x: Double, y: Double): this.type = {
      val deltaX = x - xAvg
      val deltaY = y - yAvg
      count += 1
      xAvg += deltaX / count
      yAvg += deltaY / count
      Ck += deltaX * (y - yAvg)
      MkX += deltaX * (x - xAvg)
      MkY += deltaY * (y - yAvg)
      this
    }
    // merge counters from other partitions. Formula can be found at:
    // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
    def merge(other: CovarianceCounter): this.type = {
      if (other.count > 0) {
        val totalCount = count + other.count
        val deltaX = xAvg - other.xAvg
        val deltaY = yAvg - other.yAvg
        Ck += other.Ck + deltaX * deltaY * count / totalCount * other.count
        xAvg = (xAvg * count + other.xAvg * other.count) / totalCount
        yAvg = (yAvg * count + other.yAvg * other.count) / totalCount
        MkX += other.MkX + deltaX * deltaX * count / totalCount * other.count
        MkY += other.MkY + deltaY * deltaY * count / totalCount * other.count
        count = totalCount
      }
      this
    }
    // return the sample covariance for the observed examples
    def cov: Double = Ck / (count - 1)
  }

  private def collectStatisticalData(df: DataFrame, cols: Seq[String],
              functionName: String): CovarianceCounter = {
    require(cols.length == 2, s"Currently $functionName calculation is supported " +
      "between two columns.")
    cols.map(name => (name, df.schema.fields.find(_.name == name))).foreach { case (name, data) =>
      require(data.nonEmpty, s"Couldn't find column with name $name")
      require(data.get.dataType.isInstanceOf[NumericType], s"Currently $functionName calculation " +
        s"for columns with dataType ${data.get.dataType} not supported.")
    }
    val columns = cols.map(n => Column(Cast(Column(n).expr, DoubleType)))
    df.select(columns: _*).queryExecution.toRdd.aggregate(new CovarianceCounter)(
      seqOp = (counter, row) => {
        counter.add(row.getDouble(0), row.getDouble(1))
      },
      combOp = (baseCounter, other) => {
        baseCounter.merge(other)
    })
  }

  /**
   * Calculate the covariance of two numerical columns of a DataFrame.
   * @param df The DataFrame
   * @param cols the column names
   * @return the covariance of the two columns.
   */
  private[sql] def calculateCov(df: DataFrame, cols: Seq[String]): Double = {
    val counts = collectStatisticalData(df, cols, "covariance")
    counts.cov
  }

  /** Generate a table of frequencies for the elements of two columns. */
  private[sql] def crossTabulate(df: DataFrame, col1: String, col2: String): DataFrame = {
    val tableName = s"${col1}_$col2"
    val counts = df.groupBy(col1, col2).agg(count("*")).take(1e6.toInt)
    if (counts.length == 1e6.toInt) {
      logWarning("The maximum limit of 1e6 pairs have been collected, which may not be all of " +
        "the pairs. Please try reducing the amount of distinct items in your columns.")
    }
    def cleanElement(element: Any): String = {
      if (element == null) "null" else element.toString
    }
    // get the distinct values of column 2, so that we can make them the column names
    val distinctCol2: Map[Any, Int] =
      counts.map(e => cleanElement(e.get(1))).distinct.zipWithIndex.toMap
    val columnSize = distinctCol2.size
    require(columnSize < 1e4, s"The number of distinct values for $col2, can't " +
      s"exceed 1e4. Currently $columnSize")
    val table = counts.groupBy(_.get(0)).map { case (col1Item, rows) =>
      val countsRow = new GenericMutableRow(columnSize + 1)
      rows.foreach { (row: Row) =>
        // row.get(0) is column 1
        // row.get(1) is column 2
        // row.get(2) is the frequency
        val columnIndex = distinctCol2.get(cleanElement(row.get(1))).get
        countsRow.setLong(columnIndex + 1, row.getLong(2))
      }
      // the value of col1 is the first value, the rest are the counts
      countsRow.update(0, UTF8String.fromString(cleanElement(col1Item)))
      countsRow
    }.toSeq
    // Back ticks can't exist in DataFrame column names, therefore drop them. To be able to accept
    // special keywords and `.`, wrap the column names in ``.
    def cleanColumnName(name: String): String = {
      name.replace("`", "")
    }
    // In the map, the column names (._1) are not ordered by the index (._2). This was the bug in
    // SPARK-8681. We need to explicitly sort by the column index and assign the column names.
    val headerNames = distinctCol2.toSeq.sortBy(_._2).map { r =>
      StructField(cleanColumnName(r._1.toString), LongType)
    }
    val schema = StructType(StructField(tableName, StringType) +: headerNames)

    Dataset.ofRows(df.sparkSession, LocalRelation(schema.toAttributes, table)).na.fill(0.0)
  }
}
