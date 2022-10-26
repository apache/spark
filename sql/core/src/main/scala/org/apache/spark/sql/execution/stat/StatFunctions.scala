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

import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.expressions.{Cast, ElementAt, EvalMode}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StatFunctions extends Logging {

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
   * The algorithm was first present in <a href="https://doi.org/10.1145/375663.375670">
   * Space-efficient Online Computation of Quantile Summaries</a> by Greenwald and Khanna.
   *
   * @param df the dataframe
   * @param cols numerical columns of the dataframe
   * @param probabilities a list of quantile probabilities
   *   Each number must belong to [0, 1].
   *   For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
   * @param relativeError The relative target precision to achieve (greater than or equal 0).
   *   If set to zero, the exact quantiles are computed, which could be very expensive.
   *   Note that values greater than 1 are accepted but give the same result as 1.
   *
   * @return for each column, returns the requested approximations
   *
   * @note null and NaN values will be ignored in numerical columns before calculation. For
   *   a column only containing null or NaN values, an empty array is returned.
   */
  def multipleApproxQuantiles(
      df: DataFrame,
      cols: Seq[String],
      probabilities: Seq[Double],
      relativeError: Double): Seq[Seq[Double]] = {
    require(relativeError >= 0,
      s"Relative Error must be non-negative but got $relativeError")
    val columns: Seq[Column] = cols.map { colName =>
      val field = df.resolve(colName)
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
        if (!row.isNullAt(i)) {
          val v = row.getDouble(i)
          if (!v.isNaN) summaries(i) = summaries(i).insert(v)
        }
        i += 1
      }
      summaries
    }

    def merge(
        sum1: Array[QuantileSummaries],
        sum2: Array[QuantileSummaries]): Array[QuantileSummaries] = {
      sum1.zip(sum2).map { case (s1, s2) => s1.compress().merge(s2.compress()) }
    }
    val summaries = df.select(columns: _*).rdd.treeAggregate(emptySummaries)(apply, merge)

    summaries.map {
      summary => summary.query(probabilities) match {
        case Some(q) => q
        case None => Seq()
      }
    }
  }

  /** Calculate the Pearson Correlation Coefficient for the given columns */
  def pearsonCorrelation(df: DataFrame, cols: Seq[String]): Double = {
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
    cols.map(name => (name, df.resolve(name))).foreach { case (name, data) =>
      require(data.dataType.isInstanceOf[NumericType], s"Currently $functionName calculation " +
        s"for columns with dataType ${data.dataType.catalogString} not supported.")
    }
    val columns = cols.map(n => Column(Cast(Column(n).expr, DoubleType)))
    df.select(columns: _*).queryExecution.toRdd.treeAggregate(new CovarianceCounter)(
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
  def calculateCov(df: DataFrame, cols: Seq[String]): Double = {
    val counts = collectStatisticalData(df, cols, "covariance")
    counts.cov
  }

  /** Generate a table of frequencies for the elements of two columns. */
  def crossTabulate(df: DataFrame, col1: String, col2: String): DataFrame = {
    df.groupBy(
      when(isnull(col(col1)), "null")
        .otherwise(col(col1).cast("string"))
        .as(s"${col1}_$col2")
    ).pivot(
      when(isnull(col(col2)), "null")
        .otherwise(regexp_replace(col(col2).cast("string"), "`", ""))
    ).count().na.fill(0L)
  }

  /** Calculate selected summary statistics for a dataset */
  def summary(ds: Dataset[_], statistics: Seq[String]): DataFrame = {
    val selectedStatistics = if (statistics.nonEmpty) {
      statistics.toArray
    } else {
      Array("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")
    }

    val percentiles = selectedStatistics.filter(a => a.endsWith("%")).map { p =>
      try {
        p.stripSuffix("%").toDouble / 100.0
      } catch {
        case e: NumberFormatException =>
          throw QueryExecutionErrors.cannotParseStatisticAsPercentileError(p, e)
      }
    }
    require(percentiles.forall(p => p >= 0 && p <= 1), "Percentiles must be in the range [0, 1]")

    var mapColumns = Seq.empty[Column]
    var columnNames = Seq.empty[String]

    ds.schema.fields.foreach { field =>
      if (field.dataType.isInstanceOf[NumericType] || field.dataType.isInstanceOf[StringType]) {
        val column = col(field.name)
        var casted = column
        if (field.dataType.isInstanceOf[StringType]) {
          casted = new Column(Cast(column.expr, DoubleType, evalMode = EvalMode.TRY))
        }

        val percentilesCol = if (percentiles.nonEmpty) {
          percentile_approx(casted, lit(percentiles),
            lit(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY))
        } else null

        var aggColumns = Seq.empty[Column]
        var percentileIndex = 0
        selectedStatistics.foreach { stats =>
          aggColumns :+= lit(stats)

          stats.toLowerCase(Locale.ROOT) match {
            case "count" => aggColumns :+= count(column)

            case "count_distinct" => aggColumns :+= count_distinct(column)

            case "approx_count_distinct" => aggColumns :+= approx_count_distinct(column)

            case "mean" => aggColumns :+= avg(casted)

            case "stddev" => aggColumns :+= stddev(casted)

            case "min" => aggColumns :+= min(column)

            case "max" => aggColumns :+= max(column)

            case percentile if percentile.endsWith("%") =>
              aggColumns :+= get(percentilesCol, lit(percentileIndex))
              percentileIndex += 1

            case _ => throw QueryExecutionErrors.statisticNotRecognizedError(stats)
          }
        }

        // map { "count" -> "1024", "min" -> "1.0", ... }
        mapColumns :+= map(aggColumns.map(_.cast(StringType)): _*).as(field.name)
        columnNames :+= field.name
      }
    }

    if (mapColumns.isEmpty) {
      ds.sparkSession.createDataFrame(selectedStatistics.map(Tuple1.apply))
        .withColumnRenamed("_1", "summary")
    } else {
      val valueColumns = columnNames.map { columnName =>
        new Column(ElementAt(col(columnName).expr, col("summary").expr)).as(columnName)
      }
      ds.select(mapColumns: _*)
        .withColumn("summary", explode(lit(selectedStatistics)))
        .select(Array(col("summary")) ++ valueColumns: _*)
    }
  }
}
