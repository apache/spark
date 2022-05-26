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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Expression, GenericInternalRow, GetArrayItem, Literal, TryCast}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.{GenericArrayData, QuantileSummaries}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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
    val tableName = s"${col1}_$col2"
    val counts = df.groupBy(col1, col2).agg(count("*")).take(1e6.toInt)
    if (counts.length == 1e6.toInt) {
      logWarning("The maximum limit of 1e6 pairs have been collected, which may not be all of " +
        "the pairs. Please try reducing the amount of distinct items in your columns.")
    }
    def cleanElement(element: Any): String = {
      if (element == null) "null" else element.toString
    }
    // get the distinct sorted values of column 2, so that we can make them the column names
    val distinctCol2: Map[Any, Int] =
      counts.map(e => cleanElement(e.get(1))).distinct.sorted.zipWithIndex.toMap
    val columnSize = distinctCol2.size
    require(columnSize < 1e4, s"The number of distinct values for $col2, can't " +
      s"exceed 1e4. Currently $columnSize")
    val table = counts.groupBy(_.get(0)).map { case (col1Item, rows) =>
      val countsRow = new GenericInternalRow(columnSize + 1)
      rows.foreach { (row: Row) =>
        // row.get(0) is column 1
        // row.get(1) is column 2
        // row.get(2) is the frequency
        val columnIndex = distinctCol2(cleanElement(row.get(1)))
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

  /** Calculate selected summary statistics for a dataset */
  def summary(ds: Dataset[_], statistics: Seq[String]): DataFrame = {

    val defaultStatistics = Seq("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")
    val selectedStatistics = if (statistics.nonEmpty) statistics else defaultStatistics

    val percentiles = selectedStatistics.filter(a => a.endsWith("%")).map { p =>
      try {
        p.stripSuffix("%").toDouble / 100.0
      } catch {
        case e: NumberFormatException =>
          throw QueryExecutionErrors.cannotParseStatisticAsPercentileError(p, e)
      }
    }
    require(percentiles.forall(p => p >= 0 && p <= 1), "Percentiles must be in the range [0, 1]")

    def castAsDoubleIfNecessary(e: Expression): Expression = if (e.dataType == StringType) {
      TryCast(e, DoubleType)
    } else {
      e
    }
    var percentileIndex = 0
    val statisticFns = selectedStatistics.map { stats =>
      if (stats.endsWith("%")) {
        val index = percentileIndex
        percentileIndex += 1
        (child: Expression) =>
          GetArrayItem(
            new ApproximatePercentile(castAsDoubleIfNecessary(child),
              Literal(new GenericArrayData(percentiles), ArrayType(DoubleType, false)))
              .toAggregateExpression(),
            Literal(index))
      } else {
        stats.toLowerCase(Locale.ROOT) match {
          case "count" => (child: Expression) => Count(child).toAggregateExpression()
          case "count_distinct" => (child: Expression) =>
            Count(child).toAggregateExpression(isDistinct = true)
          case "approx_count_distinct" => (child: Expression) =>
            HyperLogLogPlusPlus(child).toAggregateExpression()
          case "mean" => (child: Expression) =>
            Average(castAsDoubleIfNecessary(child)).toAggregateExpression()
          case "stddev" => (child: Expression) =>
            StddevSamp(castAsDoubleIfNecessary(child)).toAggregateExpression()
          case "min" => (child: Expression) => Min(child).toAggregateExpression()
          case "max" => (child: Expression) => Max(child).toAggregateExpression()
          case _ => throw QueryExecutionErrors.statisticNotRecognizedError(stats)
        }
      }
    }

    val selectedCols = ds.logicalPlan.output
      .filter(a => a.dataType.isInstanceOf[NumericType] || a.dataType.isInstanceOf[StringType])

    val aggExprs = statisticFns.flatMap { func =>
      selectedCols.map(c => Column(Cast(func(c), StringType)).as(c.name))
    }

    // If there is no selected columns, we don't need to run this aggregate, so make it a lazy val.
    lazy val aggResult = ds.select(aggExprs: _*).queryExecution.toRdd.collect().head

    // We will have one row for each selected statistic in the result.
    val result = Array.fill[InternalRow](selectedStatistics.length) {
      // each row has the statistic name, and statistic values of each selected column.
      new GenericInternalRow(selectedCols.length + 1)
    }

    var rowIndex = 0
    while (rowIndex < result.length) {
      val statsName = selectedStatistics(rowIndex)
      result(rowIndex).update(0, UTF8String.fromString(statsName))
      for (colIndex <- selectedCols.indices) {
        val statsValue = aggResult.getUTF8String(rowIndex * selectedCols.length + colIndex)
        result(rowIndex).update(colIndex + 1, statsValue)
      }
      rowIndex += 1
    }

    // All columns are string type
    val output = AttributeReference("summary", StringType)() +:
      selectedCols.map(c => AttributeReference(c.name, StringType)())

    Dataset.ofRows(ds.sparkSession, LocalRelation(output, result))
  }
}
