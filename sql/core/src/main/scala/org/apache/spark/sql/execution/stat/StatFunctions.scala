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

import org.apache.spark.Logging
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, Cast}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

private[sql] object StatFunctions extends Logging {

  /** Calculate the approximate quantile for the given column */
  private[sql] def approxQuantile(
      df: DataFrame,
      col: String,
      quantile: Double,
      epsilon: Double = 0.05): Double = {
    require(quantile > 0.0 && quantile < 1.0, "Quantile must be in the range of (0.0, 1.0).")
    val summeries = collectQuantileSummaries(df, col, epsilon)
    summeries.query(quantile) 
  }

  private def collectQuantileSummaries(
      df: DataFrame,
      col: String,
      epsilon: Double): QuantileSummaries = {
    val data = df.schema.fields.find(_.name == col)
    require(data.nonEmpty, s"Couldn't find column with name $col")
    require(data.get.dataType.isInstanceOf[NumericType], "Quantile calculation for column " +
        s"with dataType ${data.get.dataType} not supported.")

    val column = Column(Cast(Column(col).expr, DoubleType))
    df.select(column).rdd.aggregate(new QuantileSummaries(epsilon = epsilon))(
      seqOp = (summeries, row) => {
        summeries.insert(row.getDouble(0))
      },
      combOp = (baseSummeries, other) => {
        baseSummeries.merge(other)
    })
  }

  /**
   * Helper class to compute approximate quantile summary.
   * This implementation is based on the algorithm proposed in the paper:
   * "Space-efficient Online Computation of Quantile Summaries" by Greenwald, Michael
   * and Khanna, Sanjeev. (http://dl.acm.org/citation.cfm?id=375670)
   * 
   */
  private class QuantileSummaries(
      compress_threshold: Int = 1000,
      epsilon: Double = 0.05) extends Serializable {
    var sampled = new ArrayBuffer[(Double, Int, Int)]() // sampled examples
    var count = 0L // count of observed examples

    def getConstant(): Double = 2 * epsilon * count

    def insert(x: Double): this.type = {
      var idx = sampled.indexWhere(_._1 > x)
      if (idx == -1) {
        idx = sampled.size
      } 
      val delta = if (idx == 0 || idx == sampled.size) {
        0
      } else {
        math.floor(getConstant()).toInt
      } 
      val tuple = (x, 1, delta)
      sampled.insert(idx, tuple)
      count += 1

      if (sampled.size > compress_threshold) {
        compress()
      }
      this
    }

    def compress(): Unit = {
      var i = 0
      while (i < sampled.size - 1) {
        val sample1 = sampled(i)
        val sample2 = sampled(i + 1)
        if (sample1._2 + sample2._2 + sample2._3 < math.floor(getConstant())) {
          sampled.update(i + 1, (sample2._1, sample1._2 + sample2._2, sample2._3))
          sampled.remove(i)
        }
        i += 1
      }
    }

    def merge(other: QuantileSummaries): QuantileSummaries = {
      if (other.count > 0 && count > 0) {
        other.sampled.foreach { sample =>
          val idx = sampled.indexWhere(s => s._1 > sample._1)
          if (idx == 0) {
            val new_sampled = (sampled(0)._1, sampled(0)._2, sampled(1)._3 / 2)
            sampled.update(0, new_sampled)
            val new_sample = (sample._1, sample._2, 0)
            sampled.insert(0, new_sample)
          } else if (idx == -1) {
            val new_sampled = (sampled(sampled.size - 1)._1, sampled(sampled.size - 1)._2,
              (sampled(sampled.size - 2)._3 * 2 * epsilon).toInt)
            sampled.update(sampled.size - 1, new_sampled)
            val new_sample = (sample._1, sample._2, 0)
            sampled.insert(sampled.size, new_sample)
          } else {
            val new_sample = (sample._1, sample._2, (sampled(idx - 1)._3 + sampled(idx)._3) / 2)
            sampled.insert(idx, new_sample)
          }
        }
        count += other.count
        compress()
        this
      } else if (other.count > 0) {
        other
      } else {
        this
      }
    }

    def query(quantile: Double): Double = {
      val rank = (quantile * count).toInt
      var minRank = 0
      var i = 1
      while (i < sampled.size) {
        val curSample = sampled(i)
        val prevSample = sampled(i - 1)
        minRank += prevSample._2
        if (minRank + curSample._2 + curSample._3 > rank + getConstant()) {
          return prevSample._1
        }
        i += 1
      }
      return sampled.last._1
    }
  }

  /** Calculate the Pearson Correlation Coefficient for the given columns */
  private[sql] def pearsonCorrelation(df: DataFrame, cols: Seq[String]): Double = {
    val counts = collectStatisticalData(df, cols)
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

  private def collectStatisticalData(df: DataFrame, cols: Seq[String]): CovarianceCounter = {
    require(cols.length == 2, "Currently cov supports calculating the covariance " +
      "between two columns.")
    cols.map(name => (name, df.schema.fields.find(_.name == name))).foreach { case (name, data) =>
      require(data.nonEmpty, s"Couldn't find column with name $name")
      require(data.get.dataType.isInstanceOf[NumericType], "Covariance calculation for columns " +
        s"with dataType ${data.get.dataType} not supported.")
    }
    val columns = cols.map(n => Column(Cast(Column(n).expr, DoubleType)))
    df.select(columns: _*).rdd.aggregate(new CovarianceCounter)(
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
    val counts = collectStatisticalData(df, cols)
    counts.cov
  }

  /** Generate a table of frequencies for the elements of two columns. */
  private[sql] def crossTabulate(df: DataFrame, col1: String, col2: String): DataFrame = {
    val tableName = s"${col1}_$col2"
    val counts = df.groupBy(col1, col2).agg(col(col1), col(col2), count("*")).take(1e6.toInt)
    if (counts.length == 1e6.toInt) {
      logWarning("The maximum limit of 1e6 pairs have been collected, which may not be all of " +
        "the pairs. Please try reducing the amount of distinct items in your columns.")
    }
    // get the distinct values of column 2, so that we can make them the column names
    val distinctCol2 = counts.map(_.get(1)).distinct.zipWithIndex.toMap
    val columnSize = distinctCol2.size
    require(columnSize < 1e4, s"The number of distinct values for $col2, can't " +
      s"exceed 1e4. Currently $columnSize")
    val table = counts.groupBy(_.get(0)).map { case (col1Item, rows) =>
      val countsRow = new GenericMutableRow(columnSize + 1)
      rows.foreach { row =>
        countsRow.setLong(distinctCol2.get(row.get(1)).get + 1, row.getLong(2))
      }
      // the value of col1 is the first value, the rest are the counts
      countsRow.setString(0, col1Item.toString)
      countsRow
    }.toSeq
    val headerNames = distinctCol2.map(r => StructField(r._1.toString, LongType)).toSeq
    val schema = StructType(StructField(tableName, StringType) +: headerNames)

    new DataFrame(df.sqlContext, LocalRelation(schema.toAttributes, table))
  }
}
