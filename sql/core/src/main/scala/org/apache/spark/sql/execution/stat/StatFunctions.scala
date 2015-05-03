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

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

private[sql] object StatFunctions {
  
  /** Helper class to simplify tracking and merging counts. */
  private class CovarianceCounter extends Serializable {
    var xAvg = 0.0
    var yAvg = 0.0
    var Ck = 0.0
    var count = 0L
    // add an example to the calculation
    def add(x: Double, y: Double): this.type = {
      val oldX = xAvg
      count += 1
      xAvg += (x - xAvg) / count
      yAvg += (y - yAvg) / count
      Ck += (y - yAvg) * (x - oldX)
      this
    }
    // merge counters from other partitions. Formula can be found at:
    // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Covariance
    def merge(other: CovarianceCounter): this.type = {
      val totalCount = count + other.count
      Ck += other.Ck + 
        (xAvg - other.xAvg) * (yAvg - other.yAvg) * count / totalCount * other.count
      xAvg = (xAvg * count + other.xAvg * other.count) / totalCount
      yAvg = (yAvg * count + other.yAvg * other.count) / totalCount
      count = totalCount
      this
    }
    // return the sample covariance for the observed examples
    def cov: Double = Ck / (count - 1)
  }

  /**
   * Calculate the covariance of two numerical columns of a DataFrame.
   * @param df The DataFrame
   * @param cols the column names
   * @return the covariance of the two columns.
   */
  private[sql] def calculateCov(df: DataFrame, cols: Seq[String]): Double = {
    require(cols.length == 2, "Currently cov supports calculating the covariance " +
      "between two columns.")
    cols.map(name => (name, df.schema.fields.find(_.name == name))).foreach { case (name, data) =>
      require(data.nonEmpty, s"Couldn't find column with name $name")
      require(data.get.dataType.isInstanceOf[NumericType], "Covariance calculation for columns " +
        s"with dataType ${data.get.dataType} not supported.")
    }
    val columns = cols.map(n => Column(Cast(Column(n).expr, DoubleType)))
    val counts = df.select(columns:_*).rdd.aggregate(new CovarianceCounter)(
      seqOp = (counter, row) => {
        counter.add(row.getDouble(0), row.getDouble(1))
      },
      combOp = (baseCounter, other) => {
        baseCounter.merge(other)
      })
    counts.cov
  }

  /** Generate a table of frequencies for the elements of two columns. */
  private[sql] def crossTabulate(df: DataFrame, col1: String, col2: String): DataFrame = {
    val tableName = s"${col1}_$col2"
    val counts = df.groupBy(col1, col2).agg(col(col1), col(col2), count("*")).collect()
    // We need to sort the entries to pivot them properly
    val sorted = counts.sortBy(r => (r.get(0).toString, r.get(1).toString))
    val first = sorted.head.get(0)
    // get the distinct values of column 2, so that we can make them the column names
    val distinctCol2 = sorted.takeWhile(r => r.get(0) == first)
    val columnSize = distinctCol2.length
    require(columnSize < 1e4, s"The number of distinct values for $col2, can't " +
      s"exceed 1e4. Currently $columnSize")
    val table = sorted.grouped(columnSize).map { rows =>
      // the value of col1 is the first value, the rest are the counts
      val rowValues = rows.head.get(0).toString +: rows.map(_.getLong(2))
      Row(rowValues: _*)
    }.toSeq
    val headerNames = distinctCol2.map(r => StructField(r.get(1).toString, LongType))
    val schema = StructType(StructField(tableName, StringType) +: headerNames)

    new DataFrame(df.sqlContext, LocalRelation(schema.toAttributes, table))
  }
}
