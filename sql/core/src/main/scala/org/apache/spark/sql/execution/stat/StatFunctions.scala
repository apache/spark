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

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.{DoubleType, NumericType}

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
}
