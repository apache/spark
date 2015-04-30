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

import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.{Column, DataFrame}

private[sql] object StatFunctions {
  
  /** Helper class to simplify tracking and merging counts. */
  private class CovarianceCounter extends Serializable {
    var xAvg = 0.0
    var yAvg = 0.0
    var Ck = 0.0
    var count = 0
    // add an example to the calculation
    def add(x: Number, y: Number): this.type = {
      val oldX = xAvg
      val otherX = x.doubleValue()
      val otherY = y.doubleValue()
      count += 1
      xAvg += (otherX - xAvg) / count
      yAvg += (otherY - yAvg) / count
      println(oldX)
      Ck += (otherY - yAvg) * (otherX - oldX)
      this
    }
    // merge counters from other partitions
    def merge(other: CovarianceCounter): this.type = {
      val totalCount = count + other.count
      Ck += other.Ck + 
        (xAvg - other.xAvg) * (yAvg - other.yAvg) * (count * other.count) / totalCount
      xAvg = (xAvg * count + other.xAvg * other.count) / totalCount
      yAvg = (yAvg * count + other.yAvg * other.count) / totalCount
      count = totalCount
      this
    }
    // return the covariance for the observed examples
    def cov: Double = Ck / count
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
    val counts = df.select(cols.map(Column(_)):_*).rdd.aggregate(new CovarianceCounter)(
      seqOp = (counter, row) => {
        counter.add(row.getAs[Number](0), row.getAs[Number](1))
      },
      combOp = (baseCounter, other) => {
        baseCounter.merge(other)
      })
    counts.cov
  }

}
