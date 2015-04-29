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

package org.apache.spark.sql.ml


import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.{StructType, ArrayType, StructField}

import scala.collection.mutable.{Map => MutableMap}

import org.apache.spark.Logging
import org.apache.spark.sql.{Row, DataFrame, functions}

private[sql] object FrequentItems extends Logging {

  /**
   * Merge two maps of counts. Subtracts the sum of `otherMap` from `baseMap`, and fills in
   * any emptied slots with the most frequent of `otherMap`.
   * @param baseMap The map containing the global counts
   * @param otherMap The map containing the counts for that partition
   * @param maxSize The maximum number of counts to keep in memory
   */
  private def mergeCounts[A](
      baseMap: MutableMap[A, Long],
      otherMap: MutableMap[A, Long],
      maxSize: Int): Unit = {
    val otherSum = otherMap.foldLeft(0L) { case (sum, (k, v)) =>
      if (!baseMap.contains(k)) sum + v else sum
    }
    baseMap.retain((k, v) => v > otherSum)
    // sort in decreasing order, so that we will add the most frequent items first
    val sorted = otherMap.toSeq.sortBy(-_._2)
    var i = 0
    val otherSize = sorted.length
    while (i < otherSize && baseMap.size < maxSize) {
      val keyVal = sorted(i)
      baseMap += keyVal._1 -> keyVal._2
      i += 1
    }
  }
  

  /**
   * Finding frequent items for columns, possibly with false positives. Using the algorithm 
   * described in `http://www.cs.umd.edu/~samir/498/karp.pdf`.
   * For Internal use only.
   *
   * @param df The input DataFrame
   * @param cols the names of the columns to search frequent items in
   * @param support The minimum frequency for an item to be considered `frequent`
   * @return A Local DataFrame with the Array of frequent items for each column.
   */
  private[sql] def singlePassFreqItems(
      df: DataFrame, 
      cols: Array[String], 
      support: Double): DataFrame = {
    val numCols = cols.length
    // number of max items to keep counts for
    val sizeOfMap = math.floor(1 / support).toInt
    val countMaps = Array.tabulate(numCols)(i => MutableMap.empty[Any, Long])
    val originalSchema = df.schema
    val colInfo = cols.map { name =>
      val index = originalSchema.fieldIndex(name)
      val dataType = originalSchema.fields(index)
      (index, dataType.dataType)
    }
    val colIndices = colInfo.map(_._1)
    
    val freqItems: Array[MutableMap[Any, Long]] = df.rdd.aggregate(countMaps)(
      seqOp = (counts, row) => {
        var i = 0
        colIndices.foreach { index =>
          val thisMap = counts(i)
          val key = row.get(index)
          if (thisMap.contains(key))  {
            thisMap(key) += 1
          } else {
            if (thisMap.size < sizeOfMap) {
              thisMap += key -> 1
            } else {
              // TODO: Make this more efficient... A flatMap?
              thisMap.retain((k, v) => v > 1)
              thisMap.transform((k, v) => v - 1)
            }
          }
          i += 1
        }
        counts
      },
      combOp = (baseCounts, counts) => {
        var i = 0
        while (i < numCols) {
          mergeCounts(baseCounts(i), counts(i), sizeOfMap)
          i += 1
        }
        baseCounts
      }
    )
    //
    val justItems = freqItems.map(m => m.keys.toSeq)
    val resultRow = Row(justItems:_*)
    // append frequent Items to the column name for easy debugging
    val outputCols = cols.zip(colInfo).map{ v =>
      StructField(v._1 + "-freqItems", ArrayType(v._2._2, false))
    }
    val schema = StructType(outputCols).toAttributes
    new DataFrame(df.sqlContext, LocalRelation(schema, Seq(resultRow)))
  }
}
