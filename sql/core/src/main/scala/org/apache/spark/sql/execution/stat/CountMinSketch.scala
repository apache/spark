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

import scala.collection.mutable.{Map => MutableMap}

import org.apache.spark.Logging
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.util.Utils

private[sql] object CountMinSketch extends Logging {
 
  private def initHash(depth: Int): Seq[(Int, Int)] = {
    val MOD: Int = 2147483647
    val random = Utils.random
    (0 until depth).map { n =>
      val hash1 = random.nextInt() & MOD
      val hash2 = random.nextInt() & MOD
      (hash1, hash2) 
    }.toSeq
  }
 
  /** A helper class for performing CountMinSketch. */
  private class CountMinSketchCounter(
      depth: Int,
      width: Int,
      hashFunctions: Seq[(Int, Int)]) extends Serializable {
    val counters: Seq[MutableMap[Int, Long]] = Seq.tabulate(depth)(i =>
      MutableMap.empty[Int, Long])
    val MOD: Int = 2147483647
    val HL: Int = 31

    def query(item: Long): Long = {
      (0 until depth).map { n =>
        val hashKey = hashFunctions(n)
        val hashValue = hash31(hashKey._1, hashKey._2, item)
        val idx = hashValue % width
        if (counters(n).contains(idx)) {
          counters(n)(idx)
        } else {
          0
        }
      }.min
    }

    def update(item: Long, count: Int = 1): Unit = {
      (0 until depth).map { n =>
        val hashKey = hashFunctions(n)
        val hashValue = hash31(hashKey._1, hashKey._2, item)
        val idx = hashValue % width
        add(n, idx, count)
      }
    }

    private def initHash(): Seq[(Int, Int)] = {
      val random = Utils.random
      (0 until depth).map { n =>
        val hash1 = random.nextInt() & MOD
        val hash2 = random.nextInt() & MOD
        (hash1, hash2) 
      }.toSeq
    }

    /**
     * Calculate the hashing value for updating counters.
     * Ported from http://www.cs.rutgers.edu/~muthu/prng.c.
     */
    private def hash31(a: Long, b: Long, c: Long): Int = {
      var result: Long = (a * c) + b
      result = ((result >> HL) + result) & MOD
      result.toInt
    }

    /**
     * Add an event count to the counters.
     */
    def add(row: Int, key: Int, count: Long): this.type = {
      if (counters(row).contains(key)) {
        counters(row)(key) += count
      } else {
        counters(row)(key) = count
      }
      this
    }

    /**
     * Merge two counters.
     * @param other The counters containing the counts for that partition
     */
    def merge(other: CountMinSketchCounter): this.type = {
      var i = 0
      other.counters.foreach { c =>
        c.foreach { case (k, v) =>
          add(i, k, v)
        }
        i += 1
      }
      this
    }
  }

  /**
   * Calculate the estimated frequencies by using count-min sketch algorithm described in
   * [[http://dx.doi.org/10.1016/j.jalgor.2003.12.001, proposed by Cormode and Muthukrishnan.]].
   *
   * @param df The input DataFrame
   * @param col the name of the column to estimate frequencies
   * @param width the width of the array of counters
   * @param depth the depth of the array of counters 
   * @return A Local DataFrame with the Array of estimated frequencies for each value in the column
   */
  private[sql] def countMinSketch(
      df: DataFrame, 
      col: String,
      width: Int,
      depth: Int): DataFrame = {
    require(width > 0, s"width ($width) must be greater than 0.")
    require(depth > 0, s"depth ($depth) must be greater than 0.")

    import df.sqlContext.implicits._

    val originalSchema = df.schema
    val colInfo = {
      val index = originalSchema.fieldIndex(col)
      originalSchema.fields(index).dataType
    }

    // Assign an unique id to each distinct value in the column
    val rddIndexed = df.select(Column(col)).rdd.zipWithUniqueId()
      .groupByKey().flatMap { kv =>
        val len = kv._2.size
        val first = kv._2.head
        (0 until len).map(_ => (kv._1, first))
      }

    val hashFunctions = initHash(depth)

    val freqs = rddIndexed.aggregate(new CountMinSketchCounter(depth, width, hashFunctions))(
      seqOp = (counter, rowAndId) => {
        counter.update(rowAndId._2, 1)
        counter
      },
      combOp = (baseCounter, counter) => {
        baseCounter.merge(counter)
        baseCounter
      }
    )
    val freqRows = rddIndexed.map{ rowAndId =>
      Row(rowAndId._1(0), freqs.query(rowAndId._2))
    }
    val outputCols = Seq(StructField(col, colInfo), StructField(col + "_freq", LongType))
    val schema = StructType(outputCols)
    df.sqlContext.createDataFrame(freqRows, schema).distinct
  }
}
