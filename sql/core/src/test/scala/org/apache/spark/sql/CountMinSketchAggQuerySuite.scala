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

package org.apache.spark.sql

import java.io.ByteArrayInputStream

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, _}
import org.apache.spark.util.sketch.CountMinSketch

class CountMinSketchAggQuerySuite extends QueryTest with SharedSQLContext {

  private val table = "count_min_sketch_table"

  /** Uses fixed seed to ensure reproducible test execution */
  private val r = new Random(42)
  private val numAllItems = 500000
  private val numSamples = numAllItems / 10

  private val eps = 0.0001
  private val confidence = 0.95
  private val seed = 11

  Seq("cmsketch", "count_min_sketch").foreach { cmsAgg =>
    test(s"compute count-min sketch for multiple columns of different types - with name $cmsAgg") {
      val (allBytes, sampledBytesIndices, exactBytesFreq) =
        generateTestData[Byte] { _.nextInt().toByte }
      val (allShorts, sampledShortsIndices, exactShortsFreq) =
        generateTestData[Short] { _.nextInt().toShort }
      val (allInts, sampledIntsIndices, exactIntsFreq) =
        generateTestData[Int] { _.nextInt() }
      val (allLongs, sampledLongsIndices, exactLongsFreq) =
        generateTestData[Long] { _.nextLong() }
      val (allStrings, sampledStringsIndices, exactStringsFreq) =
        generateTestData[String] { r => r.nextString(r.nextInt(20)) }

      val data = (0 until numSamples).map { i =>
        Row(allBytes(sampledBytesIndices(i)),
          allShorts(sampledShortsIndices(i)),
          allInts(sampledIntsIndices(i)),
          allLongs(sampledLongsIndices(i)),
          allStrings(sampledStringsIndices(i)))
      }

      val schema = StructType(Seq(
        StructField("c1", ByteType),
        StructField("c2", ShortType),
        StructField("c3", IntegerType),
        StructField("c4", LongType),
        StructField("c5", StringType)))

      val query =
        s"""
           |SELECT
           |  $cmsAgg(c1, $eps, $confidence, $seed),
           |  $cmsAgg(c2, $eps, $confidence, $seed),
           |  $cmsAgg(c3, $eps, $confidence, $seed),
           |  $cmsAgg(c4, $eps, $confidence, $seed),
           |  $cmsAgg(c5, $eps, $confidence, $seed)
           |FROM $table
       """.stripMargin

      withTempView(table) {
        val rdd: RDD[Row] = spark.sparkContext.parallelize(data)
        spark.createDataFrame(rdd, schema).createOrReplaceTempView(table)
        val result = sql(query).queryExecution.toRdd.collect().head
        schema.indices.foreach { i =>
          val binaryData = result.getBinary(i)
          val in = new ByteArrayInputStream(binaryData)
          val cms = CountMinSketch.readFrom(in)
          schema.fields(i).dataType match {
            case ByteType => checkResult(cms, allBytes, exactBytesFreq)
            case ShortType => checkResult(cms, allShorts, exactShortsFreq)
            case IntegerType => checkResult(cms, allInts, exactIntsFreq)
            case LongType => checkResult(cms, allLongs, exactLongsFreq)
            case StringType => checkResult(cms, allStrings, exactStringsFreq)
          }
        }
      }
    }
  }

  private def checkResult[T: ClassTag](
      cms: CountMinSketch,
      data: Array[T],
      exactFreq: Map[T, Long]): Unit = {
    val probCorrect = {
      val numErrors = data.map { i =>
        val count = exactFreq.getOrElse(i, 0L)
        val ratio = (cms.estimateCount(i) - count).toDouble / data.length
        if (ratio > eps) 1 else 0
      }.sum

      1D - numErrors.toDouble / data.length
    }

    assert(
      probCorrect > confidence,
      s"Confidence not reached: required $confidence, reached $probCorrect"
    )
  }

  private def generateTestData[T: ClassTag](
      itemGenerator: Random => T): (Array[T], Array[Int], Map[T, Long]) = {
    val allItems = Array.fill(numAllItems)(itemGenerator(r))
    val sampledItemIndices = Array.fill(numSamples)(r.nextInt(numAllItems))
    val exactFreq = {
      val sampledItems = sampledItemIndices.map(allItems)
      sampledItems.groupBy(identity).mapValues(_.length.toLong)
    }
    (allItems, sampledItemIndices, exactFreq)
  }
}
