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
import java.sql.{Date, Timestamp}

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.DateTimeUtils
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

  val startDate = DateTimeUtils.fromJavaDate(Date.valueOf("1900-01-01"))
  val endDate = DateTimeUtils.fromJavaDate(Date.valueOf("2016-01-01"))
  val startTS = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("1900-01-01 00:00:00"))
  val endTS = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2016-01-01 00:00:00"))

  test(s"compute count-min sketch for multiple columns of different types") {
    val (allBytes, sampledByteIndices, exactByteFreq) =
      generateTestData[Byte] { _.nextInt().toByte }
    val (allShorts, sampledShortIndices, exactShortFreq) =
      generateTestData[Short] { _.nextInt().toShort }
    val (allInts, sampledIntIndices, exactIntFreq) =
      generateTestData[Int] { _.nextInt() }
    val (allLongs, sampledLongIndices, exactLongFreq) =
      generateTestData[Long] { _.nextLong() }
    val (allStrings, sampledStringIndices, exactStringFreq) =
      generateTestData[String] { r => r.nextString(r.nextInt(20)) }
    val (allDates, sampledDateIndices, exactDateFreq) = generateTestData[Date] { r =>
      DateTimeUtils.toJavaDate(r.nextInt(endDate - startDate) + startDate)
    }
    val (allTimestamps, sampledTSIndices, exactTSFreq) = generateTestData[Timestamp] { r =>
      DateTimeUtils.toJavaTimestamp(r.nextLong() % (endTS - startTS) + startTS)
    }

    val data = (0 until numSamples).map { i =>
      Row(allBytes(sampledByteIndices(i)),
        allShorts(sampledShortIndices(i)),
        allInts(sampledIntIndices(i)),
        allLongs(sampledLongIndices(i)),
        allStrings(sampledStringIndices(i)),
        allDates(sampledDateIndices(i)),
        allTimestamps(sampledTSIndices(i)))
    }

    val schema = StructType(Seq(
      StructField("c1", ByteType),
      StructField("c2", ShortType),
      StructField("c3", IntegerType),
      StructField("c4", LongType),
      StructField("c5", StringType),
      StructField("c6", DateType),
      StructField("c7", TimestampType)))

    val query =
      s"""
         |SELECT
         |  count_min_sketch(c1, $eps, $confidence, $seed),
         |  count_min_sketch(c2, $eps, $confidence, $seed),
         |  count_min_sketch(c3, $eps, $confidence, $seed),
         |  count_min_sketch(c4, $eps, $confidence, $seed),
         |  count_min_sketch(c5, $eps, $confidence, $seed),
         |  count_min_sketch(c6, $eps, $confidence, $seed),
         |  count_min_sketch(c7, $eps, $confidence, $seed)
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
          case ByteType => checkResult(cms, allBytes, exactByteFreq)
          case ShortType => checkResult(cms, allShorts, exactShortFreq)
          case IntegerType => checkResult(cms, allInts, exactIntFreq)
          case LongType => checkResult(cms, allLongs, exactLongFreq)
          case StringType => checkResult(cms, allStrings, exactStringFreq)
          case DateType =>
            checkResult(cms,
              allDates.map(DateTimeUtils.fromJavaDate),
              exactDateFreq.map(e => (DateTimeUtils.fromJavaDate(e._1), e._2)))
          case TimestampType =>
            checkResult(cms,
              allTimestamps.map(DateTimeUtils.fromJavaTimestamp),
              exactTSFreq.map(e => (DateTimeUtils.fromJavaTimestamp(e._1), e._2)))
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
