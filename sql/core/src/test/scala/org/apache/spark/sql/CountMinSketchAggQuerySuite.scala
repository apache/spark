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
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{Decimal, StringType, _}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.sketch.CountMinSketch

class CountMinSketchAggQuerySuite extends QueryTest with SharedSQLContext {

  private val table = "count_min_sketch_table"

  /** Uses fixed seed to ensure reproducible test execution */
  private val r = new Random(42)
  private val numAllItems = 1000
  private val numSamples = numAllItems / 10

  private val eps = 0.1D
  private val confidence = 0.95D
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
    val (allFloats, sampledFloatIndices, exactFloatFreq) =
      generateTestData[Float] { _.nextFloat() }
    val (allDoubles, sampledDoubleIndices, exactDoubleFreq) =
      generateTestData[Double] { _.nextDouble() }
    val (allDeciamls, sampledDecimalIndices, exactDecimalFreq) =
      generateTestData[Decimal] { r => Decimal(r.nextDouble()) }
    val (allBooleans, sampledBooleanIndices, exactBooleanFreq) =
      generateTestData[Boolean] { _.nextBoolean() }
    val (allBinaries, sampledBinaryIndices, exactBinaryFreq) = generateTestData[Array[Byte]] { r =>
      r.nextString(r.nextInt(20)).getBytes(StandardCharsets.UTF_8)
    }

    val data = (0 until numSamples).map { i =>
      Row(allBytes(sampledByteIndices(i)),
        allShorts(sampledShortIndices(i)),
        allInts(sampledIntIndices(i)),
        allLongs(sampledLongIndices(i)),
        allStrings(sampledStringIndices(i)),
        allDates(sampledDateIndices(i)),
        allTimestamps(sampledTSIndices(i)),
        allFloats(sampledFloatIndices(i)),
        allDoubles(sampledDoubleIndices(i)),
        allDeciamls(sampledDecimalIndices(i)),
        allBooleans(sampledBooleanIndices(i)),
        allBinaries(sampledBinaryIndices(i)))
    }

    val schema = StructType(Seq(
      StructField("c1", ByteType),
      StructField("c2", ShortType),
      StructField("c3", IntegerType),
      StructField("c4", LongType),
      StructField("c5", StringType),
      StructField("c6", DateType),
      StructField("c7", TimestampType),
      StructField("c8", FloatType),
      StructField("c9", DoubleType),
      StructField("c10", new DecimalType()),
      StructField("c11", BooleanType),
      StructField("c12", BinaryType)))

    withTempView(table) {
      val rdd: RDD[Row] = spark.sparkContext.parallelize(data)
      spark.createDataFrame(rdd, schema).createOrReplaceTempView(table)

      val cmsSql = schema.fieldNames.map { col =>
        s"count_min_sketch($col, ${eps}D, ${confidence}D, $seed)"
      }
      val result = sql(s"SELECT ${cmsSql.mkString(", ")} FROM $table").head()
      schema.indices.foreach { i =>
        val binaryData = result.getAs[Array[Byte]](i)
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
              exactDateFreq.map { e =>
                (DateTimeUtils.fromJavaDate(e._1.asInstanceOf[Date]), e._2)
              })
          case TimestampType =>
            checkResult(cms,
              allTimestamps.map(DateTimeUtils.fromJavaTimestamp),
              exactTSFreq.map { e =>
                (DateTimeUtils.fromJavaTimestamp(e._1.asInstanceOf[Timestamp]), e._2)
              })
          case FloatType => checkResult(cms, allFloats, exactFloatFreq)
          case DoubleType => checkResult(cms, allDoubles, exactDoubleFreq)
          case DecimalType() => checkResult(cms, allDeciamls, exactDecimalFreq)
          case BooleanType => checkResult(cms, allBooleans, exactBooleanFreq)
          case BinaryType => checkResult(cms, allBinaries, exactBinaryFreq)
        }
      }
    }
  }

  private def checkResult[T: ClassTag](
      cms: CountMinSketch,
      data: Array[T],
      exactFreq: Map[Any, Long]): Unit = {
    val probCorrect = {
      val numErrors = data.map { i =>
        val count = exactFreq.getOrElse(getProbeItem(i), 0L)
        val item = i match {
          case dec: Decimal => dec.toJavaBigDecimal
          case str: UTF8String => str.getBytes
          case _ => i
        }
        val ratio = (cms.estimateCount(item) - count).toDouble / data.length
        if (ratio > eps) 1 else 0
      }.sum

      1D - numErrors.toDouble / data.length
    }

    assert(
      probCorrect > confidence,
      s"Confidence not reached: required $confidence, reached $probCorrect"
    )
  }

  private def getProbeItem[T: ClassTag](item: T): Any = item match {
    // Use a string to represent the content of an array of bytes
    case bytes: Array[Byte] => new String(bytes, StandardCharsets.UTF_8)
    case i => identity(i)
  }

  private def generateTestData[T: ClassTag](
      itemGenerator: Random => T): (Array[T], Array[Int], Map[Any, Long]) = {
    val allItems = Array.fill(numAllItems)(itemGenerator(r))
    val sampledItemIndices = Array.fill(numSamples)(r.nextInt(numAllItems))
    val exactFreq = {
      val sampledItems = sampledItemIndices.map(allItems)
      sampledItems.groupBy(getProbeItem).mapValues(_.length.toLong)
    }
    (allItems, sampledItemIndices, exactFreq)
  }
}
