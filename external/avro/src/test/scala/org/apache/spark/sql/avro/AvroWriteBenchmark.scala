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

package org.apache.spark.sql.avro

import java.sql.Date
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

import com.google.common.io.Files
import org.apache.commons.io.FileUtils

import org.apache.spark.sql._
import org.apache.spark.sql.types._

// scalastyle:off println

/**
 * This object runs a simple benchmark test to find out how long does it take to write a large
 * DataFrame to an avro file. It reads one argument, which specifies how many rows does the
 * DataFrame that we're writing contain.
 */
object AvroWriteBenchmark {

  val defaultNumberOfRows = 1000000
  val defaultSize = 100 // Size used for items in generated RDD like strings, arrays and maps

  val testSchema = StructType(Seq(
    StructField("StringField", StringType, false),
    StructField("IntField", IntegerType, true),
    StructField("dateField", DateType, true),
    StructField("DoubleField", DoubleType, false),
    StructField("DecimalField", DecimalType(10, 10), true),
    StructField("ArrayField", ArrayType(BooleanType), false),
    StructField("MapField", MapType(StringType, IntegerType), true),
    StructField("StructField", StructType(Seq(StructField("id", IntegerType, true))), false)))

  private def generateRandomRow(): Row = {
    val rand = new Random()
    Row(rand.nextString(defaultSize), rand.nextInt(), new Date(rand.nextLong()), rand.nextDouble(),
      BigDecimal(rand.nextDouble()).setScale(10, RoundingMode.HALF_UP),
      TestUtils.generateRandomArray(rand, defaultSize).asScala,
      TestUtils.generateRandomMap(rand, defaultSize).asScala, Row(rand.nextInt()))
  }

  def main(args: Array[String]) {
    var numberOfRows = defaultNumberOfRows
    if (args.size > 0) {
      numberOfRows = args(0).toInt
    }

    println(s"\n\n\nPreparing for a benchmark test - creating a RDD with $numberOfRows rows\n\n\n")

    val spark = SparkSession.builder().master("local[2]").appName("AvroReadBenchmark")
      .getOrCreate()

    val tempDir = Files.createTempDir()
    val avroDir = tempDir + "/avro"
    val testDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(0 until numberOfRows).map(_ => generateRandomRow()),
      testSchema)

    println("\n\n\nStaring benchmark test - writing a DataFrame as avro file\n\n\n")

    val startTime = System.nanoTime

    testDataFrame.write.avro(avroDir)

    val endTime = System.nanoTime
    val executionTime = TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS)

    println(s"\n\n\nFinished benchmark test - result was $executionTime seconds\n\n\n")

    FileUtils.deleteDirectory(tempDir)
    spark.sparkContext.stop()  // Otherwise scary exception message appears
  }
}
// scalastyle:on println
