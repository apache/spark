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

package org.apache.spark.sql.execution.benchmark

import scala.util.Random

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.streaming.{SerializationType, StateEncoder}

object TWSSerializationDataType extends Enumeration {
  type TWSSerializationDataType = Value
  val PRIMITVE, CASE_CLASS, POJO = Value
}

/**
 * Synthetic benchmark for State Store basic operations.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/TWSSerializationBenchmark-results.txt".
 * }}}
 */
object TWSSerializationBenchmark extends SqlBasedBenchmark {
  private val NUM_OF_ROWS = 100
  private val ITERATIONS = 10

  // Use random with static seed to generate value sizes
  private val randomNumGenerator = new scala.util.Random(100)

  // Construct Seq[obj, encodedObjToDecode], primitive string type
  private def constructPrimitiveRandomizedTestData(
      numRows: Int,
      serializer: SerializationType.Value): Seq[(String, UnsafeRow)] = {
    (1 to numRows).map { idx =>
      val valueStr = Random.alphanumeric.take(randomNumGenerator.nextInt(100)).mkString
      val encodedVal = serializer match {
        case SerializationType.AVRO =>
          StateEncoder.encodeValToAvro[String](valueStr, Encoders.STRING)
        case SerializationType.SPARK_SQL =>
          StateEncoder.encodeValSparkSQL[String](valueStr, Encoders.STRING)
        case _ =>
          StateEncoder.encodeValue[String](valueStr)
      }

      (valueStr, encodedVal)
    }
  }

  private def registerBenchmark(
      benchmark: Benchmark,
      serializer: SerializationType.Value)(f: => Unit): Unit = {
    val testName = s"serializerType: $serializer"
    benchmark.addTimerCase(testName) { timer =>
      timer.startTiming()
      f
      timer.stopTiming()
    }
  }

  private def encodeValToRow[T](
     serializer: SerializationType.Value,
     rows: Seq[T],
     valEnc: Encoder[T]): Unit = {
    rows.foreach { row =>
      serializer match {
        case SerializationType.AVRO =>
          StateEncoder.encodeValToAvro[T](row, valEnc)
        case SerializationType.SPARK_SQL =>
          StateEncoder.encodeValSparkSQL[T](row, valEnc)
        case _ =>
          StateEncoder.encodeValue[T](row)
      }
    }
  }

  private def decodeRowToVal[T](
     serializer: SerializationType.Value,
     rows: Seq[UnsafeRow],
     valEnc: Encoder[T]): Unit = {
    rows.foreach { row =>
      serializer match {
        case SerializationType.AVRO =>
          StateEncoder.decodeAvroToValue[T](row, valEnc)
        case SerializationType.SPARK_SQL =>
          StateEncoder.decodeValSparkSQL[T](row, valEnc)
        case _ =>
          StateEncoder.decodeValue[T](row)
      }
    }
  }

  private def serializeRoundTrip[T](
     serializer: SerializationType.Value,
     rows: Seq[T],
     valEnc: Encoder[T]): Unit = {
    rows.foreach { row =>
      val unsafeRow = serializer match {
        case SerializationType.AVRO =>
          StateEncoder.encodeValToAvro[T](row, valEnc)
        case SerializationType.SPARK_SQL =>
          StateEncoder.encodeValSparkSQL[T](row, valEnc)
        case _ =>
          StateEncoder.encodeValue[T](row)
      }
      serializer match {
        case SerializationType.AVRO =>
          StateEncoder.decodeAvroToValue[T](unsafeRow, valEnc)
        case SerializationType.SPARK_SQL =>
          StateEncoder.decodeValSparkSQL[T](unsafeRow, valEnc)
        case _ =>
          StateEncoder.decodeValue[T](unsafeRow)
      }
    }
  }

  private def executeEncoding[T](
     benchmark: Benchmark,
     serializer: SerializationType.Value,
     rows: Seq[T],
     valEnc: Encoder[T]): Unit = {
    registerBenchmark(benchmark, serializer) {
      encodeValToRow[T](serializer, rows, valEnc)
    }
  }

  private def executeDecoding[T](
     benchmark: Benchmark,
     serializer: SerializationType.Value,
     rows: Seq[UnsafeRow],
     valEnc: Encoder[T]): Unit = {
    registerBenchmark(benchmark, serializer) {
      decodeRowToVal[T](serializer, rows, valEnc)
    }
  }

  private def executeRoundTrip[T](
     benchmark: Benchmark,
     serializer: SerializationType.Value,
     rows: Seq[T],
     valEnc: Encoder[T]): Unit = {
    registerBenchmark(benchmark, serializer) {
      serializeRoundTrip[T](serializer, rows, valEnc)
    }
  }

  private def runEncodeBenchmark(): Unit = {
    val numRows = NUM_OF_ROWS

    // primitive type
    Seq(SerializationType.JAVA, SerializationType.SPARK_SQL, SerializationType.AVRO).foreach { se =>
      val testData = constructPrimitiveRandomizedTestData(numRows, se)
      val valsToEncode = testData.map(_._1)

      val benchmarkName = s"encode benchmark with numRows=$NUM_OF_ROWS and primitive type"
      val benchmark = new Benchmark(benchmarkName, NUM_OF_ROWS, ITERATIONS, output = output)
      executeEncoding[String](benchmark, se, valsToEncode, Encoders.STRING)

      benchmark.run()
    }
  }

  private def runDecodeBenchmark(): Unit = {
    val numRows = NUM_OF_ROWS

    // primitive type
    Seq(SerializationType.JAVA, SerializationType.SPARK_SQL, SerializationType.AVRO).foreach { se =>
      val testData = constructPrimitiveRandomizedTestData(numRows, se)
      val valsToEncode = testData.map(_._2)

      val benchmarkName = s"decode benchmark with numRows=$NUM_OF_ROWS and primitive type"
      val benchmark = new Benchmark(benchmarkName, NUM_OF_ROWS, ITERATIONS, output = output)
      executeDecoding[String](benchmark, se, valsToEncode, Encoders.STRING)

      benchmark.run()
    }
  }

  private def runRoundTripBenchmark(): Unit = {
    val numRows = NUM_OF_ROWS

    // primitive type
    Seq(SerializationType.JAVA, SerializationType.SPARK_SQL, SerializationType.AVRO).foreach { se =>
      val testData = constructPrimitiveRandomizedTestData(numRows, se)
      val rowsToDecode = testData.map(_._1)

      val benchmarkName = s"serializer round trip benchmark with " +
        s"numRows=$NUM_OF_ROWS and primitive type"
      val benchmark = new Benchmark(benchmarkName, NUM_OF_ROWS, ITERATIONS, output = output)
      executeRoundTrip[String](benchmark, se, rowsToDecode, Encoders.STRING)

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runEncodeBenchmark()
    runDecodeBenchmark()
    runRoundTripBenchmark()
  }
}
