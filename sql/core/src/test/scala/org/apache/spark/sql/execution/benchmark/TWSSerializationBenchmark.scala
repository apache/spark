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
import org.apache.spark.sql.execution.streaming.state.{Person, TestClass}
import org.apache.spark.sql.streaming.{SerializationType, StateEncoder}

object TWSSerializationDataType extends Enumeration {
  type TWSSerializationDataType = Value
  val PRIMITIVE_STRING, PRIMITIVE_INT, PRIMITIVE_LONG, PRIMITIVE_DOUBLE,
      CASE_CLASS, POJO = Value
}

object TWSSerializationOpType extends Enumeration {
  type TWSSerializationOpType = Value
  val ENCODE, DECODE, ROUND_TRIP = Value
}

/**
 * Synthetic benchmark for TransformWithState Serialization methods
 * on different data type - primitive, case class, POJO.
 *
 * Note that Java serialization cannot serialize POJO type.
 *
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
  private val NUM_OF_ROWS = 1000
  private val ITERATIONS = 10

  // Use random with static seed to generate value sizes
  private val randomNumGenerator = new scala.util.Random(100)

  // Construct Seq[obj, encodedObjToDecode], primitive string type
  private def constructPrimitiveRandomizedTestDataBase[T](
      numRows: Int,
      serializer: SerializationType.Value,
      valEnc: Encoder[T],
      generateRandomEntry: => T): Seq[(T, UnsafeRow)] = {
    (1 to numRows).map { idx =>
      val valEntry = generateRandomEntry
      val stateEncoder = new StateEncoder[T](valEnc)
      val encodedVal = serializer match {
        case SerializationType.AVRO =>
          stateEncoder.encodeValToAvro(valEntry)
        case SerializationType.SPARK_SQL =>
          stateEncoder.encodeValSparkSQL(valEntry)
        case _ =>
          stateEncoder.encodeValue(valEntry)
      }

      (valEntry, encodedVal)
    }
  }

  // Construct Seq[obj, encodedObjToDecode], case class TestClass type
  private def constructCaseClassRandomizedTestData(numRows: Int,
      serializer: SerializationType.Value): Seq[(TestClass, UnsafeRow)] = {
    (1 to numRows).map { idx =>
      val valueStr = Random.alphanumeric.take(randomNumGenerator.nextInt(100)).mkString
      val valueInt = randomNumGenerator.nextInt(100)
      val newCaseClass = new TestClass(valueInt, valueStr)
      val stateEncoder = new StateEncoder[TestClass](Encoders.product[TestClass])
      val encodedVal = serializer match {
        case SerializationType.AVRO =>
          stateEncoder.encodeValToAvro(newCaseClass)
        case SerializationType.SPARK_SQL =>
          stateEncoder.encodeValSparkSQL(newCaseClass)
        case _ =>
          stateEncoder.encodeValue(newCaseClass)
      }

      (newCaseClass, encodedVal)
    }
  }

  // Construct Seq[obj, encodedObjToDecode], POJO type
  private def constructPOJORandomizedTestData(numRows: Int,
      serializer: SerializationType.Value): Seq[(Person, UnsafeRow)] = {
    (1 to numRows).map { idx =>
      val valueStr = Random.alphanumeric.take(randomNumGenerator.nextInt(100)).mkString
      val valueInt = randomNumGenerator.nextInt(100)
      val newCaseClass = new Person(valueStr, valueInt)
      val stateEncoder = new StateEncoder[Person](Encoders.bean(classOf[Person]))
      val encodedVal = serializer match {
        case SerializationType.AVRO =>
          stateEncoder.encodeValToAvro(newCaseClass)
        case SerializationType.SPARK_SQL =>
          stateEncoder.encodeValSparkSQL(newCaseClass)
        case _ =>
          stateEncoder.encodeValue(newCaseClass)
      }

      (newCaseClass, encodedVal)
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

  private def runBenchmarkWithDataType(
      dataType: TWSSerializationDataType.Value,
      benchmarkOp: String)
    (f: (SerializationType.Value, Benchmark) => Seq[UnsafeRow]): Unit = {
    Seq(SerializationType.JAVA, SerializationType.SPARK_SQL, SerializationType.AVRO).foreach { se =>
      if (!(se == SerializationType.JAVA && dataType == TWSSerializationDataType.POJO)) {
        val benchmarkName = s"$benchmarkOp benchmark with numRows=$NUM_OF_ROWS and $dataType"
        val benchmark = new Benchmark(benchmarkName, NUM_OF_ROWS, ITERATIONS, output = output)
        val rows = f(se, benchmark)
        benchmark.run()
        printSizeInfo(benchmark, rows)
      }
    }
  }

  private def encodeValToRow[T](
      serializer: SerializationType.Value,
      rows: Seq[T],
      stateEncoder: StateEncoder[T]): Unit = {
    rows.foreach { row =>
      serializer match {
        case SerializationType.AVRO =>
          stateEncoder.encodeValToAvro(row)
        case SerializationType.SPARK_SQL =>
          stateEncoder.encodeValSparkSQL(row)
        case _ =>
          stateEncoder.encodeValue(row)
      }
    }
  }

  private def decodeRowToVal[T](
      serializer: SerializationType.Value,
      rows: Seq[UnsafeRow],
      stateEncoder: StateEncoder[T]): Unit = {
    rows.foreach { row =>
      serializer match {
        case SerializationType.AVRO =>
          stateEncoder.decodeAvroToValue(row)
        case SerializationType.SPARK_SQL =>
          stateEncoder.decodeValSparkSQL(row)
        case _ =>
          stateEncoder.decodeValue(row)
      }
    }
  }

  private def serializeRoundTrip[T](
      serializer: SerializationType.Value,
      rows: Seq[T],
      stateEncoder: StateEncoder[T]): Unit = {
    rows.foreach { row =>
      val unsafeRow = serializer match {
        case SerializationType.AVRO =>
          stateEncoder.encodeValToAvro(row)
        case SerializationType.SPARK_SQL =>
          stateEncoder.encodeValSparkSQL(row)
        case _ =>
          stateEncoder.encodeValue(row)
      }
      serializer match {
        case SerializationType.AVRO =>
          stateEncoder.decodeAvroToValue(unsafeRow)
        case SerializationType.SPARK_SQL =>
          stateEncoder.decodeValSparkSQL(unsafeRow)
        case _ =>
          stateEncoder.decodeValue(unsafeRow)
      }
    }
  }

  private def executeEncoding[T](
      benchmark: Benchmark,
      serializer: SerializationType.Value,
      rows: Seq[T],
      valEnc: Encoder[T]): Unit = {
    val stateEncoder = new StateEncoder[T](valEnc)
    registerBenchmark(benchmark, serializer) {
      encodeValToRow[T](serializer, rows, stateEncoder)
    }
  }

  private def executeDecoding[T](
      benchmark: Benchmark,
      serializer: SerializationType.Value,
      rows: Seq[UnsafeRow],
      valEnc: Encoder[T]): Unit = {
    val stateEncoder = new StateEncoder[T](valEnc)
    registerBenchmark(benchmark, serializer) {
      decodeRowToVal[T](serializer, rows, stateEncoder)
    }
  }

  private def executeRoundTrip[T](
      benchmark: Benchmark,
      serializer: SerializationType.Value,
      rows: Seq[T],
      valEnc: Encoder[T]): Unit = {
    val stateEncoder = new StateEncoder[T](valEnc)
    registerBenchmark(benchmark, serializer) {
      serializeRoundTrip[T](serializer, rows, stateEncoder)
    }
  }

  private def printSizeInfo(benchmark: Benchmark, testData: Seq[UnsafeRow]): Unit = {
    val sizeSeq: Seq[Int] = testData.map(_.getSizeInBytes)
    // scalastyle:off
    val prompt = s"Size info for ${sizeSeq.length} rows"
    val out = benchmark.out
    val nameLen = Math.max(40, prompt.length)
    out.printf(s"%-${nameLen}s %18s %18s %14s\n",
        s"$prompt:",
        "Smallest size",
        "Largest size",
        "Avg Bytes"
      )
    out.println("-" * (nameLen + 80))

    out.printf(s"%-${nameLen}s %18s %18s %14s\n",
      "results:",
      "%5.0f" format sizeSeq.min.toDouble,
      "%5.0f" format sizeSeq.max.toDouble,
      "%4.0f" format sizeSeq.reduce(_ + _).toDouble / sizeSeq.size)

    out.println("\n")
    // scalastyle:on
  }

  private def runBenchmarkWithOp(op: TWSSerializationOpType.Value): Unit = {
    // primitive type - String
    runBenchmarkWithDataType(TWSSerializationDataType.PRIMITIVE_STRING, op.toString) {
      (serializer, benchmark) =>
      val testData = constructPrimitiveRandomizedTestDataBase[String](
        NUM_OF_ROWS, serializer, Encoders.STRING,
        Random.alphanumeric.take(randomNumGenerator.nextInt(100)).mkString
      )
      op match {
        case TWSSerializationOpType.ENCODE =>
          executeEncoding[String](benchmark, serializer, testData.map(_._1), Encoders.STRING)
        case TWSSerializationOpType.DECODE =>
          executeDecoding[String](benchmark, serializer, testData.map(_._2), Encoders.STRING)
        case TWSSerializationOpType.ROUND_TRIP =>
          executeRoundTrip[String](benchmark, serializer, testData.map(_._1), Encoders.STRING)
      }
      testData.map(_._2)
    }

    // primitive type - Int
    runBenchmarkWithDataType(TWSSerializationDataType.PRIMITIVE_INT, op.toString) {
      (serializer, benchmark) =>
      val testData = constructPrimitiveRandomizedTestDataBase[Int](
        NUM_OF_ROWS, serializer, Encoders.scalaInt,
        randomNumGenerator.nextInt(100)
      )
      op match {
        case TWSSerializationOpType.ENCODE =>
          executeEncoding[Int](benchmark, serializer, testData.map(_._1), Encoders.scalaInt)
        case TWSSerializationOpType.DECODE =>
          executeDecoding[Int](benchmark, serializer, testData.map(_._2), Encoders.scalaInt)
        case TWSSerializationOpType.ROUND_TRIP =>
          executeRoundTrip[Int](benchmark, serializer, testData.map(_._1), Encoders.scalaInt)
      }
      testData.map(_._2)
    }

    // primitive type - Long
    runBenchmarkWithDataType(TWSSerializationDataType.PRIMITIVE_LONG, op.toString) {
      (serializer, benchmark) =>
        val testData = constructPrimitiveRandomizedTestDataBase[Long](
          NUM_OF_ROWS, serializer, Encoders.scalaLong,
          randomNumGenerator.nextLong(100L)
        )
        op match {
          case TWSSerializationOpType.ENCODE =>
            executeEncoding[Long](benchmark, serializer, testData.map(_._1), Encoders.scalaLong)
          case TWSSerializationOpType.DECODE =>
            executeDecoding[Long](benchmark, serializer, testData.map(_._2), Encoders.scalaLong)
          case TWSSerializationOpType.ROUND_TRIP =>
            executeRoundTrip[Long](benchmark, serializer, testData.map(_._1), Encoders.scalaLong)
        }
        testData.map(_._2)
    }

    // primitive type - Double
    runBenchmarkWithDataType(TWSSerializationDataType.PRIMITIVE_DOUBLE, op.toString) {
      (serializer, benchmark) =>
        val testData = constructPrimitiveRandomizedTestDataBase[Double](
          NUM_OF_ROWS, serializer, Encoders.scalaDouble,
          randomNumGenerator.nextDouble()
        )
        op match {
          case TWSSerializationOpType.ENCODE =>
            executeEncoding[Double](benchmark, serializer, testData.map(_._1), Encoders.scalaDouble)
          case TWSSerializationOpType.DECODE =>
            executeDecoding[Double](benchmark, serializer, testData.map(_._2), Encoders.scalaDouble)
          case TWSSerializationOpType.ROUND_TRIP =>
            executeRoundTrip[Double](benchmark, serializer,
              testData.map(_._1), Encoders.scalaDouble)
        }
        testData.map(_._2)
    }

    // case class
    runBenchmarkWithDataType(TWSSerializationDataType.CASE_CLASS,
      op.toString) { (serializer, benchmark) =>
      val testData = constructCaseClassRandomizedTestData(NUM_OF_ROWS, serializer)
      op match {
        case TWSSerializationOpType.ENCODE =>
          executeEncoding[TestClass](benchmark, serializer,
            testData.map(_._1), Encoders.product[TestClass])
        case TWSSerializationOpType.DECODE =>
          executeDecoding[TestClass](benchmark, serializer,
            testData.map(_._2), Encoders.product[TestClass])
        case TWSSerializationOpType.ROUND_TRIP =>
          executeRoundTrip[TestClass](benchmark, serializer,
            testData.map(_._1), Encoders.product[TestClass])
      }
      testData.map(_._2)
    }

    // POJO
    runBenchmarkWithDataType(TWSSerializationDataType.POJO,
      op.toString) { (serializer, benchmark) =>
      val testData = constructPOJORandomizedTestData(NUM_OF_ROWS, serializer)
      op match {
        case TWSSerializationOpType.ENCODE =>
          executeEncoding[Person](benchmark, serializer,
            testData.map(_._1), Encoders.bean(classOf[Person]))
        case TWSSerializationOpType.DECODE =>
          executeDecoding[Person](benchmark, serializer,
            testData.map(_._2), Encoders.bean(classOf[Person]))
        case TWSSerializationOpType.ROUND_TRIP =>
          executeRoundTrip[Person](benchmark, serializer,
            testData.map(_._1), Encoders.bean(classOf[Person]))
      }
      testData.map(_._2)
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    Seq(TWSSerializationOpType.ENCODE, TWSSerializationOpType.DECODE,
      TWSSerializationOpType.ROUND_TRIP).foreach { op =>
      runBenchmarkWithOp(op)
    }
  }
}

