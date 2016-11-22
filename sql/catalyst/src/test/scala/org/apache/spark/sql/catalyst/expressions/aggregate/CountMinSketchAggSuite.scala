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

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Cast, GenericInternalRow, Literal}
import org.apache.spark.sql.types.{DecimalType, _}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.sketch.CountMinSketch

class CountMinSketchAggSuite extends SparkFunSuite {
  private val childExpression = BoundReference(0, IntegerType, nullable = true)
  private val epsOfTotalCount = 0.0001
  private val confidence = 0.99
  private val seed = 42

  test("serialize and de-serialize") {
    // Check empty serialize and de-serialize
    val agg = new CountMinSketchAgg(childExpression, Literal(epsOfTotalCount), Literal(confidence),
      Literal(seed))
    val buffer = CountMinSketch.create(epsOfTotalCount, confidence, seed)
    assert(buffer.equals(agg.deserialize(agg.serialize(buffer))))

    // Check non-empty serialize and de-serialize
    val random = new Random(31)
    (0 until 10000).map(_ => random.nextInt(100)).foreach { value =>
      buffer.add(value)
    }
    assert(buffer.equals(agg.deserialize(agg.serialize(buffer))))
  }

  def testHighLevelInterface[T: ClassTag](
      dataType: DataType,
      sampledItemIndices: Array[Int],
      allItems: Array[T],
      exactFreq: Map[Any, Long]): Any = {
    test(s"high level interface, update, merge, eval... - $dataType") {
      val agg = new CountMinSketchAgg(BoundReference(0, dataType, nullable = true),
        Literal(epsOfTotalCount), Literal(confidence), Literal(seed))
      assert(!agg.nullable)

      val group1 = 0 until sampledItemIndices.length / 2
      val group1Buffer = agg.createAggregationBuffer()
      group1.foreach { index =>
        val input = InternalRow(allItems(sampledItemIndices(index)))
        agg.update(group1Buffer, input)
      }

      val group2 = sampledItemIndices.length / 2 until sampledItemIndices.length
      val group2Buffer = agg.createAggregationBuffer()
      group2.foreach { index =>
        val input = InternalRow(allItems(sampledItemIndices(index)))
        agg.update(group2Buffer, input)
      }

      var mergeBuffer = agg.createAggregationBuffer()
      agg.merge(mergeBuffer, group1Buffer)
      agg.merge(mergeBuffer, group2Buffer)
      checkResult(agg.eval(mergeBuffer), allItems, exactFreq)

      // Merge in a different order
      mergeBuffer = agg.createAggregationBuffer()
      agg.merge(mergeBuffer, group2Buffer)
      agg.merge(mergeBuffer, group1Buffer)
      checkResult(agg.eval(mergeBuffer), allItems, exactFreq)

      // Merge with an empty partition
      val emptyBuffer = agg.createAggregationBuffer()
      agg.merge(mergeBuffer, emptyBuffer)
      checkResult(agg.eval(mergeBuffer), allItems, exactFreq)
    }
  }

  def testLowLevelInterface[T: ClassTag](
      dataType: DataType,
      sampledItemIndices: Array[Int],
      allItems: Array[T],
      exactFreq: Map[Any, Long]): Any = {
    test(s"low level interface, update, merge, eval... - ${dataType.typeName}") {
      val inputAggregationBufferOffset = 1
      val mutableAggregationBufferOffset = 2

      // Phase one, partial mode aggregation
      val agg = new CountMinSketchAgg(BoundReference(0, dataType, nullable = true),
        Literal(epsOfTotalCount), Literal(confidence), Literal(seed))
        .withNewInputAggBufferOffset(inputAggregationBufferOffset)
        .withNewMutableAggBufferOffset(mutableAggregationBufferOffset)

      val mutableAggBuffer = new GenericInternalRow(
        new Array[Any](mutableAggregationBufferOffset + 1))
      agg.initialize(mutableAggBuffer)

      sampledItemIndices.foreach { i =>
        agg.update(mutableAggBuffer, InternalRow(allItems(i)))
      }
      agg.serializeAggregateBufferInPlace(mutableAggBuffer)

      // Serialize the aggregation buffer
      val serialized = mutableAggBuffer.getBinary(mutableAggregationBufferOffset)
      val inputAggBuffer = new GenericInternalRow(Array[Any](null, serialized))

      // Phase 2: final mode aggregation
      // Re-initialize the aggregation buffer
      agg.initialize(mutableAggBuffer)
      agg.merge(mutableAggBuffer, inputAggBuffer)
      checkResult(agg.eval(mutableAggBuffer), allItems, exactFreq)
    }
  }

  private def checkResult[T: ClassTag](
      result: Any,
      data: Array[T],
      exactFreq: Map[Any, Long]): Unit = {
    result match {
      case bytesData: Array[Byte] =>
        val in = new ByteArrayInputStream(bytesData)
        val cms = CountMinSketch.readFrom(in)
        val probCorrect = {
          val numErrors = data.map { i =>
            val count = exactFreq.getOrElse(getProbeItem(i), 0L)
            val item = i match {
              case dec: Decimal => dec.toJavaBigDecimal
              case str: UTF8String => str.getBytes
              case _ => i
            }
            val ratio = (cms.estimateCount(item) - count).toDouble / data.length
            if (ratio > epsOfTotalCount) 1 else 0
          }.sum

          1D - numErrors.toDouble / data.length
        }

        assert(
          probCorrect > confidence,
          s"Confidence not reached: required $confidence, reached $probCorrect"
        )
      case _ => fail("unexpected return type")
    }
  }

  private def getProbeItem[T: ClassTag](item: T): Any = item match {
    // Use a string to represent the content of an array of bytes
    case bytes: Array[Byte] => new String(bytes, StandardCharsets.UTF_8)
    case i => identity(i)
  }

  def testItemType[T: ClassTag](dataType: DataType)(itemGenerator: Random => T): Unit = {
    // Uses fixed seed to ensure reproducible test execution
    val r = new Random(31)

    val numAllItems = 1000000
    val allItems = Array.fill(numAllItems)(itemGenerator(r))

    val numSamples = numAllItems / 10
    val sampledItemIndices = Array.fill(numSamples)(r.nextInt(numAllItems))

    val exactFreq = {
      val sampledItems = sampledItemIndices.map(allItems)
      sampledItems.groupBy(getProbeItem).mapValues(_.length.toLong)
    }

    testLowLevelInterface[T](dataType, sampledItemIndices, allItems, exactFreq)
    testHighLevelInterface[T](dataType, sampledItemIndices, allItems, exactFreq)
  }

  testItemType[Byte](ByteType) { _.nextInt().toByte }

  testItemType[Short](ShortType) { _.nextInt().toShort }

  testItemType[Int](IntegerType) { _.nextInt() }

  testItemType[Long](LongType) { _.nextLong() }

  testItemType[UTF8String](StringType) { r => UTF8String.fromString(r.nextString(r.nextInt(20))) }

  testItemType[Float](FloatType) { _.nextFloat() }

  testItemType[Double](DoubleType) { _.nextDouble() }

  testItemType[Decimal](new DecimalType()) { r => Decimal(r.nextDouble()) }

  testItemType[Boolean](BooleanType) { _.nextBoolean() }

  testItemType[Array[Byte]](BinaryType) { r =>
    r.nextString(r.nextInt(20)).getBytes(StandardCharsets.UTF_8)
  }


  test("fails analysis if eps, confidence or seed provided is not a literal or constant foldable") {
    val wrongEps = new CountMinSketchAgg(
      childExpression,
      epsExpression = AttributeReference("a", DoubleType)(),
      confidenceExpression = Literal(confidence),
      seedExpression = Literal(seed))
    val wrongConfidence = new CountMinSketchAgg(
      childExpression,
      epsExpression = Literal(epsOfTotalCount),
      confidenceExpression = AttributeReference("b", DoubleType)(),
      seedExpression = Literal(seed))
    val wrongSeed = new CountMinSketchAgg(
      childExpression,
      epsExpression = Literal(epsOfTotalCount),
      confidenceExpression = Literal(confidence),
      seedExpression = AttributeReference("c", IntegerType)())

    Seq(wrongEps, wrongConfidence, wrongSeed).foreach { wrongAgg =>
      assertEqual(
        wrongAgg.checkInputDataTypes(),
        TypeCheckFailure(
          "The eps, confidence or seed provided must be a literal or constant foldable")
      )
    }
  }

  test("fails analysis if parameters are invalid") {
    // parameters are null
    val wrongEps = new CountMinSketchAgg(
      childExpression,
      epsExpression = Cast(Literal(null), DoubleType),
      confidenceExpression = Literal(confidence),
      seedExpression = Literal(seed))
    val wrongConfidence = new CountMinSketchAgg(
      childExpression,
      epsExpression = Literal(epsOfTotalCount),
      confidenceExpression = Cast(Literal(null), DoubleType),
      seedExpression = Literal(seed))
    val wrongSeed = new CountMinSketchAgg(
      childExpression,
      epsExpression = Literal(epsOfTotalCount),
      confidenceExpression = Literal(confidence),
      seedExpression = Cast(Literal(null), IntegerType))

    Seq(wrongEps, wrongConfidence, wrongSeed).foreach { wrongAgg =>
      assertEqual(
        wrongAgg.checkInputDataTypes(),
        TypeCheckFailure("The eps, confidence or seed provided should not be null")
      )
    }

    // parameters are out of the valid range
    Seq(0.0, -1000.0).foreach { invalidEps =>
      val invalidAgg = new CountMinSketchAgg(
        childExpression,
        epsExpression = Literal(invalidEps),
        confidenceExpression = Literal(confidence),
        seedExpression = Literal(seed))
      assertEqual(
        invalidAgg.checkInputDataTypes(),
        TypeCheckFailure(s"Relative error must be positive (current value = $invalidEps)")
      )
    }

    Seq(0.0, 1.0, -2.0, 2.0).foreach { invalidConfidence =>
      val invalidAgg = new CountMinSketchAgg(
        childExpression,
        epsExpression = Literal(epsOfTotalCount),
        confidenceExpression = Literal(invalidConfidence),
        seedExpression = Literal(seed))
      assertEqual(
        invalidAgg.checkInputDataTypes(),
        TypeCheckFailure(
          s"Confidence must be within range (0.0, 1.0) (current value = $invalidConfidence)")
      )
    }
  }

  private def assertEqual[T](left: T, right: T): Unit = {
    assert(left == right)
  }

  test("null handling") {
    def isEqual(result: Any, other: CountMinSketch): Boolean = {
      result match {
        case bytesData: Array[Byte] =>
          val in = new ByteArrayInputStream(bytesData)
          val cms = CountMinSketch.readFrom(in)
          cms.equals(other)
        case _ => fail("unexpected return type")
      }
    }

    val agg = new CountMinSketchAgg(childExpression, Literal(epsOfTotalCount), Literal(confidence),
      Literal(seed))
    val emptyCms = CountMinSketch.create(epsOfTotalCount, confidence, seed)
    val buffer = new GenericInternalRow(new Array[Any](1))
    agg.initialize(buffer)
    // Empty aggregation buffer
    assert(isEqual(agg.eval(buffer), emptyCms))
    // Empty input row
    agg.update(buffer, InternalRow(null))
    assert(isEqual(agg.eval(buffer), emptyCms))

    // Add some non-empty row
    agg.update(buffer, InternalRow(0))
    assert(!isEqual(agg.eval(buffer), emptyCms))
  }
}
