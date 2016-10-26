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

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.PercentileDigest
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


class HistogramEndpointsSuite extends SparkFunSuite {

  test("fails analysis if inputs are illegal") {
    def assertEqual(left: TypeCheckResult, right: TypeCheckResult): Unit = {
      assert(left == right)
    }

    val attribute = AttributeReference("a", FloatType)()
    val nonLiterals = Seq(
      new HistogramEndpoints(
        attribute,
        numBinsExpression = AttributeReference("b", IntegerType)()),
      new HistogramEndpoints(
        attribute,
        numBinsExpression = Literal(10),
        accuracyExpression = AttributeReference("c", IntegerType)()))
    nonLiterals.foreach { wrongAgg =>
      assertEqual(
        wrongAgg.checkInputDataTypes(),
        TypeCheckFailure(
          "The maximum number of bins or accuracy provided must be a constant literal"))
    }

    val wrongNumBins = new HistogramEndpoints(attribute, numBinsExpression = Literal(1))
    assertEqual(
      wrongNumBins.checkInputDataTypes(),
      TypeCheckFailure(
        "The maximum number of bins provided must be a positive integer literal >= 2 " +
          s"(current value = 1)"))

    val wrongAccuracy = new HistogramEndpoints(attribute, numBinsExpression = Literal(10),
      accuracyExpression = Literal(0))
    assertEqual(
      wrongAccuracy.checkInputDataTypes(),
      TypeCheckFailure(
        "The accuracy provided must be a positive integer literal (current value = 0)"))

    var wrongType = new HistogramEndpoints(AttributeReference("a", BinaryType)(),
      numBinsExpression = Literal(5))
    wrongType.checkInputDataTypes() match {
      case TypeCheckFailure(message) =>
        assert(message.contains("requires (numeric or timestamp or date or string) type"))
    }
    wrongType = new HistogramEndpoints(attribute, numBinsExpression = Literal(0.5))
    wrongType.checkInputDataTypes() match {
      case TypeCheckFailure(message) => assert(message.contains("requires int type"))
    }
    wrongType = new HistogramEndpoints(attribute, numBinsExpression = Literal(5),
      accuracyExpression = Literal(0.1))
    wrongType.checkInputDataTypes() match {
      case TypeCheckFailure(message) => assert(message.contains("requires int type"))
    }
  }

  test("serialize and de-serialize StringEndpointsDigest") {
    def compareEquals(left: StringEndpointsDigest, right: StringEndpointsDigest): Boolean = {
      left.invalid == right.invalid && left.bins.equals(right.bins)
    }

    // Check empty serialize and de-serialize
    val emptyBuffer = new StringEndpointsDigest()
    assert(compareEquals(emptyBuffer,
      StringEndpointsDigest.deserialize(StringEndpointsDigest.serialize(emptyBuffer))))

    val random = new java.util.Random()
    val data = Seq("a", "bb", "ccc").map { s =>
      (UTF8String.fromString(s), random.nextInt(10000).toLong)
    }
    val buffer = new StringEndpointsDigest()
    data.foreach { case (key, value) =>
      buffer.bins.put(key, value)
    }
    assert(compareEquals(buffer,
      StringEndpointsDigest.deserialize(StringEndpointsDigest.serialize(buffer))))

    val agg = new HistogramEndpoints(BoundReference(0, StringType, nullable = true), Literal(10))
    assert(compareEquals(buffer,
      agg.deserialize(agg.serialize(buffer)).asInstanceOf[StringEndpointsDigest]))
  }

  test("serialize and de-serialize NumericEndpointsDigest") {
    def comparePercentileDigest(left: PercentileDigest, right: PercentileDigest): Boolean = {
      val leftSummary = left.quantileSummaries
      val rightSummary = right.quantileSummaries
      leftSummary.compressThreshold == rightSummary.compressThreshold &&
        leftSummary.relativeError == rightSummary.relativeError &&
        leftSummary.count == rightSummary.count &&
        leftSummary.sampled.sameElements(rightSummary.sampled)
    }

    def compareEquals(left: NumericEndpointsDigest, right: NumericEndpointsDigest): Boolean = {
      left.invalid == right.invalid &&
        left.mapInvalid == right.mapInvalid &&
        comparePercentileDigest(left.percentileDigest, right.percentileDigest) &&
        left.bins.equals(right.bins)
    }

    // Check empty serialize and de-serialize
    val relativeError = 0.001d
    val emptyBuffer = new NumericEndpointsDigest(new PercentileDigest(relativeError))
    assert(compareEquals(emptyBuffer,
      NumericEndpointsDigest.deserialize(NumericEndpointsDigest.serialize(emptyBuffer))))

    val random = new java.util.Random()
    val data = new ArrayBuffer[Int]()
    (1 to 10).foreach { s =>
      val repeat = random.nextInt(100)
      for (i <- 1 to repeat) {
        data += s
      }
    }
    val input = Random.shuffle(data)
    val buffer = new NumericEndpointsDigest(new PercentileDigest(relativeError))
    // both hashmap and PercentileDigest have data
    val numBins = data.distinct.length
    input.foreach { s =>
      buffer.update(IntegerType, s, numBins)
    }
    assert(compareEquals(buffer,
      NumericEndpointsDigest.deserialize(NumericEndpointsDigest.serialize(buffer))))

    val agg = new HistogramEndpoints(BoundReference(0, IntegerType, nullable = true),
      Literal(numBins))
    assert(compareEquals(buffer,
      agg.deserialize(agg.serialize(buffer)).asInstanceOf[NumericEndpointsDigest]))
  }

  test("HistogramEndpoints for string type, high level interface, update, merge, eval...") {
    checkOperationsForString(mergePartialsHighLevel)
  }

  test("HistogramEndpoints for string type, low level interface, update, merge, eval...") {
    checkOperationsForString(mergePartialsLowLevel)
  }

  private def checkOperationsForString(
      mergePartials: (Seq[UTF8String], Expression, Int) => (HistogramEndpoints, Any)): Unit = {
    val data = Seq("a", "bb", "a", "ccc", "dddd", "a").map(UTF8String.fromString)
    val childExpression = BoundReference(0, StringType, nullable = true)
    val numBins = data.distinct.length
    val (agg, mergedBuffer) = mergePartials(data, childExpression, numBins)
    checkMapResult(data, eval(agg, mergedBuffer))
    // Returns empty when the size of hashmap is large than numBins.
    update(agg, mergedBuffer, InternalRow(UTF8String.fromString("e")))
    assert(eval(agg, mergedBuffer).asInstanceOf[MapData].numElements() == 0)
  }

  test("HistogramEndpoints for numeric type, high level interface, update, merge, eval...") {
    checkOperationsForNumeric(mergePartialsHighLevel)
  }

  test("HistogramEndpoints for numeric type, low level interface, update, merge, eval...") {
    checkOperationsForNumeric(mergePartialsLowLevel)
  }

  private def checkOperationsForNumeric(
      mergePartials: (Seq[Decimal], Expression, Int) => (HistogramEndpoints, Any)): Unit = {
    val data = Seq(1, 2, 3, 4, 5).map(_.toDouble)
    val decimalData = data.map(Decimal(_))
    val childExpression = BoundReference(0, new DecimalType(), nullable = true)
    val numBins = data.length
    val (agg, mergedBuffer) = mergePartials(decimalData, childExpression, numBins)
    checkMapResult(data, eval(agg, mergedBuffer))

    // Switches to percentile approximation when the size of hashmap is large than numBins.
    Seq(6, 7, 8, 9, 10).foreach { i =>
      update(agg, mergedBuffer, InternalRow(Decimal(i)))
    }
    val count = 10
    val expectedPercentiles = new Array[Double](numBins + 1)
    expectedPercentiles(0) = data.min
    for (i <- 1 to numBins) {
      expectedPercentiles(i) = count * i / numBins.toDouble
    }
    val error = count / agg.accuracyExpression.eval().asInstanceOf[Int].toDouble
    checkPercentileResult(eval(agg, mergedBuffer), expectedPercentiles, error)

    // Returns empty when a decimal value has too great a magnitude to represent as a Double.
    update(agg, mergedBuffer, InternalRow(Decimal(Double.MaxValue) + Decimal(Double.MaxValue)))
    assert(eval(agg, mergedBuffer).asInstanceOf[MapData].numElements() == 0)
  }

  private def mergePartialsHighLevel[T](
      data: Seq[T],
      child: Expression,
      numBins: Int): (HistogramEndpoints, EndpointsDigest) = {
    val agg = new HistogramEndpoints(child, Literal(numBins))
    assert(!agg.nullable)
    val group1 = 0 until data.length / 2
    val group1Buffer = agg.createAggregationBuffer()
    group1.foreach { index =>
      val input = InternalRow(data(index))
      agg.update(group1Buffer, input)
    }

    val group2 = data.length / 2 until data.length
    val group2Buffer = agg.createAggregationBuffer()
    group2.foreach { index =>
      val input = InternalRow(data(index))
      agg.update(group2Buffer, input)
    }

    val mergeBuffer = agg.createAggregationBuffer()
    agg.merge(mergeBuffer, group1Buffer)
    agg.merge(mergeBuffer, group2Buffer)
    (agg, mergeBuffer)
  }

  private def checkMapResult[T](data: Seq[T], result: Any): Unit = {
    result match {
      case mapData: MapData =>
        val expectedMap = data.groupBy(w => w).mapValues(_.length.toLong)
        val keys = mapData.keyArray().array.map(_.asInstanceOf[T])
        val values = mapData.valueArray().toLongArray()
        val resultMap = Map(keys.zip(values): _*)
        assert(expectedMap.equals(resultMap))
    }
  }

  private def checkPercentileResult(
      result: Any,
      expectedPercentiles: Array[Double],
      error: Double): Unit = {
    result match {
      case mapData: MapData =>
        val percentiles = mapData.keyArray().toArray[Double](DoubleType)
        assert(percentiles.zip(expectedPercentiles)
          .forall(pair => Math.abs(pair._1 - pair._2) < error))
    }
  }

  private def eval(agg: HistogramEndpoints, buffer: Any): Any = buffer match {
    case digest: EndpointsDigest => agg.eval(digest)
    case row: InternalRow => agg.eval(row)
  }

  private def update(agg: HistogramEndpoints, buffer: Any, input: InternalRow): Any = buffer match {
    case digest: EndpointsDigest => agg.update(digest, input)
    case row: InternalRow => agg.update(row, input)
  }

  private def mergePartialsLowLevel[T](
      data: Seq[T],
      child: Expression,
      numBins: Int): (HistogramEndpoints, InternalRow) = {
    val inputAggregationBufferOffset = 1
    val mutableAggregationBufferOffset = 2
    val agg = new HistogramEndpoints(child, Literal(numBins))
      .withNewInputAggBufferOffset(inputAggregationBufferOffset)
      .withNewMutableAggBufferOffset(mutableAggregationBufferOffset)

    // Phase one, partial mode aggregation
    val mutableAggBuffer = new GenericInternalRow(
      new Array[Any](mutableAggregationBufferOffset + 1))
    agg.initialize(mutableAggBuffer)
    data.foreach { s =>
      agg.update(mutableAggBuffer, InternalRow(s))
    }
    agg.serializeAggregateBufferInPlace(mutableAggBuffer)

    // Serialize the aggregation buffer
    val serialized = mutableAggBuffer.getBinary(mutableAggregationBufferOffset)
    val inputAggBuffer = new GenericInternalRow(Array[Any](null, serialized))

    // Phase 2: final mode aggregation
    // Re-initialize the aggregation buffer
    agg.initialize(mutableAggBuffer)
    agg.merge(mutableAggBuffer, inputAggBuffer)
    (agg, mutableAggBuffer)
  }
}
