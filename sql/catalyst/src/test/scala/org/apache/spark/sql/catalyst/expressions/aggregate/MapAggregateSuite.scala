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

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Cast, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


class MapAggregateSuite extends SparkFunSuite {

  test("fails analysis if inputs are illegal") {
    def assertEqual(left: TypeCheckResult, right: TypeCheckResult): Unit = {
      assert(left == right)
    }

    val attribute = AttributeReference("a", FloatType)()
    val nonLiterals = new MapAggregate(attribute,
      numBinsExpression = AttributeReference("b", IntegerType)())
    assertEqual(
      nonLiterals.checkInputDataTypes(),
      TypeCheckFailure(
        "The maximum number of bins provided must be a constant literal"))

    Seq(Literal(1), Cast(Literal(null), IntegerType)).foreach { expr =>
      val wrongNumBins = new MapAggregate(attribute, numBinsExpression = expr)
      assertEqual(
        wrongNumBins.checkInputDataTypes(),
        TypeCheckFailure(
          "The maximum number of bins provided must be a positive integer literal >= 2 " +
            s"(current value = ${expr.eval()})"))
    }

    val unsupportedTypes: Seq[DataType] = Seq(BinaryType, BooleanType, ArrayType(IntegerType),
      MapType(IntegerType, IntegerType), StructType(Seq(StructField("s", IntegerType))))
    unsupportedTypes.foreach { dataType =>
      val wrongColType = new MapAggregate(AttributeReference("a", dataType)(),
        numBinsExpression = Literal(5))
      wrongColType.checkInputDataTypes() match {
        case TypeCheckFailure(message) =>
          assert(message.contains("requires (numeric or timestamp or date or string) type"))
      }
    }

    val wrongNumberType = new MapAggregate(attribute, numBinsExpression = Literal(0.5))
    wrongNumberType.checkInputDataTypes() match {
      case TypeCheckFailure(message) => assert(message.contains("requires int type"))
    }
  }

  test("serialize and de-serialize StringMapDigest") {
    checkSerialization(Seq("a", "bb", "ccc").map(UTF8String.fromString), StringType)
  }

  test("serialize and de-serialize NumericMapDigest") {
    checkSerialization((1 to 5).map(_.toDouble), DoubleType)
  }

  private def checkSerialization[T](data: Seq[T], dataType: DataType): Unit = {
    def compareEquals(left: MapDigest, right: MapDigest): Boolean = {
      (left, right) match {
        case (strDigest1: StringMapDigest, strDigest2: StringMapDigest) =>
          strDigest1.isInvalid == strDigest2.isInvalid && strDigest1.bins.equals(strDigest2.bins)
        case (numDigest1: NumericMapDigest, numDigest2: NumericMapDigest) =>
          numDigest1.isInvalid == numDigest2.isInvalid && numDigest1.bins.equals(numDigest2.bins)
      }
    }

    val numBins = data.distinct.length
    val agg = new MapAggregate(BoundReference(0, dataType, nullable = true), Literal(numBins))

    // Check empty serialize and de-serialize
    val emptyBuffer = agg.createAggregationBuffer()
    assert(compareEquals(emptyBuffer, agg.deserialize(agg.serialize(emptyBuffer))))

    val random = new java.util.Random()
    val mapData = mutable.HashMap(data.map(s => (s, random.nextInt(10000).toLong)): _*)
    val buffer = agg.createAggregationBuffer().asInstanceOf[MapDigestBase[T]]
    buffer.mergeMap(mapData, numBins)
    assert(compareEquals(buffer, agg.deserialize(agg.serialize(buffer))))
  }

  test("HistogramEndpoints for string type, high level interface, update, merge, eval...") {
    val data = Seq("a", "b", "c", "d").map(UTF8String.fromString)
    val extraData = UTF8String.fromString("e")
    checkOperations(data, extraData, StringType, mergePartialsHighLevel)
  }

  test("HistogramEndpoints for string type, low level interface, update, merge, eval...") {
    val data = Seq("a", "bb", "a", "ccc", "dddd", "a").map(UTF8String.fromString)
    val extraData = UTF8String.fromString("e")
    checkOperations(data, extraData, StringType, mergePartialsLowLevel)
  }

  private def checkOperations[T](
      data: Seq[T],
      extraData: T,
      dataType: DataType,
      mergePartials: (Seq[T], Expression, Int) => (MapAggregate, Any)): Unit = {
    val childExpression = BoundReference(0, dataType, nullable = true)
    val numBins = data.distinct.length
    val (agg, mergedBuffer) = mergePartials(data, childExpression, numBins)
    checkMapResult(data, eval(agg, mergedBuffer))
    // Returns empty when the size of hashmap is large than numBins.
    update(agg, mergedBuffer, InternalRow(extraData))
    assert(eval(agg, mergedBuffer).asInstanceOf[MapData].numElements() == 0)
  }

  test("HistogramEndpoints for numeric type, high level interface, update, merge, eval...") {
    val data = Seq(1, 2, 3, 4, 5).map(_.toDouble)
    val extraData = 6.0D
    checkOperations(data, extraData, DoubleType, mergePartialsHighLevel)
  }

  test("HistogramEndpoints for numeric type, low level interface, update, merge, eval...") {
    val data = Seq(4, 3, 1, 2, 1, 4).map(_.toDouble)
    val extraData = 6.0D
    checkOperations(data, extraData, DoubleType, mergePartialsLowLevel)
  }

  private def mergePartialsHighLevel[T](
      data: Seq[T],
      child: Expression,
      numBins: Int): (MapAggregate, MapDigest) = {
    val agg = new MapAggregate(child, Literal(numBins))
    assert(agg.nullable)
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

  private def eval(agg: MapAggregate, buffer: Any): Any = buffer match {
    case digest: MapDigest => agg.eval(digest)
    case row: InternalRow => agg.eval(row)
  }

  private def update(agg: MapAggregate, buffer: Any, input: InternalRow): Any = buffer match {
    case digest: MapDigest => agg.update(digest, input)
    case row: InternalRow => agg.update(row, input)
  }

  private def mergePartialsLowLevel[T](
      data: Seq[T],
      child: Expression,
      numBins: Int): (MapAggregate, InternalRow) = {
    val inputAggregationBufferOffset = 1
    val mutableAggregationBufferOffset = 2
    val agg = new MapAggregate(child, Literal(numBins))
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
