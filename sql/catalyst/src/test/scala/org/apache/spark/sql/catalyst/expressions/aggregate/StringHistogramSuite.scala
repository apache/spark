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

import scala.collection.immutable.TreeMap

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.StringHistogram.{StringHistogramInfo, StringHistogramInfoSerializer}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


class StringHistogramSuite extends SparkFunSuite {

  test("serialize and de-serialize") {
    def compareEquals(left: StringHistogramInfo, right: StringHistogramInfo): Boolean = {
      left.invalid == right.invalid && left.bins.equals(right.bins)
    }

    val serializer = new StringHistogramInfoSerializer
    // Check empty serialize and de-serialize
    val emptyBuffer = new StringHistogramInfo()
    assert(compareEquals(emptyBuffer, serializer.deserialize(serializer.serialize(emptyBuffer))))

    val random = new java.util.Random()
    val data = Seq("a", "bb", "ccc").map { s =>
      (UTF8String.fromString(s), random.nextInt(10000).toLong)
    }
    val buffer = new StringHistogramInfo()
    data.foreach { case (key, value) =>
      buffer.bins.put(key, value)
    }
    assert(compareEquals(buffer, serializer.deserialize(serializer.serialize(buffer))))

    val agg = new StringHistogram(BoundReference(0, StringType, nullable = true), Literal(10))
    assert(compareEquals(agg.deserialize(agg.serialize(buffer)), buffer))
  }

  private def checkResult(data: Seq[UTF8String], result: Any): Unit = {
    val expectedMap = data.groupBy(w => w).mapValues(_.length.toLong)
    val expectedSortedMap = TreeMap[UTF8String, Long](expectedMap.toSeq: _*)
    result match {
      case mapData: MapData =>
        val keys = mapData.keyArray().toArray[UTF8String](StringType)
        val values = mapData.valueArray().toArray[Long](LongType)
        assert(expectedSortedMap.keys.toArray.sameElements(keys))
        assert(expectedSortedMap.values.toArray.sameElements(values))
    }
  }

  test("class StringHistogram, high level interface, update, merge, eval...") {
    val data = Seq("a", "bb", "a", "ccc", "dddd", "a").map(UTF8String.fromString)
    val childExpression = BoundReference(0, StringType, nullable = true)
    val numBinsExpression = Literal(10)
    val agg = new StringHistogram(childExpression, numBinsExpression)

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
    checkResult(data, agg.eval(mergeBuffer))
  }

  test("class StringHistogram, low level interface, update, merge, eval...") {
    val childExpression = BoundReference(0, StringType, nullable = true)
    val inputAggregationBufferOffset = 1
    val mutableAggregationBufferOffset = 2
    val numBins = 10

    // Phase one, partial mode aggregation
    val agg = new StringHistogram(childExpression, Literal(numBins))
      .withNewInputAggBufferOffset(inputAggregationBufferOffset)
      .withNewMutableAggBufferOffset(mutableAggregationBufferOffset)

    val mutableAggBuffer = new GenericInternalRow(
      new Array[Any](mutableAggregationBufferOffset + 1))
    agg.initialize(mutableAggBuffer)
    val data = Seq("a", "bb", "a", "ccc", "dddd", "a").map(UTF8String.fromString)
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
    checkResult(data, agg.eval(mutableAggBuffer))
  }

  test("fails analysis if inputs are illegal") {
    def assertEqual(left: TypeCheckResult, right: TypeCheckResult): Unit = {
      assert(left == right)
    }

    val attribute = AttributeReference("a", StringType)()
    var wrongNumBins = new StringHistogram(
      attribute, numBinsExpression = AttributeReference("b", IntegerType)())
    assertEqual(
      wrongNumBins.checkInputDataTypes(),
      TypeCheckFailure("The maximum number of bins provided must be a constant literal"))

    wrongNumBins = new StringHistogram(attribute, numBinsExpression = Literal(-1))
    assertEqual(
      wrongNumBins.checkInputDataTypes(),
      TypeCheckFailure(
        "The maximum number of bins provided must be a positive integer literal (current value = " +
          "-1)"))

    wrongNumBins = new StringHistogram(attribute, numBinsExpression = Literal(0.5))
    var typeCheckResult = wrongNumBins.checkInputDataTypes()
    assert(typeCheckResult.isFailure && typeCheckResult.asInstanceOf[TypeCheckFailure]
      .message.contains("requires int type"))

    val wrongAttribute = new StringHistogram(AttributeReference("a", IntegerType)(), Literal(1000))
    typeCheckResult = wrongAttribute.checkInputDataTypes()
    assert(typeCheckResult.isFailure && typeCheckResult.asInstanceOf[TypeCheckFailure]
      .message.contains("requires string type"))
  }
}
