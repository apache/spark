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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap

class PercentileSuite extends SparkFunSuite {

  private val random = new java.util.Random()

  private val data = (0 until 10000).map { _ =>
    random.nextInt(10000)
  }

  test("serialize and de-serialize") {
    val agg = new Percentile(BoundReference(0, IntegerType, true), Literal(0.5))

    // Check empty serialize and deserialize
    val buffer = new OpenHashMap[Number, Long]()
    assert(compareEquals(agg.deserialize(agg.serialize(buffer)), buffer))

    // Check non-empty buffer serializa and deserialize.
    data.foreach { key =>
      buffer.changeValue(key, 1L, _ + 1L)
    }
    assert(compareEquals(agg.deserialize(agg.serialize(buffer)), buffer))
  }

  test("class Percentile, high level interface, update, merge, eval...") {
    val count = 10000
    val data = (1 to count)
    val percentages = Seq(0, 0.25, 0.5, 0.75, 1)
    val expectedPercentiles = Seq(1, 2500.75, 5000.5, 7500.25, 10000)
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = false), DoubleType)
    val percentageExpression = CreateArray(percentages.toSeq.map(Literal(_)))
    val agg = new Percentile(childExpression, percentageExpression)

    assert(agg.nullable)
    val group1 = (0 until data.length / 2)
    val group1Buffer = agg.createAggregationBuffer()
    group1.foreach { index =>
      val input = InternalRow(data(index))
      agg.update(group1Buffer, input)
    }

    val group2 = (data.length / 2 until data.length)
    val group2Buffer = agg.createAggregationBuffer()
    group2.foreach { index =>
      val input = InternalRow(data(index))
      agg.update(group2Buffer, input)
    }

    val mergeBuffer = agg.createAggregationBuffer()
    agg.merge(mergeBuffer, group1Buffer)
    agg.merge(mergeBuffer, group2Buffer)

    agg.eval(mergeBuffer) match {
      case arrayData: ArrayData =>
        val percentiles = arrayData.toDoubleArray()
        assert(percentiles.zip(expectedPercentiles)
          .forall(pair => pair._1 == pair._2))
    }
  }
  
  test("class Percentile with frequency, high level interface, update, merge, eval...") {
    def runTest(agg: Percentile , 
        data : Seq[Seq[Any]], 
        expectedPercentiles : Seq[Double]) = {
      assert(agg.nullable)
      val group1 = (0 until data.length / 2)
      val group1Buffer = agg.createAggregationBuffer()
      group1.foreach { index =>
        val input = InternalRow(data(index): _*)
        agg.update(group1Buffer, input)
      }

      val group2 = (data.length / 2 until data.length)
      val group2Buffer = agg.createAggregationBuffer()
      group2.foreach { index =>
        val input = InternalRow(data(index): _*)
        agg.update(group2Buffer, input)
      }

      val mergeBuffer = agg.createAggregationBuffer()
      agg.merge(mergeBuffer, group1Buffer)
      agg.merge(mergeBuffer, group2Buffer)

      agg.eval(mergeBuffer) match {
        case arrayData: ArrayData =>
          val percentiles = arrayData.toDoubleArray()
          assert(percentiles.zip(expectedPercentiles)
            .forall(pair => pair._1 == pair._2))
      }
    }

    val count = 10
    val data = (1 to count).map( x=> Seq(x, x):+ x.toLong)
    val flattenData = (1 to count).flatMap( x=> (1 to x).map( y => x )).map( Seq(_))
    val percentages = Seq(0, 0.25, 0.5, 0.75, 1)
    val expectedPercentiles = Seq(1.0, 5.0, 7.0, 9.0, 10.0)
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = false), DoubleType)
    val frequencyExpressionInt = BoundReference(1, IntegerType, nullable = false)
    val frequencyExpressionLong = BoundReference(2, LongType, nullable = false)
    val percentageExpression = CreateArray(percentages.toSeq.map(Literal(_)))

    val aggInt = new Percentile(childExpression,frequencyExpressionInt, percentageExpression)
    runTest( aggInt, data,expectedPercentiles)
    
    val aggLong = new Percentile(childExpression,frequencyExpressionLong, percentageExpression)
    runTest( aggLong, data, expectedPercentiles)
    
    val agg = new Percentile(childExpression, percentageExpression)
    runTest(agg, flattenData, expectedPercentiles)
    
  }


  test("class Percentile, low level interface, update, merge, eval...") {
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = true), DoubleType)
    val inputAggregationBufferOffset = 1
    val mutableAggregationBufferOffset = 2
    val percentage = 0.5

    // Phase one, partial mode aggregation
    val agg = new Percentile(childExpression, Literal(percentage))
      .withNewInputAggBufferOffset(inputAggregationBufferOffset)
      .withNewMutableAggBufferOffset(mutableAggregationBufferOffset)

    val mutableAggBuffer = new GenericInternalRow(
      new Array[Any](mutableAggregationBufferOffset + 1))
    agg.initialize(mutableAggBuffer)
    val dataCount = 10
    (1 to dataCount).foreach { data =>
      agg.update(mutableAggBuffer, InternalRow(data))
    }
    agg.serializeAggregateBufferInPlace(mutableAggBuffer)

    // Serialize the aggregation buffer
    val serialized = mutableAggBuffer.getBinary(mutableAggregationBufferOffset)
    val inputAggBuffer = new GenericInternalRow(Array[Any](null, serialized))

    // Phase 2: final mode aggregation
    // Re-initialize the aggregation buffer
    agg.initialize(mutableAggBuffer)
    agg.merge(mutableAggBuffer, inputAggBuffer)
    val expectedPercentile = 5.5
    assert(agg.eval(mutableAggBuffer).asInstanceOf[Double] == expectedPercentile)
  }

  test("call from sql query") {
    // sql, single percentile
    assertEqual(
      s"percentile(`a`, 0.5D)",
      new Percentile("a".attr, Literal(0.5)).sql: String)
    
    // sql, single percentile with frequency
    assertEqual(
      s"percentile(`a`, `frq`, 0.5D)",
      new Percentile("a".attr, "frq".attr, Literal(0.5)).sql: String)

    // sql, array of percentile
    assertEqual(
      s"percentile(`a`, array(0.25D, 0.5D, 0.75D))",
      new Percentile("a".attr, CreateArray(Seq(0.25, 0.5, 0.75).map(Literal(_)))).sql: String)

    // sql, array of percentile with frequency
    assertEqual(
      s"percentile(`a`, `frq`, array(0.25D, 0.5D, 0.75D))",
      new Percentile("a".attr, "frq".attr, CreateArray(Seq(0.25, 0.5, 0.75).map(Literal(_)))).sql: String)
      
    // sql(isDistinct = false), single percentile
    assertEqual(
      s"percentile(`a`, 0.5D)",
      new Percentile("a".attr, Literal(0.5)).sql(isDistinct = false))

    // sql(isDistinct = false), array of percentile
    assertEqual(
      s"percentile(`a`, array(0.25D, 0.5D, 0.75D))",
      new Percentile("a".attr, CreateArray(Seq(0.25, 0.5, 0.75).map(Literal(_))))
        .sql(isDistinct = false))

    // sql(isDistinct = false) single percentile with frequency
    assertEqual(
      s"percentile(`a`, `frq`, 0.5D)",
      new Percentile("a".attr, "frq".attr, Literal(0.5))
      .sql(isDistinct = false))

    // sql(isDistinct = true), single percentile
    assertEqual(
      s"percentile(DISTINCT `a`, 0.5D)",
      new Percentile("a".attr, Literal(0.5)).sql(isDistinct = true))

    // sql(isDistinct = true), array of percentile
    assertEqual(
      s"percentile(DISTINCT `a`, array(0.25D, 0.5D, 0.75D))",
      new Percentile("a".attr, CreateArray(Seq(0.25, 0.5, 0.75).map(Literal(_))))
        .sql(isDistinct = true))
  }

  test("fail analysis if childExpression is invalid") {
    val validDataTypes = Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType)
    val percentage = Literal(0.5)

    validDataTypes.foreach { dataType =>
      val child = AttributeReference("a", dataType)()
      val percentile = new Percentile(child, percentage)
      assertEqual(percentile.checkInputDataTypes(), TypeCheckSuccess)
    }

    val validFrequencyTypes = Seq(ByteType, ShortType, IntegerType, LongType)
    for( dataType <- validDataTypes;
      frequencyType <-validFrequencyTypes  )  { 
      val child = AttributeReference("a", dataType)()
      val frq = AttributeReference("frq", frequencyType)()
      val percentile = new Percentile(child, frq, percentage)
      assertEqual(percentile.checkInputDataTypes(), TypeCheckSuccess)
    }

    val invalidDataTypes = Seq(BooleanType, StringType, DateType, TimestampType,
      CalendarIntervalType, NullType)

    invalidDataTypes.foreach { dataType =>
      val child = AttributeReference("a", dataType)()
      val percentile = new Percentile(child, percentage)
      assertEqual(percentile.checkInputDataTypes(),
        TypeCheckFailure(s"argument 1 requires numeric type, however, " +
            s"'`a`' is of ${dataType.simpleString} type."))
    }

    val invalidFrequencyDataTypes = Seq(FloatType, DoubleType , BooleanType, 
        StringType, DateType, TimestampType,
      CalendarIntervalType, NullType)

    for( dataType <- invalidDataTypes;
        frequencyType <-validFrequencyTypes) { 
      val child = AttributeReference("a", dataType)()
      val frq = AttributeReference("frq", frequencyType)()
      val percentile = new Percentile(child, frq, percentage)
      assertEqual(percentile.checkInputDataTypes(),
        TypeCheckFailure(s"argument 1 requires numeric type, however, " +
            s"'`a`' is of ${dataType.simpleString} type."))
    }
 
    for( dataType <- validDataTypes;
        frequencyType <-invalidFrequencyDataTypes) { 
      val child = AttributeReference("a", dataType)()
      val frq = AttributeReference("frq", frequencyType)()
      val percentile = new Percentile(child,frq, percentage)
      assertEqual(percentile.checkInputDataTypes(),
        TypeCheckFailure(s"argument 2 requires integral type, however, " +
            s"'`frq`' is of ${frequencyType.simpleString} type."))
    }
  }

  test("fails analysis if percentage(s) are invalid") {
    val child = Cast(BoundReference(0, IntegerType, nullable = false), DoubleType)
    val input = InternalRow(1)

    val validPercentages = Seq(Literal(0D), Literal(0.5), Literal(1D),
      CreateArray(Seq(0, 0.5, 1).map(Literal(_))))

    validPercentages.foreach { percentage =>
      val percentile1 = new Percentile(child, percentage)
      assertEqual(percentile1.checkInputDataTypes(), TypeCheckSuccess)
    }

    val invalidPercentages = Seq(Literal(-0.5), Literal(1.5), Literal(2D),
      CreateArray(Seq(-0.5, 0, 2).map(Literal(_))))

    invalidPercentages.foreach { percentage =>
      val percentile2 = new Percentile(child, percentage)
      assertEqual(percentile2.checkInputDataTypes(),
        TypeCheckFailure(s"Percentage(s) must be between 0.0 and 1.0, " +
        s"but got ${percentage.simpleString}"))
    }

    val nonFoldablePercentage = Seq(NonFoldableLiteral(0.5),
      CreateArray(Seq(0, 0.5, 1).map(NonFoldableLiteral(_))))

    nonFoldablePercentage.foreach { percentage =>
      val percentile3 = new Percentile(child, percentage)
      assertEqual(percentile3.checkInputDataTypes(),
        TypeCheckFailure(s"The percentage(s) must be a constant literal, " +
          s"but got ${percentage}"))
    }

    val invalidDataTypes = Seq(ByteType, ShortType, IntegerType, LongType, FloatType,
      BooleanType, StringType, DateType, TimestampType, CalendarIntervalType, NullType)

    invalidDataTypes.foreach { dataType =>
      val percentage = Literal(0.5, dataType)
      val percentile4 = new Percentile(child, percentage)
      assertEqual(percentile4.checkInputDataTypes(),
        TypeCheckFailure(s"argument 2 requires double type, however, " +
          s"'0.5' is of ${dataType.simpleString} type."))
    }
  }

  test("null handling") {
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = true), DoubleType)
    val agg = new Percentile(childExpression, Literal(0.5))
    val buffer = new GenericInternalRow(new Array[Any](1))
    agg.initialize(buffer)
    // Empty aggregation buffer
    assert(agg.eval(buffer) == null)
    // Empty input row
    agg.update(buffer, InternalRow(null))
    assert(agg.eval(buffer) == null)

    // Add some non-empty row
    agg.update(buffer, InternalRow(0))
    assert(agg.eval(buffer) != null)
  }
  
  test("null and invalid values( 0 and negatives ) handling of frequency column") {
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = true), DoubleType)
    val freqExpression = Cast(BoundReference(1, IntegerType, nullable = true), IntegerType)
    val agg = new Percentile(childExpression, freqExpression, Literal(0.5))
    val buffer = new GenericInternalRow(new Array[Any](2))
    agg.initialize(buffer)
    // Empty aggregation buffer
    assert(agg.eval(buffer) == null)
    // Empty input row
    agg.update(buffer, InternalRow(null, null))
    assert(agg.eval(buffer) == null)

    // Add some non-empty row with empty frequency col
    agg.update(buffer, InternalRow(0, null))
    assert(agg.eval(buffer) == null)
    
    // Add some non-empty row with zero frequency 
    agg.update(buffer, InternalRow(1, 0))
    assert(agg.eval(buffer) == null)
    
    // Add some non-empty row with negative frequency 
    agg.update(buffer, InternalRow(1, -5))
    assert(agg.eval(buffer) == null)
    
    // Add some non-empty row with non zero frequency
    agg.update(buffer, InternalRow(2, 1))
    assert(agg.eval(buffer) != null)
    assert(agg.eval(buffer) == 2)
  }


  private def compareEquals(
      left: OpenHashMap[Number, Long], right: OpenHashMap[Number, Long]): Boolean = {
    left.size == right.size && left.forall { case (key, count) =>
      right.apply(key) == count
    }
  }

  private def assertEqual[T](left: T, right: T): Unit = {
    assert(left == right)
  }
}
