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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckFailure}
import org.apache.spark.sql.catalyst.expressions.{Abs, ApproxTopKEstimate, BoundReference, Literal}
import org.apache.spark.sql.catalyst.expressions.Cast.ordinalNumber
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class ApproxTopKSuite extends SparkFunSuite {

  /////////////////////////////
  // ApproxTopK tests
  /////////////////////////////

  test("SPARK-52515: Accepts literal and foldable inputs") {
    val agg = new ApproxTopK(
      expr = BoundReference(0, IntegerType, nullable = true),
      k = Abs(Literal(10)),
      maxItemsTracked = Abs(Literal(-10))
    )
    assert(agg.checkInputDataTypes().isSuccess)
  }

  test("SPARK-52515: Fail if parameters are not foldable") {
    val badAgg = new ApproxTopK(
      expr = BoundReference(0, IntegerType, nullable = true),
      k = Sum(BoundReference(1, IntegerType, nullable = true)),
      maxItemsTracked = Literal(10)
    )
    assert(badAgg.checkInputDataTypes().isFailure)
    assert(badAgg.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"INT\"",
          "inputSql" -> "\"sum(boundreference())\"",
          "inputType" -> "\"BIGINT\""
        )
      )
    )

    val badAgg2 = new ApproxTopK(
      expr = BoundReference(0, IntegerType, nullable = true),
      k = Literal(10),
      maxItemsTracked = Sum(BoundReference(1, IntegerType, nullable = true))
    )
    assert(badAgg2.checkInputDataTypes().isFailure)
    assert(badAgg2.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(2),
          "requiredType" -> "\"INT\"",
          "inputSql" -> "\"sum(boundreference())\"",
          "inputType" -> "\"BIGINT\""
        )
      )
    )
  }

  gridTest("SPARK-52515: invalid ApproxTopK with unsupported item types")(
    Seq(
      ("array", ArrayType(IntegerType)),
      ("map", MapType(StringType, IntegerType)),
      ("struct", StructType(Seq(StructField("a", IntegerType)))),
      ("binary", BinaryType)
    )) { unSupportedType =>
    val (typeName, dataType) = unSupportedType
    val agg = new ApproxTopK(
      expr = BoundReference(0, dataType, nullable = true),
      k = Literal(10),
      maxItemsTracked = Literal(10000)
    )
    assert(agg.checkInputDataTypes().isFailure)
    assert(agg.checkInputDataTypes() == TypeCheckFailure(s"$typeName columns are not supported"))
  }

  /////////////////////////////
  // ApproxTopKAccumulate tests
  /////////////////////////////

  gridTest("SPARK-52588: invalid accumulate if item type is not supported")(
    Seq(
      ("array", ArrayType(IntegerType)),
      ("map", MapType(StringType, IntegerType)),
      ("struct", StructType(Seq(StructField("a", IntegerType)))),
      ("binary", BinaryType)
    )) { unSupportedType =>
    val (typeName, dataType) = unSupportedType
    val badAccumulate = ApproxTopKAccumulate(
      expr = BoundReference(0, dataType, nullable = true),
      maxItemsTracked = Literal(10)
    )
    assert(badAccumulate.checkInputDataTypes().isFailure)
    assert(badAccumulate.checkInputDataTypes() ==
      TypeCheckFailure(s"$typeName columns are not supported"))
  }

  test("SPARK-52588: invalid accumulate if maxItemsTracked are not foldable") {
    val badAccumulate = ApproxTopKAccumulate(
      expr = BoundReference(0, IntegerType, nullable = true),
      maxItemsTracked = Sum(BoundReference(1, IntegerType, nullable = true))
    )
    assert(badAccumulate.checkInputDataTypes().isFailure)
    assert(badAccumulate.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"INT\"",
          "inputSql" -> "\"sum(boundreference())\"",
          "inputType" -> "\"BIGINT\""
        )
      )
    )
  }

  /////////////////////////////
  // ApproxTopKEstimate tests
  /////////////////////////////

  test("SPARK-52588: invalid estimate if k are not foldable") {
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, StructType(Seq(
        StructField("sketch", BinaryType),
        StructField("maxItemsTracked", IntegerType),
        StructField("itemDataType", IntegerType),
        StructField("itemDataTypeDDL", StringType)
      )), nullable = false),
      k = Sum(BoundReference(1, IntegerType, nullable = true))
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"INT\"",
          "inputSql" -> "\"sum(boundreference())\"",
          "inputType" -> "\"BIGINT\""
        )
      )
    )
  }

  test("SPARK-52588: invalid estimate if state is not a struct") {
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, IntegerType, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> "\"STRUCT\"",
          "inputSql" -> "\"boundreference()\"",
          "inputType" -> "\"INT\""
        )
      )
    )
  }

  test("SPARK-52588: invalid estimate if state struct length is not 4") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", IntegerType)
      // Missing "itemDataType", "itemDataTypeDDL" fields
    ))
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure("State must be a struct with 4 fields. " +
        "Expected struct: " +
        "struct<sketch:binary,maxItemsTracked:int,itemDataType:any,itemDataTypeDDL:string>. " +
        "Got: struct<sketch:binary,maxItemsTracked:int>"))
  }

  test("SPARK-52588: invalid estimate if state struct's first field is not binary") {
    val invalidState = StructType(Seq(
      StructField("notSketch", IntegerType), // Should be BinaryType
      StructField("maxItemsTracked", IntegerType),
      StructField("itemDataType", IntegerType),
      StructField("itemDataTypeDDL", StringType)
    ))
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the first field to be binary. Got: int"))
  }

  test("SPARK-52588: invalid estimate if state struct's second field is not int") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", LongType), // Should be IntegerType
      StructField("itemDataType", IntegerType),
      StructField("itemDataTypeDDL", StringType)
    ))
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the second field to be int. Got: bigint"))
  }

  gridTest("SPARK-52588: invalid estimate if state struct's third field is not supported")(
    Seq(
      ("array<int>", ArrayType(IntegerType)),
      ("map<string,int>", MapType(StringType, IntegerType)),
      ("struct<a:int>", StructType(Seq(StructField("a", IntegerType)))),
      ("binary", BinaryType)
    )) { unSupportedType =>
    val (typeName, dataType) = unSupportedType
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", IntegerType),
      StructField("itemDataType", dataType),
      StructField("itemDataTypeDDL", StringType)
    ))
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure(s"State struct must have the third field to be a supported data type. " +
        s"Got: $typeName"))
  }

  test("SPARK-52588: invalid estimate if state struct's fourth field is not string") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", IntegerType),
      StructField("itemDataType", IntegerType),
      StructField("itemDataTypeDDL", BinaryType) // Should be StringType
    ))
    val badEstimate = ApproxTopKEstimate(
      state = BoundReference(0, invalidState, nullable = false),
      k = Literal(5)
    )
    assert(badEstimate.checkInputDataTypes().isFailure)
    assert(badEstimate.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the fourth field to be string. Got: binary"))
  }

  /////////////////////////////
  // ApproxTopKCombine tests
  /////////////////////////////
  test("SPARK-52798: invalid combine if maxItemsTracked is not foldable") {
    val badCombine = ApproxTopKCombine(
      state = BoundReference(0, StructType(Seq(
        StructField("sketch", BinaryType),
        StructField("maxItemsTracked", IntegerType),
        StructField("itemDataType", IntegerType),
        StructField("itemDataTypeDDL", StringType)
      )), nullable = false),
      maxItemsTracked = Sum(BoundReference(1, IntegerType, nullable = true))
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"INT\"",
          "inputSql" -> "\"sum(boundreference())\"",
          "inputType" -> "\"BIGINT\""
        )
      )
    )
  }

  test("SPARK-52798: invalid combine if state is not a struct") {
    val badCombine = ApproxTopKCombine(
      state = BoundReference(0, IntegerType, nullable = false),
      maxItemsTracked = Literal(10)
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> "\"STRUCT\"",
          "inputSql" -> "\"boundreference()\"",
          "inputType" -> "\"INT\""
        )
      )
    )
  }

  test("SPARK-52798: invalid combine if state struct length is not 4") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", IntegerType)
      // Missing "itemDataType", "itemDataTypeDDL" fields
    ))
    val badCombine = ApproxTopKCombine(
      state = BoundReference(0, invalidState, nullable = false),
      maxItemsTracked = Literal(10)
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      TypeCheckFailure("State must be a struct with 4 fields. " +
        "Expected struct: " +
        "struct<sketch:binary,maxItemsTracked:int,itemDataType:any,itemDataTypeDDL:string>. " +
        "Got: struct<sketch:binary,maxItemsTracked:int>"))
  }

  test("SPARK-52798: invalid combine if state struct's first field is not binary") {
    val invalidState = StructType(Seq(
      StructField("sketch", IntegerType), // Should be BinaryType
      StructField("maxItemsTracked", IntegerType),
      StructField("itemDataType", IntegerType),
      StructField("itemDataTypeDDL", StringType)
    ))
    val badCombine = ApproxTopKCombine(
      state = BoundReference(0, invalidState, nullable = false),
      maxItemsTracked = Literal(10)
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the first field to be binary. Got: int"))
  }

  test("SPARK-52798: invalid combine if state struct's second field is not int") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", LongType), // Should be IntegerType
      StructField("itemDataType", IntegerType),
      StructField("itemDataTypeDDL", StringType)
    ))
    val badCombine = ApproxTopKCombine(
      state = BoundReference(0, invalidState, nullable = false),
      maxItemsTracked = Literal(10)
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the second field to be int. Got: bigint"))
  }

  gridTest("SPARK-52798: invalid combine if state struct's third field is not supported")(
    Seq(
      ("array<int>", ArrayType(IntegerType)),
      ("map<string,int>", MapType(StringType, IntegerType)),
      ("struct<a:int>", StructType(Seq(StructField("a", IntegerType)))),
      ("binary", BinaryType)
    )) { unSupportedType =>
    val (typeName, dataType) = unSupportedType
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", IntegerType),
      StructField("itemDataType", dataType),
      StructField("itemDataTypeDDL", StringType)
    ))
    val badCombine = ApproxTopKCombine(
      state = BoundReference(0, invalidState, nullable = false),
      maxItemsTracked = Literal(10)
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      TypeCheckFailure(s"State struct must have the third field to be a supported data type. " +
        s"Got: $typeName"))
  }

  test("SPARK-52798: invalid combine if state struct's fourth field is not string") {
    val invalidState = StructType(Seq(
      StructField("sketch", BinaryType),
      StructField("maxItemsTracked", IntegerType),
      StructField("itemDataType", IntegerType),
      StructField("itemDataTypeDDL", BinaryType) // Should be StringType
    ))
    val badCombine = ApproxTopKCombine(
      state = BoundReference(0, invalidState, nullable = false),
      maxItemsTracked = Literal(10)
    )
    assert(badCombine.checkInputDataTypes().isFailure)
    assert(badCombine.checkInputDataTypes() ==
      TypeCheckFailure("State struct must have the fourth field to be string. Got: binary"))
  }

  /////////////////////////////
  // Aggregation buffer serialization tests
  /////////////////////////////

  private def stringRows(values: String*): Seq[InternalRow] =
    values.map(v => InternalRow(if (v == null) null else UTF8String.fromString(v)))

  private def buildBuffer(agg: ApproxTopK, rows: Seq[InternalRow]): ApproxTopKAggregateBuffer[Any] =
    rows.foldLeft(agg.createAggregationBuffer())((buffer, row) => agg.update(buffer, row))

  private def topItems(result: Any, itemType: DataType): Seq[(Any, Long)] =
    result.asInstanceOf[GenericArrayData].array.toSeq.map {
      case row: InternalRow => (row.get(0, itemType), row.getLong(1))
    }

  // Inputs with four distinct counts (including null), used to build approx_top_k buffers.
  private val bufferInputs: Seq[(DataType, Seq[InternalRow])] = Seq(
    StringType -> stringRows("a", "b", "b", null, "c", "c", "c"),
    LongType -> (Seq(1L, 2L, 2L, 3L, 3L, 3L).map(InternalRow(_)) :+ InternalRow(null)))

  test("SPARK-62341: approx_top_k buffers round-trip through serialize and deserialize") {
    bufferInputs.foreach { case (dataType, rows) =>
      val agg = new ApproxTopK(BoundReference(0, dataType, nullable = true), 4, 10)
      val buffer = buildBuffer(agg, rows)
      val restored = agg.deserialize(agg.serialize(buffer))
      val expected = topItems(agg.eval(buffer), dataType)
      assert(expected.size == 4)
      assert(topItems(agg.eval(restored), dataType) == expected)
    }
  }

  test("SPARK-62341: deserialized approx_top_k buffer does not reference the input bytes") {
    // The sketch is decoded in place from the input array, so the decoded buffer must copy
    // everything it needs: overwriting the input afterwards must not change it.
    bufferInputs.foreach { case (dataType, rows) =>
      val agg = new ApproxTopK(BoundReference(0, dataType, nullable = true), 4, 10)
      val buffer = buildBuffer(agg, rows)
      val bytes = agg.serialize(buffer)
      val restored = agg.deserialize(bytes)
      java.util.Arrays.fill(bytes, 0.toByte)
      assert(topItems(agg.eval(restored), dataType) == topItems(agg.eval(buffer), dataType))
    }
  }

  test("SPARK-62341: approx_top_k buffer deserializes from a region of a larger array") {
    val agg = new ApproxTopK(BoundReference(0, StringType, nullable = true), 3, 10)
    val buffer = buildBuffer(agg, stringRows("x", "y", "y", null, null, "z", "z", "z"))
    val bytes = agg.serialize(buffer)
    val padded = Array.fill[Byte](5)(7) ++ bytes ++ Array.fill[Byte](3)(9)
    val restored = ApproxTopKAggregateBuffer.deserialize(
      padded, 5, bytes.length, ApproxTopK.genSketchSerDe(StringType))
    assert(topItems(restored.eval(3, StringType, StringType), StringType) ==
      topItems(buffer.eval(3, StringType, StringType), StringType))
  }

  test("SPARK-62341: approx_top_k_combine state round-trips through serialize and deserialize") {
    val agg = new ApproxTopK(BoundReference(0, StringType, nullable = true), 3, 10)
    val sketch = buildBuffer(agg, stringRows("p", "q", "q", "r", "r", "r", null))
    val restored = CombineInternal.deserialize(
      new CombineInternal[Any](sketch, StringType, 10).serialize())
    assert(restored.getItemDataType == StringType)
    assert(restored.getMaxItemsTracked == 10)
    assert(topItems(restored.getSketchWithNullCount.eval(3, StringType, StringType), StringType) ==
      topItems(sketch.eval(3, StringType, StringType), StringType))
  }
}
