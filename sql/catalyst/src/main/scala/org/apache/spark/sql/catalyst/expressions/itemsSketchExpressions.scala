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

package org.apache.spark.sql.catalyst.expressions

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Locale

import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch}
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{GenericArrayData, ItemsSketchUtils}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Helper object for ItemsSketch serialization that embeds the data type DDL
 * alongside the sketch bytes, enabling type-safe deserialization from binary blobs.
 *
 * Wire format: [ddlLength (4 bytes)][ddlBytes (n bytes)][sketchBytes (remaining)]
 */
private[expressions] object ItemsSketchSerDeHelper {

  def serialize(sketch: ItemsSketch[Any], dataType: DataType): Array[Byte] = {
    val serDe = ItemsSketchUtils.genSketchSerDe(dataType)
    val sketchBytes = sketch.toByteArray(serDe)
    val ddl = dataTypeToDDL(dataType)
    val ddlBytes = ddl.getBytes(StandardCharsets.UTF_8)
    val result = new Array[Byte](Integer.BYTES + ddlBytes.length + sketchBytes.length)
    val buf = ByteBuffer.wrap(result)
    buf.putInt(ddlBytes.length)
    buf.put(ddlBytes)
    buf.put(sketchBytes)
    result
  }

  def deserialize(
      bytes: Array[Byte],
      prettyName: String): (ItemsSketch[Any], DataType) = {
    try {
      val buf = ByteBuffer.wrap(bytes)
      val ddlLength = buf.getInt
      val ddlBytes = new Array[Byte](ddlLength)
      buf.get(ddlBytes)
      val ddl = new String(ddlBytes, StandardCharsets.UTF_8)
      val dataType = DDLToDataType(ddl)
      val sketchBytes = new Array[Byte](bytes.length - Integer.BYTES - ddlLength)
      buf.get(sketchBytes)
      val serDe = ItemsSketchUtils.genSketchSerDe(dataType)
      val sketch = ItemsSketch.getInstance(Memory.wrap(sketchBytes), serDe)
        .asInstanceOf[ItemsSketch[Any]]
      (sketch, dataType)
    } catch {
      case _: Exception =>
        throw QueryExecutionErrors.itemsSketchInvalidInputBuffer(prettyName)
    }
  }

  private def dataTypeToDDL(dataType: DataType): String = dataType match {
    case _: StringType => "item string not null"
    case other => StructField("item", other, nullable = false).toDDL
  }

  private def DDLToDataType(ddl: String): DataType = {
    StructType.fromDDL(ddl).fields.head.dataType
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(sketch, errorType) - Returns the frequent items from an ItemsSketch binary
    representation as an array of structs containing the item, estimated frequency,
    lower bound, and upper bound.
    `errorType` must be 'NO_FALSE_POSITIVES' or 'NO_FALSE_NEGATIVES'. """,
  examples = """
    Examples:
      > SELECT _FUNC_(items_sketch_agg(col), 'NO_FALSE_POSITIVES') FROM VALUES ('a'), ('a'), ('a'), ('b'), ('c') tab(col);
       [{"item":"a","estimate":3,"lowerBound":3,"upperBound":3},{"item":"c","estimate":1,"lowerBound":1,"upperBound":1},{"item":"b","estimate":1,"lowerBound":1,"upperBound":1}]
  """,
  group = "sketch_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class ItemsSketchGetFrequentItems(sketch: Expression, errorTypeExpr: Expression)
    extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  override def left: Expression = sketch
  override def right: Expression = errorTypeExpr

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ItemsSketchGetFrequentItems =
    copy(sketch = newLeft, errorTypeExpr = newRight)

  override def prettyName: String = "items_sketch_get_frequent_items"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, StringType)

  // The output schema uses StringType for items since we cannot know the exact type
  // at analysis time from a binary blob. Numeric items will be returned as their
  // string representation.
  override def dataType: DataType = {
    val entryType = StructType(Seq(
      StructField("item", StringType, nullable = true),
      StructField("estimate", LongType, nullable = false),
      StructField("lowerBound", LongType, nullable = false),
      StructField("upperBound", LongType, nullable = false)))
    ArrayType(entryType, containsNull = false)
  }

  private def parseErrorType(errorTypeStr: String): ErrorType = {
    errorTypeStr.toUpperCase(Locale.ROOT) match {
      case "NO_FALSE_POSITIVES" => ErrorType.NO_FALSE_POSITIVES
      case "NO_FALSE_NEGATIVES" => ErrorType.NO_FALSE_NEGATIVES
      case _ =>
        throw QueryExecutionErrors.itemsSketchInvalidErrorType(prettyName, errorTypeStr)
    }
  }

  override def nullSafeEval(sketchBytes: Any, errorTypeValue: Any): Any = {
    val bytes = sketchBytes.asInstanceOf[Array[Byte]]
    val errorTypeStr = errorTypeValue.asInstanceOf[UTF8String].toString
    val errorType = parseErrorType(errorTypeStr)

    val (itemsSketch, _) =
      ItemsSketchSerDeHelper.deserialize(bytes, prettyName)
    val frequentItems = itemsSketch.getFrequentItems(errorType)

    val result = new Array[Any](frequentItems.length)
    var i = 0
    while (i < frequentItems.length) {
      val fi = frequentItems(i)
      val itemStr = UTF8String.fromString(fi.getItem.toString)
      result(i) = InternalRow(itemStr, fi.getEstimate, fi.getLowerBound, fi.getUpperBound)
      i += 1
    }

    new GenericArrayData(result)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(sketch, item) - Returns the estimated frequency of a specific item from an
    ItemsSketch binary representation. The item is matched as a string. """,
  examples = """
    Examples:
      > SELECT _FUNC_(items_sketch_agg(col), 'a') FROM VALUES ('a'), ('a'), ('a'), ('b'), ('c') tab(col);
       3
  """,
  group = "sketch_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class ItemsSketchGetEstimate(sketch: Expression, item: Expression)
    extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  override def left: Expression = sketch
  override def right: Expression = item

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ItemsSketchGetEstimate =
    copy(sketch = newLeft, item = newRight)

  override def prettyName: String = "items_sketch_get_estimate"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, StringType)

  override def dataType: DataType = LongType

  override def nullSafeEval(sketchBytes: Any, itemValue: Any): Any = {
    val bytes = sketchBytes.asInstanceOf[Array[Byte]]
    val (itemsSketch, itemDataType) =
      ItemsSketchSerDeHelper.deserialize(bytes, prettyName)

    // Convert the string query item to the sketch's native type for lookup
    val queryStr = itemValue.asInstanceOf[UTF8String].toString
    val queryItem: Any = itemDataType match {
      case _: StringType => queryStr
      case _: BooleanType => queryStr.toBoolean
      case _: ByteType => java.lang.Byte.valueOf(queryStr)
      case _: ShortType => java.lang.Short.valueOf(queryStr)
      case _: IntegerType | _: DateType => java.lang.Integer.valueOf(queryStr)
      case _: LongType | _: TimestampType | _: TimestampNTZType =>
        java.lang.Long.valueOf(queryStr)
      case _: FloatType => java.lang.Float.valueOf(queryStr)
      case _: DoubleType => java.lang.Double.valueOf(queryStr)
      case _: DecimalType =>
        Decimal(new java.math.BigDecimal(queryStr))
      case _ => queryStr
    }

    itemsSketch.getEstimate(queryItem)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(first, second) - Merges two ItemsSketch binary representations into one. """,
  examples = """
    Examples:
      > SELECT items_sketch_get_frequent_items(_FUNC_(items_sketch_agg(col1), items_sketch_agg(col2)), 'NO_FALSE_POSITIVES') FROM VALUES ('a', 'b'), ('a', 'b'), ('a', 'c') tab(col1, col2);
       [{"item":"a","estimate":3,"lowerBound":3,"upperBound":3},{"item":"b","estimate":2,"lowerBound":2,"upperBound":2},{"item":"c","estimate":1,"lowerBound":1,"upperBound":1}]
  """,
  group = "sketch_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class ItemsSketchMerge(first: Expression, second: Expression)
    extends BinaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  override def left: Expression = first
  override def right: Expression = second

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ItemsSketchMerge =
    copy(first = newLeft, second = newRight)

  override def prettyName: String = "items_sketch_merge"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)

  override def dataType: DataType = BinaryType

  override def nullSafeEval(value1: Any, value2: Any): Any = {
    val bytes1 = value1.asInstanceOf[Array[Byte]]
    val (sketch1, dataType1) = ItemsSketchSerDeHelper.deserialize(bytes1, prettyName)

    val bytes2 = value2.asInstanceOf[Array[Byte]]
    val (sketch2, _) = ItemsSketchSerDeHelper.deserialize(bytes2, prettyName)

    sketch1.merge(sketch2)
    ItemsSketchSerDeHelper.serialize(sketch1, dataType1)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(sketch) - Returns a human-readable string summary of an ItemsSketch. """,
  examples = """
    Examples:
      > SELECT length(_FUNC_(items_sketch_agg(col))) > 0 FROM VALUES ('a'), ('b') tab(col);
       true
  """,
  group = "sketch_funcs",
  since = "4.2.0")
// scalastyle:on line.size.limit
case class ItemsSketchToString(child: Expression)
    extends UnaryExpression
    with CodegenFallback
    with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true

  override protected def withNewChildInternal(newChild: Expression): ItemsSketchToString =
    copy(child = newChild)

  override def prettyName: String = "items_sketch_to_string"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = StringType

  override def nullSafeEval(input: Any): Any = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val (itemsSketch, _) = ItemsSketchSerDeHelper.deserialize(bytes, prettyName)
    UTF8String.fromString(itemsSketch.toString)
  }
}
