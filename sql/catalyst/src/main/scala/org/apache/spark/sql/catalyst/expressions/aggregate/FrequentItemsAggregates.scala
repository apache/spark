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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.apache.datasketches.common._
import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch}
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{ArrayOfDecimalsSerDe, Expression, ExpressionDescription, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.trees.{BinaryLike, TernaryLike}
import org.apache.spark.sql.catalyst.util.{CollationFactory, GenericArrayData}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * The ApproxFrequentItems function (i.e., "approx_frequent_items") is an aggregate function that
 * estimates the approximate frequent items (heavy hitters) in a column.
 *
 * The result is an array of structs, each containing a frequent item and its estimated frequency.
 * The items are sorted by their estimated frequency in descending order.
 *
 * This function uses the ItemsSketch from the Apache DataSketches library to do the estimation.
 *
 * IMPORTANT: This function does NOT guarantee exact top-k ranking semantics. It provides
 * mathematical error-bound guarantees rather than a strict top-k list.
 *
 * Guarantees and Behavior:
 * 1. The result count can be less than `k` (or even empty / zero items) depending on the
 *    frequency distribution in the stream and the configured sketch size (`maxItemsTracked`).
 *    For example, if no single item's frequency exceeds the sketch's error threshold, the sketch
 *    can legitimately return an empty array, even if `k` is set to a large number.
 * 2. It utilizes `ErrorType.NO_FALSE_POSITIVES` under the hood. This means:
 *    - No False Positives: Every item returned is guaranteed to have a true frequency
 *      greater than the threshold. There will be no incorrect items (spurious results) in the output.
 *    - Potential False Negatives: Some items whose true frequency is slightly above the threshold
 *      might be missed (omitted) due to the approximate nature of the sketch.
 *
 * For more information, see:
 * https://datasketches.apache.org/docs/Frequency/FrequencySketches.html
 *
 * @param expr                   the child expression to estimate the frequent items from
 * @param k                      the number of items to return (K)
 * @param maxItemsTracked        the maximum number of items to track in the sketch
 * @param mutableAggBufferOffset the offset for mutable aggregation buffer
 * @param inputAggBufferOffset   the offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, k, maxItemsTracked) - Returns frequent items (heavy hitters) with their frequency.
      `k` An optional INTEGER literal greater than 0. If k is not specified, it defaults to 5.
      `maxItemsTracked` An optional INTEGER literal greater than or equal to k and has upper limit of 1000000. If maxItemsTracked is not specified, it defaults to 10000.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(expr) FROM VALUES (0), (0), (1), (1), (2), (3), (4), (4) AS tab(expr);
       [{"item":0,"count":2},{"item":4,"count":2},{"item":1,"count":2},{"item":2,"count":1},{"item":3,"count":1}]

      > SELECT _FUNC_(expr, 2) FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);
       [{"item":"c","count":4},{"item":"d","count":2}]

      > SELECT _FUNC_(expr, 10, 100) FROM VALUES (0), (1), (1), (2), (2), (2) AS tab(expr);
       [{"item":2,"count":3},{"item":1,"count":2},{"item":0,"count":1}]
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class ApproxFrequentItems(
    expr: Expression,
    k: Expression,
    maxItemsTracked: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ApproxFrequentItemsAggregateBuffer[Any]]
  with ImplicitCastInputTypes
  with TernaryLike[Expression] {

  def this(child: Expression, topK: Expression, maxItemsTracked: Expression) =
    this(child, topK, maxItemsTracked, 0, 0)

  def this(child: Expression, topK: Int, maxItemsTracked: Int) =
    this(child, Literal(topK), Literal(maxItemsTracked), 0, 0)

  def this(child: Expression, topK: Expression) =
    this(child, topK, Literal(ApproxFrequentItems.DEFAULT_MAX_ITEMS_TRACKED), 0, 0)

  def this(child: Expression, topK: Int) =
    this(child, Literal(topK), Literal(ApproxFrequentItems.DEFAULT_MAX_ITEMS_TRACKED), 0, 0)

  def this(child: Expression) =
    this(child, Literal(ApproxFrequentItems.DEFAULT_K), Literal(ApproxFrequentItems.DEFAULT_MAX_ITEMS_TRACKED), 0, 0)

  private lazy val itemDataType: DataType = expr.dataType
  private lazy val kVal: Int = {
    ApproxFrequentItems.checkExpressionNotNull(k, "k", prettyName)
    val kVal = k.eval().asInstanceOf[Int]
    ApproxFrequentItems.checkK(kVal, prettyName)
    kVal
  }
  private lazy val maxItemsTrackedVal: Int = {
    ApproxFrequentItems.checkExpressionNotNull(maxItemsTracked, "maxItemsTracked", prettyName)
    val maxItemsTrackedVal = maxItemsTracked.eval().asInstanceOf[Int]
    ApproxFrequentItems.checkMaxItemsTracked(maxItemsTrackedVal, kVal, prettyName)
    maxItemsTrackedVal
  }

  override def first: Expression = expr

  override def second: Expression = k

  override def third: Expression = maxItemsTracked

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegerType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!ApproxFrequentItems.isDataTypeSupported(itemDataType)) {
      TypeCheckFailure(f"${itemDataType.typeName} columns are not supported")
    } else if (!k.foldable) {
      TypeCheckFailure("K must be a constant literal")
    } else if (!maxItemsTracked.foldable) {
      TypeCheckFailure("Number of items tracked must be a constant literal")
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = ApproxFrequentItems.getResultDataType(itemDataType)

  override def createAggregationBuffer(): ApproxFrequentItemsAggregateBuffer[Any] = {
    val maxMapSize = ApproxFrequentItems.calMaxMapSize(maxItemsTrackedVal)
    val sketch = ApproxFrequentItems.createItemsSketch(expr, maxMapSize)
    new ApproxFrequentItemsAggregateBuffer[Any](sketch, 0L)
  }

  override def update(buffer: ApproxFrequentItemsAggregateBuffer[Any], input: InternalRow):
    ApproxFrequentItemsAggregateBuffer[Any] =
    buffer.update(expr, input)

  override def merge(
      buffer: ApproxFrequentItemsAggregateBuffer[Any],
      input: ApproxFrequentItemsAggregateBuffer[Any]):
    ApproxFrequentItemsAggregateBuffer[Any] =
    buffer.merge(input)

  override def eval(buffer: ApproxFrequentItemsAggregateBuffer[Any]): GenericArrayData =
    buffer.eval(kVal, itemDataType)

  override def serialize(buffer: ApproxFrequentItemsAggregateBuffer[Any]): Array[Byte] =
    buffer.serialize(ApproxFrequentItems.genSketchSerDe(itemDataType))

  override def deserialize(storageFormat: Array[Byte]): ApproxFrequentItemsAggregateBuffer[Any] =
    ApproxFrequentItemsAggregateBuffer.deserialize(storageFormat, ApproxFrequentItems.genSketchSerDe(itemDataType))

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newExpr: Expression,
      newK: Expression,
      newMaxItemsTracked: Expression): Expression =
    copy(expr = newExpr, k = newK, maxItemsTracked = newMaxItemsTracked)

  override def nullable: Boolean = false

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("approx_frequent_items")
}

object ApproxFrequentItems {

  val DEFAULT_K: Int = 5
  val DEFAULT_MAX_ITEMS_TRACKED: Int = 10000
  val MAX_ITEMS_TRACKED_LIMIT: Int = 1000000
  // A special value indicating no explicit maxItemsTracked input in function approx_frequent_items_combine
  val VOID_MAX_ITEMS_TRACKED = -1

  def checkExpressionNotNull(expr: Expression, exprName: String, functionName: String): Unit = {
    if (expr == null || expr.eval() == null) {
      throw QueryExecutionErrors.approxFrequentItemsNullArg(functionName, exprName)
    }
  }

  def checkK(k: Int, functionName: String): Unit = {
    if (k <= 0) {
      throw QueryExecutionErrors.approxFrequentItemsNonPositiveValue(functionName, "k", k)
    }
  }

  def checkMaxItemsTracked(maxItemsTracked: Int, functionName: String): Unit = {
    if (maxItemsTracked > MAX_ITEMS_TRACKED_LIMIT) {
      throw QueryExecutionErrors.approxFrequentItemsMaxItemsTrackedExceedsLimit(
        functionName, maxItemsTracked, MAX_ITEMS_TRACKED_LIMIT)
    }
    if (maxItemsTracked <= 0) {
      throw QueryExecutionErrors.approxFrequentItemsNonPositiveValue(functionName, "maxItemsTracked", maxItemsTracked)
    }
  }

  def checkMaxItemsTracked(maxItemsTracked: Int, k: Int, functionName: String): Unit = {
    checkMaxItemsTracked(maxItemsTracked, functionName)
    if (maxItemsTracked < k) {
      throw QueryExecutionErrors.approxFrequentItemsMaxItemsTrackedLessThanK(functionName, maxItemsTracked, k)
    }
  }

  def getResultDataType(itemDataType: DataType): DataType = {
    val resultEntryType = StructType(
      StructField("item", itemDataType, nullable = true) ::
        StructField("count", LongType, nullable = false) :: Nil)
    ArrayType(resultEntryType, containsNull = false)
  }

  def isDataTypeSupported(itemType: DataType): Boolean = {
    itemType match {
      case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType |
           _: LongType | _: FloatType | _: DoubleType | _: DateType |
           _: TimestampType | _: TimestampNTZType | _: StringType | _: DecimalType => true
      case _ => false
    }
  }

  def calMaxMapSize(maxItemsTracked: Int): Int = {
    val ceilMaxMapSize = math.ceil(maxItemsTracked / 0.75).toInt
    math.pow(2, math.ceil(math.log(ceilMaxMapSize) / math.log(2))).toInt
  }

  def createItemsSketch(itemExpression: Expression, maxMapSize: Int): ItemsSketch[Any] = {
    itemExpression.dataType match {
      case _: BooleanType =>
        new ItemsSketch[Boolean](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: ByteType | _: ShortType | _: IntegerType | _: FloatType | _: DateType =>
        new ItemsSketch[Number](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: LongType | _: TimestampType | _: TimestampNTZType =>
        new ItemsSketch[Long](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: DoubleType =>
        new ItemsSketch[Double](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: StringType =>
        new ItemsSketch[String](maxMapSize).asInstanceOf[ItemsSketch[Any]]
      case _: DecimalType =>
        new ItemsSketch[Decimal](maxMapSize).asInstanceOf[ItemsSketch[Any]]
    }
  }

  def genSketchSerDe(dataType: DataType): ArrayOfItemsSerDe[Any] = {
    dataType match {
      case _: BooleanType => new ArrayOfBooleansSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
      case _: ByteType | _: ShortType | _: IntegerType | _: FloatType | _: DateType =>
        new ArrayOfNumbersSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
      case _: LongType | _: TimestampType | _: TimestampNTZType =>
        new ArrayOfLongsSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
      case _: DoubleType =>
        new ArrayOfDoublesSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
      case _: StringType =>
        new ArrayOfStringsSerDe().asInstanceOf[ArrayOfItemsSerDe[Any]]
      case dt: DecimalType =>
        new ArrayOfDecimalsSerDe(dt).asInstanceOf[ArrayOfItemsSerDe[Any]]
    }
  }

  def getSketchStateDataType(itemDataType: DataType): StructType =
    StructType(
      StructField("sketch", BinaryType, nullable = false) ::
        StructField("maxItemsTracked", IntegerType, nullable = false) ::
        StructField("itemDataType", itemDataType) ::
        StructField("itemDataTypeDDL", StringType, nullable = false) :: Nil)

  def dataTypeToDDL(dataType: DataType): String = dataType match {
    case _: StringType =>
      s"item string not null"
    case other =>
      StructField("item", other, nullable = false).toDDL
  }

  def DDLToDataType(ddl: String): DataType = {
    StructType.fromDDL(ddl).fields.head.dataType
  }

  def checkStateFieldAndType(state: Expression): TypeCheckResult = {
    val stateStructType = state.dataType.asInstanceOf[StructType]
    if (stateStructType.length != 4) {
      return TypeCheckFailure("State must be a struct with 4 fields. " +
        "Expected struct: " +
        "struct<sketch:binary,maxItemsTracked:int,itemDataType:any,itemDataTypeDDL:string>. " +
        "Got: " + state.dataType.simpleString)
    }

    val fieldType1 = stateStructType.head.dataType
    val fieldType2 = stateStructType(1).dataType
    val fieldType3 = stateStructType(2).dataType
    val fieldType4 = stateStructType(3).dataType
    if (fieldType1 != BinaryType) {
      TypeCheckFailure("State struct must have the first field to be binary. " +
        "Got: " + fieldType1.simpleString)
    } else if (fieldType2 != IntegerType) {
      TypeCheckFailure("State struct must have the second field to be int. " +
        "Got: " + fieldType2.simpleString)
    } else if (!ApproxFrequentItems.isDataTypeSupported(fieldType3)) {
      TypeCheckFailure("State struct must have the third field to be a supported data type. " +
        "Got: " + fieldType3.simpleString)
    } else if (fieldType4 != StringType) {
      TypeCheckFailure("State struct must have the fourth field to be string. " +
        "Got: " + fieldType4.simpleString)
    } else {
      TypeCheckSuccess
    }
  }
}

/**
 * An internal class used as the aggregation buffer for ApproxFrequentItems.
 *
 * @param sketch    the ItemsSketch instance for counting not-null items
 * @param nullCount the count of null items
 */
class ApproxFrequentItemsAggregateBuffer[T](val sketch: ItemsSketch[T], private var nullCount: Long) {
  def update(itemExpression: Expression, input: InternalRow): ApproxFrequentItemsAggregateBuffer[T] = {
    val v = itemExpression.eval(input)
    if (v != null) {
      itemExpression.dataType match {
        case _: BooleanType =>
          sketch.asInstanceOf[ItemsSketch[Boolean]].update(v.asInstanceOf[Boolean])
        case _: ByteType =>
          sketch.asInstanceOf[ItemsSketch[Byte]].update(v.asInstanceOf[Byte])
        case _: ShortType =>
          sketch.asInstanceOf[ItemsSketch[Short]].update(v.asInstanceOf[Short])
        case _: IntegerType =>
          sketch.asInstanceOf[ItemsSketch[Int]].update(v.asInstanceOf[Int])
        case _: LongType =>
          sketch.asInstanceOf[ItemsSketch[Long]].update(v.asInstanceOf[Long])
        case _: FloatType =>
          sketch.asInstanceOf[ItemsSketch[Float]].update(v.asInstanceOf[Float])
        case _: DoubleType =>
          sketch.asInstanceOf[ItemsSketch[Double]].update(v.asInstanceOf[Double])
        case _: DateType =>
          sketch.asInstanceOf[ItemsSketch[Int]].update(v.asInstanceOf[Int])
        case _: TimestampType =>
          sketch.asInstanceOf[ItemsSketch[Long]].update(v.asInstanceOf[Long])
        case _: TimestampNTZType =>
          sketch.asInstanceOf[ItemsSketch[Long]].update(v.asInstanceOf[Long])
        case st: StringType =>
          val cKey = CollationFactory.getCollationKey(v.asInstanceOf[UTF8String], st.collationId)
          sketch.asInstanceOf[ItemsSketch[String]].update(cKey.toString)
        case _: DecimalType =>
          sketch.asInstanceOf[ItemsSketch[Decimal]].update(v.asInstanceOf[Decimal])
      }
    } else {
      nullCount += 1
    }
    this
  }

  def merge(other: ApproxFrequentItemsAggregateBuffer[T]): ApproxFrequentItemsAggregateBuffer[T] = {
    sketch.merge(other.sketch)
    nullCount += other.nullCount
    this
  }

  def serialize(serDe: ArrayOfItemsSerDe[T]): Array[Byte] = {
    val sketchBytes = sketch.toByteArray(serDe)
    val result = new Array[Byte](sketchBytes.length + java.lang.Long.BYTES)
    val byteBuffer = java.nio.ByteBuffer.wrap(result)
    byteBuffer.put(sketchBytes)
    byteBuffer.putLong(nullCount)
    result
  }

  def eval(k: Int, itemDataType: DataType): GenericArrayData = {
    val frequentItems = sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES)
    val itemsLength = frequentItems.length + (if (nullCount > 0) 1 else 0)
    val resultLength = math.min(itemsLength, k)
    val result = new Array[AnyRef](resultLength)

    var fiIndex = 0
    var resultIndex = 0
    var isNullAdded = false

    @inline def getNullEstimate: Long = if (!isNullAdded) nullCount else Long.MinValue

    while (resultIndex < resultLength && fiIndex < frequentItems.length) {
      val curFrequentItem = frequentItems(fiIndex)
      val itemEstimate = curFrequentItem.getEstimate
      val nullEstimate = getNullEstimate

      val (item, estimate) = if (nullEstimate > itemEstimate) {
        isNullAdded = true
        (null, nullCount.toLong)
      } else {
        val item: Any = itemDataType match {
          case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType |
               _: LongType | _: FloatType | _: DoubleType | _: DecimalType |
               _: DateType | _: TimestampType | _: TimestampNTZType =>
            curFrequentItem.getItem
          case _: StringType =>
            UTF8String.fromString(curFrequentItem.getItem.asInstanceOf[String])
        }
        fiIndex += 1
        (item, itemEstimate)
      }
      result(resultIndex) = InternalRow(item, estimate)
      resultIndex += 1
    }

    if (resultIndex < resultLength && nullCount > 0 && !isNullAdded) {
      result(resultIndex) = InternalRow(null, nullCount.toLong)
    }

    new GenericArrayData(result)
  }
}

object ApproxFrequentItemsAggregateBuffer {
  def deserialize(bytes: Array[Byte], serDe: ArrayOfItemsSerDe[Any]):
  ApproxFrequentItemsAggregateBuffer[Any] = {
    val byteBuffer = java.nio.ByteBuffer.wrap(bytes)
    val sketchBytesLength = bytes.length - 8
    val sketchBytes = new Array[Byte](sketchBytesLength)
    byteBuffer.get(sketchBytes, 0, sketchBytesLength)
    val nullCount = byteBuffer.getLong(sketchBytesLength)
    val deserializedSketch = ItemsSketch.getInstance(Memory.wrap(sketchBytes), serDe)
    new ApproxFrequentItemsAggregateBuffer[Any](deserializedSketch, nullCount)
  }
}

/**
 * An aggregate function that accumulates items into a sketch, which can then be used
 * to combine with other sketches, via ApproxFrequentItemsCombine,
 * or to estimate the frequent items, via ApproxFrequentItemsEstimate.
 *
 * @param expr                   the child expression to accumulate items from
 * @param maxItemsTracked        the maximum number of items to track in the sketch
 * @param mutableAggBufferOffset the offset for mutable aggregation buffer
 * @param inputAggBufferOffset   the offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, maxItemsTracked) - Accumulates items into a sketch.
      `maxItemsTracked` An optional positive INTEGER literal with upper limit of 1000000. If maxItemsTracked is not specified, it defaults to 10000.
  """,
  examples = """
    Examples:
      > SELECT approx_frequent_items_estimate(_FUNC_(expr)) FROM VALUES (0), (0), (1), (1), (2), (3), (4), (4) AS tab(expr);
       [{"item":0,"count":2},{"item":4,"count":2},{"item":1,"count":2},{"item":2,"count":1},{"item":3,"count":1}]

      > SELECT approx_frequent_items_estimate(_FUNC_(expr, 100), 2) FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);
       [{"item":"c","count":4},{"item":"d","count":2}]
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class ApproxFrequentItemsAccumulate(
    expr: Expression,
    maxItemsTracked: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ApproxFrequentItemsAggregateBuffer[Any]]
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {

  def this(child: Expression, maxItemsTracked: Expression) = this(child, maxItemsTracked, 0, 0)

  def this(child: Expression, maxItemsTracked: Int) = this(child, Literal(maxItemsTracked), 0, 0)

  def this(child: Expression) = this(child, Literal(ApproxFrequentItems.DEFAULT_MAX_ITEMS_TRACKED), 0, 0)

  private lazy val itemDataType: DataType = expr.dataType

  private lazy val maxItemsTrackedVal: Int = {
    ApproxFrequentItems.checkExpressionNotNull(maxItemsTracked, "maxItemsTracked", prettyName)
    val maxItemsTrackedVal = maxItemsTracked.eval().asInstanceOf[Int]
    ApproxFrequentItems.checkMaxItemsTracked(maxItemsTrackedVal, prettyName)
    maxItemsTrackedVal
  }

  override def left: Expression = expr

  override def right: Expression = maxItemsTracked

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!ApproxFrequentItems.isDataTypeSupported(itemDataType)) {
      TypeCheckFailure(f"${itemDataType.typeName} columns are not supported")
    } else if (!maxItemsTracked.foldable) {
      TypeCheckFailure("Number of items tracked must be a constant literal")
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = ApproxFrequentItems.getSketchStateDataType(itemDataType)

  override def createAggregationBuffer(): ApproxFrequentItemsAggregateBuffer[Any] = {
    val maxMapSize = ApproxFrequentItems.calMaxMapSize(maxItemsTrackedVal)
    val sketch = ApproxFrequentItems.createItemsSketch(expr, maxMapSize)
    new ApproxFrequentItemsAggregateBuffer[Any](sketch, 0L)
  }

  override def update(buffer: ApproxFrequentItemsAggregateBuffer[Any], input: InternalRow):
    ApproxFrequentItemsAggregateBuffer[Any] =
    buffer.update(expr, input)

  override def merge(
      buffer: ApproxFrequentItemsAggregateBuffer[Any],
      input: ApproxFrequentItemsAggregateBuffer[Any]):
    ApproxFrequentItemsAggregateBuffer[Any] =
    buffer.merge(input)

  override def eval(buffer: ApproxFrequentItemsAggregateBuffer[Any]): Any = {
    val sketchBytes = serialize(buffer)
    val itemDataTypeDDL = ApproxFrequentItems.dataTypeToDDL(itemDataType)
    InternalRow.apply(
      sketchBytes,
      maxItemsTrackedVal,
      null,
      UTF8String.fromString(itemDataTypeDDL))
  }

  override def serialize(buffer: ApproxFrequentItemsAggregateBuffer[Any]): Array[Byte] =
    buffer.serialize(ApproxFrequentItems.genSketchSerDe(itemDataType))

  override def deserialize(storageFormat: Array[Byte]): ApproxFrequentItemsAggregateBuffer[Any] =
    ApproxFrequentItemsAggregateBuffer.deserialize(storageFormat, ApproxFrequentItems.genSketchSerDe(itemDataType))

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression =
    copy(expr = newLeft, maxItemsTracked = newRight)

  override def nullable: Boolean = false

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("approx_frequent_items_accumulate")
}

/**
 * An internal class used as the aggregation buffer for ApproxFrequentItemsCombine.
 *
 * @param sketch          the ItemsSketch instance
 * @param itemDataType    the data type of items in the sketch
 * @param maxItemsTracked the maximum number of items tracked in the sketch
 */
class CombineInternal[T](
    sketchWithNullCount: ApproxFrequentItemsAggregateBuffer[T],
    var itemDataType: DataType,
    var maxItemsTracked: Int) {
  def getSketchWithNullCount: ApproxFrequentItemsAggregateBuffer[T] = sketchWithNullCount

  def getItemDataType: DataType = itemDataType

  def getMaxItemsTracked: Int = maxItemsTracked

  def updateMaxItemsTracked(combineSizeSpecified: Boolean, newMaxItemsTracked: Int, functionName: String): Unit = {
    if (!combineSizeSpecified) {
      if (this.maxItemsTracked == ApproxFrequentItems.VOID_MAX_ITEMS_TRACKED) {
        this.maxItemsTracked = newMaxItemsTracked
      } else {
        if (this.maxItemsTracked != newMaxItemsTracked) {
          throw QueryExecutionErrors.approxFrequentItemsSketchSizeNotMatch(
            functionName, this.maxItemsTracked, newMaxItemsTracked)
        }
      }
    }
  }

  def updateItemDataType(inputItemDataType: DataType, functionName: String): Unit = {
    if (this.itemDataType == null) {
      this.itemDataType = inputItemDataType
    } else {
      if (inputItemDataType != null && this.itemDataType != inputItemDataType) {
        throw QueryExecutionErrors.approxFrequentItemsSketchTypeNotMatch(
          functionName, this.itemDataType, inputItemDataType)
      }
    }
  }

  def updateSketchWithNullCount(otherSketchWithNullCount: ApproxFrequentItemsAggregateBuffer[T]): Unit =
    sketchWithNullCount.merge(otherSketchWithNullCount)

  def serialize(): Array[Byte] = {
    val sketchWithNullCountBytes = sketchWithNullCount.serialize(
      ApproxFrequentItems.genSketchSerDe(itemDataType).asInstanceOf[ArrayOfItemsSerDe[T]])
    val itemDataTypeDDL = ApproxFrequentItems.dataTypeToDDL(itemDataType)
    val ddlBytes: Array[Byte] = itemDataTypeDDL.getBytes(StandardCharsets.UTF_8)
    val byteArray = new Array[Byte](
      sketchWithNullCountBytes.length + Integer.BYTES + Integer.BYTES + ddlBytes.length)

    val byteBuffer = ByteBuffer.wrap(byteArray)
    byteBuffer.putInt(maxItemsTracked)
    byteBuffer.putInt(ddlBytes.length)
    byteBuffer.put(ddlBytes)
    byteBuffer.put(sketchWithNullCountBytes)
    byteArray
  }
}

object CombineInternal {
  def deserialize(buffer: Array[Byte]): CombineInternal[Any] = {
    val byteBuffer = ByteBuffer.wrap(buffer)
    val maxItemsTracked = byteBuffer.getInt
    val ddlLength = byteBuffer.getInt
    val ddlBytes = new Array[Byte](ddlLength)
    byteBuffer.get(ddlBytes)
    val itemDataTypeDDL = new String(ddlBytes, StandardCharsets.UTF_8)
    val itemDataType = ApproxFrequentItems.DDLToDataType(itemDataTypeDDL)
    val sketchBytes = new Array[Byte](buffer.length - Integer.BYTES - Integer.BYTES - ddlLength)
    byteBuffer.get(sketchBytes)
    val sketchWithNullCount = ApproxFrequentItemsAggregateBuffer.deserialize(
      sketchBytes, ApproxFrequentItems.genSketchSerDe(itemDataType))
    new CombineInternal[Any](sketchWithNullCount, itemDataType, maxItemsTracked)
  }
}

/**
 * An aggregate function that combines multiple sketches into a single sketch.
 *
 * @param state                  the expression containing the sketches to combine
 * @param maxItemsTracked        the maximum number of items to track in the sketch
 * @param mutableAggBufferOffset the offset for mutable aggregation buffer
 * @param inputAggBufferOffset   the offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(state, maxItemsTracked) - Combines multiple sketches into a single sketch.
      `maxItemsTracked` An optional positive INTEGER literal with upper limit of 1000000. If maxItemsTracked is specified, it will be set for the combined sketch. If maxItemsTracked is not specified, the input sketches must have the same maxItemsTracked value, otherwise an error will be thrown. The output sketch will use the same value from the input sketches.
  """,
  examples = """
    Examples:
      > SELECT approx_frequent_items_estimate(_FUNC_(sketch, 10000), 5) FROM (SELECT approx_frequent_items_accumulate(expr) AS sketch FROM VALUES (0), (0), (1), (1) AS tab(expr) UNION ALL SELECT approx_frequent_items_accumulate(expr) AS sketch FROM VALUES (2), (3), (4), (4) AS tab(expr));
       [{"item":0,"count":2},{"item":4,"count":2},{"item":1,"count":2},{"item":2,"count":1},{"item":3,"count":1}]
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class ApproxFrequentItemsCombine(
    state: Expression,
    maxItemsTracked: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[CombineInternal[Any]]
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {

  def this(child: Expression, maxItemsTracked: Expression) = {
    this(child, maxItemsTracked, 0, 0)
    ApproxFrequentItems.checkExpressionNotNull(maxItemsTracked, "maxItemsTracked", prettyName)
    ApproxFrequentItems.checkMaxItemsTracked(maxItemsTrackedVal, prettyName)
  }

  def this(child: Expression, maxItemsTracked: Int) = this(child, Literal(maxItemsTracked))

  def this(child: Expression) = this(child, Literal(ApproxFrequentItems.VOID_MAX_ITEMS_TRACKED), 0, 0)

  private lazy val uncheckedItemDataType: DataType =
    state.dataType.asInstanceOf[StructType](2).dataType
  private lazy val maxItemsTrackedVal: Int = maxItemsTracked.eval().asInstanceOf[Int]
  private lazy val combineSizeSpecified: Boolean =
    maxItemsTrackedVal != ApproxFrequentItems.VOID_MAX_ITEMS_TRACKED

  override def left: Expression = state

  override def right: Expression = maxItemsTracked

  override def inputTypes: Seq[AbstractDataType] = Seq(StructType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      val stateCheck = ApproxFrequentItems.checkStateFieldAndType(state)
      if (stateCheck.isFailure) {
        stateCheck
      } else if (!maxItemsTracked.foldable) {
        TypeCheckFailure("Number of items tracked must be a constant literal")
      } else {
        TypeCheckSuccess
      }
    }
  }

  override def dataType: DataType = ApproxFrequentItems.getSketchStateDataType(uncheckedItemDataType)

  override def createAggregationBuffer(): CombineInternal[Any] = {
    if (combineSizeSpecified) {
      val maxMapSize = ApproxFrequentItems.calMaxMapSize(maxItemsTrackedVal)
      new CombineInternal[Any](
        new ApproxFrequentItemsAggregateBuffer[Any](new ItemsSketch[Any](maxMapSize), 0L),
        null,
        maxItemsTrackedVal)
    } else {
      val maxMapSize = ApproxFrequentItems.calMaxMapSize(ApproxFrequentItems.MAX_ITEMS_TRACKED_LIMIT)
      new CombineInternal[Any](
        new ApproxFrequentItemsAggregateBuffer[Any](new ItemsSketch[Any](maxMapSize), 0L),
        null,
        ApproxFrequentItems.VOID_MAX_ITEMS_TRACKED)
    }
  }

  override def update(buffer: CombineInternal[Any], input: InternalRow): CombineInternal[Any] = {
    val inputState = state.eval(input).asInstanceOf[InternalRow]
    val inputSketchBytes = inputState.getBinary(0)
    val inputMaxItemsTracked = inputState.getInt(1)
    val inputItemDataTypeDDL = inputState.getUTF8String(3).toString
    val inputItemDataType = ApproxFrequentItems.DDLToDataType(inputItemDataTypeDDL)
    buffer.updateMaxItemsTracked(combineSizeSpecified, inputMaxItemsTracked, prettyName)
    buffer.updateItemDataType(inputItemDataType, prettyName)
    val inputSketchWithNullCount = ApproxFrequentItemsAggregateBuffer.deserialize(
      inputSketchBytes, ApproxFrequentItems.genSketchSerDe(inputItemDataType))
    buffer.updateSketchWithNullCount(inputSketchWithNullCount)
    buffer
  }

  override def merge(
      buffer: CombineInternal[Any],
      input: CombineInternal[Any]): CombineInternal[Any] = {
    buffer.updateMaxItemsTracked(combineSizeSpecified, input.getMaxItemsTracked, prettyName)
    buffer.updateItemDataType(input.getItemDataType, prettyName)
    buffer.getSketchWithNullCount.merge(input.getSketchWithNullCount)
    buffer
  }

  override def eval(buffer: CombineInternal[Any]): Any = {
    val sketchBytes = buffer.getSketchWithNullCount
      .serialize(ApproxFrequentItems.genSketchSerDe(buffer.getItemDataType))
    val maxItemsTracked = buffer.getMaxItemsTracked
    val itemDataTypeDDL = ApproxFrequentItems.dataTypeToDDL(buffer.getItemDataType)
    InternalRow.apply(
      sketchBytes,
      maxItemsTracked,
      null,
      UTF8String.fromString(itemDataTypeDDL))
  }

  override def serialize(buffer: CombineInternal[Any]): Array[Byte] = {
    buffer.serialize()
  }

  override def deserialize(buffer: Array[Byte]): CombineInternal[Any] = {
    CombineInternal.deserialize(buffer)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression =
    copy(state = newLeft, maxItemsTracked = newRight)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("approx_frequent_items_combine")
}
