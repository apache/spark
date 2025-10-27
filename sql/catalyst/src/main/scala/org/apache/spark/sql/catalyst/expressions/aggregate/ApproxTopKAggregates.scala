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
 * The ApproxTopK function (i.e., "approx_top_k") is an aggregate function that estimates
 * the approximate top K (aka. k-most-frequent) items in a column.
 *
 * The result is an array of structs, each containing a frequent item and its estimated frequency.
 * The items are sorted by their estimated frequency in descending order.
 *
 * The function uses the ItemsSketch from the DataSketches library to do the estimation.
 *
 * See [[https://datasketches.apache.org/docs/Frequency/FrequencySketches.html]]
 * for more information.
 *
 * @param expr                   the child expression to estimate the top K items from
 * @param k                      the number of top items to return (K)
 * @param maxItemsTracked        the maximum number of items to track in the sketch
 * @param mutableAggBufferOffset the offset for mutable aggregation buffer
 * @param inputAggBufferOffset   the offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, k, maxItemsTracked) - Returns top k items with their frequency.
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
case class ApproxTopK(
    expr: Expression,
    k: Expression,
    maxItemsTracked: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ApproxTopKAggregateBuffer[Any]]
  with ImplicitCastInputTypes
  with TernaryLike[Expression] {

  def this(child: Expression, topK: Expression, maxItemsTracked: Expression) =
    this(child, topK, maxItemsTracked, 0, 0)

  def this(child: Expression, topK: Int, maxItemsTracked: Int) =
    this(child, Literal(topK), Literal(maxItemsTracked), 0, 0)

  def this(child: Expression, topK: Expression) =
    this(child, topK, Literal(ApproxTopK.DEFAULT_MAX_ITEMS_TRACKED), 0, 0)

  def this(child: Expression, topK: Int) =
    this(child, Literal(topK), Literal(ApproxTopK.DEFAULT_MAX_ITEMS_TRACKED), 0, 0)

  def this(child: Expression) =
    this(child, Literal(ApproxTopK.DEFAULT_K), Literal(ApproxTopK.DEFAULT_MAX_ITEMS_TRACKED), 0, 0)

  private lazy val itemDataType: DataType = expr.dataType
  private lazy val kVal: Int = {
    ApproxTopK.checkExpressionNotNull(k, "k")
    val kVal = k.eval().asInstanceOf[Int]
    ApproxTopK.checkK(kVal)
    kVal
  }
  private lazy val maxItemsTrackedVal: Int = {
    ApproxTopK.checkExpressionNotNull(maxItemsTracked, "maxItemsTracked")
    val maxItemsTrackedVal = maxItemsTracked.eval().asInstanceOf[Int]
    ApproxTopK.checkMaxItemsTracked(maxItemsTrackedVal, kVal)
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
    } else if (!ApproxTopK.isDataTypeSupported(itemDataType)) {
      TypeCheckFailure(f"${itemDataType.typeName} columns are not supported")
    } else if (!k.foldable) {
      TypeCheckFailure("K must be a constant literal")
    } else if (!maxItemsTracked.foldable) {
      TypeCheckFailure("Number of items tracked must be a constant literal")
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = ApproxTopK.getResultDataType(itemDataType)

  override def createAggregationBuffer(): ApproxTopKAggregateBuffer[Any] = {
    val maxMapSize = ApproxTopK.calMaxMapSize(maxItemsTrackedVal)
    val sketch = ApproxTopK.createItemsSketch(expr, maxMapSize)
    new ApproxTopKAggregateBuffer[Any](sketch, 0L)
  }

  override def update(buffer: ApproxTopKAggregateBuffer[Any], input: InternalRow):
    ApproxTopKAggregateBuffer[Any] =
    buffer.update(expr, input)

  override def merge(
      buffer: ApproxTopKAggregateBuffer[Any],
      input: ApproxTopKAggregateBuffer[Any]):
    ApproxTopKAggregateBuffer[Any] =
    buffer.merge(input)

  override def eval(buffer: ApproxTopKAggregateBuffer[Any]): GenericArrayData =
    buffer.eval(kVal, itemDataType)

  override def serialize(buffer: ApproxTopKAggregateBuffer[Any]): Array[Byte] =
    buffer.serialize(ApproxTopK.genSketchSerDe(itemDataType))

  override def deserialize(storageFormat: Array[Byte]): ApproxTopKAggregateBuffer[Any] =
    ApproxTopKAggregateBuffer.deserialize(storageFormat, ApproxTopK.genSketchSerDe(itemDataType))

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
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("approx_top_k")
}

object ApproxTopK {

  val DEFAULT_K: Int = 5
  val DEFAULT_MAX_ITEMS_TRACKED: Int = 10000
  val MAX_ITEMS_TRACKED_LIMIT: Int = 1000000
  // A special value indicating no explicit maxItemsTracked input in function approx_top_k_combine
  val VOID_MAX_ITEMS_TRACKED = -1

  def checkExpressionNotNull(expr: Expression, exprName: String): Unit = {
    if (expr == null || expr.eval() == null) {
      throw QueryExecutionErrors.approxTopKNullArg(exprName)
    }
  }

  def checkK(k: Int): Unit = {
    if (k <= 0) {
      throw QueryExecutionErrors.approxTopKNonPositiveValue("k", k)
    }
  }

  def checkMaxItemsTracked(maxItemsTracked: Int): Unit = {
    if (maxItemsTracked > MAX_ITEMS_TRACKED_LIMIT) {
      throw QueryExecutionErrors.approxTopKMaxItemsTrackedExceedsLimit(
        maxItemsTracked, MAX_ITEMS_TRACKED_LIMIT)
    }
    if (maxItemsTracked <= 0) {
      throw QueryExecutionErrors.approxTopKNonPositiveValue("maxItemsTracked", maxItemsTracked)
    }
  }

  def checkMaxItemsTracked(maxItemsTracked: Int, k: Int): Unit = {
    checkMaxItemsTracked(maxItemsTracked)
    if (maxItemsTracked < k) {
      throw QueryExecutionErrors.approxTopKMaxItemsTrackedLessThanK(maxItemsTracked, k)
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
      // BinaryType is not supported now, as ItemsSketch seems cannot count the frequency correctly
      case _ => false
    }
  }

  def calMaxMapSize(maxItemsTracked: Int): Int = {
    // The maximum capacity of this internal hash map has maxMapCap = 0.75 * maxMapSize
    // Therefore, the maxMapSize must be at least ceil(maxItemsTracked / 0.75)
    // https://datasketches.apache.org/docs/Frequency/FrequentItemsOverview.html
    val ceilMaxMapSize = math.ceil(maxItemsTracked / 0.75).toInt
    // The maxMapSize must be a power of 2 and greater than ceilMaxMapSize
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
      // Hide collation information in DDL format, otherwise CollationExpressionWalkerSuite fails
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
    } else if (!ApproxTopK.isDataTypeSupported(fieldType3)) {
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
 * In internal class used as the aggregation buffer for ApproxTopK.
 *
 * @param sketch    the ItemsSketch instance for counting not-null items
 * @param nullCount the count of null items
 */
class ApproxTopKAggregateBuffer[T](val sketch: ItemsSketch[T], private var nullCount: Long) {
  def update(itemExpression: Expression, input: InternalRow): ApproxTopKAggregateBuffer[T] = {
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

  def merge(other: ApproxTopKAggregateBuffer[T]): ApproxTopKAggregateBuffer[T] = {
    sketch.merge(other.sketch)
    nullCount += other.nullCount
    this
  }

  /**
   * Serialize the buffer into bytes.
   * The format is:
   * [sketch bytes][null count (8 bytes Long)]
   */
  def serialize(serDe: ArrayOfItemsSerDe[T]): Array[Byte] = {
    val sketchBytes = sketch.toByteArray(serDe)
    val result = new Array[Byte](sketchBytes.length + java.lang.Long.BYTES)
    val byteBuffer = java.nio.ByteBuffer.wrap(result)
    byteBuffer.put(sketchBytes)
    byteBuffer.putLong(nullCount)
    result
  }

  /**
   * Evaluate the buffer and return top K items (including null) with their estimated frequency.
   * The result is sorted by frequency in descending order.
   */
  def eval(k: Int, itemDataType: DataType): GenericArrayData = {
    // frequent items from sketch
    val frequentItems = sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES)
    // total number of frequent items (including null, if any)
    val itemsLength = frequentItems.length + (if (nullCount > 0) 1 else 0)
    // actual number of items to return
    val resultLength = math.min(itemsLength, k)
    val result = new Array[AnyRef](resultLength)

    // variable pointers for merging frequent items and nullCount into result
    var fiIndex = 0 // pointer for frequentItems
    var resultIndex = 0 // pointer for result
    var isNullAdded = false // whether nullCount has been added to result

    // helper function to get nullCount estimate: if nullCount has been added, return Long.MinValue
    // so that it won't be added again; otherwise return nullCount
    @inline def getNullEstimate: Long = if (!isNullAdded) nullCount else Long.MinValue

    // looping until result is full or run out of frequent items
    while (resultIndex < resultLength && fiIndex < frequentItems.length) {
      val curFrequentItem = frequentItems(fiIndex)
      val itemEstimate = curFrequentItem.getEstimate
      val nullEstimate = getNullEstimate

      val (item, estimate) = if (nullEstimate > itemEstimate) {
        // insert (null, nullCount) into result
        isNullAdded = true
        (null, nullCount.toLong)
      } else {
        // insert frequent item into result
        val item: Any = itemDataType match {
          case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType |
               _: LongType | _: FloatType | _: DoubleType | _: DecimalType |
               _: DateType | _: TimestampType | _: TimestampNTZType =>
            curFrequentItem.getItem
          case _: StringType =>
            UTF8String.fromString(curFrequentItem.getItem.asInstanceOf[String])
        }
        fiIndex += 1 // move to next frequent item
        (item, itemEstimate)
      }
      result(resultIndex) = InternalRow(item, estimate)
      resultIndex += 1 // move to next result position
    }

    // in case there is still space in result and nullCount > 0 has not been added
    if (resultIndex < resultLength && nullCount > 0 && !isNullAdded) {
      result(resultIndex) = InternalRow(null, nullCount.toLong)
    }

    new GenericArrayData(result)
  }
}

object ApproxTopKAggregateBuffer {
  /**
   * Deserialize the buffer from bytes.
   * The format is:
   * [sketch bytes][null count (8 bytes)]
   */
  def deserialize(bytes: Array[Byte], serDe: ArrayOfItemsSerDe[Any]):
  ApproxTopKAggregateBuffer[Any] = {
    val byteBuffer = java.nio.ByteBuffer.wrap(bytes)
    val sketchBytesLength = bytes.length - 8
    val sketchBytes = new Array[Byte](sketchBytesLength)
    byteBuffer.get(sketchBytes, 0, sketchBytesLength)
    val nullCount = byteBuffer.getLong(sketchBytesLength)
    val deserializedSketch = ItemsSketch.getInstance(Memory.wrap(sketchBytes), serDe)
    new ApproxTopKAggregateBuffer[Any](deserializedSketch, nullCount)
  }
}

/**
 * An aggregate function that accumulates items into a sketch, which can then be used
 * to combine with other sketches, via ApproxTopKCombine,
 * or to estimate the top K items, via ApproxTopKEstimate.
 *
 * The output of this function is a struct containing the sketch in binary format,
 * the maximum number of items tracked by the sketch,
 * a null object indicating the type of items in the sketch,
 * and a DDL string representing the data type of items in the sketch.
 * The null object is used in approx_top_k_estimate,
 * while the DDL is used in approx_top_k_combine.
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
      > SELECT approx_top_k_estimate(_FUNC_(expr)) FROM VALUES (0), (0), (1), (1), (2), (3), (4), (4) AS tab(expr);
       [{"item":0,"count":2},{"item":4,"count":2},{"item":1,"count":2},{"item":2,"count":1},{"item":3,"count":1}]

      > SELECT approx_top_k_estimate(_FUNC_(expr, 100), 2) FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);
       [{"item":"c","count":4},{"item":"d","count":2}]
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class ApproxTopKAccumulate(
    expr: Expression,
    maxItemsTracked: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ApproxTopKAggregateBuffer[Any]]
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {

  def this(child: Expression, maxItemsTracked: Expression) = this(child, maxItemsTracked, 0, 0)

  def this(child: Expression, maxItemsTracked: Int) = this(child, Literal(maxItemsTracked), 0, 0)

  def this(child: Expression) = this(child, Literal(ApproxTopK.DEFAULT_MAX_ITEMS_TRACKED), 0, 0)

  private lazy val itemDataType: DataType = expr.dataType

  private lazy val maxItemsTrackedVal: Int = {
    ApproxTopK.checkExpressionNotNull(maxItemsTracked, "maxItemsTracked")
    val maxItemsTrackedVal = maxItemsTracked.eval().asInstanceOf[Int]
    ApproxTopK.checkMaxItemsTracked(maxItemsTrackedVal)
    maxItemsTrackedVal
  }

  override def left: Expression = expr

  override def right: Expression = maxItemsTracked

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!ApproxTopK.isDataTypeSupported(itemDataType)) {
      TypeCheckFailure(f"${itemDataType.typeName} columns are not supported")
    } else if (!maxItemsTracked.foldable) {
      TypeCheckFailure("Number of items tracked must be a constant literal")
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = ApproxTopK.getSketchStateDataType(itemDataType)

  override def createAggregationBuffer(): ApproxTopKAggregateBuffer[Any] = {
    val maxMapSize = ApproxTopK.calMaxMapSize(maxItemsTrackedVal)
    val sketch = ApproxTopK.createItemsSketch(expr, maxMapSize)
    new ApproxTopKAggregateBuffer[Any](sketch, 0L)
  }

  override def update(buffer: ApproxTopKAggregateBuffer[Any], input: InternalRow):
    ApproxTopKAggregateBuffer[Any] =
    buffer.update(expr, input)

  override def merge(
      buffer: ApproxTopKAggregateBuffer[Any],
      input: ApproxTopKAggregateBuffer[Any]):
    ApproxTopKAggregateBuffer[Any] =
    buffer.merge(input)

  override def eval(buffer: ApproxTopKAggregateBuffer[Any]): Any = {
    val sketchBytes = serialize(buffer)
    val itemDataTypeDDL = ApproxTopK.dataTypeToDDL(itemDataType)
    InternalRow.apply(
      sketchBytes,
      maxItemsTrackedVal,
      null,
      UTF8String.fromString(itemDataTypeDDL))
  }

  override def serialize(buffer: ApproxTopKAggregateBuffer[Any]): Array[Byte] =
    buffer.serialize(ApproxTopK.genSketchSerDe(itemDataType))

  override def deserialize(storageFormat: Array[Byte]): ApproxTopKAggregateBuffer[Any] =
    ApproxTopKAggregateBuffer.deserialize(storageFormat, ApproxTopK.genSketchSerDe(itemDataType))

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
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("approx_top_k_accumulate")
}

/**
 * In internal class used as the aggregation buffer for ApproxTopKCombine.
 *
 * @param sketch          the ItemsSketch instance
 * @param itemDataType    the data type of items in the sketch
 * @param maxItemsTracked the maximum number of items tracked in the sketch
 */
class CombineInternal[T](
    sketchWithNullCount: ApproxTopKAggregateBuffer[T],
    var itemDataType: DataType,
    var maxItemsTracked: Int) {
  def getSketchWithNullCount: ApproxTopKAggregateBuffer[T] = sketchWithNullCount

  def getItemDataType: DataType = itemDataType

  def getMaxItemsTracked: Int = maxItemsTracked

  def updateMaxItemsTracked(combineSizeSpecified: Boolean, newMaxItemsTracked: Int): Unit = {
    if (!combineSizeSpecified) {
      // check size
      if (this.maxItemsTracked == ApproxTopK.VOID_MAX_ITEMS_TRACKED) {
        // If buffer's maxItemsTracked VOID_MAX_ITEMS_TRACKED, it means the buffer is a placeholder
        // sketch that has not beed updated by any input sketch yet.
        // So we can set it to the input sketch's max items tracked.
        this.maxItemsTracked = newMaxItemsTracked
      } else {
        if (this.maxItemsTracked != newMaxItemsTracked) {
          // If buffer's maxItemsTracked is not VOID_MAX_ITEMS_TRACKED, it means the buffer has been
          // updated by some input sketch. So if buffer and input sketch have different
          // maxItemsTracked values, it means at least two of the input sketches have different
          // maxItemsTracked values. In this case, we should throw an error.
          throw QueryExecutionErrors.approxTopKSketchSizeNotMatch(
            this.maxItemsTracked, newMaxItemsTracked)
        }
      }
    }
  }

  def updateItemDataType(inputItemDataType: DataType): Unit = {
    // When the buffer's dataType hasn't been set, set it to the input sketch's item data type
    // When input sketch's item data type is null, buffer's item data type will remain null
    if (this.itemDataType == null) {
      this.itemDataType = inputItemDataType
    } else {
      // When the buffer's dataType has been set, throw an error
      // if the input sketch's item data type is not null the two data types don't match
      if (inputItemDataType != null && this.itemDataType != inputItemDataType) {
        throw QueryExecutionErrors.approxTopKSketchTypeNotMatch(
          this.itemDataType, inputItemDataType)
      }
    }
  }

  def updateSketchWithNullCount(otherSketchWithNullCount: ApproxTopKAggregateBuffer[T]): Unit =
    sketchWithNullCount.merge(otherSketchWithNullCount)

  /**
   * Serialize the CombineInternal instance to a byte array.
   * Serialization format:
   *     maxItemsTracked (4 bytes int) +
   *     itemDataTypeDDL length n in byte  (4 bytes int) +
   *     itemDataTypeDDL (n bytes) +
   *     sketchBytes
   */
  def serialize(): Array[Byte] = {
    val sketchWithNullCountBytes = sketchWithNullCount.serialize(
      ApproxTopK.genSketchSerDe(itemDataType).asInstanceOf[ArrayOfItemsSerDe[T]])
    val itemDataTypeDDL = ApproxTopK.dataTypeToDDL(itemDataType)
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
  /**
   * Deserialize a byte array to a CombineInternal instance.
   * Serialization format:
   *     maxItemsTracked (4 bytes int) +
   *     itemDataTypeDDL length n in byte  (4 bytes int) +
   *     itemDataTypeDDL (n bytes) +
   *     sketchBytes
   */
  def deserialize(buffer: Array[Byte]): CombineInternal[Any] = {
    val byteBuffer = ByteBuffer.wrap(buffer)
    // read maxItemsTracked
    val maxItemsTracked = byteBuffer.getInt
    // read itemDataTypeDDL
    val ddlLength = byteBuffer.getInt
    val ddlBytes = new Array[Byte](ddlLength)
    byteBuffer.get(ddlBytes)
    val itemDataTypeDDL = new String(ddlBytes, StandardCharsets.UTF_8)
    val itemDataType = ApproxTopK.DDLToDataType(itemDataTypeDDL)
    // read sketchBytes
    val sketchBytes = new Array[Byte](buffer.length - Integer.BYTES - Integer.BYTES - ddlLength)
    byteBuffer.get(sketchBytes)
    val sketchWithNullCount = ApproxTopKAggregateBuffer.deserialize(
      sketchBytes, ApproxTopK.genSketchSerDe(itemDataType))
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
      > SELECT approx_top_k_estimate(_FUNC_(sketch, 10000), 5) FROM (SELECT approx_top_k_accumulate(expr) AS sketch FROM VALUES (0), (0), (1), (1) AS tab(expr) UNION ALL SELECT approx_top_k_accumulate(expr) AS sketch FROM VALUES (2), (3), (4), (4) AS tab(expr));
       [{"item":0,"count":2},{"item":4,"count":2},{"item":1,"count":2},{"item":2,"count":1},{"item":3,"count":1}]
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class ApproxTopKCombine(
    state: Expression,
    maxItemsTracked: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[CombineInternal[Any]]
  with ImplicitCastInputTypes
  with BinaryLike[Expression] {

  def this(child: Expression, maxItemsTracked: Expression) = {
    this(child, maxItemsTracked, 0, 0)
    ApproxTopK.checkExpressionNotNull(maxItemsTracked, "maxItemsTracked")
    ApproxTopK.checkMaxItemsTracked(maxItemsTrackedVal)
  }

  def this(child: Expression, maxItemsTracked: Int) = this(child, Literal(maxItemsTracked))

  // If maxItemsTracked is not specified, set it to VOID_MAX_ITEMS_TRACKED.
  // This indicates that there is no explicit maxItemsTracked input from the function call.
  // Hence, function needs to check the input sketches' maxItemsTracked values during merge.
  def this(child: Expression) = this(child, Literal(ApproxTopK.VOID_MAX_ITEMS_TRACKED), 0, 0)

  // The item data type extracted from the third field of the state struct.
  // It is named "unchecked" because it may be inaccurate when input sketches have different
  // item data types. For example, if one sketch has int type null and another has string type
  // null, the union of the two sketches will have bigint type null.
  // The accurate item data type will be tracked in the aggregation buffer during update/merge.
  // It is okay to use uncheckedItemDataType to create the output data type of this function,
  // because if the input sketches have different item data types, an error will be thrown
  // during update/merge. Otherwise, the uncheckedItemDataType is accurate.
  private lazy val uncheckedItemDataType: DataType =
    state.dataType.asInstanceOf[StructType](2).dataType
  private lazy val maxItemsTrackedVal: Int = maxItemsTracked.eval().asInstanceOf[Int]
  private lazy val combineSizeSpecified: Boolean =
    maxItemsTrackedVal != ApproxTopK.VOID_MAX_ITEMS_TRACKED

  override def left: Expression = state

  override def right: Expression = maxItemsTracked

  override def inputTypes: Seq[AbstractDataType] = Seq(StructType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      val stateCheck = ApproxTopK.checkStateFieldAndType(state)
      if (stateCheck.isFailure) {
        stateCheck
      } else if (!maxItemsTracked.foldable) {
        TypeCheckFailure("Number of items tracked must be a constant literal")
      } else {
        TypeCheckSuccess
      }
    }
  }

  override def dataType: DataType = ApproxTopK.getSketchStateDataType(uncheckedItemDataType)

  /**
   * If maxItemsTracked is specified in function call, use it for the output sketch.
   * Otherwise, create a placeholder sketch with VOID_MAX_ITEMS_TRACKED. The actual value will be
   * decided during the first update.
   */
  override def createAggregationBuffer(): CombineInternal[Any] = {
    if (combineSizeSpecified) {
      val maxMapSize = ApproxTopK.calMaxMapSize(maxItemsTrackedVal)
      new CombineInternal[Any](
        new ApproxTopKAggregateBuffer[Any](new ItemsSketch[Any](maxMapSize), 0L),
        null,
        maxItemsTrackedVal)
    } else {
      // If maxItemsTracked is not specified, create a sketch with the maximum allowed size.
      // No need to worry about memory waste, as the sketch always grows from a small init size.
      // The actual maxItemsTracked will be checked during the updates.
      val maxMapSize = ApproxTopK.calMaxMapSize(ApproxTopK.MAX_ITEMS_TRACKED_LIMIT)
      new CombineInternal[Any](
        new ApproxTopKAggregateBuffer[Any](new ItemsSketch[Any](maxMapSize), 0L),
        null,
        ApproxTopK.VOID_MAX_ITEMS_TRACKED)
    }
  }

  /**
   * Update the aggregation buffer with an input sketch. The input has the same schema as the
   * ApproxTopKAccumulate output, i.e., sketchBytes + maxItemsTracked + null + DDL.
   */
  override def update(buffer: CombineInternal[Any], input: InternalRow): CombineInternal[Any] = {
    val inputState = state.eval(input).asInstanceOf[InternalRow]
    val inputSketchBytes = inputState.getBinary(0)
    val inputMaxItemsTracked = inputState.getInt(1)
    val inputItemDataTypeDDL = inputState.getUTF8String(3).toString
    val inputItemDataType = ApproxTopK.DDLToDataType(inputItemDataTypeDDL)
    // update maxItemsTracked (throw error if not match)
    buffer.updateMaxItemsTracked(combineSizeSpecified, inputMaxItemsTracked)
    // update itemDataType (throw error if not match)
    buffer.updateItemDataType(inputItemDataType)
    // update sketch
    val inputSketchWithNullCount = ApproxTopKAggregateBuffer.deserialize(
      inputSketchBytes, ApproxTopK.genSketchSerDe(inputItemDataType))
    buffer.updateSketchWithNullCount(inputSketchWithNullCount)
    buffer
  }

  override def merge(
      buffer: CombineInternal[Any],
      input: CombineInternal[Any]): CombineInternal[Any] = {
    // update maxItemsTracked (throw error if not match)
    buffer.updateMaxItemsTracked(combineSizeSpecified, input.getMaxItemsTracked)
    // update itemDataType (throw error if not match)
    buffer.updateItemDataType(input.getItemDataType)
    // update sketchWithNullCount
    buffer.getSketchWithNullCount.merge(input.getSketchWithNullCount)
    buffer
  }

  override def eval(buffer: CombineInternal[Any]): Any = {
    val sketchBytes = buffer.getSketchWithNullCount
      .serialize(ApproxTopK.genSketchSerDe(buffer.getItemDataType))
    val maxItemsTracked = buffer.getMaxItemsTracked
    val itemDataTypeDDL = ApproxTopK.dataTypeToDDL(buffer.getItemDataType)
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
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("approx_top_k_combine")
}
