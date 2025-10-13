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
  extends TypedImperativeAggregate[ItemsSketch[Any]]
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

  override def createAggregationBuffer(): ItemsSketch[Any] = {
    val maxMapSize = ApproxTopK.calMaxMapSize(maxItemsTrackedVal)
    ApproxTopK.createAggregationBuffer(expr, maxMapSize)
  }

  override def update(buffer: ItemsSketch[Any], input: InternalRow): ItemsSketch[Any] =
    ApproxTopK.updateSketchBuffer(expr, buffer, input)

  override def merge(buffer: ItemsSketch[Any], input: ItemsSketch[Any]): ItemsSketch[Any] =
    buffer.merge(input)

  override def eval(buffer: ItemsSketch[Any]): GenericArrayData =
    ApproxTopK.genEvalResult(buffer, kVal, itemDataType)

  override def serialize(buffer: ItemsSketch[Any]): Array[Byte] =
    buffer.toByteArray(ApproxTopK.genSketchSerDe(itemDataType))

  override def deserialize(storageFormat: Array[Byte]): ItemsSketch[Any] =
    ItemsSketch.getInstance(Memory.wrap(storageFormat), ApproxTopK.genSketchSerDe(itemDataType))

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
  private val MAX_ITEMS_TRACKED_LIMIT: Int = 1000000
  val VOID_MAX_ITEMS_TRACKED = -1
  val SKETCH_SIZE_PLACEHOLDER = 8

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
      StructField("item", itemDataType, nullable = false) ::
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

  def createAggregationBuffer(itemExpression: Expression, maxMapSize: Int): ItemsSketch[Any] = {
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

  def updateSketchBuffer(
      itemExpression: Expression,
      buffer: ItemsSketch[Any],
      input: InternalRow): ItemsSketch[Any] = {
    val v = itemExpression.eval(input)
    if (v != null) {
      itemExpression.dataType match {
        case _: BooleanType => buffer.update(v.asInstanceOf[Boolean])
        case _: ByteType => buffer.update(v.asInstanceOf[Byte])
        case _: ShortType => buffer.update(v.asInstanceOf[Short])
        case _: IntegerType => buffer.update(v.asInstanceOf[Int])
        case _: LongType => buffer.update(v.asInstanceOf[Long])
        case _: FloatType => buffer.update(v.asInstanceOf[Float])
        case _: DoubleType => buffer.update(v.asInstanceOf[Double])
        case _: DateType => buffer.update(v.asInstanceOf[Int])
        case _: TimestampType => buffer.update(v.asInstanceOf[Long])
        case _: TimestampNTZType => buffer.update(v.asInstanceOf[Long])
        case st: StringType =>
          val cKey = CollationFactory.getCollationKey(v.asInstanceOf[UTF8String], st.collationId)
          buffer.update(cKey.toString)
        case _: DecimalType => buffer.update(v.asInstanceOf[Decimal])
      }
    }
    buffer
  }

  def genEvalResult(
      itemsSketch: ItemsSketch[Any],
      k: Int,
      itemDataType: DataType): GenericArrayData = {
    val items = itemsSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES)
    val resultLength = math.min(items.length, k)
    val result = new Array[AnyRef](resultLength)
    for (i <- 0 until resultLength) {
      val row = items(i)
      itemDataType match {
        case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType |
             _: LongType | _: FloatType | _: DoubleType | _: DecimalType |
             _: DateType | _: TimestampType | _: TimestampNTZType =>
          result(i) = InternalRow.apply(row.getItem, row.getEstimate)
        case _: StringType =>
          val item = UTF8String.fromString(row.getItem.asInstanceOf[String])
          result(i) = InternalRow.apply(item, row.getEstimate)
      }
    }
    new GenericArrayData(result)
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
        StructField("itemDataType", itemDataType) ::
        StructField("maxItemsTracked", IntegerType, nullable = false) ::
        StructField("typeCode", BinaryType, nullable = false) :: Nil)

  def dataTypeToBytes(dataType: DataType): Array[Byte] = {
    dataType match {
      case _: BooleanType => Array(0, 0, 0)
      case _: ByteType => Array(1, 0, 0)
      case _: ShortType => Array(2, 0, 0)
      case _: IntegerType => Array(3, 0, 0)
      case _: LongType => Array(4, 0, 0)
      case _: FloatType => Array(5, 0, 0)
      case _: DoubleType => Array(6, 0, 0)
      case _: DateType => Array(7, 0, 0)
      case _: TimestampType => Array(8, 0, 0)
      case _: TimestampNTZType => Array(9, 0, 0)
      case _: StringType => Array(10, 0, 0)
      case dt: DecimalType => Array(11, dt.precision.toByte, dt.scale.toByte)
    }
  }

  def bytesToDataType(bytes: Array[Byte]): DataType = {
    bytes(0) match {
      case 0 => BooleanType
      case 1 => ByteType
      case 2 => ShortType
      case 3 => IntegerType
      case 4 => LongType
      case 5 => FloatType
      case 6 => DoubleType
      case 7 => DateType
      case 8 => TimestampType
      case 9 => TimestampNTZType
      case 10 => StringType
      case 11 => DecimalType(bytes(1).toInt, bytes(2).toInt)
    }
  }

  def checkStateFieldAndType(state: Expression): TypeCheckResult = {
    val stateStructType = state.dataType.asInstanceOf[StructType]
    if (stateStructType.length != 4) {
      return TypeCheckFailure("State must be a struct with 4 fields. " +
        "Expected struct: " +
        "struct<sketch:binary,itemDataType:any,maxItemsTracked:int,typeCode:binary>. " +
        "Got: " + state.dataType.simpleString)
    }

    val fieldType1 = stateStructType.head.dataType
    val fieldType2 = stateStructType(1).dataType
    val fieldType3 = stateStructType(2).dataType
    val fieldType4 = stateStructType(3).dataType
    if (fieldType1 != BinaryType) {
      TypeCheckFailure("State struct must have the first field to be binary. " +
        "Got: " + fieldType1.simpleString)
    } else if (!ApproxTopK.isDataTypeSupported(fieldType2)) {
      TypeCheckFailure("State struct must have the second field to be a supported data type. " +
        "Got: " + fieldType2.simpleString)
    } else if (fieldType3 != IntegerType) {
      TypeCheckFailure("State struct must have the third field to be int. " +
        "Got: " + fieldType3.simpleString)
    } else if (fieldType4 != BinaryType) {
      TypeCheckFailure("State struct must have the fourth field to be binary. " +
        "Got: " + fieldType4.simpleString)
    } else {
      TypeCheckSuccess
    }
  }
}

/**
 * An aggregate function that accumulates items into a sketch, which can then be used
 * to combine with other sketches, via ApproxTopKCombine,
 * or to estimate the top K items, via ApproxTopKEstimate.
 *
 * The output of this function is a struct containing the sketch in binary format,
 * a null object indicating the type of items in the sketch,
 * the maximum number of items tracked by the sketch,
 * and a binary typeCode encoding the data type of the items in the sketch.
 *
 * The null object is used in approx_top_k_estimate,
 * while the typeCode is used in approx_top_k_combine.
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
  extends TypedImperativeAggregate[ItemsSketch[Any]]
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

  override def createAggregationBuffer(): ItemsSketch[Any] = {
    val maxMapSize = ApproxTopK.calMaxMapSize(maxItemsTrackedVal)
    ApproxTopK.createAggregationBuffer(expr, maxMapSize)
  }

  override def update(buffer: ItemsSketch[Any], input: InternalRow): ItemsSketch[Any] =
    ApproxTopK.updateSketchBuffer(expr, buffer, input)

  override def merge(buffer: ItemsSketch[Any], input: ItemsSketch[Any]): ItemsSketch[Any] =
    buffer.merge(input)

  override def eval(buffer: ItemsSketch[Any]): Any = {
    val sketchBytes = serialize(buffer)
    val typeCode = ApproxTopK.dataTypeToBytes(itemDataType)
    InternalRow.apply(sketchBytes, null, maxItemsTrackedVal, typeCode)
  }

  override def serialize(buffer: ItemsSketch[Any]): Array[Byte] =
    buffer.toByteArray(ApproxTopK.genSketchSerDe(itemDataType))

  override def deserialize(storageFormat: Array[Byte]): ItemsSketch[Any] =
    ItemsSketch.getInstance(Memory.wrap(storageFormat), ApproxTopK.genSketchSerDe(itemDataType))

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

class CombineInternal[T](
    sketch: ItemsSketch[T],
    var itemDataType: DataType,
    var maxItemsTracked: Int) {
  def getSketch: ItemsSketch[T] = sketch

  def getItemDataType: DataType = itemDataType

  def setItemDataType(dataType: DataType): Unit = {
    if (this.itemDataType == null) {
      this.itemDataType = dataType
    } else if (this.itemDataType != dataType) {
      throw QueryExecutionErrors.approxTopKSketchTypeNotMatch(this.itemDataType, dataType)
    }
  }

  def getMaxItemsTracked: Int = maxItemsTracked

  def setMaxItemsTracked(maxItemsTracked: Int): Unit = this.maxItemsTracked = maxItemsTracked
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
      `maxItemsTracked` An optional positive INTEGER literal with upper limit of 1000000. If maxItemsTracked is not specified, it defaults to 10000.
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

  def this(child: Expression) = this(child, Literal(ApproxTopK.VOID_MAX_ITEMS_TRACKED), 0, 0)

  private lazy val uncheckedItemDataType: DataType =
    state.dataType.asInstanceOf[StructType](1).dataType
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

  override def createAggregationBuffer(): CombineInternal[Any] = {
    if (combineSizeSpecified) {
      val maxMapSize = ApproxTopK.calMaxMapSize(maxItemsTrackedVal)
      new CombineInternal[Any](
        new ItemsSketch[Any](maxMapSize),
        null,
        maxItemsTrackedVal)
    } else {
      new CombineInternal[Any](
        new ItemsSketch[Any](ApproxTopK.SKETCH_SIZE_PLACEHOLDER),
        null,
        ApproxTopK.VOID_MAX_ITEMS_TRACKED)
    }
  }

  override def update(buffer: CombineInternal[Any], input: InternalRow): CombineInternal[Any] = {
    val inputState = state.eval(input).asInstanceOf[InternalRow]
    val inputSketchBytes = inputState.getBinary(0)
    val inputMaxItemsTracked = inputState.getInt(2)
    val typeCode = inputState.getBinary(3)
    val actualItemDataType = ApproxTopK.bytesToDataType(typeCode)
    buffer.setItemDataType(actualItemDataType)
    val inputSketch = ItemsSketch.getInstance(
      Memory.wrap(inputSketchBytes), ApproxTopK.genSketchSerDe(buffer.getItemDataType))
    buffer.getSketch.merge(inputSketch)
    if (!combineSizeSpecified) {
      buffer.setMaxItemsTracked(inputMaxItemsTracked)
    }
    buffer
  }

  override def merge(buffer: CombineInternal[Any], input: CombineInternal[Any])
  : CombineInternal[Any] = {
    if (!combineSizeSpecified) {
      // check size
      if (buffer.getMaxItemsTracked == ApproxTopK.VOID_MAX_ITEMS_TRACKED) {
        // If buffer is a placeholder sketch, set it to the input sketch's max items tracked
        buffer.setMaxItemsTracked(input.getMaxItemsTracked)
      }
      if (buffer.getMaxItemsTracked != input.getMaxItemsTracked) {
        throw QueryExecutionErrors.approxTopKSketchSizeNotMatch(
          buffer.getMaxItemsTracked,
          input.getMaxItemsTracked
        )
      }
    }
    // check item data type
    if (buffer.getItemDataType != null && input.getItemDataType != null &&
      buffer.getItemDataType != input.getItemDataType) {
      throw QueryExecutionErrors.approxTopKSketchTypeNotMatch(
        buffer.getItemDataType,
        input.getItemDataType
      )
    } else if (buffer.getItemDataType == null) {
      // If buffer is a placeholder sketch, set it to the input sketch's item data type
      buffer.setItemDataType(input.getItemDataType)
    }
    buffer.getSketch.merge(input.getSketch)
    buffer
  }

  override def eval(buffer: CombineInternal[Any]): Any = {
    val sketchBytes =
      buffer.getSketch.toByteArray(ApproxTopK.genSketchSerDe(buffer.getItemDataType))
    val maxItemsTracked = buffer.getMaxItemsTracked
    val typeCode = ApproxTopK.dataTypeToBytes(buffer.getItemDataType)
    InternalRow.apply(sketchBytes, null, maxItemsTracked, typeCode)
  }

  override def serialize(buffer: CombineInternal[Any]): Array[Byte] = {
    val sketchBytes = buffer.getSketch.toByteArray(
      ApproxTopK.genSketchSerDe(buffer.getItemDataType))
    val maxItemsTracked = buffer.getMaxItemsTracked
    val itemDataTypeBytes = ApproxTopK.dataTypeToBytes(buffer.getItemDataType)
    // byteArray = maxItemsTracked (4 bytes) + itemDataType (3 bytes) + sketchBytes
    val byteArray = new Array[Byte](sketchBytes.length + 7)
    val byteBuffer = ByteBuffer.wrap(byteArray)
    byteBuffer.putInt(maxItemsTracked)
    byteBuffer.put(itemDataTypeBytes)
    byteBuffer.put(sketchBytes)
    byteArray
  }

  override def deserialize(buffer: Array[Byte]): CombineInternal[Any] = {
    val byteBuffer = ByteBuffer.wrap(buffer)
    val maxItemsTracked = byteBuffer.getInt()
    val itemDataTypeBytes = new Array[Byte](3)
    byteBuffer.get(itemDataTypeBytes)
    val actualItemDataType = ApproxTopK.bytesToDataType(itemDataTypeBytes)
    val sketchBytes = new Array[Byte](buffer.length - 7)
    byteBuffer.get(sketchBytes)
    val sketch = ItemsSketch.getInstance(
      Memory.wrap(sketchBytes), ApproxTopK.genSketchSerDe(actualItemDataType))
    new CombineInternal[Any](sketch, actualItemDataType, maxItemsTracked)
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
