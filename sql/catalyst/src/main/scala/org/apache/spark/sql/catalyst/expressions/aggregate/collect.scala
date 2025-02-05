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
import scala.collection.mutable.{ArrayBuffer, Growable}
import scala.util.{Left, Right}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, TypeUtils, UnsafeRowUtils}
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLExpr
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.errors.DataTypeErrors.{toSQLId, toSQLType}
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}
import org.apache.spark.util.BoundedPriorityQueue

/**
 * A base class for collect_list and collect_set aggregate functions.
 *
 * We have to store all the collected elements in memory, and so notice that too many elements
 * can cause GC paused and eventually OutOfMemory Errors.
 */
abstract class Collect[T <: Growable[Any] with Iterable[Any]] extends TypedImperativeAggregate[T] {

  val child: Expression

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(child.dataType, false)

  override def defaultResult: Option[Literal] = Option(Literal.create(Array(), dataType))

  protected def convertToBufferElement(value: Any): Any

  override def update(buffer: T, input: InternalRow): T = {
    val value = child.eval(input)

    // Do not allow null values. We follow the semantics of Hive's collect_list/collect_set here.
    // See: org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMkCollectionEvaluator
    if (value != null) {
      buffer += convertToBufferElement(value)
    }
    buffer
  }

  override def merge(buffer: T, other: T): T = {
    buffer ++= other
  }

  protected val bufferElementType: DataType

  private lazy val projection = UnsafeProjection.create(
    Array[DataType](ArrayType(elementType = bufferElementType, containsNull = false)))
  private lazy val row = new UnsafeRow(1)

  override def serialize(obj: T): Array[Byte] = {
    val array = new GenericArrayData(obj.toArray)
    projection.apply(InternalRow.apply(array)).getBytes()
  }

  override def deserialize(bytes: Array[Byte]): T = {
    val buffer = createAggregationBuffer()
    row.pointTo(bytes, bytes.length)
    row.getArray(0).foreach(bufferElementType, (_, x: Any) => buffer += x)
    buffer
  }
}

/**
 * Collect a list of elements.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a list of non-unique elements.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (1) AS tab(col);
       [1,2,1]
  """,
  note = """
    The function is non-deterministic because the order of collected results depends
    on the order of the rows which may be non-deterministic after a shuffle.
  """,
  group = "agg_funcs",
  since = "2.0.0")
case class CollectList(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends Collect[mutable.ArrayBuffer[Any]]
  with UnaryLike[Expression] {

  def this(child: Expression) = this(child, 0, 0)

  override lazy val bufferElementType = child.dataType

  override def convertToBufferElement(value: Any): Any = InternalRow.copyValue(value)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def createAggregationBuffer(): mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty

  override def prettyName: String = "collect_list"

  override def eval(buffer: mutable.ArrayBuffer[Any]): Any = {
    new GenericArrayData(buffer.toArray)
  }

  override protected def withNewChildInternal(newChild: Expression): CollectList =
    copy(child = newChild)
}

/**
 * Collect a set of unique elements.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a set of unique elements.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (1) AS tab(col);
       [1,2]
  """,
  note = """
    The function is non-deterministic because the order of collected results depends
    on the order of the rows which may be non-deterministic after a shuffle.
  """,
  group = "agg_funcs",
  since = "2.0.0")
// TODO: Make CollectSet collation aware
case class CollectSet(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends Collect[mutable.HashSet[Any]] with QueryErrorsBase with UnaryLike[Expression] {

  def this(child: Expression) = this(child, 0, 0)

  override lazy val bufferElementType = child.dataType match {
    case BinaryType => ArrayType(ByteType)
    case other => other
  }

  override def convertToBufferElement(value: Any): Any = child.dataType match {
    /*
     * collect_set() of BinaryType should not return duplicate elements,
     * Java byte arrays use referential equality and identity hash codes
     * so we need to use a different catalyst value for arrays
     */
    case BinaryType => UnsafeArrayData.fromPrimitiveArray(value.asInstanceOf[Array[Byte]])
    case _ => InternalRow.copyValue(value)
  }

  override def eval(buffer: mutable.HashSet[Any]): Any = {
    val array = child.dataType match {
      case BinaryType =>
        buffer.iterator.map(_.asInstanceOf[ArrayData].toByteArray()).toArray[Any]
      case _ => buffer.toArray
    }
    new GenericArrayData(array)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!child.dataType.existsRecursively(_.isInstanceOf[MapType]) &&
        UnsafeRowUtils.isBinaryStable(child.dataType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      DataTypeMismatch(
        errorSubClass = "UNSUPPORTED_INPUT_TYPE",
        messageParameters = Map(
          "functionName" -> toSQLId(prettyName),
          "dataType" -> (s"${toSQLType(MapType)} " + "or \"COLLATED STRING\"")
        )
      )
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "collect_set"

  override def createAggregationBuffer(): mutable.HashSet[Any] = mutable.HashSet.empty

  override protected def withNewChildInternal(newChild: Expression): CollectSet =
    copy(child = newChild)
}

/**
 * Collect the top-k elements. This expression is dedicated only for Spark-ML.
 * @param reverse when true, returns the smallest k elements.
 */
case class CollectTopK(
    child: Expression,
    num: Int,
    reverse: Boolean = false,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends Collect[BoundedPriorityQueue[Any]]
  with UnaryLike[Expression] {
  assert(num > 0)

  def this(child: Expression, num: Int) = this(child, num, false, 0, 0)
  def this(child: Expression, num: Int, reverse: Boolean) = this(child, num, reverse, 0, 0)

  def this(child: Expression, num: Expression, reverse: Expression) =
    this(child, CollectTopK.expressionToNum(num), CollectTopK.expressionToReverse(reverse))

  override protected lazy val bufferElementType: DataType = child.dataType
  override protected def convertToBufferElement(value: Any): Any = InternalRow.copyValue(value)

  private def ordering: Ordering[Any] = if (reverse) {
    TypeUtils.getInterpretedOrdering(child.dataType).reverse
  } else {
    TypeUtils.getInterpretedOrdering(child.dataType)
  }

  override def createAggregationBuffer(): BoundedPriorityQueue[Any] =
    new BoundedPriorityQueue[Any](num)(ordering)

  override def eval(buffer: BoundedPriorityQueue[Any]): Any =
    new GenericArrayData(buffer.toArray.sorted(ordering.reverse))

  override def prettyName: String = "collect_top_k"

  override protected def withNewChildInternal(newChild: Expression): CollectTopK =
    copy(child = newChild)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): CollectTopK =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): CollectTopK =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}

private[aggregate] object CollectTopK {
  def expressionToReverse(e: Expression): Boolean = e.eval() match {
    case b: Boolean => b
    case _ => throw QueryCompilationErrors.invalidReverseParameter(e)
  }

  def expressionToNum(e: Expression): Int = e.eval() match {
    case l: Long => l.toInt
    case i: Int => i
    case s: Short => s.toInt
    case b: Byte => b.toInt
    case _ => throw QueryCompilationErrors.invalidNumParameter(e)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, delimiter])[ WITHIN GROUP (ORDER BY key [ASC | DESC] [,...])] - Returns
    the concatenation of non-null input values, separated by the delimiter ordered by key.
    If all values are null, null is returned.
    """,
  arguments = """
    Arguments:
      * expr - a string or binary expression to be concatenated.
      * delimiter - an optional string or binary foldable expression used to separate the input values.
        If null, the concatenation will be performed without a delimiter. Default is null.
      * key - an optional expression for ordering the input values. Multiple keys can be specified.
        If none are specified, the order of the rows in the result is non-deterministic.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES ('a'), ('b'), ('c') AS tab(col);
       abc
      > SELECT _FUNC_(col) WITHIN GROUP (ORDER BY col DESC) FROM VALUES ('a'), ('b'), ('c') AS tab(col);
       cba
      > SELECT _FUNC_(col) FROM VALUES ('a'), (NULL), ('b') AS tab(col);
       ab
      > SELECT _FUNC_(col) FROM VALUES ('a'), ('a') AS tab(col);
       aa
      > SELECT _FUNC_(DISTINCT col) FROM VALUES ('a'), ('a'), ('b') AS tab(col);
       ab
      > SELECT _FUNC_(col, ', ') FROM VALUES ('a'), ('b'), ('c') AS tab(col);
       a, b, c
      > SELECT _FUNC_(col) FROM VALUES (NULL), (NULL) AS tab(col);
       NULL
  """,
  note = """
    * If the order is not specified, the function is non-deterministic because
    the order of the rows may be non-deterministic after a shuffle.
    * If DISTINCT is specified, then expr and key must be the same expression.
  """,
  group = "agg_funcs",
  since = "4.0.0"
)
// scalastyle:on line.size.limit
case class ListAgg(
    child: Expression,
    delimiter: Expression = Literal(null),
    orderExpressions: Seq[SortOrder] = Nil,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends Collect[mutable.ArrayBuffer[Any]]
  with SupportsOrderingWithinGroup
  with ImplicitCastInputTypes {

  override def orderingFilled: Boolean = orderExpressions.nonEmpty

  override def isOrderingMandatory: Boolean = false

  override def isDistinctSupported: Boolean = true

  override def withOrderingWithinGroup(orderingWithinGroup: Seq[SortOrder]): AggregateFunction =
    copy(orderExpressions = orderingWithinGroup)

  override protected lazy val bufferElementType: DataType = {
    if (!needSaveOrderValue) {
      child.dataType
    } else {
      StructType(
        StructField("value", child.dataType)
        +: orderValuesField
      )
    }
  }
  /** Indicates that the result of [[child]] is not enough for evaluation  */
  lazy val needSaveOrderValue: Boolean = !isOrderCompatible(orderExpressions)

  def this(child: Expression) =
    this(child, Literal(null), Nil, 0, 0)

  def this(child: Expression, delimiter: Expression) =
    this(child, delimiter, Nil, 0, 0)

  override def nullable: Boolean = true

  override def createAggregationBuffer(): mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def defaultResult: Option[Literal] = Option(Literal.create(null, dataType))

  override def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    val withinGroup = if (orderingFilled) {
      s" WITHIN GROUP (ORDER BY ${orderExpressions.map(_.sql).mkString(", ")})"
    } else {
      ""
    }
    s"$prettyName($distinct${child.sql}, ${delimiter.sql})$withinGroup"
  }

  override def inputTypes: Seq[AbstractDataType] =
    TypeCollection(
      StringTypeWithCollation(supportsTrimCollation = true),
      BinaryType
    ) +:
    TypeCollection(
      StringTypeWithCollation(supportsTrimCollation = true),
      BinaryType,
      NullType
    ) +:
    orderExpressions.map(_ => AnyDataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val matchInputTypes = super.checkInputDataTypes()
    if (matchInputTypes.isFailure) {
      matchInputTypes
    } else if (!delimiter.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("delimiter"),
          "inputType" -> toSQLType(delimiter.dataType),
          "inputExpr" -> toSQLExpr(delimiter)
        )
      )
    } else if (delimiter.dataType == NullType) {
      // null is the default empty delimiter so type is not important
      TypeCheckSuccess
    } else {
      TypeUtils.checkForSameTypeInputExpr(child.dataType :: delimiter.dataType :: Nil, prettyName)
    }
  }

  override def eval(buffer: mutable.ArrayBuffer[Any]): Any = {
    if (buffer.nonEmpty) {
      val sortedBufferWithoutNulls = sortBuffer(buffer)
      concatSkippingNulls(sortedBufferWithoutNulls)
    } else {
      null
    }
  }

  /**
   * Sort buffer according orderExpressions.
   * If orderExpressions is empty then returns buffer as is.
   * The format of buffer is determined by [[needSaveOrderValue]]
   * @return sorted buffer containing only child's values
   */
  private[this] def sortBuffer(buffer: mutable.ArrayBuffer[Any]): mutable.ArrayBuffer[Any] = {
    if (!orderingFilled) {
      // without order return as is.
      return buffer
    }
    if (!needSaveOrderValue) {
      // Here the buffer has structure [childValue0, childValue1, ...]
      // and we want to sort it by childValues
      val sortOrderExpression = orderExpressions.head
      val ascendingOrdering = PhysicalDataType.ordering(sortOrderExpression.dataType)
      val ordering =
        if (sortOrderExpression.direction == Ascending) ascendingOrdering
        else ascendingOrdering.reverse
      buffer.sorted(ordering)
    } else {
      // Here the buffer has structure
      // [[childValue, orderValue0, orderValue1, ...],
      //  [childValue, orderValue0, orderValue1, ...],
      //  ...]
      // and we want to sort it by tuples (orderValue0, orderValue1, ...)
      buffer
        .asInstanceOf[mutable.ArrayBuffer[InternalRow]]
        .sorted(bufferOrdering)
        // drop orderValues after sort
        .map(_.get(0, child.dataType))
    }
  }

  /**
   * @return ordering by (orderValue0, orderValue1, ...)
   *         for InternalRow with format [childValue, orderValue0, orderValue1, ...]
   */
  private[this] def bufferOrdering: Ordering[InternalRow] = {
    val bufferSortOrder = orderExpressions.zipWithIndex.map {
      case (originalOrder, i) =>
        originalOrder.copy(
          // first value is the evaluated child so add +1 for order's values
          child = BoundReference(i + 1, originalOrder.dataType, originalOrder.child.nullable)
        )
    }
    new InterpretedOrdering(bufferSortOrder)
  }

  private[this] def concatSkippingNulls(buffer: mutable.ArrayBuffer[Any]): Any = {
    getDelimiterValue match {
      case Right(delimiterValue: Array[Byte]) =>
        val inputs = buffer.filter(_ != null).map(_.asInstanceOf[Array[Byte]])
        ByteArray.concatWS(delimiterValue, inputs.toSeq: _*)
      case Left(delimiterValue: UTF8String) =>
        val inputs = buffer.filter(_ != null).map(_.asInstanceOf[UTF8String])
        UTF8String.concatWs(delimiterValue, inputs.toSeq: _*)
    }
  }

  /**
   * @return delimiter value or default empty value if delimiter is null. Type respects [[dataType]]
   */
  private[this] def getDelimiterValue: Either[UTF8String, Array[Byte]] = {
    val delimiterValue = delimiter.eval()
    dataType match {
      case _: StringType =>
        Left(
          if (delimiterValue == null) UTF8String.fromString("")
          else delimiterValue.asInstanceOf[UTF8String]
        )
      case _: BinaryType =>
        Right(
          if (delimiterValue == null) ByteArray.EMPTY_BYTE
          else delimiterValue.asInstanceOf[Array[Byte]]
        )
    }
  }

  override def dataType: DataType = child.dataType

  override def update(buffer: ArrayBuffer[Any], input: InternalRow): ArrayBuffer[Any] = {
    val value = child.eval(input)
    if (value != null) {
      val v = if (!needSaveOrderValue) {
        convertToBufferElement(value)
      } else {
        InternalRow.fromSeq(convertToBufferElement(value) +: evalOrderValues(input))
      }
      buffer += v
    }
    buffer
  }

  private[this] def evalOrderValues(internalRow: InternalRow): Seq[Any] = {
    orderExpressions.map(order => convertToBufferElement(order.child.eval(internalRow)))
  }

  override protected def convertToBufferElement(value: Any): Any = InternalRow.copyValue(value)

  override def children: Seq[Expression] = child +: delimiter +: orderExpressions

  /**
   * Utility func to check if given order is defined and different from [[child]].
   *
   * @see [[QueryCompilationErrors.functionAndOrderExpressionMismatchError]]
   * @see [[needSaveOrderValue]]
   */
  private[this] def isOrderCompatible(someOrder: Seq[SortOrder]): Boolean = {
    if (someOrder.isEmpty) {
      return true
    }
    if (someOrder.size == 1 && someOrder.head.child.semanticEquals(child)) {
      return true
    }
    false
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(
      child = newChildren.head,
      delimiter = newChildren(1),
      orderExpressions = newChildren
        .drop(2)
        .map(_.asInstanceOf[SortOrder])
    )

  private[this] def orderValuesField: Seq[StructField] = {
    orderExpressions.zipWithIndex.map {
      case (order, i) => StructField(s"sortOrderValue[$i]", order.dataType)
    }
  }
}
