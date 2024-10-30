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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{catalyst, Encoder, Row}
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedDeserializer}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.plans.{InnerLike, LeftAnti, LeftSemi, ReferenceAllColumns}
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, StatefulProcessor, TimeMode}
import org.apache.spark.sql.types._

object CatalystSerde {
  def deserialize[T : Encoder](child: LogicalPlan): DeserializeToObject = {
    val deserializer = UnresolvedDeserializer(encoderFor[T].deserializer)
    DeserializeToObject(deserializer, generateObjAttr[T], child)
  }

  def serialize[T : Encoder](child: LogicalPlan): SerializeFromObject = {
    SerializeFromObject(encoderFor[T].namedExpressions, child)
  }

  def generateObjAttr[T : Encoder]: Attribute = {
    val enc = encoderFor[T]
    val dataType = enc.deserializer.dataType
    val nullable = !enc.clsTag.runtimeClass.isPrimitive
    AttributeReference("obj", dataType, nullable)()
  }
}

/**
 * A trait for logical operators that produces domain objects as output.
 * The output of this operator is a single-field safe row containing the produced object.
 */
trait ObjectProducer extends LogicalPlan {
  // The attribute that reference to the single object field this operator outputs.
  def outputObjAttr: Attribute

  override def output: Seq[Attribute] = outputObjAttr :: Nil

  override def producedAttributes: AttributeSet = AttributeSet(outputObjAttr)
}

/**
 * A trait for logical operators that consumes domain objects as input.
 * The output of its child must be a single-field row containing the input object.
 */
trait ObjectConsumer extends UnaryNode with ReferenceAllColumns[LogicalPlan] {
  assert(child.output.length == 1)
  def inputObjAttr: Attribute = child.output.head
}

/**
 * Takes the input row from child and turns it into object using the given deserializer expression.
 */
case class DeserializeToObject(
    deserializer: Expression,
    outputObjAttr: Attribute,
    child: LogicalPlan) extends UnaryNode with ObjectProducer {
  final override val nodePatterns: Seq[TreePattern] = Seq(DESERIALIZE_TO_OBJECT)
  override protected def withNewChildInternal(newChild: LogicalPlan): DeserializeToObject =
    copy(child = newChild)
}

/**
 * Takes the input object from child and turns it into unsafe row using the given serializer
 * expression.
 */
case class SerializeFromObject(
    serializer: Seq[NamedExpression],
    child: LogicalPlan) extends ObjectConsumer {

  override def output: Seq[Attribute] = serializer.map(_.toAttribute)

  final override val nodePatterns: Seq[TreePattern] = Seq(SERIALIZE_FROM_OBJECT)

  override protected def withNewChildInternal(newChild: LogicalPlan): SerializeFromObject =
    copy(child = newChild)
}

object MapPartitions {
  def apply[T : Encoder, U : Encoder](
      func: Iterator[T] => Iterator[U],
      child: LogicalPlan): LogicalPlan = {
    val deserialized = CatalystSerde.deserialize[T](child)
    val mapped = MapPartitions(
      func.asInstanceOf[Iterator[Any] => Iterator[Any]],
      CatalystSerde.generateObjAttr[U],
      deserialized)
    CatalystSerde.serialize[U](mapped)
  }
}

/**
 * A relation produced by applying `func` to each partition of the `child`.
 */
case class MapPartitions(
    func: Iterator[Any] => Iterator[Any],
    outputObjAttr: Attribute,
    child: LogicalPlan) extends ObjectConsumer with ObjectProducer {
  override protected def withNewChildInternal(newChild: LogicalPlan): MapPartitions =
    copy(child = newChild)
}

object MapPartitionsInR {
  def apply(
      func: Array[Byte],
      packageNames: Array[Byte],
      broadcastVars: Array[Broadcast[Object]],
      schema: StructType,
      encoder: ExpressionEncoder[Row],
      child: LogicalPlan): LogicalPlan = {
    if (SQLConf.get.arrowSparkREnabled) {
      MapPartitionsInRWithArrow(
        func,
        packageNames,
        broadcastVars,
        encoder.schema,
        toAttributes(schema),
        child)
    } else {
      val deserialized = CatalystSerde.deserialize(child)(encoder)
      CatalystSerde.serialize(MapPartitionsInR(
        func,
        packageNames,
        broadcastVars,
        encoder.schema,
        schema,
        CatalystSerde.generateObjAttr(ExpressionEncoder(schema)),
        deserialized))(ExpressionEncoder(schema))
    }
  }
}

/**
 * A relation produced by applying a serialized R function `func` to each partition of the `child`.
 *
 */
case class MapPartitionsInR(
    func: Array[Byte],
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    inputSchema: StructType,
    outputSchema: StructType,
    outputObjAttr: Attribute,
    child: LogicalPlan) extends ObjectConsumer with ObjectProducer {
  override lazy val schema = outputSchema

  override protected def stringArgs: Iterator[Any] = Iterator(inputSchema, outputSchema,
    outputObjAttr, child)

  override protected def withNewChildInternal(newChild: LogicalPlan): MapPartitionsInR =
    copy(child = newChild)
}

/**
 * Similar with `MapPartitionsInR` but serializes and deserializes input/output in
 * Arrow format.
 *
 * This is somewhat similar with `org.apache.spark.sql.execution.python.ArrowEvalPython`
 */
case class MapPartitionsInRWithArrow(
    func: Array[Byte],
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    inputSchema: StructType,
    output: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {
  // This operator always need all columns of its child, even it doesn't reference to.
  @transient
  override lazy val references: AttributeSet = child.outputSet

  override protected def stringArgs: Iterator[Any] = Iterator(
    inputSchema, DataTypeUtils.fromAttributes(output), child)

  override val producedAttributes = AttributeSet(output)

  override protected def withNewChildInternal(newChild: LogicalPlan): MapPartitionsInRWithArrow =
    copy(child = newChild)
}

object MapElements {
  def apply[T : Encoder, U : Encoder](
      func: AnyRef,
      child: LogicalPlan): LogicalPlan = {
    val deserialized = CatalystSerde.deserialize[T](child)
    val mapped = MapElements(
      func,
      implicitly[Encoder[T]].clsTag.runtimeClass,
      implicitly[Encoder[T]].schema,
      CatalystSerde.generateObjAttr[U],
      deserialized)
    CatalystSerde.serialize[U](mapped)
  }
}

/**
 * A relation produced by applying `func` to each element of the `child`.
 */
case class MapElements(
    func: AnyRef,
    argumentClass: Class[_],
    argumentSchema: StructType,
    outputObjAttr: Attribute,
    child: LogicalPlan) extends ObjectConsumer with ObjectProducer {
  override protected def withNewChildInternal(newChild: LogicalPlan): MapElements =
    copy(child = newChild)
}

object TypedFilter {
  def apply[T : Encoder](func: AnyRef, child: LogicalPlan): TypedFilter = {
    TypedFilter(
      func,
      implicitly[Encoder[T]].clsTag.runtimeClass,
      implicitly[Encoder[T]].schema,
      UnresolvedDeserializer(encoderFor[T].deserializer),
      child)
  }
}

/**
 * A relation produced by applying `func` to each element of the `child` and filter them by the
 * resulting boolean value.
 *
 * This is logically equal to a normal [[Filter]] operator whose condition expression is decoding
 * the input row to object and apply the given function with decoded object. However we need the
 * encapsulation of [[TypedFilter]] to make the concept more clear and make it easier to write
 * optimizer rules.
 */
case class TypedFilter(
    func: AnyRef,
    argumentClass: Class[_],
    argumentSchema: StructType,
    deserializer: Expression,
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  final override val nodePatterns: Seq[TreePattern] = Seq(TYPED_FILTER)

  def withObjectProducerChild(obj: LogicalPlan): Filter = {
    assert(obj.output.length == 1)
    Filter(typedCondition(obj.output.head), obj)
  }

  def typedCondition(input: Expression): Expression = {
    val funcMethod = func match {
      case _: FilterFunction[_] => (classOf[FilterFunction[_]], "call")
      case _ => FunctionUtils.getFunctionOneName(BooleanType, input.dataType)
    }
    val funcObj = Literal.create(func, ObjectType(funcMethod._1))
    Invoke(funcObj, funcMethod._2, BooleanType, input :: Nil)
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): TypedFilter =
    copy(child = newChild)
}

object FunctionUtils {
  private def getMethodType(dt: DataType, isOutput: Boolean): Option[String] = {
    dt match {
      case BooleanType if isOutput => Some("Z")
      case IntegerType => Some("I")
      case LongType => Some("J")
      case FloatType => Some("F")
      case DoubleType => Some("D")
      case _ => None
    }
  }

  def getFunctionOneName(outputDT: DataType, inputDT: DataType):
      (Class[scala.Function1[_, _]], String) = {
    classOf[scala.Function1[_, _]] -> {
      // if a pair of an argument and return types is one of specific types
      // whose specialized method (apply$mc..$sp) is generated by scalac,
      // Catalyst generated a direct method call to the specialized method.
      // The followings are references for this specialization:
      //   http://www.scala-lang.org/api/2.12.0/scala/Function1.html
      //   https://github.com/scala/scala/blob/2.11.x/src/compiler/scala/tools/nsc/transform/
      //     SpecializeTypes.scala
      //   http://www.cakesolutions.net/teamblogs/scala-dissection-functions
      //   http://axel22.github.io/2013/11/03/specialization-quirks.html
      val inputType = getMethodType(inputDT, false)
      val outputType = getMethodType(outputDT, true)
      if (inputType.isDefined && outputType.isDefined) {
        s"apply$$mc${outputType.get}${inputType.get}$$sp"
      } else {
        "apply"
      }
    }
  }
}

/** Factory for constructing new `AppendColumn` nodes. */
object AppendColumns {
  def apply[T : Encoder, U : Encoder](
      func: T => U,
      child: LogicalPlan): AppendColumns = {
    new AppendColumns(
      func.asInstanceOf[Any => Any],
      implicitly[Encoder[T]].clsTag.runtimeClass,
      implicitly[Encoder[T]].schema,
      UnresolvedDeserializer(encoderFor[T].deserializer),
      encoderFor[U].namedExpressions,
      child)
  }

  def apply[T : Encoder, U : Encoder](
      func: T => U,
      inputAttributes: Seq[Attribute],
      child: LogicalPlan): AppendColumns = {
    new AppendColumns(
      func.asInstanceOf[Any => Any],
      implicitly[Encoder[T]].clsTag.runtimeClass,
      implicitly[Encoder[T]].schema,
      UnresolvedDeserializer(encoderFor[T].deserializer, inputAttributes),
      encoderFor[U].namedExpressions,
      child)
  }

  private[sql] def apply(
      func: AnyRef,
      inEncoder: ExpressionEncoder[_],
      outEncoder: ExpressionEncoder[_],
      child: LogicalPlan,
      inputAttributes: Seq[Attribute] = Nil): AppendColumns = {
    new AppendColumns(
      func.asInstanceOf[Any => Any],
      inEncoder.clsTag.runtimeClass,
      inEncoder.schema,
      UnresolvedDeserializer(inEncoder.deserializer, inputAttributes),
      outEncoder.namedExpressions,
      child
    )
  }
}

/**
 * A relation produced by applying `func` to each element of the `child`, concatenating the
 * resulting columns at the end of the input row.
 *
 * @param deserializer used to extract the input to `func` from an input row.
 * @param serializer use to serialize the output of `func`.
 */
case class AppendColumns(
    func: Any => Any,
    argumentClass: Class[_],
    argumentSchema: StructType,
    deserializer: Expression,
    serializer: Seq[NamedExpression],
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output ++ newColumns

  final override val nodePatterns: Seq[TreePattern] = Seq(APPEND_COLUMNS)

  def newColumns: Seq[Attribute] = serializer.map(_.toAttribute)

  override protected def withNewChildInternal(newChild: LogicalPlan): AppendColumns =
    copy(child = newChild)
}

/**
 * An optimized version of [[AppendColumns]], that can be executed on deserialized object directly.
 */
case class AppendColumnsWithObject(
    func: Any => Any,
    childSerializer: Seq[NamedExpression],
    newColumnsSerializer: Seq[NamedExpression],
    child: LogicalPlan) extends ObjectConsumer {

  override def output: Seq[Attribute] = (childSerializer ++ newColumnsSerializer).map(_.toAttribute)

  override protected def withNewChildInternal(newChild: LogicalPlan): AppendColumnsWithObject =
    copy(child = newChild)
}

/** Factory for constructing new `MapGroups` nodes. */
object MapGroups {
  def apply[K : Encoder, T : Encoder, U : Encoder](
      func: (K, Iterator[T]) => IterableOnce[U],
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      dataOrder: Seq[SortOrder],
      child: LogicalPlan): LogicalPlan = {
    val mapped = new MapGroups(
      func.asInstanceOf[(Any, Iterator[Any]) => IterableOnce[Any]],
      UnresolvedDeserializer(encoderFor[K].deserializer, groupingAttributes),
      UnresolvedDeserializer(encoderFor[T].deserializer, dataAttributes),
      groupingAttributes,
      dataAttributes,
      dataOrder,
      CatalystSerde.generateObjAttr[U],
      child)
    CatalystSerde.serialize[U](mapped)
  }

  private[sql] def sortOrder(sortExprs: Seq[Expression]): Seq[SortOrder] = {
    sortExprs.map {
      case expr: SortOrder => expr
      case expr: Expression => SortOrder(expr, Ascending)
    }
  }
}

/**
 * Applies func to each unique group in `child`, based on the evaluation of `groupingAttributes`.
 * Func is invoked with an object representation of the grouping key an iterator containing the
 * object representation of all the rows with that key. Given an additional `dataOrder`, data in
 * the iterator will be sorted accordingly. That sorting does not add computational complexity.
 *
 * @param keyDeserializer used to extract the key object for each group.
 * @param valueDeserializer used to extract the items in the iterator from an input row.
 */
case class MapGroups(
    func: (Any, Iterator[Any]) => IterableOnce[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    dataOrder: Seq[SortOrder],
    outputObjAttr: Attribute,
    child: LogicalPlan) extends UnaryNode with ObjectProducer {
  override protected def withNewChildInternal(newChild: LogicalPlan): MapGroups =
    copy(child = newChild)
}

/** Factory for constructing new `MapGroupsWithState` nodes. */
object FlatMapGroupsWithState {
  def apply[K: Encoder, V: Encoder, S: Encoder, U: Encoder](
      func: (Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any],
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      outputMode: OutputMode,
      isMapGroupsWithState: Boolean,
      timeout: GroupStateTimeout,
      child: LogicalPlan): LogicalPlan = {
    val stateEncoder = encoderFor[S]

    val mapped = new FlatMapGroupsWithState(
      func,
      UnresolvedDeserializer(encoderFor[K].deserializer, groupingAttributes),
      UnresolvedDeserializer(encoderFor[V].deserializer, dataAttributes),
      groupingAttributes,
      dataAttributes,
      CatalystSerde.generateObjAttr[U],
      stateEncoder.asInstanceOf[ExpressionEncoder[Any]],
      outputMode,
      isMapGroupsWithState,
      timeout,
      hasInitialState = false,
      groupingAttributes,
      dataAttributes,
      UnresolvedDeserializer(encoderFor[K].deserializer, groupingAttributes),
      LocalRelation(stateEncoder.schema), // empty data set
      child
    )
    CatalystSerde.serialize[U](mapped)
  }

  def apply[K: Encoder, V: Encoder, S: Encoder, U: Encoder](
      func: (Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any],
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      outputMode: OutputMode,
      isMapGroupsWithState: Boolean,
      timeout: GroupStateTimeout,
      child: LogicalPlan,
      initialStateGroupAttrs: Seq[Attribute],
      initialStateDataAttrs: Seq[Attribute],
      initialState: LogicalPlan): LogicalPlan = {
    val stateEncoder = encoderFor[S]

    val mapped = new FlatMapGroupsWithState(
      func,
      UnresolvedDeserializer(encoderFor[K].deserializer, groupingAttributes),
      UnresolvedDeserializer(encoderFor[V].deserializer, dataAttributes),
      groupingAttributes,
      dataAttributes,
      CatalystSerde.generateObjAttr[U],
      stateEncoder.asInstanceOf[ExpressionEncoder[Any]],
      outputMode,
      isMapGroupsWithState,
      timeout,
      hasInitialState = true,
      initialStateGroupAttrs,
      initialStateDataAttrs,
      UnresolvedDeserializer(encoderFor[S].deserializer, initialStateDataAttrs),
      initialState,
      child)
    CatalystSerde.serialize[U](mapped)
  }
}

/**
 * Applies func to each unique group in `child`, based on the evaluation of `groupingAttributes`,
 * while using state data.
 * Func is invoked with an object representation of the grouping key an iterator containing the
 * object representation of all the rows with that key.
 *
 * @param func function called on each group
 * @param keyDeserializer used to extract the key object for each group.
 * @param valueDeserializer used to extract the items in the iterator from an input row.
 * @param groupingAttributes used to group the data
 * @param dataAttributes used to read the data
 * @param outputObjAttr used to define the output object
 * @param stateEncoder used to serialize/deserialize state before calling `func`
 * @param outputMode the output mode of `func`
 * @param isMapGroupsWithState whether it is created by the `mapGroupsWithState` method
 * @param timeout used to timeout groups that have not received data in a while
 * @param hasInitialState Indicates whether initial state needs to be applied or not.
 * @param initialStateGroupAttrs grouping attributes for the initial state
 * @param initialStateDataAttrs used to read the initial state
 * @param initialStateDeserializer used to extract the initial state objects.
 * @param initialState user defined initial state that is applied in the first batch.
 * @param child logical plan of the underlying data
 */
case class FlatMapGroupsWithState(
    func: (Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    outputObjAttr: Attribute,
    stateEncoder: ExpressionEncoder[Any],
    outputMode: OutputMode,
    isMapGroupsWithState: Boolean = false,
    timeout: GroupStateTimeout,
    hasInitialState: Boolean = false,
    initialStateGroupAttrs: Seq[Attribute],
    initialStateDataAttrs: Seq[Attribute],
    initialStateDeserializer: Expression,
    initialState: LogicalPlan,
    child: LogicalPlan) extends BinaryNode with ObjectProducer {

  if (isMapGroupsWithState) {
    assert(outputMode == OutputMode.Update)
  }

  override def left: LogicalPlan = child

  override def right: LogicalPlan = initialState

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan, newRight: LogicalPlan): FlatMapGroupsWithState =
    copy(child = newLeft, initialState = newRight)
}

object TransformWithState {
  def apply[K: Encoder, V: Encoder, U: Encoder](
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      statefulProcessor: StatefulProcessor[K, V, U],
      timeMode: TimeMode,
      outputMode: OutputMode,
      child: LogicalPlan): LogicalPlan = {
    val keyEncoder = encoderFor[K]
    val mapped = new TransformWithState(
      UnresolvedDeserializer(keyEncoder.deserializer, groupingAttributes),
      UnresolvedDeserializer(encoderFor[V].deserializer, dataAttributes),
      groupingAttributes,
      dataAttributes,
      statefulProcessor.asInstanceOf[StatefulProcessor[Any, Any, Any]],
      timeMode,
      outputMode,
      keyEncoder.asInstanceOf[ExpressionEncoder[Any]],
      CatalystSerde.generateObjAttr[U],
      child,
      hasInitialState = false,
      // the following parameters will not be used in physical plan if hasInitialState = false
      initialStateGroupingAttrs = groupingAttributes,
      initialStateDataAttrs = dataAttributes,
      initialStateDeserializer =
        UnresolvedDeserializer(encoderFor[K].deserializer, groupingAttributes),
      initialState = LocalRelation(encoderFor[K].schema) // empty data set
    )
    CatalystSerde.serialize[U](mapped)
  }

  // This apply() is to invoke TransformWithState object with hasInitialState set to true
  def apply[K: Encoder, V: Encoder, U: Encoder, S: Encoder](
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      statefulProcessor: StatefulProcessor[K, V, U],
      timeMode: TimeMode,
      outputMode: OutputMode,
      child: LogicalPlan,
      initialStateGroupingAttrs: Seq[Attribute],
      initialStateDataAttrs: Seq[Attribute],
      initialState: LogicalPlan): LogicalPlan = {
    val keyEncoder = encoderFor[K]
    val mapped = new TransformWithState(
      UnresolvedDeserializer(encoderFor[K].deserializer, groupingAttributes),
      UnresolvedDeserializer(encoderFor[V].deserializer, dataAttributes),
      groupingAttributes,
      dataAttributes,
      statefulProcessor.asInstanceOf[StatefulProcessor[Any, Any, Any]],
      timeMode,
      outputMode,
      keyEncoder.asInstanceOf[ExpressionEncoder[Any]],
      CatalystSerde.generateObjAttr[U],
      child,
      hasInitialState = true,
      initialStateGroupingAttrs,
      initialStateDataAttrs,
      UnresolvedDeserializer(encoderFor[S].deserializer, initialStateDataAttrs),
      initialState
    )
    CatalystSerde.serialize[U](mapped)
  }
}

case class TransformWithState(
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    statefulProcessor: StatefulProcessor[Any, Any, Any],
    timeMode: TimeMode,
    outputMode: OutputMode,
    keyEncoder: ExpressionEncoder[Any],
    outputObjAttr: Attribute,
    child: LogicalPlan,
    hasInitialState: Boolean = false,
    initialStateGroupingAttrs: Seq[Attribute],
    initialStateDataAttrs: Seq[Attribute],
    initialStateDeserializer: Expression,
    initialState: LogicalPlan) extends BinaryNode with ObjectProducer {

  override def left: LogicalPlan = child
  override def right: LogicalPlan = initialState
  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan, newRight: LogicalPlan): TransformWithState =
    copy(child = newLeft, initialState = newRight)
}

/** Factory for constructing new `FlatMapGroupsInR` nodes. */
object FlatMapGroupsInR {
  def apply(
      func: Array[Byte],
      packageNames: Array[Byte],
      broadcastVars: Array[Broadcast[Object]],
      schema: StructType,
      keyDeserializer: Expression,
      valueDeserializer: Expression,
      inputSchema: StructType,
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      child: LogicalPlan): LogicalPlan = {
    if (SQLConf.get.arrowSparkREnabled) {
      FlatMapGroupsInRWithArrow(
        func,
        packageNames,
        broadcastVars,
        inputSchema,
        toAttributes(schema),
        UnresolvedDeserializer(keyDeserializer, groupingAttributes),
        groupingAttributes,
        child)
    } else {
      CatalystSerde.serialize(FlatMapGroupsInR(
        func,
        packageNames,
        broadcastVars,
        inputSchema,
        schema,
        UnresolvedDeserializer(keyDeserializer, groupingAttributes),
        UnresolvedDeserializer(valueDeserializer, dataAttributes),
        groupingAttributes,
        dataAttributes,
        CatalystSerde.generateObjAttr(ExpressionEncoder(schema)),
        child))(ExpressionEncoder(schema))
    }
  }
}

case class FlatMapGroupsInR(
    func: Array[Byte],
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    inputSchema: StructType,
    outputSchema: StructType,
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    outputObjAttr: Attribute,
    child: LogicalPlan) extends UnaryNode with ObjectProducer {

  override lazy val schema = outputSchema

  override protected def stringArgs: Iterator[Any] = Iterator(inputSchema, outputSchema,
    keyDeserializer, valueDeserializer, groupingAttributes, dataAttributes, outputObjAttr,
    child)

  override protected def withNewChildInternal(newChild: LogicalPlan): FlatMapGroupsInR =
    copy(child = newChild)
}

/**
 * Similar with `FlatMapGroupsInR` but serializes and deserializes input/output in
 * Arrow format.
 * This is also somewhat similar with [[FlatMapGroupsInPandas]].
 */
case class FlatMapGroupsInRWithArrow(
    func: Array[Byte],
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    inputSchema: StructType,
    output: Seq[Attribute],
    keyDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {
  // This operator always need all columns of its child, even it doesn't reference to.
  @transient
  override lazy val references: AttributeSet = child.outputSet

  override protected def stringArgs: Iterator[Any] = Iterator(
    inputSchema, DataTypeUtils.fromAttributes(output), keyDeserializer, groupingAttributes, child)

  override val producedAttributes = AttributeSet(output)

  override protected def withNewChildInternal(newChild: LogicalPlan): FlatMapGroupsInRWithArrow =
    copy(child = newChild)
}

/** Factory for constructing new `CoGroup` nodes. */
object CoGroup {
  def apply[K : Encoder, L : Encoder, R : Encoder, OUT : Encoder](
      func: (K, Iterator[L], Iterator[R]) => IterableOnce[OUT],
      leftGroup: Seq[Attribute],
      rightGroup: Seq[Attribute],
      leftAttr: Seq[Attribute],
      rightAttr: Seq[Attribute],
      leftOrder: Seq[SortOrder],
      rightOrder: Seq[SortOrder],
      left: LogicalPlan,
      right: LogicalPlan): LogicalPlan = {
    require(DataTypeUtils.fromAttributes(leftGroup) == DataTypeUtils.fromAttributes(rightGroup))

    val cogrouped = CoGroup(
      func.asInstanceOf[(Any, Iterator[Any], Iterator[Any]) => IterableOnce[Any]],
      // The `leftGroup` and `rightGroup` are guaranteed te be of same schema, so it's safe to
      // resolve the `keyDeserializer` based on either of them, here we pick the left one.
      UnresolvedDeserializer(encoderFor[K].deserializer, leftGroup),
      UnresolvedDeserializer(encoderFor[L].deserializer, leftAttr),
      UnresolvedDeserializer(encoderFor[R].deserializer, rightAttr),
      leftGroup,
      rightGroup,
      leftAttr,
      rightAttr,
      leftOrder,
      rightOrder,
      CatalystSerde.generateObjAttr[OUT],
      left,
      right)
    CatalystSerde.serialize[OUT](cogrouped)
  }
}

/**
 * A relation produced by applying `func` to each grouping key and associated values from left and
 * right children.
 */
case class CoGroup(
    func: (Any, Iterator[Any], Iterator[Any]) => IterableOnce[Any],
    keyDeserializer: Expression,
    leftDeserializer: Expression,
    rightDeserializer: Expression,
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    leftAttr: Seq[Attribute],
    rightAttr: Seq[Attribute],
    leftOrder: Seq[SortOrder],
    rightOrder: Seq[SortOrder],
    outputObjAttr: Attribute,
    left: LogicalPlan,
    right: LogicalPlan) extends BinaryNode with ObjectProducer {
  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan, newRight: LogicalPlan): CoGroup = copy(left = newLeft, right = newRight)
}

// TODO (SPARK-44225): Move this into analyzer
object JoinWith {
  /**
   * find the trivially true predicates and automatically resolves them to both sides.
   */
  private[sql] def resolveSelfJoinCondition(resolver: Resolver, plan: Join): Join = {
    val cond = plan.condition.map {
      _.transform {
        case catalyst.expressions.EqualTo(a: AttributeReference, b: AttributeReference)
          if a.sameRef(b) =>
          catalyst.expressions.EqualTo(
            plan.left.resolveQuoted(a.name, resolver).getOrElse(
              throw QueryCompilationErrors.unresolvedColumnError(
                a.name, plan.left.schema.fieldNames)),
            plan.right.resolveQuoted(b.name, resolver).getOrElse(
              throw QueryCompilationErrors.unresolvedColumnError(
                b.name, plan.right.schema.fieldNames)))
        case catalyst.expressions.EqualNullSafe(a: AttributeReference, b: AttributeReference)
          if a.sameRef(b) =>
          catalyst.expressions.EqualNullSafe(
            plan.left.resolveQuoted(a.name, resolver).getOrElse(
              throw QueryCompilationErrors.unresolvedColumnError(
                a.name, plan.left.schema.fieldNames)),
            plan.right.resolveQuoted(b.name, resolver).getOrElse(
              throw QueryCompilationErrors.unresolvedColumnError(
                b.name, plan.right.schema.fieldNames)))
      }
    }
    plan.copy(condition = cond)
  }

  private[sql] def typedJoinWith(
      plan: Join,
      isAutoSelfJoinAliasEnable: Boolean,
      resolver: Resolver,
      isLeftFlattenableToRow: Boolean,
      isRightFlattenableToRow: Boolean): LogicalPlan = {
    var joined = plan
    if (joined.joinType == LeftSemi || joined.joinType == LeftAnti) {
      throw QueryCompilationErrors.invalidJoinTypeInJoinWithError(joined.joinType)
    }
    // If auto self join alias is enable
    if (isAutoSelfJoinAliasEnable) {
      joined = resolveSelfJoinCondition(resolver, joined)
    }

    val leftResultExpr = {
      if (!isLeftFlattenableToRow) {
        assert(joined.left.output.length == 1)
        Alias(joined.left.output.head, "_1")()
      } else {
        Alias(CreateStruct(joined.left.output), "_1")()
      }
    }

    val rightResultExpr = {
      if (!isRightFlattenableToRow) {
        assert(joined.right.output.length == 1)
        Alias(joined.right.output.head, "_2")()
      } else {
        Alias(CreateStruct(joined.right.output), "_2")()
      }
    }

    if (joined.joinType.isInstanceOf[InnerLike]) {
      // For inner joins, we can directly perform the join and then can project the join
      // results into structs. This ensures that data remains flat during shuffles /
      // exchanges (unlike the outer join path, which nests the data before shuffling).
      Project(Seq(leftResultExpr, rightResultExpr), joined)
    } else { // outer joins
      // For both join sides, combine all outputs into a single column and alias it with "_1
      // or "_2", to match the schema for the encoder of the join result.
      // Note that we do this before joining them, to enable the join operator to return null
      // for one side, in cases like outer-join.
      val left = Project(leftResultExpr :: Nil, joined.left)
      val right = Project(rightResultExpr :: Nil, joined.right)

      // Rewrites the join condition to make the attribute point to correct column/field,
      // after we combine the outputs of each join side.
      val conditionExpr = joined.condition.get transformUp {
        case a: Attribute if joined.left.outputSet.contains(a) =>
          if (!isLeftFlattenableToRow) {
            left.output.head
          } else {
            val index = joined.left.output.indexWhere(_.exprId == a.exprId)
            GetStructField(left.output.head, index)
          }
        case a: Attribute if joined.right.outputSet.contains(a) =>
          if (!isRightFlattenableToRow) {
            right.output.head
          } else {
            val index = joined.right.output.indexWhere(_.exprId == a.exprId)
            GetStructField(right.output.head, index)
          }
      }

      Join(left, right, joined.joinType, Some(conditionExpr), JoinHint.NONE)
    }
  }
}
