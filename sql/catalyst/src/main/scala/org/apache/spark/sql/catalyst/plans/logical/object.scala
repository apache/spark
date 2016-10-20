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

import scala.language.existentials

import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedDeserializer
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
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
    AttributeReference("obj", encoderFor[T].deserializer.dataType, nullable = false)()
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
trait ObjectConsumer extends UnaryNode {
  assert(child.output.length == 1)

  // This operator always need all columns of its child, even it doesn't reference to.
  override def references: AttributeSet = child.outputSet

  def inputObjAttr: Attribute = child.output.head
}

/**
 * Takes the input row from child and turns it into object using the given deserializer expression.
 */
case class DeserializeToObject(
    deserializer: Expression,
    outputObjAttr: Attribute,
    child: LogicalPlan) extends UnaryNode with ObjectProducer

/**
 * Takes the input object from child and turns it into unsafe row using the given serializer
 * expression.
 */
case class SerializeFromObject(
    serializer: Seq[NamedExpression],
    child: LogicalPlan) extends ObjectConsumer {

  override def output: Seq[Attribute] = serializer.map(_.toAttribute)
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
    child: LogicalPlan) extends ObjectConsumer with ObjectProducer

object MapPartitionsInR {
  def apply(
      func: Array[Byte],
      packageNames: Array[Byte],
      broadcastVars: Array[Broadcast[Object]],
      schema: StructType,
      encoder: ExpressionEncoder[Row],
      child: LogicalPlan): LogicalPlan = {
    val deserialized = CatalystSerde.deserialize(child)(encoder)
    val mapped = MapPartitionsInR(
      func,
      packageNames,
      broadcastVars,
      encoder.schema,
      schema,
      CatalystSerde.generateObjAttr(RowEncoder(schema)),
      deserialized)
    CatalystSerde.serialize(mapped)(RowEncoder(schema))
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
    child: LogicalPlan) extends ObjectConsumer with ObjectProducer

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

  def withObjectProducerChild(obj: LogicalPlan): Filter = {
    assert(obj.output.length == 1)
    Filter(typedCondition(obj.output.head), obj)
  }

  def typedCondition(input: Expression): Expression = {
    val (funcClass, methodName) = func match {
      case m: FilterFunction[_] => classOf[FilterFunction[_]] -> "call"
      case _ => classOf[Any => Boolean] -> "apply"
    }
    val funcObj = Literal.create(func, ObjectType(funcClass))
    Invoke(funcObj, methodName, BooleanType, input :: Nil)
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

  def newColumns: Seq[Attribute] = serializer.map(_.toAttribute)
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
}

/** Factory for constructing new `MapGroups` nodes. */
object MapGroups {
  def apply[K : Encoder, T : Encoder, U : Encoder](
      func: (K, Iterator[T]) => TraversableOnce[U],
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      child: LogicalPlan): LogicalPlan = {
    val mapped = new MapGroups(
      func.asInstanceOf[(Any, Iterator[Any]) => TraversableOnce[Any]],
      UnresolvedDeserializer(encoderFor[K].deserializer, groupingAttributes),
      UnresolvedDeserializer(encoderFor[T].deserializer, dataAttributes),
      groupingAttributes,
      dataAttributes,
      CatalystSerde.generateObjAttr[U],
      child)
    CatalystSerde.serialize[U](mapped)
  }
}

/**
 * Applies func to each unique group in `child`, based on the evaluation of `groupingAttributes`.
 * Func is invoked with an object representation of the grouping key an iterator containing the
 * object representation of all the rows with that key.
 *
 * @param keyDeserializer used to extract the key object for each group.
 * @param valueDeserializer used to extract the items in the iterator from an input row.
 */
case class MapGroups(
    func: (Any, Iterator[Any]) => TraversableOnce[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    outputObjAttr: Attribute,
    child: LogicalPlan) extends UnaryNode with ObjectProducer

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
    val mapped = FlatMapGroupsInR(
      func,
      packageNames,
      broadcastVars,
      inputSchema,
      schema,
      UnresolvedDeserializer(keyDeserializer, groupingAttributes),
      UnresolvedDeserializer(valueDeserializer, dataAttributes),
      groupingAttributes,
      dataAttributes,
      CatalystSerde.generateObjAttr(RowEncoder(schema)),
      child)
    CatalystSerde.serialize(mapped)(RowEncoder(schema))
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
    child: LogicalPlan) extends UnaryNode with ObjectProducer{

  override lazy val schema = outputSchema

  override protected def stringArgs: Iterator[Any] = Iterator(inputSchema, outputSchema,
    keyDeserializer, valueDeserializer, groupingAttributes, dataAttributes, outputObjAttr,
    child)
}

/** Factory for constructing new `CoGroup` nodes. */
object CoGroup {
  def apply[K : Encoder, L : Encoder, R : Encoder, OUT : Encoder](
      func: (K, Iterator[L], Iterator[R]) => TraversableOnce[OUT],
      leftGroup: Seq[Attribute],
      rightGroup: Seq[Attribute],
      leftAttr: Seq[Attribute],
      rightAttr: Seq[Attribute],
      left: LogicalPlan,
      right: LogicalPlan): LogicalPlan = {
    require(StructType.fromAttributes(leftGroup) == StructType.fromAttributes(rightGroup))

    val cogrouped = CoGroup(
      func.asInstanceOf[(Any, Iterator[Any], Iterator[Any]) => TraversableOnce[Any]],
      // The `leftGroup` and `rightGroup` are guaranteed te be of same schema, so it's safe to
      // resolve the `keyDeserializer` based on either of them, here we pick the left one.
      UnresolvedDeserializer(encoderFor[K].deserializer, leftGroup),
      UnresolvedDeserializer(encoderFor[L].deserializer, leftAttr),
      UnresolvedDeserializer(encoderFor[R].deserializer, rightAttr),
      leftGroup,
      rightGroup,
      leftAttr,
      rightAttr,
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
    func: (Any, Iterator[Any], Iterator[Any]) => TraversableOnce[Any],
    keyDeserializer: Expression,
    leftDeserializer: Expression,
    rightDeserializer: Expression,
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    leftAttr: Seq[Attribute],
    rightAttr: Seq[Attribute],
    outputObjAttr: Attribute,
    left: LogicalPlan,
    right: LogicalPlan) extends BinaryNode with ObjectProducer
