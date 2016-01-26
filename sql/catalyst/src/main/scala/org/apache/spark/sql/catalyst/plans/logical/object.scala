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

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{ObjectType, StructType}

/**
 * A trait for logical operators that apply user defined functions to domain objects.
 */
trait ObjectOperator extends LogicalPlan {

  /** The serializer that is used to produce the output of this operator. */
  def serializer: Seq[NamedExpression]

  override def output: Seq[Attribute] = serializer.map(_.toAttribute)

  /**
   * An [[ObjectOperator]] may have one or more deserializers to convert internal rows to objects.
   * It must also provide the attributes that are available during the resolution of each
   * deserializer.
   */
  def deserializers: Seq[(Expression, Seq[Attribute])]

  /**
   * The object type that is produced by the user defined function. Note that the return type here
   * is the same whether or not the operator is output serialized data.
   */
  def outputObject: NamedExpression =
    Alias(serializer.head.collect { case b: BoundReference => b }.head, "obj")()

  /**
   * Returns a copy of this operator that will produce an object instead of an encoded row.
   * Used in the optimizer when transforming plans to remove unneeded serialization.
   */
  def withObjectOutput: LogicalPlan = if (output.head.dataType.isInstanceOf[ObjectType]) {
    this
  } else {
    withNewSerializer(outputObject :: Nil)
  }

  /** Returns a copy of this operator with a different serializer. */
  def withNewSerializer(newSerializer: Seq[NamedExpression]): LogicalPlan = makeCopy {
    productIterator.map {
      case c if c == serializer => newSerializer
      case other: AnyRef => other
    }.toArray
  }
}

object MapPartitions {
  def apply[T : Encoder, U : Encoder](
      func: Iterator[T] => Iterator[U],
      child: LogicalPlan): MapPartitions = {
    MapPartitions(
      func.asInstanceOf[Iterator[Any] => Iterator[Any]],
      encoderFor[T].fromRowExpression,
      encoderFor[U].namedExpressions,
      child)
  }
}

/**
 * A relation produced by applying `func` to each partition of the `child`.
 *
 * @param deserializer used to extract the input to `func` from an input row.
 * @param serializer use to serialize the output of `func`.
 */
case class MapPartitions(
    func: Iterator[Any] => Iterator[Any],
    deserializer: Expression,
    serializer: Seq[NamedExpression],
    child: LogicalPlan) extends UnaryNode with ObjectOperator {
  override def deserializers: Seq[(Expression, Seq[Attribute])] = Seq(deserializer -> child.output)
}

/** Factory for constructing new `AppendColumn` nodes. */
object AppendColumns {
  def apply[T : Encoder, U : Encoder](
      func: T => U,
      child: LogicalPlan): AppendColumns = {
    new AppendColumns(
      func.asInstanceOf[Any => Any],
      encoderFor[T].fromRowExpression,
      encoderFor[U].namedExpressions,
      child)
  }
}

/**
 * A relation produced by applying `func` to each partition of the `child`, concatenating the
 * resulting columns at the end of the input row.
 *
 * @param deserializer used to extract the input to `func` from an input row.
 * @param serializer use to serialize the output of `func`.
 */
case class AppendColumns(
    func: Any => Any,
    deserializer: Expression,
    serializer: Seq[NamedExpression],
    child: LogicalPlan) extends UnaryNode with ObjectOperator {

  override def output: Seq[Attribute] = child.output ++ newColumns

  def newColumns: Seq[Attribute] = serializer.map(_.toAttribute)

  override def deserializers: Seq[(Expression, Seq[Attribute])] = Seq(deserializer -> child.output)
}

/** Factory for constructing new `MapGroups` nodes. */
object MapGroups {
  def apply[K : Encoder, T : Encoder, U : Encoder](
      func: (K, Iterator[T]) => TraversableOnce[U],
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      child: LogicalPlan): MapGroups = {
    new MapGroups(
      func.asInstanceOf[(Any, Iterator[Any]) => TraversableOnce[Any]],
      encoderFor[K].fromRowExpression,
      encoderFor[T].fromRowExpression,
      encoderFor[U].namedExpressions,
      groupingAttributes,
      dataAttributes,
      child)
  }
}

/**
 * Applies func to each unique group in `child`, based on the evaluation of `groupingAttributes`.
 * Func is invoked with an object representation of the grouping key an iterator containing the
 * object representation of all the rows with that key.
 *
 * @param keyDeserializer used to extract the key object for each group.
 * @param valueDeserializer used to extract the items in the iterator from an input row.
 * @param serializer use to serialize the output of `func`.
 */
case class MapGroups(
    func: (Any, Iterator[Any]) => TraversableOnce[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    serializer: Seq[NamedExpression],
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode with ObjectOperator {

  override def deserializers: Seq[(Expression, Seq[Attribute])] =
    Seq(keyDeserializer -> groupingAttributes, valueDeserializer -> dataAttributes)
}

/** Factory for constructing new `CoGroup` nodes. */
object CoGroup {
  def apply[Key : Encoder, Left : Encoder, Right : Encoder, Result : Encoder](
      func: (Key, Iterator[Left], Iterator[Right]) => TraversableOnce[Result],
      leftGroup: Seq[Attribute],
      rightGroup: Seq[Attribute],
      leftData: Seq[Attribute],
      rightData: Seq[Attribute],
      left: LogicalPlan,
      right: LogicalPlan): CoGroup = {
    require(StructType.fromAttributes(leftGroup) == StructType.fromAttributes(rightGroup))

    CoGroup(
      func.asInstanceOf[(Any, Iterator[Any], Iterator[Any]) => TraversableOnce[Any]],
      encoderFor[Key].fromRowExpression,
      encoderFor[Left].fromRowExpression,
      encoderFor[Right].fromRowExpression,
      encoderFor[Result].namedExpressions,
      leftGroup,
      rightGroup,
      leftData,
      rightData,
      left,
      right)
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
    serializer: Seq[NamedExpression],
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    leftAttr: Seq[Attribute],
    rightAttr: Seq[Attribute],
    left: LogicalPlan,
    right: LogicalPlan) extends BinaryNode with ObjectOperator {

  override def producedAttributes: AttributeSet = outputSet

  override def deserializers: Seq[(Expression, Seq[Attribute])] =
    // The `leftGroup` and `rightGroup` are guaranteed te be of same schema, so it's safe to resolve
    // the `keyDeserializer` based on either of them, here we pick the left one.
    Seq(keyDeserializer -> leftGroup, leftDeserializer -> leftAttr, rightDeserializer -> rightAttr)
}
