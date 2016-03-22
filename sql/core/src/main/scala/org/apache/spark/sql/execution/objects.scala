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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, GenerateUnsafeProjection, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types.ObjectType

/**
 * Helper functions for physical operators that work with user defined objects.
 */
trait ObjectOperator extends SparkPlan {
  def generateToObject(objExpr: Expression, inputSchema: Seq[Attribute]): InternalRow => Any = {
    val objectProjection = GenerateSafeProjection.generate(objExpr :: Nil, inputSchema)
    (i: InternalRow) => objectProjection(i).get(0, objExpr.dataType)
  }

  def generateToRow(serializer: Seq[Expression]): Any => InternalRow = {
    val outputProjection = if (serializer.head.dataType.isInstanceOf[ObjectType]) {
      GenerateSafeProjection.generate(serializer)
    } else {
      GenerateUnsafeProjection.generate(serializer)
    }
    val inputType = serializer.head.collect { case b: BoundReference => b.dataType }.head
    val outputRow = new SpecificMutableRow(inputType :: Nil)
    (o: Any) => {
      outputRow(0) = o
      outputProjection(outputRow)
    }
  }
}

/**
 * Applies the given function to each input row and encodes the result.
 */
case class MapPartitions(
    func: Iterator[Any] => Iterator[Any],
    deserializer: Expression,
    serializer: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with ObjectOperator {
  override def output: Seq[Attribute] = serializer.map(_.toAttribute)

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val getObject = generateToObject(deserializer, child.output)
      val outputObject = generateToRow(serializer)
      func(iter.map(getObject)).map(outputObject)
    }
  }
}

/**
 * Applies the given function to each input row, appending the encoded result at the end of the row.
 */
case class AppendColumns(
    func: Any => Any,
    deserializer: Expression,
    serializer: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with ObjectOperator {

  override def output: Seq[Attribute] = child.output ++ serializer.map(_.toAttribute)

  private def newColumnSchema = serializer.map(_.toAttribute).toStructType

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val getObject = generateToObject(deserializer, child.output)
      val combiner = GenerateUnsafeRowJoiner.create(child.schema, newColumnSchema)
      val outputObject = generateToRow(serializer)

      iter.map { row =>
        val newColumns = outputObject(func(getObject(row)))

        // This operates on the assumption that we always serialize the result...
        combiner.join(row.asInstanceOf[UnsafeRow], newColumns.asInstanceOf[UnsafeRow]): InternalRow
      }
    }
  }
}

/**
 * Groups the input rows together and calls the function with each group and an iterator containing
 * all elements in the group.  The result of this function is encoded and flattened before
 * being output.
 */
case class MapGroups(
    func: (Any, Iterator[Any]) => TraversableOnce[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    serializer: Seq[NamedExpression],
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    child: SparkPlan) extends UnaryNode with ObjectOperator {

  override def output: Seq[Attribute] = serializer.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val grouped = GroupedIterator(iter, groupingAttributes, child.output)

      val getKey = generateToObject(keyDeserializer, groupingAttributes)
      val getValue = generateToObject(valueDeserializer, dataAttributes)
      val outputObject = generateToRow(serializer)

      grouped.flatMap { case (key, rowIter) =>
        val result = func(
          getKey(key),
          rowIter.map(getValue))
        result.map(outputObject)
      }
    }
  }
}

/**
 * Co-groups the data from left and right children, and calls the function with each group and 2
 * iterators containing all elements in the group from left and right side.
 * The result of this function is encoded and flattened before being output.
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
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode with ObjectOperator {

  override def output: Seq[Attribute] = serializer.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftGroup) :: ClusteredDistribution(rightGroup) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    leftGroup.map(SortOrder(_, Ascending)) :: rightGroup.map(SortOrder(_, Ascending)) :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    left.execute().zipPartitions(right.execute()) { (leftData, rightData) =>
      val leftGrouped = GroupedIterator(leftData, leftGroup, left.output)
      val rightGrouped = GroupedIterator(rightData, rightGroup, right.output)

      val getKey = generateToObject(keyDeserializer, leftGroup)
      val getLeft = generateToObject(leftDeserializer, leftAttr)
      val getRight = generateToObject(rightDeserializer, rightAttr)
      val outputObject = generateToRow(serializer)

      new CoGroupedIterator(leftGrouped, rightGrouped, leftGroup).flatMap {
        case (key, leftResult, rightResult) =>
          val result = func(
            getKey(key),
            leftResult.map(getLeft),
            rightResult.map(getRight))
          result.map(outputObject)
      }
    }
  }
}
