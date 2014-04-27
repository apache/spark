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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.types._

case class Project(projectList: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  def output = projectList.map(_.toAttribute)
  def references = projectList.flatMap(_.references).toSet
}

/**
 * Applies a [[catalyst.expressions.Generator Generator]] to a stream of input rows, combining the
 * output of each into a new stream of rows.  This operation is similar to a `flatMap` in functional
 * programming with one important additional feature, which allows the input rows to be joined with
 * their output.
 * @param join  when true, each output row is implicitly joined with the input tuple that produced
 *              it.
 * @param outer when true, each input row will be output at least once, even if the output of the
 *              given `generator` is empty. `outer` has no effect when `join` is false.
 * @param alias when set, this string is applied to the schema of the output of the transformation
 *              as a qualifier.
 */
case class Generate(
    generator: Generator,
    join: Boolean,
    outer: Boolean,
    alias: Option[String],
    child: LogicalPlan)
  extends UnaryNode {

  protected def generatorOutput =
    alias
      .map(a => generator.output.map(_.withQualifiers(a :: Nil)))
      .getOrElse(generator.output)

  def output =
    if (join) child.output ++ generatorOutput else generatorOutput

  def references =
    if (join) child.outputSet else generator.references
}

case class Filter(condition: Expression, child: LogicalPlan) extends UnaryNode {
  def output = child.output
  def references = condition.references
}

case class Union(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
  // TODO: These aren't really the same attributes as nullability etc might change.
  def output = left.output

  override lazy val resolved =
    childrenResolved &&
    !left.output.zip(right.output).exists { case (l,r) => l.dataType != r.dataType }

  def references = Set.empty
}

case class Join(
  left: LogicalPlan,
  right: LogicalPlan,
  joinType: JoinType,
  condition: Option[Expression]) extends BinaryNode {

  def references = condition.map(_.references).getOrElse(Set.empty)
  def output = left.output ++ right.output
}

case class InsertIntoTable(
    table: LogicalPlan,
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: Boolean)
  extends LogicalPlan {
  // The table being inserted into is a child for the purposes of transformations.
  def children = table :: child :: Nil
  def references = Set.empty
  def output = child.output

  override lazy val resolved = childrenResolved && child.output.zip(table.output).forall {
    case (childAttr, tableAttr) => childAttr.dataType == tableAttr.dataType
  }
}

case class InsertIntoCreatedTable(
    databaseName: Option[String],
    tableName: String,
    child: LogicalPlan) extends UnaryNode {
  def references = Set.empty
  def output = child.output
}

case class WriteToFile(
    path: String,
    child: LogicalPlan) extends UnaryNode {
  def references = Set.empty
  def output = child.output
}

case class Sort(order: Seq[SortOrder], child: LogicalPlan) extends UnaryNode {
  def output = child.output
  def references = order.flatMap(_.references).toSet
}

case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalPlan)
  extends UnaryNode {

  def output = aggregateExpressions.map(_.toAttribute)
  def references = (groupingExpressions ++ aggregateExpressions).flatMap(_.references).toSet
}

case class Limit(limit: Expression, child: LogicalPlan) extends UnaryNode {
  def output = child.output
  def references = limit.references
}

case class Subquery(alias: String, child: LogicalPlan) extends UnaryNode {
  def output = child.output.map(_.withQualifiers(alias :: Nil))
  def references = Set.empty
}

/**
 * Converts the schema of `child` to all lowercase, together with LowercaseAttributeReferences
 * this allows for optional case insensitive attribute resolution.  This node can be elided after
 * analysis.
 */
case class LowerCaseSchema(child: LogicalPlan) extends UnaryNode {
  protected def lowerCaseSchema(dataType: DataType): DataType = dataType match {
    case StructType(fields) =>
      StructType(fields.map(f =>
        StructField(f.name.toLowerCase(), lowerCaseSchema(f.dataType), f.nullable)))
    case ArrayType(elemType) => ArrayType(lowerCaseSchema(elemType))
    case otherType => otherType
  }

  val output = child.output.map {
    case a: AttributeReference =>
      AttributeReference(
        a.name.toLowerCase,
        lowerCaseSchema(a.dataType),
        a.nullable)(
        a.exprId,
        a.qualifiers)
    case other => other
  }

  def references = Set.empty
}

case class Sample(fraction: Double, withReplacement: Boolean, seed: Long, child: LogicalPlan)
    extends UnaryNode {

  def output = child.output
  def references = Set.empty
}

case class Distinct(child: LogicalPlan) extends UnaryNode {
  def output = child.output
  def references = child.outputSet
}

case object NoRelation extends LeafNode {
  def output = Nil
}
