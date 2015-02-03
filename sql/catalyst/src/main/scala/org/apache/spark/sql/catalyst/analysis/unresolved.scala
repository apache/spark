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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.{errors, trees}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Thrown when an invalid attempt is made to access a property of a tree that has yet to be fully
 * resolved.
 */
class UnresolvedException[TreeType <: TreeNode[_]](tree: TreeType, function: String) extends
  errors.TreeNodeException(tree, s"Invalid call to $function on unresolved object", null)

/**
 * Holds the name of a relation that has yet to be looked up in a [[Catalog]].
 */
case class UnresolvedRelation(
    tableIdentifier: Seq[String],
    alias: Option[String] = None) extends LeafNode {
  override def output = Nil
  override lazy val resolved = false
}

/**
 * Holds the name of an attribute that has yet to be resolved.
 */
case class UnresolvedAttribute(name: String) extends Attribute with trees.LeafNode[Expression] {
  override def exprId = throw new UnresolvedException(this, "exprId")
  override def dataType = throw new UnresolvedException(this, "dataType")
  override def nullable = throw new UnresolvedException(this, "nullable")
  override def qualifiers = throw new UnresolvedException(this, "qualifiers")
  override lazy val resolved = false

  override def newInstance() = this
  override def withNullability(newNullability: Boolean) = this
  override def withQualifiers(newQualifiers: Seq[String]) = this
  override def withName(newName: String) = UnresolvedAttribute(name)

  // Unresolved attributes are transient at compile time and don't get evaluated during execution.
  override def eval(input: Row = null): EvaluatedType =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  override def toString: String = s"'$name"
}

case class UnresolvedFunction(name: String, children: Seq[Expression]) extends Expression {
  override def dataType = throw new UnresolvedException(this, "dataType")
  override def foldable = throw new UnresolvedException(this, "foldable")
  override def nullable = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false

  // Unresolved functions are transient at compile time and don't get evaluated during execution.
  override def eval(input: Row = null): EvaluatedType =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  override def toString = s"'$name(${children.mkString(",")})"
}

/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...". A [[Star]] gets automatically expanded during analysis.
 */
trait Star extends Attribute with trees.LeafNode[Expression] {
  self: Product =>

  override def name = throw new UnresolvedException(this, "name")
  override def exprId = throw new UnresolvedException(this, "exprId")
  override def dataType = throw new UnresolvedException(this, "dataType")
  override def nullable = throw new UnresolvedException(this, "nullable")
  override def qualifiers = throw new UnresolvedException(this, "qualifiers")
  override lazy val resolved = false

  override def newInstance() = this
  override def withNullability(newNullability: Boolean) = this
  override def withQualifiers(newQualifiers: Seq[String]) = this
  override def withName(newName: String) = this

  // Star gets expanded at runtime so we never evaluate a Star.
  override def eval(input: Row = null): EvaluatedType =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  def expand(input: Seq[Attribute], resolver: Resolver): Seq[NamedExpression]
}


/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...".
 *
 * @param table an optional table that should be the target of the expansion.  If omitted all
 *              tables' columns are produced.
 */
case class UnresolvedStar(table: Option[String]) extends Star {

  override def expand(input: Seq[Attribute], resolver: Resolver): Seq[NamedExpression] = {
    val expandedAttributes: Seq[Attribute] = table match {
      // If there is no table specified, use all input attributes.
      case None => input
      // If there is a table, pick out attributes that are part of this table.
      case Some(t) => input.filter(_.qualifiers.filter(resolver(_, t)).nonEmpty)
    }
    expandedAttributes.zip(input).map {
      case (n: NamedExpression, _) => n
      case (e, originalAttribute) =>
        Alias(e, originalAttribute.name)(qualifiers = originalAttribute.qualifiers)
    }
  }

  override def toString = table.map(_ + ".").getOrElse("") + "*"
}

/**
 * Used to assign new names to Generator's output, such as hive udtf.
 * For example the SQL expression "stack(2, key, value, key, value) as (a, b)" could be represented
 * as follows:
 *  MultiAlias(stack_function, Seq(a, b))

 * @param child the computation being performed
 * @param names the names to be associated with each output of computing [[child]].
 */
case class MultiAlias(child: Expression, names: Seq[String])
  extends Attribute with trees.UnaryNode[Expression] {

  override def name = throw new UnresolvedException(this, "name")

  override def exprId = throw new UnresolvedException(this, "exprId")

  override def dataType = throw new UnresolvedException(this, "dataType")

  override def nullable = throw new UnresolvedException(this, "nullable")

  override def qualifiers = throw new UnresolvedException(this, "qualifiers")

  override lazy val resolved = false

  override def newInstance = this

  override def withNullability(newNullability: Boolean) = this

  override def withQualifiers(newQualifiers: Seq[String]) = this

  override def withName(newName: String) = this

  override def eval(input: Row = null): EvaluatedType =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  override def toString: String = s"$child AS $names"

}

/**
 * Represents all the resolved input attributes to a given relational operator. This is used
 * in the data frame DSL.
 *
 * @param expressions Expressions to expand.
 */
case class ResolvedStar(expressions: Seq[NamedExpression]) extends Star {
  override def expand(input: Seq[Attribute], resolver: Resolver): Seq[NamedExpression] = expressions
  override def toString = expressions.mkString("ResolvedStar(", ", ", ")")
}
