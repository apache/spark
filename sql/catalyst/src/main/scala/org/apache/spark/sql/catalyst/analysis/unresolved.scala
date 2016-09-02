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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, NonSQLPlan}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Thrown when an invalid attempt is made to access a property of a tree that has yet to be fully
 * resolved.
 */
class UnresolvedException[TreeType <: TreeNode[_]](tree: TreeType, function: String)
  extends TreeNodeException(tree, s"Invalid call to $function on unresolved object", null)

/**
 * Holds the name of a relation that has yet to be looked up in a catalog.
 */
case class UnresolvedRelation(
    tableIdentifier: TableIdentifier,
    alias: Option[String] = None) extends LeafNode {

  /** Returns a `.` separated name for this relation. */
  def tableName: String = tableIdentifier.unquotedString

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false

  override def sql: String = tableName
}

/**
 * An inline table that has not been resolved yet. Once resolved, it is turned by the analyzer into
 * a [[org.apache.spark.sql.catalyst.plans.logical.LocalRelation]].
 *
 * @param names list of column names
 * @param rows expressions for the data
 */
case class UnresolvedInlineTable(
    names: Seq[String],
    rows: Seq[Seq[Expression]])
  extends LeafNode with NonSQLPlan {

  lazy val expressionsResolved: Boolean = rows.forall(_.forall(_.resolved))
  override lazy val resolved = false
  override def output: Seq[Attribute] = Nil
}

/**
 * A table-valued function, e.g.
 * {{{
 *   select * from range(10);
 * }}}
 */
case class UnresolvedTableValuedFunction(functionName: String, functionArgs: Seq[Expression])
  extends LeafNode with NonSQLPlan {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false
}

/**
 * Holds the name of an attribute that has yet to be resolved.
 */
case class UnresolvedAttribute(nameParts: Seq[String]) extends Attribute with Unevaluable {

  def name: String =
    nameParts.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")

  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override def qualifier: Option[String] = throw new UnresolvedException(this, "qualifier")
  override lazy val resolved = false

  override def newInstance(): UnresolvedAttribute = this
  override def withNullability(newNullability: Boolean): UnresolvedAttribute = this
  override def withQualifier(newQualifier: Option[String]): UnresolvedAttribute = this
  override def withName(newName: String): UnresolvedAttribute = UnresolvedAttribute.quoted(newName)

  override def toString: String = s"'$name"

  override def sql: String = quoteIdentifier(name)
}

object UnresolvedAttribute {
  /**
   * Creates an [[UnresolvedAttribute]], parsing segments separated by dots ('.').
   */
  def apply(name: String): UnresolvedAttribute = new UnresolvedAttribute(name.split("\\."))

  /**
   * Creates an [[UnresolvedAttribute]], from a single quoted string (for example using backticks in
   * HiveQL.  Since the string is consider quoted, no processing is done on the name.
   */
  def quoted(name: String): UnresolvedAttribute = new UnresolvedAttribute(Seq(name))

  /**
   * Creates an [[UnresolvedAttribute]] from a string in an embedded language.  In this case
   * we treat it as a quoted identifier, except for '.', which must be further quoted using
   * backticks if it is part of a column name.
   */
  def quotedString(name: String): UnresolvedAttribute =
    new UnresolvedAttribute(parseAttributeName(name))

  /**
   * Used to split attribute name by dot with backticks rule.
   * Backticks must appear in pairs, and the quoted string must be a complete name part,
   * which means `ab..c`e.f is not allowed.
   * Escape character is not supported now, so we can't use backtick inside name part.
   */
  def parseAttributeName(name: String): Seq[String] = {
    def e = new AnalysisException(s"syntax error in attribute name: $name")
    val nameParts = scala.collection.mutable.ArrayBuffer.empty[String]
    val tmp = scala.collection.mutable.ArrayBuffer.empty[Char]
    var inBacktick = false
    var i = 0
    while (i < name.length) {
      val char = name(i)
      if (inBacktick) {
        if (char == '`') {
          inBacktick = false
          if (i + 1 < name.length && name(i + 1) != '.') throw e
        } else {
          tmp += char
        }
      } else {
        if (char == '`') {
          if (tmp.nonEmpty) throw e
          inBacktick = true
        } else if (char == '.') {
          if (name(i - 1) == '.' || i == name.length - 1) throw e
          nameParts += tmp.mkString
          tmp.clear()
        } else {
          tmp += char
        }
      }
      i += 1
    }
    if (inBacktick) throw e
    nameParts += tmp.mkString
    nameParts.toSeq
  }
}

/**
 * Represents an unresolved generator, which will be created by the parser for
 * the [[org.apache.spark.sql.catalyst.plans.logical.Generate]] operator.
 * The analyzer will resolve this generator.
 */
case class UnresolvedGenerator(name: FunctionIdentifier, children: Seq[Expression])
  extends Generator {

  override def elementSchema: StructType = throw new UnresolvedException(this, "elementTypes")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false

  override def prettyName: String = name.unquotedString
  override def toString: String = s"'$name(${children.mkString(", ")})"

  override def eval(input: InternalRow = null): TraversableOnce[InternalRow] =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  override def terminate(): TraversableOnce[InternalRow] =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")
}

case class UnresolvedFunction(
    name: FunctionIdentifier,
    children: Seq[Expression],
    isDistinct: Boolean)
  extends Expression with Unevaluable {

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false

  override def prettyName: String = name.unquotedString
  override def toString: String = s"'$name(${children.mkString(", ")})"
}

object UnresolvedFunction {
  def apply(name: String, children: Seq[Expression], isDistinct: Boolean): UnresolvedFunction = {
    UnresolvedFunction(FunctionIdentifier(name, None), children, isDistinct)
  }
}

/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...". A [[Star]] gets automatically expanded during analysis.
 */
abstract class Star extends LeafExpression with NamedExpression {

  override def name: String = throw new UnresolvedException(this, "name")
  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override def qualifier: Option[String] = throw new UnresolvedException(this, "qualifier")
  override def toAttribute: Attribute = throw new UnresolvedException(this, "toAttribute")
  override def newInstance(): NamedExpression = throw new UnresolvedException(this, "newInstance")
  override lazy val resolved = false

  def expand(input: LogicalPlan, resolver: Resolver): Seq[NamedExpression]
}


/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...".
 *
 * This is also used to expand structs. For example:
 * "SELECT record.* from (SELECT struct(a,b,c) as record ...)
 *
 * @param target an optional name that should be the target of the expansion.  If omitted all
 *              targets' columns are produced. This can either be a table name or struct name. This
 *              is a list of identifiers that is the path of the expansion.
 */
case class UnresolvedStar(target: Option[Seq[String]]) extends Star with Unevaluable {

  override def expand(input: LogicalPlan, resolver: Resolver): Seq[NamedExpression] = {
    // If there is no table specified, use all input attributes.
    if (target.isEmpty) return input.output

    val expandedAttributes =
      if (target.get.size == 1) {
        // If there is a table, pick out attributes that are part of this table.
        input.output.filter(_.qualifier.exists(resolver(_, target.get.head)))
      } else {
        List()
      }
    if (expandedAttributes.nonEmpty) return expandedAttributes

    // Try to resolve it as a struct expansion. If there is a conflict and both are possible,
    // (i.e. [name].* is both a table and a struct), the struct path can always be qualified.
    val attribute = input.resolve(target.get, resolver)
    if (attribute.isDefined) {
      // This target resolved to an attribute in child. It must be a struct. Expand it.
      attribute.get.dataType match {
        case s: StructType => s.zipWithIndex.map {
          case (f, i) =>
            val extract = GetStructField(attribute.get, i)
            Alias(extract, f.name)()
        }

        case _ =>
          throw new AnalysisException("Can only star expand struct data types. Attribute: `" +
            target.get + "`")
      }
    } else {
      val from = input.inputSet.map(_.name).mkString(", ")
      val targetString = target.get.mkString(".")
      throw new AnalysisException(s"cannot resolve '$targetString.*' give input columns '$from'")
    }
  }

  override def toString: String = target.map(_ + ".").getOrElse("") + "*"
}

/**
 * Used to assign new names to Generator's output, such as hive udtf.
 * For example the SQL expression "stack(2, key, value, key, value) as (a, b)" could be represented
 * as follows:
 *  MultiAlias(stack_function, Seq(a, b))
 *

 * @param child the computation being performed
 * @param names the names to be associated with each output of computing [[child]].
 */
case class MultiAlias(child: Expression, names: Seq[String])
  extends UnaryExpression with NamedExpression with CodegenFallback {

  override def name: String = throw new UnresolvedException(this, "name")

  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")

  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")

  override def qualifier: Option[String] = throw new UnresolvedException(this, "qualifier")

  override def toAttribute: Attribute = throw new UnresolvedException(this, "toAttribute")

  override def newInstance(): NamedExpression = throw new UnresolvedException(this, "newInstance")

  override lazy val resolved = false

  override def toString: String = s"$child AS $names"

}

/**
 * Represents all the resolved input attributes to a given relational operator. This is used
 * in the data frame DSL.
 *
 * @param expressions Expressions to expand.
 */
case class ResolvedStar(expressions: Seq[NamedExpression]) extends Star with Unevaluable {
  override def newInstance(): NamedExpression = throw new UnresolvedException(this, "newInstance")
  override def expand(input: LogicalPlan, resolver: Resolver): Seq[NamedExpression] = expressions
  override def toString: String = expressions.mkString("ResolvedStar(", ", ", ")")
}

/**
 * Extracts a value or values from an Expression
 *
 * @param child The expression to extract value from,
 *              can be Map, Array, Struct or array of Structs.
 * @param extraction The expression to describe the extraction,
 *                   can be key of Map, index of Array, field name of Struct.
 */
case class UnresolvedExtractValue(child: Expression, extraction: Expression)
  extends UnaryExpression with Unevaluable {

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false

  override def toString: String = s"$child[$extraction]"
  override def sql: String = s"${child.sql}[${extraction.sql}]"
}

/**
 * Holds the expression that has yet to be aliased.
 *
 * @param child The computation that is needs to be resolved during analysis.
 * @param aliasFunc The function if specified to be called to generate an alias to associate
 *                  with the result of computing [[child]]
 *
 */
case class UnresolvedAlias(
    child: Expression,
    aliasFunc: Option[Expression => String] = None)
  extends UnaryExpression with NamedExpression with Unevaluable {

  override def toAttribute: Attribute = throw new UnresolvedException(this, "toAttribute")
  override def qualifier: Option[String] = throw new UnresolvedException(this, "qualifier")
  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def name: String = throw new UnresolvedException(this, "name")
  override def newInstance(): NamedExpression = throw new UnresolvedException(this, "newInstance")

  override lazy val resolved = false
}

/**
 * Holds the deserializer expression and the attributes that are available during the resolution
 * for it.  Deserializer expression is a special kind of expression that is not always resolved by
 * children output, but by given attributes, e.g. the `keyDeserializer` in `MapGroups` should be
 * resolved by `groupingAttributes` instead of children output.
 *
 * @param deserializer The unresolved deserializer expression
 * @param inputAttributes The input attributes used to resolve deserializer expression, can be empty
 *                        if we want to resolve deserializer by children output.
 */
case class UnresolvedDeserializer(deserializer: Expression, inputAttributes: Seq[Attribute] = Nil)
  extends UnaryExpression with Unevaluable with NonSQLExpression {
  // The input attributes used to resolve deserializer expression must be all resolved.
  require(inputAttributes.forall(_.resolved), "Input attributes must all be resolved.")

  override def child: Expression = deserializer
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false
}

case class GetColumnByOrdinal(ordinal: Int, dataType: DataType) extends LeafExpression
  with Unevaluable with NonSQLExpression {
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false
}

/**
 * Represents unresolved ordinal used in order by or group by.
 *
 * For example:
 * {{{
 *   select a from table order by 1
 *   select a   from table group by 1
 * }}}
 * @param ordinal ordinal starts from 1, instead of 0
 */
case class UnresolvedOrdinal(ordinal: Int)
    extends LeafExpression with Unevaluable with NonSQLExpression {
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false
}
