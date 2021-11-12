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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.connector.expressions.TimeTravelSpec
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Thrown when an invalid attempt is made to access a property of a tree that has yet to be fully
 * resolved.
 */
class UnresolvedException(function: String)
  extends AnalysisException(s"Invalid call to $function on unresolved object")

/**
 * Holds the name of a relation that has yet to be looked up in a catalog.
 *
 * @param multipartIdentifier table name
 * @param options options to scan this relation.
 */
case class UnresolvedRelation(
    multipartIdentifier: Seq[String],
    options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty(),
    override val isStreaming: Boolean = false,
    timeTravelSpec: Option[TimeTravelSpec] = None)
  extends LeafNode with NamedRelation {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  /** Returns a `.` separated name for this relation. */
  def tableName: String = multipartIdentifier.quoted

  override def name: String = tableName

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false

  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_RELATION)
}

object UnresolvedRelation {
  def apply(
      tableIdentifier: TableIdentifier,
      extraOptions: CaseInsensitiveStringMap,
      isStreaming: Boolean,
      timeTravelSpec: Option[TimeTravelSpec]): UnresolvedRelation = {
    UnresolvedRelation(
      tableIdentifier.database.toSeq :+ tableIdentifier.table,
      extraOptions,
      isStreaming,
      timeTravelSpec)
  }

  def apply(tableIdentifier: TableIdentifier): UnresolvedRelation =
    UnresolvedRelation(tableIdentifier.database.toSeq :+ tableIdentifier.table)
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
  extends LeafNode {

  lazy val expressionsResolved: Boolean = rows.forall(_.forall(_.resolved))
  override lazy val resolved = false
  override def output: Seq[Attribute] = Nil
}

/**
 * A table-valued function, e.g.
 * {{{
 *   select id from range(10);
 *
 *   // Assign alias names
 *   select t.a from range(10) t(a);
 * }}}
 *
 * @param name qualified name of this table-value function
 * @param functionArgs list of function arguments
 * @param outputNames alias names of function output columns. If these names given, an analyzer
 *                    adds [[Project]] to rename the output columns.
 */
case class UnresolvedTableValuedFunction(
    name: FunctionIdentifier,
    functionArgs: Seq[Expression],
    outputNames: Seq[String])
  extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false
}

object UnresolvedTableValuedFunction {
  def apply(
      name: String,
      functionArgs: Seq[Expression],
      outputNames: Seq[String]): UnresolvedTableValuedFunction = {
    UnresolvedTableValuedFunction(FunctionIdentifier(name), functionArgs, outputNames)
  }
}

/**
 * Holds the name of an attribute that has yet to be resolved.
 */
case class UnresolvedAttribute(nameParts: Seq[String]) extends Attribute with Unevaluable {

  def name: String =
    nameParts.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")

  override def exprId: ExprId = throw new UnresolvedException("exprId")
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override def qualifier: Seq[String] = throw new UnresolvedException("qualifier")
  override lazy val resolved = false

  override def newInstance(): UnresolvedAttribute = this
  override def withNullability(newNullability: Boolean): UnresolvedAttribute = this
  override def withQualifier(newQualifier: Seq[String]): UnresolvedAttribute = this
  override def withName(newName: String): UnresolvedAttribute = UnresolvedAttribute.quoted(newName)
  override def withMetadata(newMetadata: Metadata): Attribute = this
  override def withExprId(newExprId: ExprId): UnresolvedAttribute = this
  override def withDataType(newType: DataType): Attribute = this
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_ATTRIBUTE)

  override def toString: String = s"'$name"

  override def sql: String = nameParts.map(quoteIfNeeded(_)).mkString(".")
}

object UnresolvedAttribute {
  /**
   * Creates an [[UnresolvedAttribute]], parsing segments separated by dots ('.').
   */
  def apply(name: String): UnresolvedAttribute =
    new UnresolvedAttribute(CatalystSqlParser.parseMultipartIdentifier(name))

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
   * We can use backtick only inside quoted name parts.
   */
  def parseAttributeName(name: String): Seq[String] = {
    def e = QueryCompilationErrors.attributeNameSyntaxError(name)
    val nameParts = scala.collection.mutable.ArrayBuffer.empty[String]
    val tmp = scala.collection.mutable.ArrayBuffer.empty[Char]
    var inBacktick = false
    var i = 0
    while (i < name.length) {
      val char = name(i)
      if (inBacktick) {
        if (char == '`') {
          if (i + 1 < name.length && name(i + 1) == '`') {
            tmp += '`'
            i += 1
          } else {
            inBacktick = false
            if (i + 1 < name.length && name(i + 1) != '.') throw e
          }
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

  override def elementSchema: StructType = throw new UnresolvedException("elementTypes")
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def foldable: Boolean = throw new UnresolvedException("foldable")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override lazy val resolved = false

  override def prettyName: String = name.unquotedString
  override def toString: String = s"'$name(${children.mkString(", ")})"

  override def eval(input: InternalRow = null): TraversableOnce[InternalRow] =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)

  override def terminate(): TraversableOnce[InternalRow] =
    throw QueryExecutionErrors.cannotTerminateGeneratorError(this)

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): UnresolvedGenerator = copy(children = newChildren)
}

case class UnresolvedFunction(
    nameParts: Seq[String],
    arguments: Seq[Expression],
    isDistinct: Boolean,
    filter: Option[Expression] = None,
    ignoreNulls: Boolean = false)
  extends Expression with Unevaluable {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def children: Seq[Expression] = arguments ++ filter.toSeq

  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override lazy val resolved = false
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_FUNCTION)

  override def prettyName: String = nameParts.quoted
  override def toString: String = {
    val distinct = if (isDistinct) "distinct " else ""
    s"'${nameParts.quoted}($distinct${children.mkString(", ")})"
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): UnresolvedFunction = {
    if (filter.isDefined) {
      copy(arguments = newChildren.dropRight(1), filter = Some(newChildren.last))
    } else {
      copy(arguments = newChildren)
    }
  }
}

object UnresolvedFunction {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  def apply(
      name: FunctionIdentifier,
      arguments: Seq[Expression],
      isDistinct: Boolean): UnresolvedFunction = {
    UnresolvedFunction(name.asMultipart, arguments, isDistinct)
  }

  def apply(name: String, arguments: Seq[Expression], isDistinct: Boolean): UnresolvedFunction = {
    UnresolvedFunction(Seq(name), arguments, isDistinct)
  }
}

/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...". A [[Star]] gets automatically expanded during analysis.
 */
abstract class Star extends LeafExpression with NamedExpression {

  override def name: String = throw new UnresolvedException("name")
  override def exprId: ExprId = throw new UnresolvedException("exprId")
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override def qualifier: Seq[String] = throw new UnresolvedException("qualifier")
  override def toAttribute: Attribute = throw new UnresolvedException("toAttribute")
  override def newInstance(): NamedExpression = throw new UnresolvedException("newInstance")
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

  /**
   * Returns true if the nameParts is a subset of the last elements of qualifier of the attribute.
   *
   * For example, the following should all return true:
   *   - `SELECT ns1.ns2.t.* FROM ns1.ns2.t` where nameParts is Seq("ns1", "ns2", "t") and
   *     qualifier is Seq("ns1", "ns2", "t").
   *   - `SELECT ns2.t.* FROM ns1.ns2.t` where nameParts is Seq("ns2", "t") and
   *     qualifier is Seq("ns1", "ns2", "t").
   *   - `SELECT t.* FROM ns1.ns2.t` where nameParts is Seq("t") and
   *     qualifier is Seq("ns1", "ns2", "t").
   */
  private def matchedQualifier(
      attribute: Attribute,
      nameParts: Seq[String],
      resolver: Resolver): Boolean = {
    val qualifierList = if (nameParts.length == attribute.qualifier.length) {
      attribute.qualifier
    } else {
      attribute.qualifier.takeRight(nameParts.length)
    }
    nameParts.corresponds(qualifierList)(resolver)
  }

  def isQualifiedByTable(input: LogicalPlan, resolver: Resolver): Boolean = {
    target.exists(nameParts => input.output.exists(matchedQualifier(_, nameParts, resolver)))
  }

  override def expand(
      input: LogicalPlan,
      resolver: Resolver): Seq[NamedExpression] = {
    // If there is no table specified, use all non-hidden input attributes.
    if (target.isEmpty) return input.output

    // If there is a table specified, use hidden input attributes as well
    val hiddenOutput = input.metadataOutput.filter(_.supportsQualifiedStar)
    val expandedAttributes = (hiddenOutput ++ input.output).filter(
      matchedQualifier(_, target.get, resolver))

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
          throw QueryCompilationErrors.starExpandDataTypeNotSupportedError(target.get)
      }
    } else {
      val from = input.inputSet.map(_.name).mkString(", ")
      val targetString = target.get.mkString(".")
      throw QueryCompilationErrors.cannotResolveStarExpandGivenInputColumnsError(
        targetString, from)
    }
  }

  override def toString: String = target.map(_ + ".").getOrElse("") + "*"
}

/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT `(id)?+.+` FROM ...".
 *
 * @param table an optional table that should be the target of the expansion.  If omitted all
 *              tables' columns are produced.
 */
case class UnresolvedRegex(regexPattern: String, table: Option[String], caseSensitive: Boolean)
  extends Star with Unevaluable {
  override def expand(input: LogicalPlan, resolver: Resolver): Seq[NamedExpression] = {
    val pattern = if (caseSensitive) regexPattern else s"(?i)$regexPattern"
    table match {
      // If there is no table specified, use all input attributes that match expr
      case None => input.output.filter(_.name.matches(pattern))
      // If there is a table, pick out attributes that are part of this table that match expr
      case Some(t) => input.output.filter(a => a.qualifier.nonEmpty &&
        resolver(a.qualifier.last, t)).filter(_.name.matches(pattern))
    }
  }

  override def toString: String = table.map(_ + "." + regexPattern).getOrElse(regexPattern)
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
  extends UnaryExpression with NamedExpression with Unevaluable {

  override def name: String = throw new UnresolvedException("name")

  override def exprId: ExprId = throw new UnresolvedException("exprId")

  override def dataType: DataType = throw new UnresolvedException("dataType")

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override def qualifier: Seq[String] = throw new UnresolvedException("qualifier")

  override def toAttribute: Attribute = throw new UnresolvedException("toAttribute")

  override def newInstance(): NamedExpression = throw new UnresolvedException("newInstance")

  final override val nodePatterns: Seq[TreePattern] = Seq(MULTI_ALIAS)

  override lazy val resolved = false

  override def toString: String = s"$child AS $names"

  override protected def withNewChildInternal(newChild: Expression): MultiAlias =
    copy(child = newChild)
}

/**
 * Represents all the resolved input attributes to a given relational operator. This is used
 * in the data frame DSL.
 *
 * @param expressions Expressions to expand.
 */
case class ResolvedStar(expressions: Seq[NamedExpression]) extends Star with Unevaluable {
  override def newInstance(): NamedExpression = throw new UnresolvedException("newInstance")
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
  extends BinaryExpression with Unevaluable {

  override def left: Expression = child
  override def right: Expression = extraction

  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override lazy val resolved = false

  override def toString: String = s"$child[$extraction]"
  override def sql: String = s"${child.sql}[${extraction.sql}]"

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): UnresolvedExtractValue = {
      copy(child = newLeft, extraction = newRight)
  }
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

  override def toAttribute: Attribute = throw new UnresolvedException("toAttribute")
  override def qualifier: Seq[String] = throw new UnresolvedException("qualifier")
  override def exprId: ExprId = throw new UnresolvedException("exprId")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def name: String = throw new UnresolvedException("name")
  override def newInstance(): NamedExpression = throw new UnresolvedException("newInstance")
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_ALIAS)

  override lazy val resolved = false

  override protected def withNewChildInternal(newChild: Expression): UnresolvedAlias =
    copy(child = newChild)
}

/**
 * Aliased column names resolved by positions for subquery. We could add alias names for output
 * columns in the subquery:
 * {{{
 *   // Assign alias names for output columns
 *   SELECT col1, col2 FROM testData AS t(col1, col2);
 * }}}
 *
 * @param outputColumnNames the [[LogicalPlan]] on which this subquery column aliases apply.
 * @param child the logical plan of this subquery.
 */
case class UnresolvedSubqueryColumnAliases(
    outputColumnNames: Seq[String],
    child: LogicalPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false

  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_SUBQUERY_COLUMN_ALIAS)

  override protected def withNewChildInternal(
    newChild: LogicalPlan): UnresolvedSubqueryColumnAliases = copy(child = newChild)
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
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override lazy val resolved = false
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_DESERIALIZER)

  override protected def withNewChildInternal(newChild: Expression): UnresolvedDeserializer =
    copy(deserializer = newChild)
}

case class GetColumnByOrdinal(ordinal: Int, dataType: DataType) extends LeafExpression
  with Unevaluable with NonSQLExpression {
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override lazy val resolved = false
}

case class GetViewColumnByNameAndOrdinal(
    viewName: String,
    colName: String,
    ordinal: Int,
    expectedNumCandidates: Int,
    // viewDDL is used to help user fix incompatible schema issue for permanent views
    // it will be None for temp views.
    viewDDL: Option[String])
  extends LeafExpression with Unevaluable with NonSQLExpression {
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override lazy val resolved = false
  override def stringArgs: Iterator[Any] = super.stringArgs.toSeq.dropRight(1).toIterator
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
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override lazy val resolved = false
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_ORDINAL)
}

/**
 * Represents unresolved having clause, the child for it can be Aggregate, GroupingSets, Rollup
 * and Cube. It is turned by the analyzer into a Filter.
 */
case class UnresolvedHaving(
    havingCondition: Expression,
    child: LogicalPlan)
  extends UnaryNode {
  override lazy val resolved: Boolean = false
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): UnresolvedHaving =
    copy(child = newChild)
}

/**
 * A place holder expression used in random functions, will be replaced after analyze.
 */
case object UnresolvedSeed extends LeafExpression with Unevaluable {
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override lazy val resolved = false
}

/**
 * An intermediate expression to hold a resolved (nested) column. Some rules may need to undo the
 * column resolution and use this expression to keep the original column name.
 */
case class TempResolvedColumn(child: Expression, nameParts: Seq[String]) extends UnaryExpression
  with Unevaluable {
  override lazy val canonicalized = child.canonicalized
  override def dataType: DataType = child.dataType
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
