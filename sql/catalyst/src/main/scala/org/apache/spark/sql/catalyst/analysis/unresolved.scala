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

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, LeafNode, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.connector.catalog.TableWritePrivilege
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * Thrown when an invalid attempt is made to access a property of a tree that has yet to be fully
 * resolved.
 */
class UnresolvedException(function: String)
  extends SparkException(
    errorClass = "INTERNAL_ERROR",
    messageParameters = Map("message" -> s"Invalid call to $function on unresolved object"),
    cause = null)

/** Parent trait for unresolved node types */
trait UnresolvedNode extends LogicalPlan {
  override def output: Seq[Attribute] = Nil
  override lazy val resolved: Boolean = false
}

/** Parent trait for unresolved leaf node types */
trait UnresolvedLeafNode extends LeafNode with UnresolvedNode

/** Parent trait for unresolved unary node types */
trait UnresolvedUnaryNode extends UnaryNode with UnresolvedNode

/**
 * A logical plan placeholder that holds the identifier clause string expression. It will be
 * replaced by the actual logical plan with the evaluated identifier string.
 */
case class PlanWithUnresolvedIdentifier(
    identifierExpr: Expression,
    children: Seq[LogicalPlan],
    planBuilder: (Seq[String], Seq[LogicalPlan]) => LogicalPlan)
  extends UnresolvedNode {

  def this(identifierExpr: Expression, planBuilder: Seq[String] => LogicalPlan) = {
    this(identifierExpr, Nil, (ident, _) => planBuilder(ident))
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_IDENTIFIER)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan =
    copy(identifierExpr, newChildren, planBuilder)
}

/**
 * A logical plan placeholder which delays CTE resolution
 * to moment when PlanWithUnresolvedIdentifier gets resolved
 */
case class UnresolvedWithCTERelations(
   unresolvedPlan: LogicalPlan,
   cteRelations: Seq[(String, CTERelationDef)])
  extends UnresolvedLeafNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_IDENTIFIER_WITH_CTE)
}

/**
 * An expression placeholder that holds the identifier clause string expression. It will be
 * replaced by the actual expression with the evaluated identifier string.
 *
 * Note, the `exprBuilder` is a lambda and may hide other expressions from the expression tree. To
 * avoid it, this placeholder has a field to hold other expressions, so that they can be properly
 * transformed by catalyst rules.
 */
case class ExpressionWithUnresolvedIdentifier(
    identifierExpr: Expression,
    otherExprs: Seq[Expression],
    exprBuilder: (Seq[String], Seq[Expression]) => Expression)
  extends Expression with Unevaluable {

  def this(identifierExpr: Expression, exprBuilder: Seq[String] => Expression) = {
    this(identifierExpr, Nil, (ident, _) => exprBuilder(ident))
  }

  override lazy val resolved = false
  override def children: Seq[Expression] = identifierExpr +: otherExprs
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_IDENTIFIER)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(identifierExpr = newChildren.head, otherExprs = newChildren.drop(1))
  }
}

/**
 * Holds the name of a relation that has yet to be looked up in a catalog.
 *
 * @param multipartIdentifier table name, the location of files or Kafka topic name, etc.
 * @param options options to scan this relation.
 */
case class UnresolvedRelation(
    multipartIdentifier: Seq[String],
    options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty(),
    override val isStreaming: Boolean = false)
  extends UnresolvedLeafNode with NamedRelation {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  /** Returns a `.` separated name for this relation. */
  def tableName: String = multipartIdentifier.quoted

  override def name: String = tableName

  def requireWritePrivileges(privileges: Seq[TableWritePrivilege]): UnresolvedRelation = {
    if (privileges.nonEmpty) {
      val newOptions = new java.util.HashMap[String, String]
      newOptions.putAll(options)
      newOptions.put(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES, privileges.mkString(","))
      copy(options = new CaseInsensitiveStringMap(newOptions))
    } else {
      this
    }
  }

  def clearWritePrivileges: UnresolvedRelation = {
    if (options.containsKey(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES)) {
      val newOptions = new java.util.HashMap[String, String]
      newOptions.putAll(options)
      newOptions.remove(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES)
      copy(options = new CaseInsensitiveStringMap(newOptions))
    } else {
      this
    }
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_RELATION)
}

object UnresolvedRelation {
  // An internal option of `UnresolvedRelation` to specify the required write privileges when
  // writing data to this relation.
  val REQUIRED_WRITE_PRIVILEGES = "__required_write_privileges__"

  def apply(
      tableIdentifier: TableIdentifier,
      extraOptions: CaseInsensitiveStringMap,
      isStreaming: Boolean): UnresolvedRelation = {
    UnresolvedRelation(tableIdentifier.nameParts, extraOptions, isStreaming)
  }

  def apply(tableIdentifier: TableIdentifier): UnresolvedRelation =
    UnresolvedRelation(tableIdentifier.nameParts)
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
  extends UnresolvedLeafNode {

  lazy val expressionsResolved: Boolean = rows.forall(_.forall(_.resolved))
}

/**
 * An resolved inline table that holds all the expressions that were checked for
 * the right shape and common data types.
 * This is a preparation step for [[org.apache.spark.sql.catalyst.optimizer.EvalInlineTables]] which
 * will produce a [[org.apache.spark.sql.catalyst.plans.logical.LocalRelation]]
 * for this inline table.
 *
 * @param output list of column attributes
 * @param rows expressions for the data rows
 */
case class ResolvedInlineTable(rows: Seq[Seq[Expression]], output: Seq[Attribute])
  extends LeafNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(INLINE_TABLE_EVAL)
}

/**
 * A table-valued function, e.g.
 * {{{
 *   select id from range(10);
 * }}}
 *
 * @param name user-specified name of this table-value function
 * @param functionArgs list of function arguments
 */
case class UnresolvedTableValuedFunction(
    name: Seq[String],
    functionArgs: Seq[Expression])
  extends UnresolvedLeafNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_TABLE_VALUED_FUNCTION)
}

object UnresolvedTableValuedFunction {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  def apply(
      name: String,
      functionArgs: Seq[Expression]): UnresolvedTableValuedFunction = {
    UnresolvedTableValuedFunction(Seq(name), functionArgs)
  }

  def apply(
      name: FunctionIdentifier,
      functionArgs: Seq[Expression]): UnresolvedTableValuedFunction = {
    UnresolvedTableValuedFunction(name.asMultipart, functionArgs)
  }
}

/**
 * A table-valued function with output column aliases, e.g.
 * {{{
 *   // Assign alias names
 *   select t.a from range(10) t(a);
 * }}}
 *
 * @param name user-specified name of the table-valued function
 * @param child logical plan of the table-valued function
 * @param outputNames alias names of function output columns. The analyzer adds [[Project]]
 *                    to rename the output columns.
 */
case class UnresolvedTVFAliases(
    name: Seq[String],
    child: LogicalPlan,
    outputNames: Seq[String]) extends UnresolvedUnaryNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_TVF_ALIASES)

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}

object UnresolvedTVFAliases {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  def apply(
      name: String,
      child: LogicalPlan,
      outputNames: Seq[String]): UnresolvedTVFAliases = {
    UnresolvedTVFAliases(Seq(name), child, outputNames)
  }

  def apply(
      name: FunctionIdentifier,
      child: LogicalPlan,
      outputNames: Seq[String]): UnresolvedTVFAliases = {
    UnresolvedTVFAliases(name.asMultipart, child, outputNames)
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

  /**
   * Returns true if this matches the token. This requires the attribute to only have one part in
   * its name and that matches the given token in a case insensitive way.
   */
  def equalsIgnoreCase(token: String): Boolean = {
    nameParts.length == 1 && nameParts.head.equalsIgnoreCase(token)
  }
}

object UnresolvedAttribute extends AttributeNameParser {
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

  override def eval(input: InternalRow = null): IterableOnce[InternalRow] =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)

  override def terminate(): IterableOnce[InternalRow] =
    throw QueryExecutionErrors.cannotTerminateGeneratorError(this)

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): UnresolvedGenerator = copy(children = newChildren)
}

/**
 * Represents an unresolved function that is being invoked. The analyzer will resolve the function
 * arguments first, then look up the function by name and arguments, and return an expression that
 * can be evaluated to get the result of this function invocation.
 */
case class UnresolvedFunction(
    nameParts: Seq[String],
    arguments: Seq[Expression],
    isDistinct: Boolean,
    filter: Option[Expression] = None,
    ignoreNulls: Boolean = false,
    orderingWithinGroup: Seq[SortOrder] = Seq.empty,
    isInternal: Boolean = false)
  extends Expression with Unevaluable {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def children: Seq[Expression] = arguments ++ filter.toSeq ++ orderingWithinGroup

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
      if (orderingWithinGroup.isEmpty) {
        copy(arguments = newChildren.dropRight(1), filter = Some(newChildren.last))
      } else {
        val nonArgs = newChildren.takeRight(orderingWithinGroup.length + 1)
        val newSortOrders = nonArgs.tail.asInstanceOf[Seq[SortOrder]]
        val newFilter = Some(nonArgs.head)
        val newArgs = newChildren.dropRight(orderingWithinGroup.length + 1)
        copy(arguments = newArgs, filter = newFilter, orderingWithinGroup = newSortOrders)
      }
    } else if (orderingWithinGroup.isEmpty) {
      copy(arguments = newChildren)
    } else {
      val newSortOrders =
        newChildren.takeRight(orderingWithinGroup.length).asInstanceOf[Seq[SortOrder]]
      val newArgs = newChildren.dropRight(orderingWithinGroup.length)
      copy(arguments = newArgs, orderingWithinGroup = newSortOrders)
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
 * "SELECT * FROM ..." or "SELECT * EXCEPT(...) FROM ..."
 *
 * This is also used to expand structs. For example:
 * "SELECT record.* from (SELECT struct(a,b,c) as record ...)
 *
 * @param target an optional name that should be the target of the expansion.  If omitted all
 *              targets' columns are produced. This can either be a table name or struct name. This
 *              is a list of identifiers that is the path of the expansion.
 *
 * This class provides the shared behavior between the classes for SELECT * ([[UnresolvedStar]])
 * and SELECT * EXCEPT ([[UnresolvedStarExcept]]). [[UnresolvedStar]] is just a case class of this,
 * while [[UnresolvedStarExcept]] adds some additional logic to the expand method.
  */
abstract class UnresolvedStarBase(target: Option[Seq[String]]) extends Star with Unevaluable {
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
  protected def matchedQualifier(
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
    expandStar(input.output, input.metadataOutput, input.resolve, input.inputSet.toSeq, resolver)
  }

  /**
   * Method used to expand a star. It uses output and metadata output attributes of the child
   * for the expansion and it supports both recursive and non-recursive data types.
   *
   * @param childOperatorOutput The output attributes of the child operator
   * @param childOperatorMetadataOutput The metadata output attributes of the child operator
   * @param resolve A function to resolve the given name parts to an attribute
   * @param suggestedAttributes A list of attributes that are suggested for expansion
   * @param resolver The resolver used to match the name parts
   */
  def expandStar(
      childOperatorOutput: Seq[Attribute],
      childOperatorMetadataOutput: Seq[Attribute],
      resolve: (Seq[String], Resolver) => Option[NamedExpression],
      suggestedAttributes: Seq[Attribute],
      resolver: Resolver): Seq[NamedExpression] = {
    // If there is no table specified, use all non-hidden input attributes.
    if (target.isEmpty) return childOperatorOutput

    // If there is a table specified, use hidden input attributes as well
    val hiddenOutput = childOperatorMetadataOutput.filter(_.qualifiedAccessOnly)
      // Remove the qualified-access-only restriction immediately. The expanded attributes will be
      // put in a logical plan node and becomes normal attributes. They can still keep the special
      // attribute metadata to indicate that they are from metadata columns, but they should not
      // keep any restrictions that may break column resolution for normal attributes.
      // See SPARK-42084 for more details.
      .map(_.markAsAllowAnyAccess())
    val expandedAttributes = (hiddenOutput ++ childOperatorOutput).filter(
      matchedQualifier(_, target.get, resolver))

    if (expandedAttributes.nonEmpty) return expandedAttributes

    // Try to resolve it as a struct expansion. If there is a conflict and both are possible,
    // (i.e. [name].* is both a table and a struct), the struct path can always be qualified.
    val attribute = resolve(target.get, resolver)
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
      val from = suggestedAttributes.map(_.name).map(toSQLId).mkString(", ")
      val targetString = target.get.mkString(".")
      throw QueryCompilationErrors.cannotResolveStarExpandGivenInputColumnsError(
        targetString, from)
    }
  }

  override def toString: String = target.map(_.mkString("", ".", ".")).getOrElse("") + "*"
}

/**
 * Represents some of the input attributes to a given relational operator, for example in
 * "SELECT * EXCEPT(a) FROM ...".
 *
 * @param target an optional name that should be the target of the expansion. If omitted all
 *              targets' columns are produced. This can only be a table name. This
 *              is a list of identifiers that is the path of the expansion.
 *
 * @param excepts a list of names that should be excluded from the expansion.
 *
 */
case class UnresolvedStarExcept(target: Option[Seq[String]], excepts: Seq[Seq[String]])
  extends UnresolvedStarBase(target) {

  /**
   * We expand the * EXCEPT by the following three steps:
   * 1. use the original .expand() to get top-level column list or struct expansion
   * 2. resolve excepts (with respect to the Seq[NamedExpression] returned from (1))
   * 3. filter the expanded columns with the resolved except list. recursively apply filtering in
   *    case of nested columns in the except list (in order to rewrite structs)
   */
  override def expand(input: LogicalPlan, resolver: Resolver): Seq[NamedExpression] = {
    // Use the UnresolvedStarBase expand method to get a seq of NamedExpressions corresponding to
    // the star expansion. This will yield a list of top-level columns from the logical plan's
    // output, or in the case of struct expansion (e.g. target=`x` for SELECT x.*) it will give
    // a seq of Alias wrapping the struct field extraction.
    val expandedCols = super.expand(input, resolver)

    // resolve except list with respect to the expandedCols
    val resolvedExcepts = excepts.map { exceptParts =>
      AttributeSeq(expandedCols.map(_.toAttribute)).resolve(exceptParts, resolver).getOrElse {
        val orderedCandidates = StringUtils.orderSuggestedIdentifiersBySimilarity(
          UnresolvedAttribute(exceptParts).name, expandedCols.map(a => a.qualifier :+ a.name))
        // if target is defined and expandedCols does not include any Attributes, it must be struct
        // expansion; give message suggesting to use unqualified names of nested fields.
        throw QueryCompilationErrors
          .unresolvedColumnError(UnresolvedAttribute(exceptParts).name, orderedCandidates)
      }
    }

    // Convert each resolved except into a pair of (col: Attribute, nestedColumn) representing the
    // top level column in expandedCols that we must 'filter' based on nestedColumn.
    @scala.annotation.tailrec
    def getRootColumn(expr: Expression, nestedColumn: Seq[String] = Nil)
      : (NamedExpression, Seq[String]) = expr match {
      case GetStructField(fieldExpr, _, Some(fieldName)) =>
        getRootColumn(fieldExpr, fieldName +: nestedColumn)
      case e: NamedExpression => e -> nestedColumn
      case other: ExtractValue => throw new AnalysisException(
        errorClass = "EXCEPT_NESTED_COLUMN_INVALID_TYPE",
        messageParameters = Map("columnName" -> other.sql, "dataType" -> other.dataType.toString))
    }
    // An exceptPair represents the column in expandedCols along with the path of a nestedColumn
    // that should be except-ed. Consider two examples:
    // 1. excepting the entire col1 = (col1, Seq())
    // 2. excepting a nested field in col2, col2.a.b = (col2, Seq(a, b))
    // INVARIANT: we rely on the structure of the resolved except being an Alias of GetStructField
    // in the case of nested columns.
    val exceptPairs = resolvedExcepts.map {
      case Alias(exceptExpr, name) => getRootColumn(exceptExpr)
      case except: NamedExpression => except -> Seq.empty
    }

    // Filter columns which correspond to ones listed in the except list and return a new list of
    // columns which exclude the columns in the except list. The 'filtering' manifests as either
    // dropping the column from the list of columns we return, or rewriting the projected column in
    // order to remove excepts that refer to nested columns. For example, with the example above:
    // excepts = Seq(
    //   (col1, Seq()),    => filter col1 from the output
    //   (col2, Seq(a, b)) => rewrite col2 in the output so that it doesn't include the nested field
    // )                      corresponding to col2.a.b
    //
    // This occurs in two steps:
    // 1. group the excepts by the column they refer to (groupedExcepts)
    // 2. filter/rewrite input columns based on four cases:
    //    a. column doesn't match any groupedExcepts => column unchanged
    //    b. column exists in groupedExcepts and:
    //       i.   none of remainingExcepts are empty => recursively apply filterColumns over the
    //            struct fields in order to rewrite the struct
    //       ii.  a remainingExcept is empty, but there are multiple remainingExcepts => we must
    //            have duplicate/overlapping excepts - throw an error
    //       iii. [otherwise] remainingExcept is exactly Seq(Seq()) => this is the base 'filtering'
    //            case. we omit the column from the output (this is a column we would like to
    //            except). NOTE: this case isn't explicitly listed in the `collect` below since we
    //            'collect' columns which match the cases above and omit ones that fall into this
    //            remaining case.
    def filterColumns(columns: Seq[NamedExpression], excepts: Seq[(NamedExpression, Seq[String])])
      : Seq[NamedExpression] = {
      // group the except pairs by the column they refer to. NOTE: no groupMap until scala 2.13
      val groupedExcepts: AttributeMap[Seq[Seq[String]]] =
        AttributeMap(excepts.groupBy(_._1.toAttribute).transform((_, v) => v.map(_._2)))

      // map input columns while searching for the except entry corresponding to the current column
      columns.map(col => col -> groupedExcepts.get(col.toAttribute)).collect {
        // pass through columns that don't match anything in groupedExcepts
        case (col, None) => col
        // found a match but nestedExcepts has remaining excepts - recurse to rewrite the struct
        case (col, Some(nestedExcepts)) if nestedExcepts.forall(_.nonEmpty) =>
          val fields = col.dataType match {
            case s: StructType => s.fields
            // we shouldn't be here since we EXCEPT_NEXTED_COLUMN_INVALID_TYPE in getRootColumn
            // for this case - just throw internal error
            case _ => throw SparkException.internalError("Invalid column type")
          }
          val extractedFields = fields.zipWithIndex.map { case (f, i) =>
            Alias(GetStructField(col, i), f.name)()
          }
          val newExcepts = nestedExcepts.map { nestedExcept =>
            // INVARIANT: we cannot have duplicate column names in nested columns, thus, this `head`
            // will find the one and only column corresponding to the correct extractedField.
            extractedFields.collectFirst { case col if resolver(col.name, nestedExcept.head) =>
              col.toAttribute -> nestedExcept.tail
            }.get
          }
          Alias(CreateStruct(
            filterColumns(extractedFields.toImmutableArraySeq, newExcepts)), col.name)()
        // if there are multiple nestedExcepts but one is empty we must have overlapping except
        // columns. throw an error.
        case (col, Some(nestedExcepts)) if nestedExcepts.size > 1 =>
          throw new AnalysisException(
            errorClass = "EXCEPT_OVERLAPPING_COLUMNS",
            messageParameters = Map(
              "columns" -> this.excepts.map(_.mkString(".")).mkString(", ")))
      }
    }

    filterColumns(expandedCols, exceptPairs)
  }
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
case class UnresolvedStar(target: Option[Seq[String]]) extends UnresolvedStarBase(target)

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

  override def toString: String = s"$child AS ${names.mkString("(", ", ", ")")}"

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
 * Represents all input attributes to a given relational operator.
 * This is used in Spark Connect dataframe, for example:
 *    df1 = spark.createDataFrame([{"id": 1}])
 *    df2 = spark.createDataFrame([{"id": 1, "val": "v"}])
 *    df1.join(df2, "id").select(df1["*"])
 * @param planId the plan id of target node.
 */
case class UnresolvedDataFrameStar(planId: Long)
  extends LeafExpression with Unevaluable {
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override lazy val resolved = false
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_DF_STAR)
  override def toString: String = "UnresolvedDataFrameStar"
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

  override def toString: String = s"$prettyName(${child.toString})"

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
  extends UnresolvedUnaryNode {

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
  override def stringArgs: Iterator[Any] = super.stringArgs.toSeq.dropRight(1).iterator
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
  extends UnresolvedUnaryNode {
  override protected def withNewChildInternal(newChild: LogicalPlan): UnresolvedHaving =
    copy(child = newChild)
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_HAVING)
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
 * column resolution and use this expression to keep the original column name, or redo the column
 * resolution with a different priority if the analyzer has tried to resolve it with the default
 * priority before but failed (i.e. `hasTried` is true).
 */
case class TempResolvedColumn(
    child: Expression,
    nameParts: Seq[String],
    hasTried: Boolean = false) extends UnaryExpression
  with Unevaluable {
  // If it has been tried to be resolved but failed, mark it as unresolved so that other rules can
  // try to resolve it again.
  override lazy val resolved = child.resolved && !hasTried
  override lazy val canonicalized = child.canonicalized
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  // `TempResolvedColumn` is logically a leaf node. We should not count it as a missing reference
  // when resolving Filter/Sort/RepartitionByExpression. However, we should not make it a real
  // leaf node, as rules that update expr IDs should update `TempResolvedColumn.child` as well.
  override def references: AttributeSet = AttributeSet.empty
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
  final override val nodePatterns: Seq[TreePattern] = Seq(TEMP_RESOLVED_COLUMN)
}

/**
 * A place holder expression used in inverse distribution functions,
 * will be replaced after analyze.
 */
case object UnresolvedWithinGroup extends LeafExpression with Unevaluable {
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override lazy val resolved = false
}

case class UnresolvedTranspose(
    indices: Seq[Expression],
    child: LogicalPlan
) extends UnresolvedUnaryNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_TRANSPOSE)

  override protected def withNewChildInternal(newChild: LogicalPlan): UnresolvedTranspose =
    copy(child = newChild)
}

case class UnresolvedOuterReference(
    nameParts: Seq[String])
  extends LeafExpression with NamedExpression with Unevaluable {

  def name: String =
    nameParts.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")

  override def exprId: ExprId = throw new UnresolvedException("exprId")
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override def qualifier: Seq[String] = throw new UnresolvedException("qualifier")
  override lazy val resolved = false

  override def toAttribute: Attribute = throw new UnresolvedException("toAttribute")
  override def newInstance(): UnresolvedOuterReference = this

  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_OUTER_REFERENCE)
}

case class LazyOuterReference(
     nameParts: Seq[String])
  extends LeafExpression with NamedExpression with Unevaluable with LazyAnalysisExpression {

  def name: String =
    nameParts.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")

  override def exprId: ExprId = throw new UnresolvedException("exprId")
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override def qualifier: Seq[String] = throw new UnresolvedException("qualifier")

  override def toAttribute: Attribute = throw new UnresolvedException("toAttribute")
  override def newInstance(): NamedExpression = LazyOuterReference(nameParts)

  override def nodePatternsInternal(): Seq[TreePattern] = Seq(LAZY_OUTER_REFERENCE)

  override def prettyName: String = "outer"
  override def sql: String = s"$prettyName($name)"
}
