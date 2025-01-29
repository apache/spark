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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{
  ResolvedInlineTable,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedInlineTable,
  UnresolvedRelation,
  UnresolvedStar
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  BinaryArithmetic,
  Cast,
  ConditionalExpression,
  CreateNamedStruct,
  Expression,
  Literal,
  Predicate,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{
  Filter,
  GlobalLimit,
  LocalLimit,
  LocalRelation,
  LogicalPlan,
  OneRowRelation,
  Project,
  SubqueryAlias
}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf.HiveCaseSensitiveInferenceMode

/**
 * [[ResolverGuard]] is a class that checks if the operator that is yet to be analyzed
 * only consists of operators and expressions that are currently supported by the
 * single-pass analyzer.
 *
 * This is a one-shot object and should not be reused after [[apply]] call.
 */
class ResolverGuard(catalogManager: CatalogManager) extends SQLConfHelper {

  /**
   * Check the top level operator of the parsed operator.
   */
  def apply(operator: LogicalPlan): Boolean =
    checkConfValues() && checkVariables() && checkOperator(operator)

  /**
   * Check if all the operators are supported. For implemented ones, recursively check
   * their children. For unimplemented ones, return false.
   */
  private def checkOperator(operator: LogicalPlan): Boolean = operator match {
    case project: Project =>
      checkProject(project)
    case filter: Filter =>
      checkFilter(filter)
    case subqueryAlias: SubqueryAlias =>
      checkSubqueryAlias(subqueryAlias)
    case globalLimit: GlobalLimit =>
      checkGlobalLimit(globalLimit)
    case localLimit: LocalLimit =>
      checkLocalLimit(localLimit)
    case unresolvedRelation: UnresolvedRelation =>
      checkUnresolvedRelation(unresolvedRelation)
    case unresolvedInlineTable: UnresolvedInlineTable =>
      checkUnresolvedInlineTable(unresolvedInlineTable)
    case resolvedInlineTable: ResolvedInlineTable =>
      checkResolvedInlineTable(resolvedInlineTable)
    case localRelation: LocalRelation =>
      checkLocalRelation(localRelation)
    case oneRowRelation: OneRowRelation =>
      checkOneRowRelation(oneRowRelation)
    case _ =>
      false
  }

  /**
   * Method used to check if expressions are supported by the new analyzer.
   * For LeafNode types, we return true or false. For other ones, check their children.
   */
  private def checkExpression(expression: Expression): Boolean = {
    expression match {
      case alias: Alias =>
        checkAlias(alias)
      case unresolvedBinaryArithmetic: BinaryArithmetic =>
        checkUnresolvedBinaryArithmetic(unresolvedBinaryArithmetic)
      case unresolvedConditionalExpression: ConditionalExpression =>
        checkUnresolvedConditionalExpression(unresolvedConditionalExpression)
      case unresolvedCast: Cast =>
        checkUnresolvedCast(unresolvedCast)
      case unresolvedStar: UnresolvedStar =>
        checkUnresolvedStar(unresolvedStar)
      case unresolvedAlias: UnresolvedAlias =>
        checkUnresolvedAlias(unresolvedAlias)
      case unresolvedAttribute: UnresolvedAttribute =>
        checkUnresolvedAttribute(unresolvedAttribute)
      case unresolvedPredicate: Predicate =>
        checkUnresolvedPredicate(unresolvedPredicate)
      case literal: Literal =>
        checkLiteral(literal)
      case attributeReference: AttributeReference =>
        checkAttributeReference(attributeReference)
      case createNamedStruct: CreateNamedStruct =>
        checkCreateNamedStruct(createNamedStruct)
      case unresolvedFunction: UnresolvedFunction =>
        checkUnresolvedFunction(unresolvedFunction)
      case _ =>
        false
    }
  }

  private def checkProject(project: Project) = {
    checkOperator(project.child) && project.projectList.forall(checkExpression)
  }

  private def checkFilter(unresolvedFilter: Filter) =
    checkOperator(unresolvedFilter.child) && checkExpression(unresolvedFilter.condition)

  private def checkSubqueryAlias(subqueryAlias: SubqueryAlias) =
    subqueryAlias.identifier.qualifier.isEmpty && checkOperator(subqueryAlias.child)

  private def checkGlobalLimit(globalLimit: GlobalLimit) =
    checkOperator(globalLimit.child) && checkExpression(globalLimit.limitExpr)

  private def checkLocalLimit(localLimit: LocalLimit) =
    checkOperator(localLimit.child) && checkExpression(localLimit.limitExpr)

  private def checkUnresolvedInlineTable(unresolvedInlineTable: UnresolvedInlineTable) =
    unresolvedInlineTable.rows.forall(_.forall(checkExpression))

  private def checkUnresolvedRelation(unresolvedRelation: UnresolvedRelation) = true

  private def checkResolvedInlineTable(resolvedInlineTable: ResolvedInlineTable) =
    resolvedInlineTable.rows.forall(_.forall(checkExpression))

  // Usually we don't check outputs of operators in unresolved plans, but in this case
  // [[LocalRelation]] is resolved in the parser.
  private def checkLocalRelation(localRelation: LocalRelation) =
    localRelation.output.forall(checkExpression)

  private def checkOneRowRelation(oneRowRelation: OneRowRelation) = true

  private def checkAlias(alias: Alias) = checkExpression(alias.child)

  private def checkUnresolvedBinaryArithmetic(unresolvedBinaryArithmetic: BinaryArithmetic) =
    checkExpression(unresolvedBinaryArithmetic.left) &&
    checkExpression(unresolvedBinaryArithmetic.right)

  private def checkUnresolvedConditionalExpression(
      unresolvedConditionalExpression: ConditionalExpression) =
    unresolvedConditionalExpression.children.forall(checkExpression)

  private def checkUnresolvedCast(cast: Cast) = checkExpression(cast.child)

  private def checkUnresolvedStar(unresolvedStar: UnresolvedStar) = true

  private def checkUnresolvedAlias(unresolvedAlias: UnresolvedAlias) =
    checkExpression(unresolvedAlias.child)

  private def checkUnresolvedAttribute(unresolvedAttribute: UnresolvedAttribute) =
    !ResolverGuard.UNSUPPORTED_ATTRIBUTE_NAMES.contains(unresolvedAttribute.nameParts.head)

  private def checkUnresolvedPredicate(unresolvedPredicate: Predicate) = {
    unresolvedPredicate match {
      case _: SubqueryExpression => false
      case other =>
        other.children.forall(checkExpression)
    }
  }

  private def checkAttributeReference(attributeReference: AttributeReference) = true

  private def checkCreateNamedStruct(createNamedStruct: CreateNamedStruct) = {
    createNamedStruct.children.forall(checkExpression)
  }

  private def checkUnresolvedFunction(unresolvedFunction: UnresolvedFunction) =
    ResolverGuard.SUPPORTED_FUNCTION_NAMES.contains(
      unresolvedFunction.nameParts.head
    ) && unresolvedFunction.children.forall(checkExpression)

  private def checkLiteral(literal: Literal) = true

  private def checkConfValues() =
    // Case sensitive analysis is not supported.
    !conf.caseSensitiveAnalysis &&
    // Case-sensitive inference is not supported for Hive table schema.
    conf.caseSensitiveInferenceMode == HiveCaseSensitiveInferenceMode.NEVER_INFER

  private def checkVariables() = catalogManager.tempVariableManager.isEmpty
}

object ResolverGuard {

  private val UNSUPPORTED_ATTRIBUTE_NAMES = {
    val map = new IdentifierMap[Unit]()

    /**
     * Some SQL functions can be called without the braces and thus they are found in the
     * parsed operator as UnresolvedAttributes. This list contains the names of those functions
     * so we can reject them. Find more information in [[ColumnResolutionHelper.literalFunctions]].
     */
    map += ("current_date", ())
    map += ("current_timestamp", ())
    map += ("current_user", ())
    map += ("user", ())
    map += ("session_user", ())
    map += ("grouping__id", ())

    /**
     * Metadata column resolution is not supported for now
     */
    map += ("_metadata", ())

    map
  }

  /**
   * Most of the functions are not supported, but we allow some explicitly supported ones.
   */
  private val SUPPORTED_FUNCTION_NAMES = {
    val map = new IdentifierMap[Unit]()
    map += ("array", ())
    // map += ("array_agg", ()) - until aggregate expressions are supported
    map += ("array_append", ())
    map += ("array_compact", ())
    map += ("array_contains", ())
    map += ("array_distinct", ())
    map += ("array_except", ())
    map += ("array_insert", ())
    map += ("array_intersect", ())
    map += ("array_join", ())
    map += ("array_max", ())
    map += ("array_min", ())
    map += ("array_position", ())
    map += ("array_prepend", ())
    map += ("array_remove", ())
    map += ("array_repeat", ())
    map += ("array_size", ())
    // map += ("array_sort", ()) - until lambda functions are supported
    map += ("array_union", ())
    map += ("arrays_overlap", ())
    map += ("arrays_zip", ())
    map += ("coalesce", ())
    map += ("if", ())
    map += ("map", ())
    map += ("map_concat", ())
    map += ("map_contains_key", ())
    map += ("map_entries", ())
    // map += ("map_filter", ()) - until lambda functions are supported
    map += ("map_from_arrays", ())
    map += ("map_from_entries", ())
    map += ("map_keys", ())
    map += ("map_values", ())
    // map += ("map_zip_with", ()) - until lambda functions are supported
    map += ("named_struct", ())
    map += ("sort_array", ())
    map += ("str_to_map", ())
    map
  }
}
