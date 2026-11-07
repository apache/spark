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

package org.apache.spark.sql.execution.datasources.v2

import java.util.{Collections, IdentityHashMap}

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeMap, Coalesce,
  EqualNullSafe, EqualTo, Expression, ExprUtils, ExternalUserDefinedFunction, Literal,
  PredicateHelper, PythonUDF}
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, ExternalLookupKeyTypes,
  Filter, LogicalPlan, PreparedExternalLookupJoin, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXTERNAL_LOOKUP_JOIN
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Predicate}
import org.apache.spark.sql.connector.read.{LookupParameter, SupportsExternalLookup}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, MapType, StructType}

/** Finalizes an optimized external lookup side immediately before ordinary V2 scan preparation. */
object PrepareExternalLookupJoin extends Rule[LogicalPlan] with PredicateHelper
  with QueryErrorsBase {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val prepared = plan.transformUpWithPruning(_.containsPattern(EXTERNAL_LOOKUP_JOIN)) {
      case join: ExternalLookupJoin if provablyEmpty(join) => emptyJoinResult(join)
      case join: ExternalLookupJoin => prepare(join)
    }
    // Prepared lookup joins no longer contain their lookup relation, so a lookup-only table that
    // is still read here cannot run.
    prepared.foreach { node =>
      ExternalLookupRelations.lookupOnlyTable(node).foreach { table =>
        throw QueryCompilationErrors.unsupportedBatchReadError(table)
      }
    }
    prepared
  }

  // The optimizer folds `WHERE false` on the lookup side into an empty relation, and a condition
  // such as `s.id = r.id AND false` into `false`. Such a join needs no connector call.
  private def provablyEmpty(join: ExternalLookupJoin): Boolean =
    join.right.maxRows.contains(0L) || (join.condition match {
      case Some(Literal(value, BooleanType)) => value == null || value == false
      case _ => false
    })

  private def emptyJoinResult(join: ExternalLookupJoin): LogicalPlan = {
    val nullLookupColumns = join.right.output.map { attr =>
      Alias(Literal(null, attr.dataType), attr.name)(attr.exprId)
    }
    val unmatched = Project(join.left.output ++ nullLookupColumns, join.left)
    join.joinType match {
      case LeftOuter => unmatched
      // The stream side is kept under a false filter rather than replaced by an empty relation,
      // so that its sources, watermarks and stateful operators still run in every micro-batch.
      // PruneFilters decides whether a streaming subplan may be pruned.
      case _ => Filter(Literal.FalseLiteral, unmatched)
    }
  }

  private def prepare(join: ExternalLookupJoin): LogicalPlan = {
    val relation = extractLookupPlan(join.right).getOrElse {
      throw QueryCompilationErrors.unsupportedExternalLookupJoinError("LOOKUP_PLAN")
    }
    val tableName = relation.table.name()
    val supportsRead = relation.table match {
      case table: SupportsRead
          if table.capabilities().contains(TableCapability.EXTERNAL_LOOKUP) => table
      case _ =>
        throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
          "TABLE", Map("table" -> toSQLId(tableName)))
    }
    val conjuncts = join.condition.toSeq.flatMap(splitConjunctivePredicates)
    // A Python or external UDF over lookup columns cannot run in the residual. The rules that move
    // one out of a join condition match only Join and run later, so this rule does it: an inner
    // join evaluates the conjunct in a filter above the join, and an outer join is rejected.
    if (join.joinType == LeftOuter) {
      conjuncts.flatMap(lookupSideUDF(join, _)).headOption.foreach {
        case _: ExternalUserDefinedFunction =>
          throw QueryCompilationErrors.useExternalUDFInJoinConditionUnsupportedError(join.joinType)
        case _ =>
          throw QueryCompilationErrors.usePythonUDFInJoinConditionUnsupportedError(join.joinType)
      }
    }
    val offeredCandidates = conjuncts.zipWithIndex.collect {
      case (EqualTo(leftExpr, rightAttr: Attribute), index)
          if isLookupEquality(join, leftExpr, rightAttr) =>
        OfferedLookupPredicate(leftExpr, rightAttr, index, "=", false)
      case (EqualTo(rightAttr: Attribute, leftExpr), index)
          if isLookupEquality(join, leftExpr, rightAttr) =>
        OfferedLookupPredicate(leftExpr, rightAttr, index, "=", false)
    }
    if (offeredCandidates.isEmpty) {
      throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
        "NO_LOOKUP_CANDIDATE", Map("table" -> toSQLId(tableName)))
    }

    val scanBuilder = supportsRead.newScanBuilder(relation.options)
    val lookupBuilder = scanBuilder match {
      case builder: SupportsExternalLookup => builder
      case _ =>
        throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
          "SCAN_BUILDER", Map("table" -> toSQLId(tableName)))
    }
    // A column is offered under the table's name for it. Under case-insensitive analysis, the
    // condition may spell it in another case, as `ID` for `id`, with the same exprId; filter
    // pushdown renames it the same way, with DataSourceStrategy.normalizeExprs.
    val columnNames = AttributeMap(relation.output.map(attr => attr -> attr.name))
    val offeredPredicates = offeredCandidates.map { candidate =>
      new V2Predicate(candidate.predicateName, Array[V2Expression](
        FieldReference.column(columnNames(candidate.projectedField)),
        new LookupParameter(candidate.parameter.dataType)))
    }.toArray
    // A copy, so a connector that reorders the array in place cannot break the identity mapping.
    val selectedPredicates = selectedCandidates(tableName, offeredPredicates, offeredCandidates,
      lookupBuilder.selectLookupPredicates(offeredPredicates.clone()))
    val selectedConjuncts = selectedPredicates.map(_.conjunctIndex).toSet
    // As a hash join matches its keys before it evaluates its other conditions, the residual
    // evaluates the equalities between the two sides first, together with the conjuncts that
    // cannot fail. A conjunct that can fail then runs only for rows that pass them. Each group
    // keeps the written order.
    val (residualConjuncts, joinFilter) = conjuncts.zipWithIndex.collect {
      case (conjunct, index) if !selectedConjuncts.contains(index) => conjunct
    }.partition(lookupSideUDF(join, _).isEmpty)
    val (residualFirst, residualRest) = residualConjuncts.partition { conjunct =>
      isJoinKeyEquality(join, conjunct) || ExprUtils.canEvaluateUnconditionally(conjunct)
    }
    // A conjunction evaluates its next conjunct after a NULL, so a NULL in the first group reads
    // as false: a hash join never matches a NULL key either. A row matches only when the whole
    // condition is true, so this changes no result.
    val residual = if (residualRest.isEmpty) residualFirst else {
      residualFirst.map { conjunct =>
        if (conjunct.nullable) Coalesce(Seq(conjunct, Literal.FalseLiteral)) else conjunct
      } ++ residualRest
    }

    def nonNull[T](value: T, method: String): T = Option(value).getOrElse {
      throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
        tableName, "NULL_RESULT", Map("method" -> method))
    }
    val scan = nonNull(scanBuilder.build(), "ScanBuilder.build()")
    // Fields bind to table columns by exact name, as in `PushDownUtils.toOutputAttrs`. CHAR and
    // VARCHAR are read as STRING, and nullability, including nested nullability, does not count
    // for the match. The value-row attributes take the read schema's type, where a column, nested
    // field, array element or map value is nullable only when both the table and the read schema
    // allow nulls: the join fails the task on a null that either rules out, because Spark relied
    // on the table's nullability while it optimized the query.
    val readSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(
      nonNull(scan.readSchema(), "Scan.readSchema()"))
    val columnsByName = relation.output.groupBy(_.name)
    val lookupInput = readSchema.fields.map { field =>
      columnsByName.getOrElse(field.name, Nil).find { attr =>
        DataType.equalsIgnoreNullability(attr.dataType, field.dataType)
      }.map { attr =>
        attr.withDataType(withStricterNullability(attr.dataType, field.dataType))
          .withNullability(attr.nullable && field.nullable)
      }.getOrElse {
        throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
          tableName, "UNKNOWN_SCAN_FIELD", Map("field" -> toSQLId(Seq(field.name))))
      }
    }.toSeq
    lookupInput.groupBy(_.name).collectFirst {
      case (name, fields) if fields.size > 1 => name
    }.foreach { name =>
      throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
        tableName, "DUPLICATE_SCAN_FIELD", Map("field" -> toSQLId(Seq(name))))
    }
    // The prepared join replaces the analyzed one, so it keeps the analyzed lookup attributes in
    // their analyzed order; execution projects the value rows into them.
    // TODO: prune lookup output to the columns required above the join.
    val lookupOutput = join.right.output
    val readExprIds = lookupInput.map(_.exprId).toSet
    lookupOutput.find(attr => !readExprIds.contains(attr.exprId)).foreach { attr =>
      throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
        tableName, "MISSING_SCAN_FIELD", Map("field" -> toSQLId(Seq(attr.name))))
    }
    val externalLookup = nonNull(scan.toExternalLookup(), "Scan.toExternalLookup()")
    val maxParameterRowsPerRequest = externalLookup.maxParameterRowsPerRequest()
    if (maxParameterRowsPerRequest <= 0) {
      throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
        tableName,
        "NON_POSITIVE_MAX_PARAMETER_ROWS",
        Map("value" -> maxParameterRowsPerRequest.toString))
    }
    val readerFactory =
      nonNull(externalLookup.createReaderFactory(), "ExternalLookup.createReaderFactory()")

    val prepared = PreparedExternalLookupJoin(
      join.left,
      tableName,
      scan,
      readerFactory,
      maxParameterRowsPerRequest,
      lookupInput,
      lookupOutput,
      selectedPredicates.map(_.parameter),
      selectedPredicates.map(_.nullSafe),
      selectedPredicates.map(_.projectedField),
      join.joinType,
      residual.reduceOption(And))
    joinFilter.reduceOption(And).map(Filter(_, prepared)).getOrElse(prepared)
  }

  /**
   * The read schema's type of a column, where a nested field, array element or map value is
   * nullable only when the table's type allows nulls there too. The two types differ only in
   * nullability.
   */
  private def withStricterNullability(tableType: DataType, readType: DataType): DataType =
    (tableType, readType) match {
      case (table: StructType, read: StructType) =>
        StructType(table.fields.zip(read.fields).map { case (tableField, readField) =>
          readField.copy(
            dataType = withStricterNullability(tableField.dataType, readField.dataType),
            nullable = tableField.nullable && readField.nullable)
        })
      case (table: ArrayType, read: ArrayType) =>
        ArrayType(
          withStricterNullability(table.elementType, read.elementType),
          table.containsNull && read.containsNull)
      case (table: MapType, read: MapType) =>
        MapType(
          withStricterNullability(table.keyType, read.keyType),
          withStricterNullability(table.valueType, read.valueType),
          table.valueContainsNull && read.valueContainsNull)
      case _ => readType
    }

  /** A Python or external UDF in the conjunct that reads lookup columns. */
  private def lookupSideUDF(join: ExternalLookupJoin, conjunct: Expression): Option[Expression] =
    conjunct.find { e =>
      (PythonUDF.isScalarPythonUDF(e) || e.isInstanceOf[ExternalUserDefinedFunction]) &&
        !e.references.subsetOf(join.left.outputSet)
    }

  /**
   * An equality between a bare lookup column and an expression over the stream side, on a type
   * that Spark offers as a lookup key.
   */
  private def isLookupEquality(
      join: ExternalLookupJoin,
      leftExpr: Expression,
      rightAttr: Attribute): Boolean =
    leftExpr.references.subsetOf(join.left.outputSet) && join.right.outputSet.contains(rightAttr) &&
      ExternalLookupKeyTypes.supportsEquality(rightAttr)

  /**
   * An equality between an expression over the stream side and one over the lookup side, each
   * reading a column, which a hash join would use as a join key.
   */
  private def isJoinKeyEquality(join: ExternalLookupJoin, conjunct: Expression): Boolean = {
    def readsOnly(expression: Expression, plan: LogicalPlan): Boolean =
      expression.references.nonEmpty && expression.references.subsetOf(plan.outputSet)
    def betweenSides(first: Expression, second: Expression): Boolean =
      (readsOnly(first, join.left) && readsOnly(second, join.right)) ||
        (readsOnly(first, join.right) && readsOnly(second, join.left))
    conjunct match {
      case EqualTo(first, second) => betweenSides(first, second)
      case EqualNullSafe(first, second) => betweenSides(first, second)
      case _ => false
    }
  }

  /**
   * Maps the connector's selection back to the offered candidates, in the connector's order, which
   * defines the fields of each request parameter row. Selections are matched by object identity
   * rather than by equality, so two candidates of the same shape, for example the same lookup field
   * compared with two different join-side values, remain distinguishable.
   */
  private def selectedCandidates(
      tableName: String,
      offered: Array[V2Predicate],
      candidates: Seq[OfferedLookupPredicate],
      selected: Array[V2Predicate]): Seq[OfferedLookupPredicate] = {
    if (selected == null) {
      throw QueryCompilationErrors.invalidDataSourceExternalLookupError(tableName, "NULL_RESULT",
        Map("method" -> "SupportsExternalLookup.selectLookupPredicates()"))
    }
    if (selected.isEmpty) {
      throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
        "NO_LOOKUP_PREDICATE", Map("table" -> toSQLId(tableName)))
    }
    val candidateByPredicate = new IdentityHashMap[V2Predicate, OfferedLookupPredicate]()
    offered.zip(candidates).foreach { case (predicate, candidate) =>
      candidateByPredicate.put(predicate, candidate)
    }
    val seen = Collections.newSetFromMap(new IdentityHashMap[V2Predicate, java.lang.Boolean]())
    selected.toSeq.map { predicate =>
      // A null element is not an offered instance either.
      val candidate = Option(predicate).map(candidateByPredicate.get).orNull
      if (candidate == null) {
        throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
          tableName, "UNOFFERED_SELECTED_PREDICATE",
          Map("predicate" -> Option(predicate).map(_.describe()).getOrElse("null")))
      }
      if (!seen.add(predicate)) {
        throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
          tableName, "DUPLICATE_SELECTED_PREDICATE", Map("predicate" -> predicate.describe()))
      }
      candidate
    }
  }

  private case class OfferedLookupPredicate(
      parameter: Expression,
      projectedField: Attribute,
      conjunctIndex: Int,
      predicateName: String,
      nullSafe: Boolean)

  private def extractLookupPlan(plan: LogicalPlan): Option[DataSourceV2Relation] = plan match {
    case relation: DataSourceV2Relation => Some(relation)
    case relation: ExternalLookupRelation => Some(relation.relation)
    case _ => None
  }
}
