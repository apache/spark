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

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, Expression,
  PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, LogicalPlan,
  PreparedExternalLookupJoin}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXTERNAL_LOOKUP_JOIN
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Predicate}
import org.apache.spark.sql.connector.read.{LookupParameter, SupportsExternalLookup}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.types.DataType

/** Finalizes an optimized external lookup side immediately before ordinary V2 scan preparation. */
object PrepareExternalLookupJoin extends Rule[LogicalPlan] with PredicateHelper
  with QueryErrorsBase {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val prepared = plan.transformUpWithPruning(_.containsPattern(EXTERNAL_LOOKUP_JOIN)) {
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

  private def prepare(join: ExternalLookupJoin): PreparedExternalLookupJoin = {
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
    // Each conjunct keeps its position, so the residual keeps the written order: a conjunct that
    // can throw is never evaluated before the conjuncts written ahead of it.
    val conjuncts = join.condition.toSeq.flatMap(splitConjunctivePredicates)
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
    val offeredPredicates = offeredCandidates.map { candidate =>
      new V2Predicate(candidate.predicateName, Array[V2Expression](
        FieldReference.column(candidate.projectedField.name),
        new LookupParameter(candidate.parameter.dataType)))
    }.toArray
    // A copy, so a connector that reorders the array in place cannot break the identity mapping.
    val selectedPredicates = selectedCandidates(tableName, offeredPredicates, offeredCandidates,
      lookupBuilder.selectLookupPredicates(offeredPredicates.clone()))
    val selectedConjuncts = selectedPredicates.map(_.conjunctIndex).toSet
    val residual = conjuncts.zipWithIndex.collect {
      case (conjunct, index) if !selectedConjuncts.contains(index) => conjunct
    }

    val scan = scanBuilder.build()
    // Fields bind to table columns by exact name, as in `PushDownUtils.toOutputAttrs`. CHAR and
    // VARCHAR are read as STRING, and nullability, including nested nullability, does not count
    // for the match. The value-row attributes take the read schema's type and nullability, so they
    // describe every value the connector may return.
    val readSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(scan.readSchema())
    val lookupInput = readSchema.fields.map { field =>
      relation.output.find { attr =>
        attr.name == field.name && DataType.equalsIgnoreNullability(attr.dataType, field.dataType)
      }.map(_.withDataType(field.dataType).withNullability(field.nullable)).getOrElse {
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
    val externalLookup = scan.toExternalLookup()
    val maxParameterRowsPerRequest = externalLookup.maxParameterRowsPerRequest()
    if (maxParameterRowsPerRequest <= 0) {
      throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
        tableName,
        "NON_POSITIVE_MAX_PARAMETER_ROWS",
        Map("value" -> maxParameterRowsPerRequest.toString))
    }
    val readerFactory = externalLookup.createReaderFactory()

    PreparedExternalLookupJoin(
      join.left,
      scan,
      readerFactory,
      maxParameterRowsPerRequest,
      lookupInput,
      lookupOutput,
      selectedPredicates.map(_.parameter),
      selectedPredicates.map(_.nullSafe),
      join.joinType,
      residual.reduceOption(And))
  }

  /** An equality between a bare lookup column and an expression over the stream side. */
  private def isLookupEquality(
      join: ExternalLookupJoin,
      leftExpr: Expression,
      rightAttr: Attribute): Boolean =
    leftExpr.references.subsetOf(join.left.outputSet) && join.right.outputSet.contains(rightAttr)

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
    if (selected == null || selected.contains(null)) {
      throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
        tableName, "NULL_PREDICATE_SELECTION")
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
      val candidate = candidateByPredicate.get(predicate)
      if (candidate == null) {
        throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
          tableName, "UNOFFERED_SELECTED_PREDICATE", Map("predicate" -> predicate.describe()))
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
