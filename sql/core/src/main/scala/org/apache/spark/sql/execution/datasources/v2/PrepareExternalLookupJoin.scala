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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, Expression,
  PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, Filter, Join, LogicalPlan,
  PreparedExternalLookupJoin, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXTERNAL_LOOKUP_JOIN
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Predicate}
import org.apache.spark.sql.connector.read.{LookupParameter, SupportsExternalLookup}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/** Finalizes an optimized external lookup side immediately before ordinary V2 scan preparation. */
object PrepareExternalLookupJoin extends Rule[LogicalPlan] with PredicateHelper
  with QueryErrorsBase {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val prepared = plan.transformUpWithPruning(_.containsPattern(EXTERNAL_LOOKUP_JOIN)) {
      case join: ExternalLookupJoin => prepare(join)
    }
    // Prepared lookup joins no longer contain their lookup relation, so a lookup-only table that
    // is still read here cannot run. Pre-order visits a join before the relation it reads.
    prepared.foreach {
      // Classic Dataset.join(right, usingColumns) first analyzes Join(left, right, joinType, None),
      // including CheckAnalysis, to disambiguate a self-join, and only then builds the USING join.
      // That plan cannot be told apart from a join without a condition, so analysis keeps it, and
      // a missing condition is reported here, on the final plan.
      case Join(_, right, _, None, _) if readsLookupOnlyTable(right) =>
        throw QueryCompilationErrors.unsupportedExternalLookupJoinError("MISSING_JOIN_CONDITION")
      case node =>
        ExternalLookupRelations.lookupOnlyTable(node).foreach { table =>
          throw QueryCompilationErrors.unsupportedBatchReadError(table)
        }
    }
    prepared
  }

  private def readsLookupOnlyTable(plan: LogicalPlan): Boolean = plan match {
    case node @ (_: Project | _: Filter) => readsLookupOnlyTable(node.children.head)
    case node => ExternalLookupRelations.lookupOnlyTable(node).isDefined
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
    val scanBuilder = supportsRead.newScanBuilder(relation.options)
    val lookupBuilder = scanBuilder match {
      case builder: SupportsExternalLookup => builder
      case _ =>
        throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
          "SCAN_BUILDER", Map("table" -> toSQLId(tableName)))
    }

    val conjuncts = join.condition.toSeq.flatMap(splitConjunctivePredicates)
    val offeredCandidates = mutable.ArrayBuffer.empty[OfferedLookupPredicate]
    val residual = mutable.ArrayBuffer.empty[Expression]
    conjuncts.foreach {
      case equality @ EqualTo(leftExpr, rightAttr: Attribute)
          if leftExpr.references.subsetOf(join.left.outputSet) &&
            join.right.outputSet.contains(rightAttr) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, equality, "=", false)
      case equality @ EqualTo(rightAttr: Attribute, leftExpr)
          if leftExpr.references.subsetOf(join.left.outputSet) &&
            join.right.outputSet.contains(rightAttr) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, equality, "=", false)
      case other => residual += other
    }

    val offeredPredicates = offeredCandidates.map { candidate =>
      new V2Predicate(candidate.predicateName, Array[V2Expression](
        FieldReference.column(candidate.projectedField.name),
        new LookupParameter(candidate.parameter.dataType)))
    }.toArray
    // A copy, so a connector that reorders the array in place cannot break the identity mapping.
    val (selectedPredicates, unselectedPredicates) = selectedCandidates(tableName,
      offeredPredicates, offeredCandidates,
      lookupBuilder.selectLookupPredicates(offeredPredicates.clone()))
    residual ++= unselectedPredicates.map(_.condition)

    val scan = scanBuilder.build()
    val resolver = SQLConf.get.resolver
    // Fields match the way ordinary V2 scans match them (`PushDownUtils.toOutputAttrs`): CHAR and
    // VARCHAR are read as STRING, and nullability, including nested nullability, does not count.
    val readSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(scan.readSchema())
    val lookupInput = readSchema.fields.map { field =>
      relation.output.find { attr =>
        resolver(attr.name, field.name) &&
          DataType.equalsIgnoreNullability(attr.dataType, field.dataType)
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
    val externalLookup = scan.toExternalLookup()
    val maxParameterRowsPerRequest = externalLookup.maxParameterRowsPerRequest()
    if (maxParameterRowsPerRequest <= 0) {
      throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
        tableName,
        "NON_POSITIVE_MAX_PARAMETER_ROWS",
        Map("value" -> maxParameterRowsPerRequest.toString))
    }
    val readerFactory = externalLookup.createReaderFactory()
    // TODO: prune lookup output to the columns required above the join.
    val lookupOutput = lookupInput

    PreparedExternalLookupJoin(
      join.left,
      scan,
      readerFactory,
      maxParameterRowsPerRequest,
      lookupInput,
      lookupOutput,
      selectedPredicates.map(_.parameter).toSeq,
      selectedPredicates.map(_.nullSafe).toSeq,
      join.joinType,
      residual.reduceOption(And))
  }

  /**
   * Maps the connector's selection back to the offered candidates. Returns the selected candidates
   * in the connector's order, which defines the fields of each request parameter row, and the
   * candidates it did not select. Selections are matched by object identity rather than by
   * equality, so two candidates of the same shape, for example the same lookup field compared with
   * two different join-side values, remain distinguishable.
   */
  private def selectedCandidates(
      tableName: String,
      offered: Array[V2Predicate],
      candidates: Iterable[OfferedLookupPredicate],
      selected: Array[V2Predicate]): (Seq[OfferedLookupPredicate], Seq[OfferedLookupPredicate]) = {
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
    val selectedCandidates = selected.toSeq.map { predicate =>
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
    val unselectedCandidates = offered.toSeq.zip(candidates).collect {
      case (predicate, candidate) if !seen.contains(predicate) => candidate
    }
    (selectedCandidates, unselectedCandidates)
  }

  private case class OfferedLookupPredicate(
      parameter: Expression,
      projectedField: Attribute,
      condition: Expression,
      predicateName: String,
      nullSafe: Boolean)

  private def extractLookupPlan(plan: LogicalPlan): Option[DataSourceV2Relation] = plan match {
    case relation: DataSourceV2Relation => Some(relation)
    case relation: ExternalLookupRelation => Some(relation.relation)
    case _ => None
  }
}
