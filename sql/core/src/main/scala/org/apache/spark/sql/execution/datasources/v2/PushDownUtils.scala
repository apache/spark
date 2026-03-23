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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, ExpressionSet, NamedExpression, PythonUDF, SchemaPruning, SubqueryExpression}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.expressions.{IdentityTransform, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownLimit, SupportsPushDownOffset, SupportsPushDownRequiredColumns, SupportsPushDownTableSample, SupportsPushDownTopN, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, DataSourceUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.{PartitionPredicateImpl, SupportsPushDownCatalystFilters}
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.ArrayImplicits.SparkArrayOps
import org.apache.spark.util.collection.Utils

object PushDownUtils {

  /**
   * Pushes down filters to the data source reader.
   *
   * @param scanBuilder The scan builder to push filters to.
   * @param filters Catalyst filter expressions to push down.
   * @param partitionSchema The schema of [[Table#partitioning() partitioning]].
   *   When set and the scan supports V2 filters,
   *   [[PartitionPredicate]] can be pushed for a second pass.
   * @return pushed filter and post-scan filters.
   */
  def pushFilters(
      scanBuilder: ScanBuilder,
      filters: Seq[Expression],
      partitionSchema: Option[StructType])
      : (Either[Seq[sources.Filter], Seq[Predicate]], Seq[Expression]) = {
    scanBuilder match {
      case r: SupportsPushDownFilters =>
        // A map from translated data source leaf node filters to original catalyst filter
        // expressions. For a `And`/`Or` predicate, it is possible that the predicate is partially
        // pushed down. This map can be used to construct a catalyst filter expression from the
        // input filter, or a superset(partial push down filter) of the input filter.
        val translatedFilterToExpr = mutable.HashMap.empty[sources.Filter, Expression]
        val translatedFilters = mutable.ArrayBuffer.empty[sources.Filter]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated =
            DataSourceStrategy.translateFilterWithMapping(filterExpr, Some(translatedFilterToExpr),
              nestedPredicatePushdownEnabled = true)
          if (translated.isEmpty) {
            untranslatableExprs += filterExpr
          } else {
            translatedFilters += translated.get
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushFilters(translatedFilters.toArray).map { filter =>
          DataSourceStrategy.rebuildExpressionFromFilter(filter, translatedFilterToExpr)
        }
        // Normally translated filters (postScanFilters) are simple filters that can be evaluated
        // faster, while the untranslated filters are complicated filters that take more time to
        // evaluate, so we want to evaluate the postScanFilters filters first.
        (Left(r.pushedFilters().toImmutableArraySeq),
          (postScanFilters ++ untranslatableExprs).toImmutableArraySeq)

      case r: SupportsPushDownV2Filters =>
        // Divide the filters into those translatable and untranslatable to data source filters.
        // For the translated filters, we will try to push them down to the data source,
        // and the data source will return the filters that it cannot guarantee to be true
        // for all returned rows.
        val translatedFilterToExpr = mutable.HashMap.empty[Predicate, Expression]
        val translatedFilters = mutable.ArrayBuffer.empty[Predicate]
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated =
            DataSourceV2Strategy.translateFilterV2WithMapping(
              filterExpr, Some(translatedFilterToExpr))
          if (translated.isEmpty) {
            untranslatableExprs += filterExpr
          } else {
            translatedFilters += translated.get
          }
        }

        val rejectedFilters = r.pushPredicates(translatedFilters.toArray).map { predicate =>
          DataSourceV2Strategy.rebuildExpressionFromFilter(predicate, translatedFilterToExpr)
        }

        val remainingFilters = (rejectedFilters ++ untranslatableExprs).toSeq
        val postScanFilters = if (partitionSchema.isEmpty || !r.supportsIterativePushdown) {
          remainingFilters
        } else {
          pushPartitionPredicates(r, partitionSchema.get, remainingFilters)
        }

        val orderedPostScanFilters = prioritizeFilters(postScanFilters,
          ExpressionSet(untranslatableExprs))
        (Right(r.pushedPredicates.toImmutableArraySeq), orderedPostScanFilters)
      case r: SupportsPushDownCatalystFilters =>
        val postScanFilters = r.pushFilters(filters)
        (Right(r.pushedFilters.toImmutableArraySeq), postScanFilters)
      case _ => (Left(Nil), filters)
    }
  }

  /**
   * Normally translated filters (postScanFilters) are simple filters that can be
   * evaluated faster, while the untranslated filters are complicated filters
   * that take more time to evaluate, so we want to evaluate the translatable
   * filters first.
   */
  private def prioritizeFilters(
      filters: Seq[Expression],
      untranslatableFilterSet: ExpressionSet): Seq[Expression] = {
    val (translatable, untranslatable) = filters.partition(!untranslatableFilterSet.contains(_))
    translatable ++ untranslatable
  }

  /**
   * If the scan supports iterative filtering, convert partition filters to
   * PartitionPredicates (see SPARK-55596) and push them down in another pass.
   */
  private def pushPartitionPredicates(
      scanBuilder: SupportsPushDownV2Filters,
      partitionSchema: StructType,
      remainingFilters: Seq[Expression]): Seq[Expression] = {
    val (partitionFilters, nonPartitionFilters) =
      DataSourceUtils.getPartitionFiltersAndDataFilters(partitionSchema, remainingFilters)
    val (pushable, nonPushable) = partitionFilters.partition(isPushablePartitionFilter)
    val partitionAttrs = toAttributes(partitionSchema)
    val partitionPredicates = pushable.map(expr => PartitionPredicateImpl(expr, partitionAttrs))
    val rejectedPartitionFilters = scanBuilder.pushPredicates(partitionPredicates.toArray).map {
      predicate => predicate.asInstanceOf[PartitionPredicateImpl].expression
    }
    nonPartitionFilters ++ nonPushable ++ rejectedPartitionFilters
  }

  /**
   * Returns a table's partitioning expression schema as a StructType, if creation of a
   * PartitionPredicate is supported for the schema.
   * Currently only supported if all partitioning expressions are identity transforms on simple
   * (single-name, non-nested) field references.
   *
   * @return Some(StructType) representing partition transform expression types, if schema
   *         is supported for PartitionPredicate. None if not supported.
   */
  def getPartitionPredicateSchema(relation: DataSourceV2Relation): Option[StructType] = {
    val transforms = relation.table.partitioning
    val fields = transforms.flatMap(toSupportedPartitionField(_, relation))
    Option.when(transforms.nonEmpty && fields.length == transforms.length)(StructType(fields))
  }

  /**
   * Returns a StructField for the given partition transform if it is
   * supported for iterative partition predicate push down.
   */
  private def toSupportedPartitionField(
      transform: Transform,
      relation: DataSourceV2Relation): Option[StructField] = {
    transform match {
      case t: IdentityTransform if t.ref.fieldNames.length == 1 =>
        val colName = t.ref.fieldNames.head
        relation.output
          .find(_.name == colName)
          .map(attr => StructField(colName, attr.dataType, attr.nullable))
      case _ =>
        None
    }
  }

  /**
   * Returns true if the given filter expression is safe to push as a partition predicate
   * when using iterative pushdown: it must be deterministic, contain
   * no subquery, and no PythonUDF.
   */
  private def isPushablePartitionFilter(f: Expression): Boolean =
    f.deterministic && !SubqueryExpression.hasSubquery(f) && !f.exists(_.isInstanceOf[PythonUDF])

  /**
   * Pushes down TableSample to the data source Scan
   */
  def pushTableSample(scanBuilder: ScanBuilder, sample: TableSampleInfo): Boolean = {
    scanBuilder match {
      case s: SupportsPushDownTableSample =>
        s.pushTableSample(
          sample.lowerBound, sample.upperBound, sample.withReplacement, sample.seed)
      case _ => false
    }
  }

  /**
   * Pushes down LIMIT to the data source Scan.
   *
   * @return the tuple of Boolean. The first Boolean value represents whether to push down, and
   *         the second Boolean value represents whether to push down partially, which means
   *         Spark will keep the Limit and do it again.
   */
  def pushLimit(scanBuilder: ScanBuilder, limit: Int): (Boolean, Boolean) = {
    scanBuilder match {
      case s: SupportsPushDownLimit if s.pushLimit(limit) =>
        (true, s.isPartiallyPushed)
      case _ => (false, false)
    }
  }

  /**
   * Pushes down OFFSET to the data source Scan.
   *
   * @return the Boolean value represents whether to push down.
   */
  def pushOffset(scanBuilder: ScanBuilder, offset: Int): Boolean = {
    scanBuilder match {
      case s: SupportsPushDownOffset =>
        s.pushOffset(offset)
      case _ => false
    }
  }

  /**
   * Pushes down top N to the data source Scan.
   *
   * @return the tuple of Boolean. The first Boolean value represents whether to push down, and
   *         the second Boolean value represents whether to push down partially, which means
   *         Spark will keep the Sort and Limit and do it again.
   */
  def pushTopN(
      scanBuilder: ScanBuilder,
      order: Array[SortOrder],
      limit: Int): (Boolean, Boolean) = {
    scanBuilder match {
      case s: SupportsPushDownTopN if s.pushTopN(order, limit) =>
        (true, s.isPartiallyPushed)
      case _ => (false, false)
    }
  }

  /**
   * Applies column pruning to the data source, w.r.t. the references of the given expressions.
   *
   * @return the `Scan` instance (since column pruning is the last step of operator pushdown),
   *         and new output attributes after column pruning.
   */
  def pruneColumns(
      scanBuilder: ScanBuilder,
      relation: DataSourceV2Relation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): (Scan, Seq[AttributeReference]) = {
    val exprs = projects ++ filters
    val requiredColumns = AttributeSet(exprs.flatMap(_.references))
    val neededOutput = relation.output.filter(requiredColumns.contains)

    scanBuilder match {
      case r: SupportsPushDownRequiredColumns if SQLConf.get.nestedSchemaPruningEnabled =>
        val rootFields = SchemaPruning.identifyRootFields(projects, filters)
        val prunedSchema = if (rootFields.nonEmpty) {
          SchemaPruning.pruneSchema(relation.schema, rootFields)
        } else {
          new StructType()
        }
        val neededFieldNames = neededOutput.map(_.name).toSet
        r.pruneColumns(StructType(prunedSchema.filter(f => neededFieldNames.contains(f.name))))
        val scan = r.build()
        scan -> toOutputAttrs(scan.readSchema(), relation)

      case r: SupportsPushDownRequiredColumns =>
        r.pruneColumns(neededOutput.toStructType)
        val scan = r.build()
        // always project, in case the relation's output has been updated and doesn't match
        // the underlying table schema
        scan -> toOutputAttrs(scan.readSchema(), relation)

      case _ => scanBuilder.build() -> relation.output
    }
  }

  def toOutputAttrs(
      schema: StructType,
      relation: DataSourceV2Relation): Seq[AttributeReference] = {
    val nameToAttr = Utils.toMap(relation.output.map(_.name), relation.output)
    val cleaned = CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema)
    toAttributes(cleaned).map {
      // we have to keep the attribute id during transformation
      a => a.withExprId(nameToAttr(a.name).exprId)
    }
  }
}
