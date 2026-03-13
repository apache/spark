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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, ExpressionSet, NamedExpression, PythonUDF, SchemaPruning, SubqueryExpression, V2ExpressionUtils}
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
import org.apache.spark.util.collection.Utils

object PushDownUtils {

  /**
   * Returns a table's partitioning expression schema as a StructType.
   * Only returned when all partitioning expressions are identity transforms on simple
   * (single-name, non-nested) field references.
   *
   * @return Some(StructType) representing partition transform expression types, if supported.
   *         None if not supported.
   */
  def getPartitionPredicateSchema(
      relation: DataSourceV2Relation): Option[StructType] = {
    val partitioning = relation.table.partitioning()
    val supported = partitioning.filter(isSupported)
    if (supported.isEmpty || supported.length != partitioning.size) {
      None
    } else {
      val partitionColNames = supported.map { transform =>
        transform.asInstanceOf[IdentityTransform].ref.fieldNames()(0)
      }
      val attrs = partitionColNames.map(name => relation.output.find(_.name == name))
      if (attrs.exists(_.isEmpty)) {
        None
      } else {
        val fields = attrs.map(_.get).zip(partitionColNames).map { case (attr, name) =>
          StructField(name, attr.dataType, attr.nullable)
        }
        Some(StructType(fields))
      }
    }
  }

  /**
   * Currently we only support PartitionPredicate for Identity Transforms
   * on top-level (non-nested) columns.
   */
  private def isSupported(transform: Transform): Boolean = {
    transform match {
      case id: IdentityTransform if id.ref.fieldNames().length == 1 => true
      case _ => false
    }
  }

  /**
   * Returns true if the given filter expression is safe to push as a partition predicate
   * in the second pass of multi-pass filtering: it must be deterministic, contain
   * no subquery, and no PythonUDF.
   */
  private def isPartitionPushableFilter(f: Expression): Boolean =
    f.deterministic && !SubqueryExpression.hasSubquery(f) && !f.exists(_.isInstanceOf[PythonUDF])

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
        (Left(r.pushedFilters().toSeq),
          (postScanFilters ++ untranslatableExprs).toSeq)

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

        val rejectedFirstPassFilters = r.pushPredicates(translatedFilters.toArray).map {
          predicate =>
            DataSourceV2Strategy.rebuildExpressionFromFilter(predicate, translatedFilterToExpr)
        }

        val finalPostScanFilters = (partitionSchema, r.supportsMultiPassFiltering()) match {
          // If the scan supports multi-pass filtering, convert partition filters
          // to PartitionPredicates (see SPARK-55596).
          // PartitionPredicates are pushed to the scan in a second pass.
          case (Some(structType), true) =>
            val (remainingPartitionFilters, dataFilters) =
              DataSourceUtils.getPartitionFiltersAndDataFilters(
                structType,
                (rejectedFirstPassFilters ++ untranslatableExprs).toSeq)
            val rejectedPartitionPredicates =
              pushdownPartitionPredicates(r, structType, remainingPartitionFilters)

            // Normally translated filters (postScanFilters) are simple filters that can be
            // evaluated faster, while the untranslated filters are complicated filters that take
            // more time to evaluate, so we want to evaluate the translatable filters first.
            val untranslatableSet = ExpressionSet(untranslatableExprs)
            (dataFilters ++ rejectedPartitionPredicates).sortBy(untranslatableSet.contains)
          case _ =>
            // Normally translated filters (postScanFilters) are simple filters that can be
            // evaluated faster, while the untranslated filters are complicated filters that take
            // more time to evaluate, so we want to evaluate the postScanFilters filters first.
            (rejectedFirstPassFilters ++ untranslatableExprs).toSeq
        }

        (Right(r.pushedPredicates.toSeq), finalPostScanFilters)
      case r: SupportsPushDownCatalystFilters =>
        val postScanFilters = r.pushFilters(filters)
        (Right(r.pushedFilters.toSeq), postScanFilters)
      case _ => (Left(Nil), filters)
    }
  }

  // Push down remaining partition filters as PartitionPredicates
  private def pushdownPartitionPredicates(r: SupportsPushDownV2Filters,
                                      structType: StructType,
                                      remainingPartitionFilters: Seq[Expression]) = {
    val (pushablePartitionFilters, unsupportedPartitionFilters) =
      remainingPartitionFilters.partition(isPartitionPushableFilter)
    val partitionSchema = toAttributes(structType)
    val partitionPredicatesForPush = pushablePartitionFilters
      .map(expr => PartitionPredicateImpl(expr, partitionSchema))
    val rejectedPartitionPredicates =
      r.pushPredicates(partitionPredicatesForPush.toArray).map { predicate =>
        V2ExpressionUtils.toCatalyst(predicate).getOrElse(
          throw SparkException.internalError(
            "PartitionPredicate has no catalyst expression"))
      }
    unsupportedPartitionFilters ++ rejectedPartitionPredicates
  }

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
