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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, ExpressionSet, GetStructField, NamedExpression, PythonUDF, SchemaPruning, SubqueryExpression}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.expressions.{IdentityTransform, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownLimit, SupportsPushDownOffset, SupportsPushDownRequiredColumns, SupportsPushDownTableSample, SupportsPushDownTopN, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, DataSourceUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.{PartitionPredicateField, PartitionPredicateImpl, SupportsPushDownCatalystFilters}
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
   * @param partitionFields When non-empty, metadata for [[Table#partitioning()]].
   *                        When set and the scan supports V2 filters,
   *                        [[PartitionPredicate]] can be pushed for a second pass.
   * @return pushed filter and post-scan filters.
   */
  def pushFilters(
      scanBuilder: ScanBuilder,
      filters: Seq[Expression],
      partitionFields: Option[Seq[PartitionPredicateField]])
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
        val postScanFilters =
          if (!partitionFields.exists(_.nonEmpty) || !r.supportsIterativePushdown) {
            remainingFilters
          } else {
            pushPartitionPredicates(r, partitionFields.get, remainingFilters)
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
      partitionFields: Seq[PartitionPredicateField],
      remainingFilters: Seq[Expression]): Seq[Expression] = {
    val partitionSchema = StructType(partitionFields.map(_.structField))
    val normalizedFilters = remainingFilters.map(
      normalizePartitionRefs(_, partitionFields, SQLConf.get.resolver))
    val (partitionFilters, nonPartitionFilters) =
      DataSourceUtils.getPartitionFiltersAndDataFilters(partitionSchema, normalizedFilters)
    val (pushable, nonPushable) = partitionFilters.partition(isPushablePartitionFilter)
    val partitionPredicates = pushable.map(
      expr => PartitionPredicateImpl(expr, partitionFields))
    val rejectedPartitionFilters = scanBuilder.pushPredicates(partitionPredicates.toArray).map {
      predicate => predicate.asInstanceOf[PartitionPredicateImpl].expression
    }
    nonPartitionFilters ++ nonPushable ++ rejectedPartitionFilters
  }

  /**
   * Return a Seq of [[PartitionPredicateField]] representing the fields of a table's
   * partitioning, only when every partition transform is a resolvable identity transform.
   */
  def getPartitionSchemaInfo(
      relation: DataSourceV2Relation): Option[Seq[PartitionPredicateField]] = {
    val transforms = relation.table.partitioning
    if (transforms.isEmpty) {
      None
    } else {
      val fields = transforms.map {
        case t: IdentityTransform =>
          toSupportedPartitionField(t, relation.output).map(PartitionPredicateField(_, t.ref))
        case _ => None
      }
      if (fields.forall(_.isDefined)) {
        Some(fields.map(_.get).toIndexedSeq)
      } else {
        None
      }
    }
  }

  /**
   * Returns a StructField for the given partition transform if it is
   * supported for iterative partition predicate push down.
   */
  private def toSupportedPartitionField(
      transform: Transform,
      relationOutput: Seq[AttributeReference]): Option[StructField] = {
    transform match {
      case t: IdentityTransform =>
        val names = t.ref.fieldNames.toIndexedSeq
        resolveIdentityPartitionField(names, relationOutput)
      case _ =>
        None
    }
  }

  /**
   * Resolves an identity partition column path to a StructField.
   */
  private def resolveIdentityPartitionField(
      names: Seq[String],
      relationOutput: Seq[AttributeReference]): Option[StructField] = {
    if (names.isEmpty) {
      None
    } else {
      val resolver = SQLConf.get.resolver
      val rootStruct =
        StructType(relationOutput.map(a => StructField(a.name, a.dataType, a.nullable)))
      rootStruct.findNestedField(names, resolver = resolver).map {
        case (_, leaf) =>
          StructField(names.mkString("."), leaf.dataType, leaf.nullable)
      }
    }
  }

  /**
   * Normalizes filter expressions so that nested struct accesses on partition fields are
   * replaced with flat [[AttributeReference]]s whose names match the partition schema.
   *
   * For example, given a table partitioned by `s.tz` (identity transform on a nested field),
   * the analyzer produces `GetStructField(attr("s"), "tz")`. This method replaces that chain
   * with `attr("s.tz")`.
   */
  private def normalizePartitionRefs(
      expr: Expression,
      partitionFields: Seq[PartitionPredicateField],
      resolver: (String, String) => Boolean): Expression = {
    val partitionAttrs = toAttributes(StructType(partitionFields.map(_.structField)))
    val pathToAttr: Map[Seq[String], AttributeReference] =
      partitionFields.map(_.identityRef).zip(partitionAttrs)
        .map { case (ref, attr) => ref.fieldNames().toIndexedSeq -> attr}.toMap
    doNormalizePartitionRefs(expr, pathToAttr, resolver)
  }

  private def doNormalizePartitionRefs(
      expr: Expression,
      pathToAttr: Map[Seq[String], AttributeReference],
      resolver: (String, String) => Boolean): Expression = {
    expr.mapChildren(doNormalizePartitionRefs(_, pathToAttr, resolver)) match {
      case g: GetStructField =>
        flattenStructFieldChain(g) match {
          case Some((root, suffix)) =>
            val fullPath = root.name +: suffix
            pathToAttr.collectFirst {
              case (path, attr) if pathsMatch(fullPath, path, resolver) =>
                attr.withNullability(g.nullable)
            }.getOrElse(g) // Not partition filter
          case None => g
        }
      case other => other
    }
  }

  /**
   * Flattens a nested [[GetStructField]] chain into the root
   * [[AttributeReference]] and its field-name path. Returns `None`
   * when the expression is not a nested struct access.
   *
   * Example: `GetStructField(GetStructField(a#1, "x"), "y")`
   *   returns `Some((a#1, Seq("x", "y")))`.
   */
  private def flattenStructFieldChain(
      expr: Expression): Option[(AttributeReference, Seq[String])] = {
    def flatten(e: Expression): Option[(AttributeReference, Seq[String])] = e match {
      case ar: AttributeReference => Some((ar, Seq.empty))
      case g: GetStructField =>
        flatten(g.child).map { case (ar, tail) => (ar, tail :+ g.extractFieldName) }
      case _ => None
    }
    flatten(expr).filter(_._2.nonEmpty)
  }

  private def pathsMatch(
      left: Seq[String],
      right: Seq[String],
      resolver: (String, String) => Boolean): Boolean = {
    left.length == right.length &&
      left.zip(right).forall { case (a, b) => resolver(a, b) }
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
