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

package org.apache.spark.sql.execution.datasources

import java.util.Locale
import java.util.concurrent.Callable

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, QualifiedTableName}
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoDir, InsertIntoTable, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Replaces generic operations with specific variants that are designed to work with Spark
 * SQL Data Sources.
 *
 * Note that, this rule must be run after `PreprocessTableCreation` and
 * `PreprocessTableInsertion`.
 */
case class DataSourceAnalysis(conf: SQLConf) extends Rule[LogicalPlan] with CastSupport {

  def resolver: Resolver = conf.resolver

  // Visible for testing.
  def convertStaticPartitions(
      sourceAttributes: Seq[Attribute],
      providedPartitions: Map[String, Option[String]],
      targetAttributes: Seq[Attribute],
      targetPartitionSchema: StructType): Seq[NamedExpression] = {

    assert(providedPartitions.exists(_._2.isDefined))

    val staticPartitions = providedPartitions.flatMap {
      case (partKey, Some(partValue)) => (partKey, partValue) :: Nil
      case (_, None) => Nil
    }

    // The sum of the number of static partition columns and columns provided in the SELECT
    // clause needs to match the number of columns of the target table.
    if (staticPartitions.size + sourceAttributes.size != targetAttributes.size) {
      throw new AnalysisException(
        s"The data to be inserted needs to have the same number of " +
          s"columns as the target table: target table has ${targetAttributes.size} " +
          s"column(s) but the inserted data has ${sourceAttributes.size + staticPartitions.size} " +
          s"column(s), which contain ${staticPartitions.size} partition column(s) having " +
          s"assigned constant values.")
    }

    if (providedPartitions.size != targetPartitionSchema.fields.size) {
      throw new AnalysisException(
        s"The data to be inserted needs to have the same number of " +
          s"partition columns as the target table: target table " +
          s"has ${targetPartitionSchema.fields.size} partition column(s) but the inserted " +
          s"data has ${providedPartitions.size} partition columns specified.")
    }

    staticPartitions.foreach {
      case (partKey, partValue) =>
        if (!targetPartitionSchema.fields.exists(field => resolver(field.name, partKey))) {
          throw new AnalysisException(
            s"$partKey is not a partition column. Partition columns are " +
              s"${targetPartitionSchema.fields.map(_.name).mkString("[", ",", "]")}")
        }
    }

    val partitionList = targetPartitionSchema.fields.map { field =>
      val potentialSpecs = staticPartitions.filter {
        case (partKey, partValue) => resolver(field.name, partKey)
      }
      if (potentialSpecs.isEmpty) {
        None
      } else if (potentialSpecs.size == 1) {
        val partValue = potentialSpecs.head._2
        Some(Alias(cast(Literal(partValue), field.dataType), field.name)())
      } else {
        throw new AnalysisException(
          s"Partition column ${field.name} have multiple values specified, " +
            s"${potentialSpecs.mkString("[", ", ", "]")}. Please only specify a single value.")
      }
    }

    // We first drop all leading static partitions using dropWhile and check if there is
    // any static partition appear after dynamic partitions.
    partitionList.dropWhile(_.isDefined).collectFirst {
      case Some(_) =>
        throw new AnalysisException(
          s"The ordering of partition columns is " +
            s"${targetPartitionSchema.fields.map(_.name).mkString("[", ",", "]")}. " +
            "All partition columns having constant values need to appear before other " +
            "partition columns that do not have an assigned constant value.")
    }

    assert(partitionList.take(staticPartitions.size).forall(_.isDefined))
    val projectList =
      sourceAttributes.take(targetAttributes.size - targetPartitionSchema.fields.size) ++
        partitionList.take(staticPartitions.size).map(_.get) ++
        sourceAttributes.takeRight(targetPartitionSchema.fields.size - staticPartitions.size)

    projectList
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CreateTable(tableDesc, mode, None) if DDLUtils.isDatasourceTable(tableDesc) =>
      DDLUtils.checkDataColNames(tableDesc)
      CreateDataSourceTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)

    case CreateTable(tableDesc, mode, Some(query))
        if query.resolved && DDLUtils.isDatasourceTable(tableDesc) =>
      DDLUtils.checkDataColNames(tableDesc.copy(schema = query.schema))
      CreateDataSourceTableAsSelectCommand(tableDesc, mode, query, query.output.map(_.name))

    case InsertIntoTable(l @ LogicalRelation(_: InsertableRelation, _, _, _),
        parts, query, overwrite, false) if parts.isEmpty =>
      InsertIntoDataSourceCommand(l, query, overwrite)

    case InsertIntoDir(_, storage, provider, query, overwrite)
      if provider.isDefined && provider.get.toLowerCase(Locale.ROOT) != DDLUtils.HIVE_PROVIDER =>

      val outputPath = new Path(storage.locationUri.get)
      if (overwrite) DDLUtils.verifyNotReadPath(query, outputPath)

      InsertIntoDataSourceDirCommand(storage, provider.get, query, overwrite)

    case i @ InsertIntoTable(
        l @ LogicalRelation(t: HadoopFsRelation, _, table, _), parts, query, overwrite, _) =>
      // If the InsertIntoTable command is for a partitioned HadoopFsRelation and
      // the user has specified static partitions, we add a Project operator on top of the query
      // to include those constant column values in the query result.
      //
      // Example:
      // Let's say that we have a table "t", which is created by
      // CREATE TABLE t (a INT, b INT, c INT) USING parquet PARTITIONED BY (b, c)
      // The statement of "INSERT INTO TABLE t PARTITION (b=2, c) SELECT 1, 3"
      // will be converted to "INSERT INTO TABLE t PARTITION (b, c) SELECT 1, 2, 3".
      //
      // Basically, we will put those partition columns having a assigned value back
      // to the SELECT clause. The output of the SELECT clause is organized as
      // normal_columns static_partitioning_columns dynamic_partitioning_columns.
      // static_partitioning_columns are partitioning columns having assigned
      // values in the PARTITION clause (e.g. b in the above example).
      // dynamic_partitioning_columns are partitioning columns that do not assigned
      // values in the PARTITION clause (e.g. c in the above example).
      val actualQuery = if (parts.exists(_._2.isDefined)) {
        val projectList = convertStaticPartitions(
          sourceAttributes = query.output,
          providedPartitions = parts,
          targetAttributes = l.output,
          targetPartitionSchema = t.partitionSchema)
        Project(projectList, query)
      } else {
        query
      }

      // Sanity check
      if (t.location.rootPaths.size != 1) {
        throw new AnalysisException("Can only write data to relations with a single path.")
      }

      val outputPath = t.location.rootPaths.head
      if (overwrite) DDLUtils.verifyNotReadPath(actualQuery, outputPath)

      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append

      val partitionSchema = actualQuery.resolve(
        t.partitionSchema, t.sparkSession.sessionState.analyzer.resolver)
      val staticPartitions = parts.filter(_._2.nonEmpty).map { case (k, v) => k -> v.get }

      InsertIntoHadoopFsRelationCommand(
        outputPath,
        staticPartitions,
        i.ifPartitionNotExists,
        partitionSchema,
        t.bucketSpec,
        t.fileFormat,
        t.options,
        actualQuery,
        mode,
        table,
        Some(t.location),
        actualQuery.output.map(_.name))
  }
}


/**
 * Replaces [[UnresolvedCatalogRelation]] with concrete relation logical plans.
 *
 * TODO: we should remove the special handling for hive tables after completely making hive as a
 * data source.
 */
class FindDataSourceTable(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private def readDataSourceTable(table: CatalogTable): LogicalPlan = {
    val qualifiedTableName = QualifiedTableName(table.database, table.identifier.table)
    val catalog = sparkSession.sessionState.catalog
    catalog.getCachedPlan(qualifiedTableName, new Callable[LogicalPlan]() {
      override def call(): LogicalPlan = {
        val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
        val dataSource =
          DataSource(
            sparkSession,
            // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
            // inferred at runtime. We should still support it.
            userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
            partitionColumns = table.partitionColumnNames,
            bucketSpec = table.bucketSpec,
            className = table.provider.get,
            options = table.storage.properties ++ pathOption,
            catalogTable = Some(table))

        LogicalRelation(dataSource.resolveRelation(checkFilesExist = false), table)
      }
    })
  }

  private def readHiveTable(table: CatalogTable): LogicalPlan = {
    HiveTableRelation(
      table,
      // Hive table columns are always nullable.
      table.dataSchema.asNullable.toAttributes,
      table.partitionSchema.asNullable.toAttributes)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case i @ InsertIntoTable(UnresolvedCatalogRelation(tableMeta), _, _, _, _)
        if DDLUtils.isDatasourceTable(tableMeta) =>
      i.copy(table = readDataSourceTable(tableMeta))

    case i @ InsertIntoTable(UnresolvedCatalogRelation(tableMeta), _, _, _, _) =>
      i.copy(table = readHiveTable(tableMeta))

    case UnresolvedCatalogRelation(tableMeta) if DDLUtils.isDatasourceTable(tableMeta) =>
      readDataSourceTable(tableMeta)

    case UnresolvedCatalogRelation(tableMeta) =>
      readHiveTable(tableMeta)
  }
}


/**
 * A Strategy for planning scans over data sources defined using the sources API.
 */
case class DataSourceStrategy(conf: SQLConf) extends Strategy with Logging with CastSupport {
  import DataSourceStrategy._

  def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: CatalystScan, _, _, _)) =>
      pruneFilterProjectRaw(
        l,
        projects,
        filters,
        (requestedColumns, allPredicates, _) =>
          toCatalystRDD(l, requestedColumns, t.buildScan(requestedColumns, allPredicates))) :: Nil

    case PhysicalOperation(projects, filters,
                           l @ LogicalRelation(t: PrunedFilteredScan, _, _, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f))) :: Nil

    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: PrunedScan, _, _, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, _) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray))) :: Nil

    case l @ LogicalRelation(baseRelation: TableScan, _, _, _) =>
      RowDataSourceScanExec(
        l.output,
        l.output.indices,
        Set.empty,
        Set.empty,
        toCatalystRDD(l, baseRelation.buildScan()),
        baseRelation,
        None) :: Nil

    case _ => Nil
  }

  // Based on Public API.
  private def pruneFilterProject(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Seq[Attribute], Array[Filter]) => RDD[InternalRow]) = {
    pruneFilterProjectRaw(
      relation,
      projects,
      filterPredicates,
      (requestedColumns, _, pushedFilters) => {
        scanBuilder(requestedColumns, pushedFilters.toArray)
      })
  }

  // Based on Catalyst expressions. The `scanBuilder` function accepts three arguments:
  //
  //  1. A `Seq[Attribute]`, containing all required column attributes. Used to handle relation
  //     traits that support column pruning (e.g. `PrunedScan` and `PrunedFilteredScan`).
  //
  //  2. A `Seq[Expression]`, containing all gathered Catalyst filter expressions, only used for
  //     `CatalystScan`.
  //
  //  3. A `Seq[Filter]`, containing all data source `Filter`s that are converted from (possibly a
  //     subset of) Catalyst filter expressions and can be handled by `relation`.  Used to handle
  //     relation traits (`CatalystScan` excluded) that support filter push-down (e.g.
  //     `PrunedFilteredScan` and `HadoopFsRelation`).
  //
  // Note that 2 and 3 shouldn't be used together.
  private def pruneFilterProjectRaw(
    relation: LogicalRelation,
    projects: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    scanBuilder: (Seq[Attribute], Seq[Expression], Seq[Filter]) => RDD[InternalRow]): SparkPlan = {

    val projectSet = AttributeSet(projects.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val candidatePredicates = filterPredicates.map { _ transform {
      case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
    }}

    val (unhandledPredicates, pushedFilters, handledFilters) =
      selectFilters(relation.relation, candidatePredicates)

    // Combines all Catalyst filter `Expression`s that are either not convertible to data source
    // `Filter`s or cannot be handled by `relation`.
    val filterCondition = unhandledPredicates.reduceLeftOption(expressions.And)

    if (projects.map(_.toAttribute) == projects &&
        projectSet.size == projects.size &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns = projects
        // Safe due to if above.
        .asInstanceOf[Seq[Attribute]]
        // Match original case of attributes.
        .map(relation.attributeMap)

      val scan = RowDataSourceScanExec(
        relation.output,
        requestedColumns.map(relation.output.indexOf),
        pushedFilters.toSet,
        handledFilters,
        scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
        relation.relation,
        relation.catalogTable.map(_.identifier))
      filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan)
    } else {
      // A set of column attributes that are only referenced by pushed down filters.  We can
      // eliminate them from requested columns.
      val handledSet = {
        val handledPredicates = filterPredicates.filterNot(unhandledPredicates.contains)
        val unhandledSet = AttributeSet(unhandledPredicates.flatMap(_.references))
        AttributeSet(handledPredicates.flatMap(_.references)) --
          (projectSet ++ unhandledSet).map(relation.attributeMap)
      }
      // Don't request columns that are only referenced by pushed filters.
      val requestedColumns =
        (projectSet ++ filterSet -- handledSet).map(relation.attributeMap).toSeq

      val scan = RowDataSourceScanExec(
        relation.output,
        requestedColumns.map(relation.output.indexOf),
        pushedFilters.toSet,
        handledFilters,
        scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
        relation.relation,
        relation.catalogTable.map(_.identifier))
      execution.ProjectExec(
        projects, filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan))
    }
  }

  /**
   * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
   */
  private[this] def toCatalystRDD(
      relation: LogicalRelation,
      output: Seq[Attribute],
      rdd: RDD[Row]): RDD[InternalRow] = {
    if (relation.relation.needConversion) {
      execution.RDDConversions.rowToRowRdd(rdd, output.map(_.dataType))
    } else {
      rdd.asInstanceOf[RDD[InternalRow]]
    }
  }

  /**
   * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
   */
  private[this] def toCatalystRDD(relation: LogicalRelation, rdd: RDD[Row]): RDD[InternalRow] = {
    toCatalystRDD(relation, relation.output, rdd)
  }
}

object DataSourceStrategy {
  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   *
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilter(predicate: Expression): Option[Filter] = {
    predicate match {
      case expressions.EqualTo(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))
      case expressions.EqualTo(Literal(v, t), a: Attribute) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))

      case expressions.EqualNullSafe(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualNullSafe(a.name, convertToScala(v, t)))
      case expressions.EqualNullSafe(Literal(v, t), a: Attribute) =>
        Some(sources.EqualNullSafe(a.name, convertToScala(v, t)))

      case expressions.GreaterThan(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))
      case expressions.GreaterThan(Literal(v, t), a: Attribute) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))

      case expressions.LessThan(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))
      case expressions.LessThan(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))

      case expressions.GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.GreaterThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))

      case expressions.LessThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.LessThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))

      case expressions.InSet(a: Attribute, set) =>
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, set.toArray.map(toScala)))

      // Because we only convert In to InSet in Optimizer when there are more than certain
      // items. So it is possible we still get an In expression here that needs to be pushed
      // down.
      case expressions.In(a: Attribute, list) if !list.exists(!_.isInstanceOf[Literal]) =>
        val hSet = list.map(e => e.eval(EmptyRow))
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, hSet.toArray.map(toScala)))

      case expressions.IsNull(a: Attribute) =>
        Some(sources.IsNull(a.name))
      case expressions.IsNotNull(a: Attribute) =>
        Some(sources.IsNotNull(a.name))

      case expressions.And(left, right) =>
        // See SPARK-12218 for detailed discussion
        // It is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have (a = 2 AND trim(b) = 'blah') OR (c > 0)
        // and we do not understand how to convert trim(b) = 'blah'.
        // If we only convert a = 2, we will end up with
        // (a = 2) OR (c > 0), which will generate wrong results.
        // Pushing one leg of AND down is only safe to do at the top level.
        // You can see ParquetFilters' createFilter for more details.
        for {
          leftFilter <- translateFilter(left)
          rightFilter <- translateFilter(right)
        } yield sources.And(leftFilter, rightFilter)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translateFilter(left)
          rightFilter <- translateFilter(right)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translateFilter(child).map(sources.Not)

      case expressions.StartsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringStartsWith(a.name, v.toString))

      case expressions.EndsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringEndsWith(a.name, v.toString))

      case expressions.Contains(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringContains(a.name, v.toString))

      case _ => None
    }
  }

  /**
   * Selects Catalyst predicate [[Expression]]s which are convertible into data source [[Filter]]s
   * and can be handled by `relation`.
   *
   * @return A triplet of `Seq[Expression]`, `Seq[Filter]`, and `Seq[Filter]` . The first element
   *         contains all Catalyst predicate [[Expression]]s that are either not convertible or
   *         cannot be handled by `relation`. The second element contains all converted data source
   *         [[Filter]]s that will be pushed down to the data source. The third element contains
   *         all [[Filter]]s that are completely filtered at the DataSource.
   */
  protected[sql] def selectFilters(
      relation: BaseRelation,
      predicates: Seq[Expression]): (Seq[Expression], Seq[Filter], Set[Filter]) = {

    // For conciseness, all Catalyst filter expressions of type `expressions.Expression` below are
    // called `predicate`s, while all data source filters of type `sources.Filter` are simply called
    // `filter`s.

    // A map from original Catalyst expressions to corresponding translated data source filters.
    // If a predicate is not in this map, it means it cannot be pushed down.
    val translatedMap: Map[Expression, Filter] = predicates.flatMap { p =>
      translateFilter(p).map(f => p -> f)
    }.toMap

    val pushedFilters: Seq[Filter] = translatedMap.values.toSeq

    // Catalyst predicate expressions that cannot be converted to data source filters.
    val nonconvertiblePredicates = predicates.filterNot(translatedMap.contains)

    // Data source filters that cannot be handled by `relation`. An unhandled filter means
    // the data source cannot guarantee the rows returned can pass the filter.
    // As a result we must return it so Spark can plan an extra filter operator.
    val unhandledFilters = relation.unhandledFilters(translatedMap.values.toArray).toSet
    val unhandledPredicates = translatedMap.filter { case (p, f) =>
      unhandledFilters.contains(f)
    }.keys
    val handledFilters = pushedFilters.toSet -- unhandledFilters

    (nonconvertiblePredicates ++ unhandledPredicates, pushedFilters, handledFilters)
  }
}
