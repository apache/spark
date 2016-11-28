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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{MapPartitionsRDD, RDD, UnionRDD}
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, expressions}
import org.apache.spark.sql.execution.PhysicalRDD.{INPUT_PATHS, PUSHED_FILTERS}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{SaveMode, Strategy, execution, sources, _}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.apache.spark.{Logging, TaskContext}

/**
 * A Strategy for planning scans over data sources defined using the sources API.
 */
private[sql] object DataSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: CatalystScan, _)) =>
      pruneFilterProjectRaw(
        l,
        projects,
        filters,
        (requestedColumns, allPredicates, _) =>
          toCatalystRDD(l, requestedColumns, t.buildScan(requestedColumns, allPredicates))) :: Nil

    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: PrunedFilteredScan, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f))) :: Nil

    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: PrunedScan, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, _) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray))) :: Nil

    // Scanning partitioned HadoopFsRelation
    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: HadoopFsRelation, _))
        if t.partitionSpec.partitionColumns.nonEmpty =>
      // We divide the filter expressions into 3 parts
      val partitionColumns = AttributeSet(
        t.partitionColumns.map(c => l.output.find(_.name == c.name).get))

      // Only pruning the partition keys
      val partitionFilters = filters.filter(_.references.subsetOf(partitionColumns))

      // Only pushes down predicates that do not reference partition keys.
      val pushedFilters = filters.filter(_.references.intersect(partitionColumns).isEmpty)

      // Predicates with both partition keys and attributes
      val partitionAndNormalColumnFilters =
        filters.toSet -- partitionFilters.toSet -- pushedFilters.toSet

      val selectedPartitions = prunePartitions(partitionFilters, t.partitionSpec).toArray

      logInfo {
        val total = t.partitionSpec.partitions.length
        val selected = selectedPartitions.length
        val percentPruned = (1 - selected.toDouble / total.toDouble) * 100
        s"Selected $selected partitions out of $total, pruned $percentPruned% partitions."
      }

      // need to add projections from "partitionAndNormalColumnAttrs" in if it is not empty
      val partitionAndNormalColumnAttrs = AttributeSet(partitionAndNormalColumnFilters)
      val partitionAndNormalColumnProjs = if (partitionAndNormalColumnAttrs.isEmpty) {
        projects
      } else {
        (partitionAndNormalColumnAttrs ++ projects).toSeq
      }

      val scan = buildPartitionedTableScan(
        l,
        partitionAndNormalColumnProjs,
        pushedFilters,
        t.partitionSpec.partitionColumns,
        selectedPartitions)

      // Add a Projection to guarantee the original projection:
      // this is because "partitionAndNormalColumnAttrs" may be different
      // from the original "projects", in elements or their ordering

      partitionAndNormalColumnFilters.reduceLeftOption(expressions.And).map(cf =>
        if (projects.isEmpty || projects == partitionAndNormalColumnProjs) {
          // if the original projection is empty, no need for the additional Project either
          execution.Filter(cf, scan)
        } else {
          execution.Project(projects, execution.Filter(cf, scan))
        }
      ).getOrElse(scan) :: Nil

    // Scanning non-partitioned HadoopFsRelation
    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: HadoopFsRelation, _)) =>
      // See buildPartitionedTableScan for the reason that we need to create a shard
      // broadcast HadoopConf.
      val sharedHadoopConf = SparkHadoopUtil.get.conf
      val confBroadcast =
        t.sqlContext.sparkContext.broadcast(new SerializableConfiguration(sharedHadoopConf))
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, f) => t.buildInternalScan(a.map(_.name).toArray, f, t.paths, confBroadcast)) :: Nil

    case l @ LogicalRelation(baseRelation: TableScan, _) =>
      execution.PhysicalRDD.createFromDataSource(
        l.output, toCatalystRDD(l, baseRelation.buildScan()), baseRelation) :: Nil

    case i @ logical.InsertIntoTable(l @ LogicalRelation(t: InsertableRelation, _),
      part, query, overwrite, false) if part.isEmpty =>
      execution.ExecutedCommand(InsertIntoDataSource(l, query, overwrite)) :: Nil

    case i @ logical.InsertIntoTable(
      l @ LogicalRelation(t: HadoopFsRelation, _), part, query, overwrite, false) =>
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
      execution.ExecutedCommand(InsertIntoHadoopFsRelation(t, query, mode)) :: Nil

    case _ => Nil
  }

  private def buildPartitionedTableScan(
      logicalRelation: LogicalRelation,
      projections: Seq[NamedExpression],
      filters: Seq[Expression],
      partitionColumns: StructType,
      partitions: Array[Partition]): SparkPlan = {
    val relation = logicalRelation.relation.asInstanceOf[HadoopFsRelation]

    // Because we are creating one RDD per partition, we need to have a shared HadoopConf.
    // Otherwise, the cost of broadcasting HadoopConf in every RDD will be high.
    val sharedHadoopConf = SparkHadoopUtil.get.conf
    val confBroadcast =
      relation.sqlContext.sparkContext.broadcast(new SerializableConfiguration(sharedHadoopConf))
    val partitionColumnNames = partitionColumns.fieldNames.toSet

    // Now, we create a scan builder, which will be used by pruneFilterProject. This scan builder
    // will union all partitions and attach partition values if needed.
    val scanBuilder = {
      (requiredColumns: Seq[Attribute], filters: Array[Filter]) => {
        val requiredDataColumns =
          requiredColumns.filterNot(c => partitionColumnNames.contains(c.name))

        // Builds RDD[Row]s for each selected partition.
        val perPartitionRows = partitions.map { case Partition(partitionValues, dir) =>
          // Don't scan any partition columns to save I/O.  Here we are being optimistic and
          // assuming partition columns data stored in data files are always consistent with those
          // partition values encoded in partition directory paths.
          val dataRows = relation.buildInternalScan(
            requiredDataColumns.map(_.name).toArray, filters, Array(dir), confBroadcast)

          // Merges data values with partition values.
          mergeWithPartitionValues(
            requiredColumns,
            requiredDataColumns,
            partitionColumns,
            partitionValues,
            dataRows)
        }

        val unionedRows =
          if (perPartitionRows.length == 0) {
            relation.sqlContext.emptyResult
          } else {
            new UnionRDD(relation.sqlContext.sparkContext, perPartitionRows)
          }

        unionedRows
      }
    }

    // Create the scan operator. If needed, add Filter and/or Project on top of the scan.
    // The added Filter/Project is on top of the unioned RDD. We do not want to create
    // one Filter/Project for every partition.
    val sparkPlan = pruneFilterProject(
      logicalRelation,
      projections,
      filters,
      scanBuilder)

    sparkPlan
  }

  private def mergeWithPartitionValues(
      requiredColumns: Seq[Attribute],
      dataColumns: Seq[Attribute],
      partitionColumnSchema: StructType,
      partitionValues: InternalRow,
      dataRows: RDD[InternalRow]): RDD[InternalRow] = {
    // If output columns contain any partition column(s), we need to merge scanned data
    // columns and requested partition columns to form the final result.
    if (requiredColumns != dataColumns) {
      // Builds `AttributeReference`s for all partition columns so that we can use them to project
      // required partition columns.  Note that if a partition column appears in `requiredColumns`,
      // we should use the `AttributeReference` in `requiredColumns`.
      val partitionColumns = {
        val requiredColumnMap = requiredColumns.map(a => a.name -> a).toMap
        partitionColumnSchema.toAttributes.map { a =>
          requiredColumnMap.getOrElse(a.name, a)
        }
      }

      val mapPartitionsFunc = (_: TaskContext, _: Int, iterator: Iterator[InternalRow]) => {
        // Note that we can't use an `UnsafeRowJoiner` to replace the following `JoinedRow` and
        // `UnsafeProjection`.  Because the projection may also adjust column order.
        val mutableJoinedRow = new JoinedRow()
        val unsafePartitionValues = UnsafeProjection.create(partitionColumnSchema)(partitionValues)
        val unsafeProjection =
          UnsafeProjection.create(requiredColumns, dataColumns ++ partitionColumns)

        iterator.map { unsafeDataRow =>
          unsafeProjection(mutableJoinedRow(unsafeDataRow, unsafePartitionValues))
        }
      }

      // This is an internal RDD whose call site the user should not be concerned with
      // Since we create many of these (one per partition), the time spent on computing
      // the call site may add up.
      Utils.withDummyCallSite(dataRows.sparkContext) {
        new MapPartitionsRDD(dataRows, mapPartitionsFunc, preservesPartitioning = false)
      }
    } else {
      dataRows
    }
  }

  protected def prunePartitions(
      predicates: Seq[Expression],
      partitionSpec: PartitionSpec): Seq[Partition] = {
    val PartitionSpec(partitionColumns, partitions) = partitionSpec
    val isCaseSensitive = SQLContext.getActive().get.conf.caseSensitiveAnalysis
    val partitionColumnNames = if (isCaseSensitive) {
      partitionColumns.map(_.name).toSet
    } else {
      partitionColumns.map(_.name.toLowerCase).toSet
    }
    val partitionPruningPredicates = predicates.filter {
      if (isCaseSensitive) {
        _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
      } else {
        _.references.map(_.name.toLowerCase).toSet.subsetOf(partitionColumnNames)
      }
    }

    if (partitionPruningPredicates.nonEmpty) {
      val predicate =
        partitionPruningPredicates
          .reduceOption(expressions.And)
          .getOrElse(Literal(true))

      val boundPredicate = InterpretedPredicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = if (isCaseSensitive) {
            partitionColumns.indexWhere(a.name == _.name)
          } else {
            partitionColumns.indexWhere(c => a.name.equalsIgnoreCase(c.name))
          }
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      })

      partitions.filter { case Partition(values, _) => boundPredicate(values) }
    } else {
      partitions
    }
  }

  // Based on Public API.
  protected def pruneFilterProject(
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
  protected def pruneFilterProjectRaw(
    relation: LogicalRelation,
    projects: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    scanBuilder: (Seq[Attribute], Seq[Expression], Seq[Filter]) => RDD[InternalRow]) = {

    val projectSet = AttributeSet(projects.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val candidatePredicates = filterPredicates.map { _ transform {
      case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
    }}

    val (unhandledPredicates, pushedFilters) =
      selectFilters(relation.relation, candidatePredicates)

    // A set of column attributes that are only referenced by pushed down filters.  We can eliminate
    // them from requested columns.
    val handledSet = {
      val handledPredicates = filterPredicates.filterNot(unhandledPredicates.contains)
      val unhandledSet = AttributeSet(unhandledPredicates.flatMap(_.references))
      AttributeSet(handledPredicates.flatMap(_.references)) --
        (projectSet ++ unhandledSet).map(relation.attributeMap)
    }

    // Combines all Catalyst filter `Expression`s that are either not convertible to data source
    // `Filter`s or cannot be handled by `relation`.
    val filterCondition = unhandledPredicates.reduceLeftOption(expressions.And)

    val metadata: Map[String, String] = {
      val pairs = ArrayBuffer.empty[(String, String)]

      if (pushedFilters.nonEmpty) {
        pairs += (PUSHED_FILTERS -> pushedFilters.mkString("[", ", ", "]"))
      }

      relation.relation match {
        case r: HadoopFsRelation => pairs += INPUT_PATHS -> r.paths.mkString(", ")
        case _ =>
      }

      pairs.toMap
    }

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
        // Don't request columns that are only referenced by pushed filters.
        .filterNot(handledSet.contains)

      val scan = execution.PhysicalRDD.createFromDataSource(
        projects.map(_.toAttribute),
        scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
        relation.relation, metadata)
      filterCondition.map(execution.Filter(_, scan)).getOrElse(scan)
    } else {
      // Don't request columns that are only referenced by pushed filters.
      val requestedColumns =
        (projectSet ++ filterSet -- handledSet).map(relation.attributeMap).toSeq

      val scan = execution.PhysicalRDD.createFromDataSource(
        requestedColumns,
        scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
        relation.relation, metadata)
      execution.Project(
        projects, filterCondition.map(execution.Filter(_, scan)).getOrElse(scan))
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
        (translateFilter(left) ++ translateFilter(right)).reduceOption(sources.And)

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
   * @return A pair of `Seq[Expression]` and `Seq[Filter]`. The first element contains all Catalyst
   *         predicate [[Expression]]s that are either not convertible or cannot be handled by
   *         `relation`. The second element contains all converted data source [[Filter]]s that
   *         will be pushed down to the data source.
   */
  protected[sql] def selectFilters(
    relation: BaseRelation,
    predicates: Seq[Expression]): (Seq[Expression], Seq[Filter]) = {

    // For conciseness, all Catalyst filter expressions of type `expressions.Expression` below are
    // called `predicate`s, while all data source filters of type `sources.Filter` are simply called
    // `filter`s.

    val translated: Seq[(Expression, Filter)] =
      for {
        predicate <- predicates
        filter <- translateFilter(predicate)
      } yield predicate -> filter

    // A map from original Catalyst expressions to corresponding translated data source filters.
    val translatedMap: Map[Expression, Filter] = translated.toMap

    // Catalyst predicate expressions that cannot be translated to data source filters.
    val unrecognizedPredicates = predicates.filterNot(translatedMap.contains)

    // Data source filters that cannot be handled by `relation`. The semantic of a unhandled filter
    // at here is that a data source may not be able to apply this filter to every row
    // of the underlying dataset.
    val unhandledFilters = relation.unhandledFilters(translatedMap.values.toArray).toSet

    val (unhandled, handled) = translated.partition {
      case (predicate, filter) =>
        unhandledFilters.contains(filter)
    }

    // Catalyst predicate expressions that can be translated to data source filters, but cannot be
    // handled by `relation`.
    val (unhandledPredicates, _) = unhandled.unzip

    // Translated data source filters that can be handled by `relation`
    val (_, handledFilters) = handled.unzip

    // translated contains all filters that have been converted to the public Filter interface.
    // We should always push them to the data source no matter whether the data source can apply
    // a filter to every row or not.
    val (_, translatedFilters) = translated.unzip

    (unrecognizedPredicates ++ unhandledPredicates, translatedFilters)
  }
}
