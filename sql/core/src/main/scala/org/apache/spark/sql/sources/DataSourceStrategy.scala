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

package org.apache.spark.sql.sources

import org.apache.spark.{Logging, SerializableWritable, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{MapPartitionsRDD, RDD, UnionRDD}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{StringType, StructType, UTF8String}
import org.apache.spark.sql.{SaveMode, Strategy, execution, sources}
import org.apache.spark.util.Utils

/**
 * A Strategy for planning scans over data sources defined using the sources API.
 */
private[sql] object DataSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    case PhysicalOperation(projectList, filters, l @ LogicalRelation(t: CatalystScan)) =>
      pruneFilterProjectRaw(
        l,
        projectList,
        filters,
        (a, f) => t.buildScan(a, f)) :: Nil

    case PhysicalOperation(projectList, filters, l @ LogicalRelation(t: PrunedFilteredScan)) =>
      pruneFilterProject(
        l,
        projectList,
        filters,
        (a, f) => t.buildScan(a, f)) :: Nil

    case PhysicalOperation(projectList, filters, l @ LogicalRelation(t: PrunedScan)) =>
      pruneFilterProject(
        l,
        projectList,
        filters,
        (a, _) => t.buildScan(a)) :: Nil

    // Scanning partitioned HadoopFsRelation
    case PhysicalOperation(projectList, filters, l @ LogicalRelation(t: HadoopFsRelation))
        if t.partitionSpec.partitionColumns.nonEmpty =>
      val selectedPartitions = prunePartitions(filters, t.partitionSpec).toArray

      logInfo {
        val total = t.partitionSpec.partitions.length
        val selected = selectedPartitions.length
        val percentPruned = (1 - total.toDouble / selected.toDouble) * 100
        s"Selected $selected partitions out of $total, pruned $percentPruned% partitions."
      }

      // Only pushes down predicates that do not reference partition columns.
      val pushedFilters = {
        val partitionColumnNames = t.partitionSpec.partitionColumns.map(_.name).toSet
        filters.filter { f =>
          val referencedColumnNames = f.references.map(_.name).toSet
          referencedColumnNames.intersect(partitionColumnNames).isEmpty
        }
      }

      buildPartitionedTableScan(
        l,
        projectList,
        pushedFilters,
        t.partitionSpec.partitionColumns,
        selectedPartitions) :: Nil

    // Scanning non-partitioned HadoopFsRelation
    case PhysicalOperation(projectList, filters, l @ LogicalRelation(t: HadoopFsRelation)) =>
      // See buildPartitionedTableScan for the reason that we need to create a shard
      // broadcast HadoopConf.
      val sharedHadoopConf = SparkHadoopUtil.get.conf
      val confBroadcast =
        t.sqlContext.sparkContext.broadcast(new SerializableWritable(sharedHadoopConf))
      pruneFilterProject(
        l,
        projectList,
        filters,
        (a, f) => t.buildScan(a, f, t.paths, confBroadcast)) :: Nil

    case l @ LogicalRelation(t: TableScan) =>
      createPhysicalRDD(l.relation, l.output, t.buildScan()) :: Nil

    case i @ logical.InsertIntoTable(
      l @ LogicalRelation(t: InsertableRelation), part, query, overwrite, false) if part.isEmpty =>
      execution.ExecutedCommand(InsertIntoDataSource(l, query, overwrite)) :: Nil

    case i @ logical.InsertIntoTable(
      l @ LogicalRelation(t: HadoopFsRelation), part, query, overwrite, false) if part.isEmpty =>
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
      execution.ExecutedCommand(
        InsertIntoHadoopFsRelation(t, query, Array.empty[String], mode)) :: Nil

    case _ => Nil
  }

  private def buildPartitionedTableScan(
      logicalRelation: LogicalRelation,
      projections: Seq[NamedExpression],
      filters: Seq[Expression],
      partitionColumns: StructType,
      partitions: Array[Partition]) = {
    val output = projections.map(_.toAttribute)
    val relation = logicalRelation.relation.asInstanceOf[HadoopFsRelation]

    // Because we are creating one RDD per partition, we need to have a shared HadoopConf.
    // Otherwise, the cost of broadcasting HadoopConf in every RDD will be high.
    val sharedHadoopConf = SparkHadoopUtil.get.conf
    val confBroadcast =
      relation.sqlContext.sparkContext.broadcast(new SerializableWritable(sharedHadoopConf))

    // Builds RDD[Row]s for each selected partition.
    val perPartitionRows = partitions.map { case Partition(partitionValues, dir) =>
      // The table scan operator (PhysicalRDD) which retrieves required columns from data files.
      // Notice that the schema of data files, represented by `relation.dataSchema`, may contain
      // some partition column(s).
      val scan =
        pruneFilterProject(
          logicalRelation,
          projections,
          filters,
          (requiredColumns, filters) => {
            val partitionColNames = partitionColumns.fieldNames

            // Don't scan any partition columns to save I/O.  Here we are being optimistic and
            // assuming partition columns data stored in data files are always consistent with those
            // partition values encoded in partition directory paths.
            val nonPartitionColumns = requiredColumns.filterNot(partitionColNames.contains)
            val dataRows =
              relation.buildScan(nonPartitionColumns, filters, Array(dir), confBroadcast)

            // Merges data values with partition values.
            mergeWithPartitionValues(
              relation.schema,
              requiredColumns,
              partitionColNames,
              partitionValues,
              dataRows)
          })

      scan.execute()
    }

    val unionedRows =
      if (perPartitionRows.length == 0) {
        relation.sqlContext.emptyResult
      } else {
        new UnionRDD(relation.sqlContext.sparkContext, perPartitionRows)
      }

    createPhysicalRDD(logicalRelation.relation, output, unionedRows)
  }

  private def mergeWithPartitionValues(
      schema: StructType,
      requiredColumns: Array[String],
      partitionColumns: Array[String],
      partitionValues: Row,
      dataRows: RDD[Row]): RDD[Row] = {
    val nonPartitionColumns = requiredColumns.filterNot(partitionColumns.contains)

    // If output columns contain any partition column(s), we need to merge scanned data
    // columns and requested partition columns to form the final result.
    if (!requiredColumns.sameElements(nonPartitionColumns)) {
      val mergers = requiredColumns.zipWithIndex.map { case (name, index) =>
        // To see whether the `index`-th column is a partition column...
        val i = partitionColumns.indexOf(name)
        if (i != -1) {
          // If yes, gets column value from partition values.
          (mutableRow: MutableRow, dataRow: expressions.Row, ordinal: Int) => {
            mutableRow(ordinal) = partitionValues(i)
          }
        } else {
          // Otherwise, inherits the value from scanned data.
          val i = nonPartitionColumns.indexOf(name)
          (mutableRow: MutableRow, dataRow: expressions.Row, ordinal: Int) => {
            mutableRow(ordinal) = dataRow(i)
          }
        }
      }

      // Since we know for sure that this closure is serializable, we can avoid the overhead
      // of cleaning a closure for each RDD by creating our own MapPartitionsRDD. Functionally
      // this is equivalent to calling `dataRows.mapPartitions(mapPartitionsFunc)` (SPARK-7718).
      val mapPartitionsFunc = (_: TaskContext, _: Int, iterator: Iterator[Row]) => {
        val dataTypes = requiredColumns.map(schema(_).dataType)
        val mutableRow = new SpecificMutableRow(dataTypes)
        iterator.map { dataRow =>
          var i = 0
          while (i < mutableRow.length) {
            mergers(i)(mutableRow, dataRow, i)
            i += 1
          }
          mutableRow.asInstanceOf[expressions.Row]
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
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    if (partitionPruningPredicates.nonEmpty) {
      val predicate =
        partitionPruningPredicates
          .reduceOption(expressions.And)
          .getOrElse(Literal(true))

      val boundPredicate = InterpretedPredicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
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
      projectList: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Array[String], Array[Filter]) => RDD[Row]) = {
    pruneFilterProjectRaw(
      relation,
      projectList,
      filterPredicates,
      (requestedColumns, pushedFilters) => {
        scanBuilder(requestedColumns.map(_.name).toArray, selectFilters(pushedFilters).toArray)
      })
  }

  // Based on Catalyst expressions.
  protected def pruneFilterProjectRaw(
      relation: LogicalRelation,
      projectList: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Seq[Attribute], Seq[Expression]) => RDD[Row]) = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition = filterPredicates.reduceLeftOption(expressions.And)

    val pushedFilters = filterPredicates.map { _ transform {
      case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
    }}

    if (projectList.map(_.toAttribute) == projectList &&
        projectSet.size == projectList.size &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns =
        projectList.asInstanceOf[Seq[Attribute]] // Safe due to if above.
          .map(relation.attributeMap)            // Match original case of attributes.

      val scan = createPhysicalRDD(relation.relation, projectList.map(_.toAttribute),
          scanBuilder(requestedColumns, pushedFilters))
      filterCondition.map(execution.Filter(_, scan)).getOrElse(scan)
    } else {
      val requestedColumns = (projectSet ++ filterSet).map(relation.attributeMap).toSeq

      val scan = createPhysicalRDD(relation.relation, requestedColumns,
        scanBuilder(requestedColumns, pushedFilters))
      execution.Project(projectList, filterCondition.map(execution.Filter(_, scan)).getOrElse(scan))
    }
  }

  private[this] def createPhysicalRDD(
      relation: BaseRelation,
      output: Seq[Attribute],
      rdd: RDD[Row]): SparkPlan = {
    val converted = if (relation.needConversion) {
      execution.RDDConversions.rowToRowRdd(rdd, relation.schema)
    } else {
      rdd
    }
    execution.PhysicalRDD(output, converted)
  }

  /**
   * Selects Catalyst predicate [[Expression]]s which are convertible into data source [[Filter]]s,
   * and convert them.
   */
  protected[sql] def selectFilters(filters: Seq[Expression]) = {
    def translate(predicate: Expression): Option[Filter] = predicate match {
      case expressions.EqualTo(a: Attribute, Literal(v, _)) =>
        Some(sources.EqualTo(a.name, v))
      case expressions.EqualTo(Literal(v, _), a: Attribute) =>
        Some(sources.EqualTo(a.name, v))

      case expressions.GreaterThan(a: Attribute, Literal(v, _)) =>
        Some(sources.GreaterThan(a.name, v))
      case expressions.GreaterThan(Literal(v, _), a: Attribute) =>
        Some(sources.LessThan(a.name, v))

      case expressions.LessThan(a: Attribute, Literal(v, _)) =>
        Some(sources.LessThan(a.name, v))
      case expressions.LessThan(Literal(v, _), a: Attribute) =>
        Some(sources.GreaterThan(a.name, v))

      case expressions.GreaterThanOrEqual(a: Attribute, Literal(v, _)) =>
        Some(sources.GreaterThanOrEqual(a.name, v))
      case expressions.GreaterThanOrEqual(Literal(v, _), a: Attribute) =>
        Some(sources.LessThanOrEqual(a.name, v))

      case expressions.LessThanOrEqual(a: Attribute, Literal(v, _)) =>
        Some(sources.LessThanOrEqual(a.name, v))
      case expressions.LessThanOrEqual(Literal(v, _), a: Attribute) =>
        Some(sources.GreaterThanOrEqual(a.name, v))

      case expressions.InSet(a: Attribute, set) =>
        Some(sources.In(a.name, set.toArray))

      case expressions.IsNull(a: Attribute) =>
        Some(sources.IsNull(a.name))
      case expressions.IsNotNull(a: Attribute) =>
        Some(sources.IsNotNull(a.name))

      case expressions.And(left, right) =>
        (translate(left) ++ translate(right)).reduceOption(sources.And)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translate(left)
          rightFilter <- translate(right)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translate(child).map(sources.Not)

      case expressions.StartsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringStartsWith(a.name, v.toString))

      case expressions.EndsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringEndsWith(a.name, v.toString))

      case expressions.Contains(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringContains(a.name, v.toString))

      case _ => None
    }

    filters.flatMap(translate)
  }
}
