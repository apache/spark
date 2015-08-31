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

import org.apache.spark.{Logging, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{MapPartitionsRDD, RDD, UnionRDD}
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, expressions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{SaveMode, Strategy, execution, sources, _}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * A Strategy for planning scans over data sources defined using the sources API.
 */
private[sql] object DataSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: CatalystScan)) =>
      pruneFilterProjectRaw(
        l,
        projects,
        filters,
        (a, f) => toCatalystRDD(l, a, t.buildScan(a, f))) :: Nil

    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: PrunedFilteredScan)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f))) :: Nil

    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: PrunedScan)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, _) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray))) :: Nil

    // Scanning partitioned HadoopFsRelation
    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: HadoopFsRelation))
        if t.partitionSpec.partitionColumns.nonEmpty =>
      val selectedPartitions = prunePartitions(filters, t.partitionSpec).toArray

      logInfo {
        val total = t.partitionSpec.partitions.length
        val selected = selectedPartitions.length
        val percentPruned = (1 - selected.toDouble / total.toDouble) * 100
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
        projects,
        pushedFilters,
        t.partitionSpec.partitionColumns,
        selectedPartitions) :: Nil

    // Scanning non-partitioned HadoopFsRelation
    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: HadoopFsRelation)) =>
      // See buildPartitionedTableScan for the reason that we need to create a shard
      // broadcast HadoopConf.
      val sharedHadoopConf = SparkHadoopUtil.get.conf
      val confBroadcast =
        t.sqlContext.sparkContext.broadcast(new SerializableConfiguration(sharedHadoopConf))
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, f) =>
          toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f, t.paths, confBroadcast))) :: Nil

    case l @ LogicalRelation(baseRelation: TableScan) =>
      execution.PhysicalRDD.createFromDataSource(
        l.output, toCatalystRDD(l, baseRelation.buildScan()), baseRelation) :: Nil

    case i @ logical.InsertIntoTable(
      l @ LogicalRelation(t: InsertableRelation), part, query, overwrite, false) if part.isEmpty =>
      execution.ExecutedCommand(InsertIntoDataSource(l, query, overwrite)) :: Nil

    case i @ logical.InsertIntoTable(
      l @ LogicalRelation(t: HadoopFsRelation), part, query, overwrite, false) =>
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

    // Now, we create a scan builder, which will be used by pruneFilterProject. This scan builder
    // will union all partitions and attach partition values if needed.
    val scanBuilder = {
      (columns: Seq[Attribute], filters: Array[Filter]) => {
        // Builds RDD[Row]s for each selected partition.
        val perPartitionRows = partitions.map { case Partition(partitionValues, dir) =>
          val partitionColNames = partitionColumns.fieldNames

          // Don't scan any partition columns to save I/O.  Here we are being optimistic and
          // assuming partition columns data stored in data files are always consistent with those
          // partition values encoded in partition directory paths.
          val needed = columns.filterNot(a => partitionColNames.contains(a.name))
          val dataRows =
            relation.buildScan(needed.map(_.name).toArray, filters, Array(dir), confBroadcast)

          // Merges data values with partition values.
          mergeWithPartitionValues(
            relation.schema,
            columns.map(_.name).toArray,
            partitionColNames,
            partitionValues,
            toCatalystRDD(logicalRelation, needed, dataRows))
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

  // TODO: refactor this thing. It is very complicated because it does projection internally.
  // We should just put a project on top of this.
  private def mergeWithPartitionValues(
      schema: StructType,
      requiredColumns: Array[String],
      partitionColumns: Array[String],
      partitionValues: InternalRow,
      dataRows: RDD[InternalRow]): RDD[InternalRow] = {
    val nonPartitionColumns = requiredColumns.filterNot(partitionColumns.contains)

    // If output columns contain any partition column(s), we need to merge scanned data
    // columns and requested partition columns to form the final result.
    if (!requiredColumns.sameElements(nonPartitionColumns)) {
      val mergers = requiredColumns.zipWithIndex.map { case (name, index) =>
        // To see whether the `index`-th column is a partition column...
        val i = partitionColumns.indexOf(name)
        if (i != -1) {
          val dt = schema(partitionColumns(i)).dataType
          // If yes, gets column value from partition values.
          (mutableRow: MutableRow, dataRow: InternalRow, ordinal: Int) => {
            mutableRow(ordinal) = partitionValues.get(i, dt)
          }
        } else {
          // Otherwise, inherits the value from scanned data.
          val i = nonPartitionColumns.indexOf(name)
          val dt = schema(nonPartitionColumns(i)).dataType
          (mutableRow: MutableRow, dataRow: InternalRow, ordinal: Int) => {
            mutableRow(ordinal) = dataRow.get(i, dt)
          }
        }
      }

      // Since we know for sure that this closure is serializable, we can avoid the overhead
      // of cleaning a closure for each RDD by creating our own MapPartitionsRDD. Functionally
      // this is equivalent to calling `dataRows.mapPartitions(mapPartitionsFunc)` (SPARK-7718).
      val mapPartitionsFunc = (_: TaskContext, _: Int, iterator: Iterator[InternalRow]) => {
        val dataTypes = requiredColumns.map(schema(_).dataType)
        val mutableRow = new SpecificMutableRow(dataTypes)
        iterator.map { dataRow =>
          var i = 0
          while (i < mutableRow.numFields) {
            mergers(i)(mutableRow, dataRow, i)
            i += 1
          }
          mutableRow.asInstanceOf[InternalRow]
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
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Seq[Attribute], Array[Filter]) => RDD[InternalRow]) = {
    pruneFilterProjectRaw(
      relation,
      projects,
      filterPredicates,
      (requestedColumns, pushedFilters) => {
        scanBuilder(requestedColumns, selectFilters(pushedFilters).toArray)
      })
  }

  // Based on Catalyst expressions.
  protected def pruneFilterProjectRaw(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Seq[Attribute], Seq[Expression]) => RDD[InternalRow]) = {

    val projectSet = AttributeSet(projects.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition = filterPredicates.reduceLeftOption(expressions.And)

    val pushedFilters = filterPredicates.map { _ transform {
      case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
    }}

    if (projects.map(_.toAttribute) == projects &&
        projectSet.size == projects.size &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns =
        projects.asInstanceOf[Seq[Attribute]] // Safe due to if above.
          .map(relation.attributeMap)            // Match original case of attributes.

      val scan = execution.PhysicalRDD.createFromDataSource(
        projects.map(_.toAttribute),
        scanBuilder(requestedColumns, pushedFilters),
        relation.relation)
      filterCondition.map(execution.Filter(_, scan)).getOrElse(scan)
    } else {
      val requestedColumns = (projectSet ++ filterSet).map(relation.attributeMap).toSeq

      val scan = execution.PhysicalRDD.createFromDataSource(
        requestedColumns,
        scanBuilder(requestedColumns, pushedFilters),
        relation.relation)
      execution.Project(projects, filterCondition.map(execution.Filter(_, scan)).getOrElse(scan))
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
   * Selects Catalyst predicate [[Expression]]s which are convertible into data source [[Filter]]s,
   * and convert them.
   */
  protected[sql] def selectFilters(filters: Seq[Expression]) = {
    def translate(predicate: Expression): Option[Filter] = predicate match {
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
