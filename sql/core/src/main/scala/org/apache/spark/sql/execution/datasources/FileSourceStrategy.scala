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

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType

/**
 * A strategy for planning scans over collections of files that might be partitioned or bucketed
 * by user specified columns.
 *
 * At a high level planning occurs in several phases:
 *  - Split filters by when they need to be evaluated.
 *  - Prune the schema of the data requested based on any projections present. Today this pruning
 *    is only done on top level columns, but formats should support pruning of nested columns as
 *    well.
 *  - Construct a reader function by passing filters and the schema into the FileFormat.
 *  - Using a partition pruning predicates, enumerate the list of files that should be read.
 *  - Split the files into tasks and construct a FileScanRDD.
 *  - Add any projection or filters that must be evaluated after the scan.
 *
 * Files are assigned into tasks using the following algorithm:
 *  - If the table is bucketed, group files by bucket id into the correct number of partitions.
 *  - If the table is not bucketed or bucketing is turned off:
 *   - If any file is larger than the threshold, split it into pieces based on that threshold
 *   - Sort the files by decreasing file size.
 *   - Assign the ordered files to buckets using the following algorithm.  If the current partition
 *     is under the threshold with the addition of the next file, add it.  If not, open a new bucket
 *     and add it.  Proceed to the next file.
 */
object FileSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters,
      l @ LogicalRelation(fsRelation: HadoopFsRelation, _, table)) =>
      // Filters on this relation fall into four categories based on where we can use them to avoid
      // reading unneeded data:
      //  - partition keys only - used to prune directories to read
      //  - bucket keys only - optionally used to prune files to read
      //  - keys stored in the data only - optionally used to skip groups of data in files
      //  - filters that need to be evaluated again after the scan
      val filterSet = ExpressionSet(filters)

      // The attribute name of predicate could be different than the one in schema in case of
      // case insensitive, we should change them to match the one in schema, so we donot need to
      // worry about case sensitivity anymore.
      val normalizedFilters = filters.map { e =>
        e transform {
          case a: AttributeReference =>
            a.withName(l.output.find(_.semanticEquals(a)).get.name)
        }
      }

      val partitionColumns =
        l.resolve(
          fsRelation.partitionSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)
      val partitionSet = AttributeSet(partitionColumns)
      val partitionKeyFilters =
        ExpressionSet(normalizedFilters.filter(_.references.subsetOf(partitionSet)))
      logInfo(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")

      val dataColumns =
        l.resolve(fsRelation.dataSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)

      // Partition keys are not available in the statistics of the files.
      val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

      // Predicates with both partition keys and attributes need to be evaluated after the scan.
      val afterScanFilters = filterSet -- partitionKeyFilters
      logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

      val filterAttributes = AttributeSet(afterScanFilters)
      val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
      val requiredAttributes = AttributeSet(requiredExpressions)

      val readDataColumns =
        dataColumns
          .filter(requiredAttributes.contains)
          .filterNot(partitionColumns.contains)
      val outputSchema = if (fsRelation.sqlContext.conf.isParquetNestColumnPruning) {
        val requiredColumnsWithNesting = generateRequiredColumnsContainsNesting(
          projects, readDataColumns.attrs.map(_.name).toArray)
        val totalSchema = readDataColumns.toStructType
        val prunedSchema = StructType(requiredColumnsWithNesting.map(totalSchema(_)))
        // Merge schema in same StructType and merge with filterAttributes
        prunedSchema.fields.map(f => StructType(Array(f))).reduceLeft(_ merge _)
          .merge(filterAttributes.toSeq.toStructType)
      } else readDataColumns.toStructType
      logInfo(s"Output Data Schema: ${outputSchema.simpleString(5)}")

      val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
      logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}")

      val outputAttributes = readDataColumns ++ partitionColumns

      val scan =
        new FileSourceScanExec(
          fsRelation,
          outputAttributes,
          outputSchema,
          partitionKeyFilters.toSeq,
          pushedDownFilters,
          table)

      val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
      val withFilter = afterScanFilter.map(execution.FilterExec(_, scan)).getOrElse(scan)
      val withProjections = if (projects == withFilter.output) {
        withFilter
      } else {
        execution.ProjectExec(projects, withFilter)
      }

      withProjections :: Nil

    case _ => Nil
  }

  private def generateRequiredColumnsContainsNesting(projects: Seq[Expression],
                                      columns: Array[String]) : Array[String] = {
    def generateAttributeMap(nestFieldMap: scala.collection.mutable.Map[String, Seq[String]],
                             isNestField: Boolean, curString: Option[String],
                             node: Expression) {
      node match {
        case ai: GetArrayItem =>
          // Here we drop the curString for simplify array and map support.
          // Same strategy in GetArrayStructFields and GetMapValue
          generateAttributeMap(nestFieldMap, isNestField = true, None, ai.child)

        case asf: GetArrayStructFields =>
          generateAttributeMap(nestFieldMap, isNestField = true, None, asf.child)

        case mv: GetMapValue =>
          generateAttributeMap(nestFieldMap, isNestField = true, None, mv.child)

        case attr: AttributeReference =>
          if (isNestField && curString.isDefined) {
            val attrStr = attr.name
            if (nestFieldMap.contains(attrStr)) {
              nestFieldMap(attrStr) = nestFieldMap(attrStr) ++ Seq(attrStr + "." + curString.get)
            } else {
              nestFieldMap += (attrStr -> Seq(attrStr + "." + curString.get))
            }
          }
        case sf: GetStructField =>
          val str = if (curString.isDefined) {
            sf.name.get + "." + curString.get
          } else sf.name.get
          generateAttributeMap(nestFieldMap, isNestField = true, Option(str), sf.child)
        case _ =>
          if (node.children.nonEmpty) {
            node.children.foreach(child => generateAttributeMap(nestFieldMap,
              isNestField, curString, child))
          }
      }
    }

    val nestFieldMap = scala.collection.mutable.Map.empty[String, Seq[String]]
    projects.foreach(p => generateAttributeMap(nestFieldMap, isNestField = false, None, p))
    val col_list = columns.toList.flatMap(col => {
      if (nestFieldMap.contains(col)) {
        nestFieldMap.get(col).get.toList
      } else {
        List(col)
      }
    })
    col_list.toArray
  }

}
