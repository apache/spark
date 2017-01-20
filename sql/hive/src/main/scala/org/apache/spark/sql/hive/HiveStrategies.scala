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

package org.apache.spark.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, SimpleCatalogRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, ScriptTransformation}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.types.StructType


/**
 * Determine the serde/format of the Hive serde table, according to the storage properties.
 */
class DetermineHiveSerde(conf: SQLConf) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case c @ CreateTable(t, _, query) if DDLUtils.isHiveTable(t) && t.storage.serde.isEmpty =>
      if (t.bucketSpec.isDefined) {
        throw new AnalysisException("Creating bucketed Hive serde table is not supported yet.")
      }
      if (t.partitionColumnNames.nonEmpty && query.isDefined) {
        val errorMessage = "A Create Table As Select (CTAS) statement is not allowed to " +
          "create a partitioned table using Hive's file formats. " +
          "Please use the syntax of \"CREATE TABLE tableName USING dataSource " +
          "OPTIONS (...) PARTITIONED BY ...\" to create a partitioned table through a " +
          "CTAS statement."
        throw new AnalysisException(errorMessage)
      }

      val defaultStorage = HiveSerDe.getDefaultStorage(conf)
      val options = new HiveOptions(t.storage.properties)

      val fileStorage = if (options.fileFormat.isDefined) {
        HiveSerDe.sourceToSerDe(options.fileFormat.get) match {
          case Some(s) =>
            CatalogStorageFormat.empty.copy(
              inputFormat = s.inputFormat,
              outputFormat = s.outputFormat,
              serde = s.serde)
          case None =>
            throw new IllegalArgumentException(s"invalid fileFormat: '${options.fileFormat.get}'")
        }
      } else if (options.hasInputOutputFormat) {
        CatalogStorageFormat.empty.copy(
          inputFormat = options.inputFormat,
          outputFormat = options.outputFormat)
      } else {
        CatalogStorageFormat.empty
      }

      val rowStorage = if (options.serde.isDefined) {
        CatalogStorageFormat.empty.copy(serde = options.serde)
      } else {
        CatalogStorageFormat.empty
      }

      val storage = t.storage.copy(
        inputFormat = fileStorage.inputFormat.orElse(defaultStorage.inputFormat),
        outputFormat = fileStorage.outputFormat.orElse(defaultStorage.outputFormat),
        serde = rowStorage.serde.orElse(fileStorage.serde).orElse(defaultStorage.serde),
        properties = options.serdeProperties)

      c.copy(tableDesc = t.copy(storage = storage))
  }
}

class HiveAnalysis(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case InsertIntoTable(table: MetastoreRelation, partSpec, query, overwrite, ifNotExists)
        if hasBeenPreprocessed(table.output, table.partitionKeys.toStructType, partSpec, query) =>
      InsertIntoHiveTable(table, partSpec, query, overwrite, ifNotExists)

    case CreateTable(tableDesc, mode, Some(query)) if DDLUtils.isHiveTable(tableDesc) =>
      // Currently `DataFrameWriter.saveAsTable` doesn't support the Append mode of hive serde
      // tables yet.
      if (mode == SaveMode.Append) {
        throw new AnalysisException(
          "CTAS for hive serde tables does not support append semantics.")
      }

      val dbName = tableDesc.identifier.database.getOrElse(session.catalog.currentDatabase)
      CreateHiveTableAsSelectCommand(
        tableDesc.copy(identifier = tableDesc.identifier.copy(database = Some(dbName))),
        query,
        mode == SaveMode.Ignore)
  }

  /**
   * Returns true if the [[InsertIntoTable]] plan has already been preprocessed by analyzer rule
   * [[PreprocessTableInsertion]]. It is important that this rule([[HiveAnalysis]]) has to
   * be run after [[PreprocessTableInsertion]], to normalize the column names in partition spec and
   * fix the schema mismatch by adding Cast.
   */
  private def hasBeenPreprocessed(
      tableOutput: Seq[Attribute],
      partSchema: StructType,
      partSpec: Map[String, Option[String]],
      query: LogicalPlan): Boolean = {
    val partColNames = partSchema.map(_.name).toSet
    query.resolved && partSpec.keys.forall(partColNames.contains) && {
      val staticPartCols = partSpec.filter(_._2.isDefined).keySet
      val expectedColumns = tableOutput.filterNot(a => staticPartCols.contains(a.name))
      expectedColumns.toStructType.sameType(query.schema)
    }
  }
}

/**
 * Replaces [[SimpleCatalogRelation]] with [[MetastoreRelation]] if its table provider is hive.
 */
class FindHiveSerdeTable(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case i @ InsertIntoTable(s: SimpleCatalogRelation, _, _, _, _)
        if DDLUtils.isHiveTable(s.metadata) =>
      i.copy(table =
        MetastoreRelation(s.metadata.database, s.metadata.identifier.table)(s.metadata, session))

    case s: SimpleCatalogRelation if DDLUtils.isHiveTable(s.metadata) =>
      MetastoreRelation(s.metadata.database, s.metadata.identifier.table)(s.metadata, session)
  }
}

private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object Scripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ScriptTransformation(input, script, output, child, ioschema) =>
        val hiveIoSchema = HiveScriptIOSchema(ioschema)
        ScriptTransformationExec(input, script, output, planLater(child), hiveIoSchema) :: Nil
      case _ => Nil
    }
  }

  /**
   * Retrieves data using a HiveTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: MetastoreRelation) =>
        // Filter out all predicates that only deal with partition keys, these are given to the
        // hive table scan operator to be used for partition pruning.
        val partitionKeyIds = AttributeSet(relation.partitionKeys)
        val (pruningPredicates, otherPredicates) = predicates.partition { predicate =>
          !predicate.references.isEmpty &&
          predicate.references.subsetOf(partitionKeyIds)
        }

        pruneFilterProject(
          projectList,
          otherPredicates,
          identity[Seq[Expression]],
          HiveTableScanExec(_, relation, pruningPredicates)(sparkSession)) :: Nil
      case _ =>
        Nil
    }
  }
}
