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
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{DDLUtils, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}


/**
 * Determine the serde/format of the Hive serde table, according to the storage properties.
 */
class DetermineHiveSerde(conf: SQLConf) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case c @ CreateTable(t, _, _) if DDLUtils.isHiveTable(t) && t.storage.inputFormat.isEmpty =>
      if (t.bucketSpec.nonEmpty) {
        throw new AnalysisException("Cannot create bucketed Hive serde table.")
      }

      val defaultStorage: CatalogStorageFormat = {
        val defaultStorageType = conf.getConfString("hive.default.fileformat", "textfile")
        val defaultHiveSerde = HiveSerDe.sourceToSerDe(defaultStorageType)
        CatalogStorageFormat(
          locationUri = None,
          inputFormat = defaultHiveSerde.flatMap(_.inputFormat)
            .orElse(Some("org.apache.hadoop.mapred.TextInputFormat")),
          outputFormat = defaultHiveSerde.flatMap(_.outputFormat)
            .orElse(Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
          serde = defaultHiveSerde.flatMap(_.serde)
            .orElse(Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")),
          compressed = false,
          properties = Map())
      }

      val options = new HiveOptions(t.storage.properties)

      val fileStorage = if (options.format.isDefined) {
        HiveSerDe.sourceToSerDe(options.format.get) match {
          case Some(s) =>
            CatalogStorageFormat.empty.copy(
              inputFormat = s.inputFormat,
              outputFormat = s.outputFormat,
              serde = s.serde)
          case None =>
            throw new IllegalArgumentException(s"invalid format: '${options.format.get}'")
        }
      } else if (options.inputFormat.isDefined) {
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

private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object Scripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ScriptTransformation(input, script, output, child, ioschema) =>
        val hiveIoSchema = HiveScriptIOSchema(ioschema)
        ScriptTransformation(input, script, output, planLater(child), hiveIoSchema) :: Nil
      case _ => Nil
    }
  }

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.InsertIntoTable(
          table: MetastoreRelation, partition, child, overwrite, ifNotExists) =>
        InsertIntoHiveTable(
          table, partition, planLater(child), overwrite, ifNotExists) :: Nil

      case CreateTable(tableDesc, mode, Some(query)) if DDLUtils.isHiveTable(tableDesc) =>
        // Currently we will never hit this branch, as SQL string API can only use `Ignore` or
        // `ErrorIfExists` mode, and `DataFrameWriter.saveAsTable` doesn't support hive serde
        // tables yet.
        if (mode == SaveMode.Append || mode == SaveMode.Overwrite) {
          throw new AnalysisException(
            "CTAS for hive serde tables does not support append or overwrite semantics.")
        }

        val dbName = tableDesc.identifier.database.getOrElse(sparkSession.catalog.currentDatabase)
        val cmd = CreateHiveTableAsSelectCommand(
          tableDesc.copy(identifier = tableDesc.identifier.copy(database = Some(dbName))),
          query,
          mode == SaveMode.Ignore)
        ExecutedCommandExec(cmd) :: Nil

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
