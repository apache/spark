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

package org.apache.spark.sql.pipelines.util

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.pipelines.common.DatasetType
import org.apache.spark.sql.pipelines.graph.{GraphElementTypeUtils, GraphErrors, ResolvedFlow}
import org.apache.spark.sql.types.{StructField, StructType}


object SchemaInferenceUtils {

  /**
   * Given a set of flows that write to the same destination and possibly a user-specified schema,
   * we infer the schema of the destination dataset. The logic is as follows:
   * 1. If there are no incoming flows, return the user-specified schema (if provided)
   *    or an empty schema.
   * 2. If there are incoming flows, we merge the schemas of all flows that write to
   *    the same destination.
   * 3. If a user-specified schema is provided, we merge it with the inferred schema.
   *    The user-specified schema will take precedence over the inferred schema.
   * Returns an error if encountered during schema inference or merging the inferred schema with
   * the user-specified one.
   *
   * All merges honor `caseSensitive`, which defaults to the active session's
   * `spark.sql.caseSensitive`. Under case-insensitive analysis, flows emitting column names that
   * differ only in case contribute a single column (the first flow's spelling wins), and a declared
   * column matches a flow column differing only in case -- consistent with how the rest of the
   * engine resolves those names.
   */
  def inferSchemaFromFlows(
      flows: Seq[ResolvedFlow],
      userSpecifiedSchema: Option[StructType],
      caseSensitive: Boolean = SparkSession.active.sessionState.conf.caseSensitiveAnalysis)
      : StructType = {
    if (flows.isEmpty) {
      return userSpecifiedSchema.getOrElse(new StructType())
    }

    require(
      flows.forall(_.destinationIdentifier == flows.head.destinationIdentifier),
      "Expected all flows to have the same destination"
    )

    val inferredSchema = flows.map(_.schema).fold(new StructType()) { (schemaSoFar, schema) =>
      try {
        SchemaMergingUtils.mergeSchemas(schemaSoFar, schema, caseSensitive)
      } catch {
        case NonFatal(e) =>
          throw GraphErrors.unableToInferSchemaError(
            flows.head.destinationIdentifier,
            schemaSoFar,
            schema,
            cause = Option(e)
          )
      }
    }

    val identifier = flows.head.destinationIdentifier
    val datasetType = GraphElementTypeUtils.getDatasetTypeForMaterializedViewOrStreamingTable(flows)
    // We merge the inferred schema with the user-specified schema to pick up any schema metadata
    // that is provided by the user, e.g., comments or column masks.
    mergeInferredAndUserSchemasIfNeeded(
      identifier,
      datasetType,
      inferredSchema,
      userSpecifiedSchema,
      caseSensitive
    )
  }

  private def mergeInferredAndUserSchemasIfNeeded(
      tableIdentifier: TableIdentifier,
      datasetType: DatasetType,
      inferredSchema: StructType,
      userSpecifiedSchema: Option[StructType],
      caseSensitive: Boolean): StructType = {
    userSpecifiedSchema match {
      case Some(userSpecifiedSchema) =>
        try {
          // Merge the inferred schema with the user-provided schema hint
          SchemaMergingUtils.mergeSchemas(userSpecifiedSchema, inferredSchema, caseSensitive)
        } catch {
          case NonFatal(e) =>
            throw GraphErrors.incompatibleUserSpecifiedAndInferredSchemasError(
              tableIdentifier,
              datasetType,
              userSpecifiedSchema,
              inferredSchema,
              cause = Option(e)
            )
        }
      case None => inferredSchema
    }
  }

  /**
   * Determines the column changes needed to transform the current schema into the target schema.
   *
   * This function compares the current schema with the target schema and produces a sequence of
   * TableChange objects representing:
   * 1. New columns that need to be added
   * 2. Existing columns that need type updates
   *
   * Column identity is keyed on the exact field name, deliberately NOT on a case-normalized one.
   * Two consequences worth being explicit about, since both were considered and rejected:
   *   - On the incremental (streaming table) path this is not where case-insensitivity belongs:
   *     `targetSchema` is the merge of the current and desired schemas, and
   *     [[SchemaMergingUtils.mergeSchemas]] has already folded an incoming case-only-differing
   *     field onto the persisted one, so a case-differing pair never reaches here.
   *   - On the non-merging paths (materialized views and any full refresh) `targetSchema` is the
   *     run's declared schema as-is, so normalizing here would make a case-only rename invisible
   *     and freeze the persisted spelling forever, with the table permanently disagreeing with its
   *     definition. Exact-name keying keeps such a rename an explicit drop-then-add.
   * Exact keying also avoids silently collapsing two genuinely distinct declared columns that
   * differ only in case (`value` and `Value`) into an arbitrary one of the two.
   *
   * @param currentSchema The current schema of the table
   * @param targetSchema The target schema that we want the table to have
   * @return A sequence of TableChange objects representing the necessary changes
   */
  def diffSchemas(currentSchema: StructType, targetSchema: StructType): Seq[TableChange] = {
    val changes = scala.collection.mutable.ArrayBuffer.empty[TableChange]

    // Helper function to get a map of field name to field
    def getFieldMap(schema: StructType): Map[String, StructField] = {
      schema.fields.map(field => field.name -> field).toMap
    }

    val currentFields = getFieldMap(currentSchema)
    val targetFields = getFieldMap(targetSchema)

    // Find columns to add (in target but not in current)
    val columnsToAdd = targetFields.keySet.diff(currentFields.keySet)
    columnsToAdd.foreach { columnName =>
      val field = targetFields(columnName)
      changes += TableChange.addColumn(
        Array(columnName),
        field.dataType,
        field.nullable,
        field.getComment().orNull
      )
    }

    // Find columns to delete (in current but not in target)
    val columnsToDelete = currentFields.keySet.diff(targetFields.keySet)
    columnsToDelete.foreach { columnName =>
      changes += TableChange.deleteColumn(Array(columnName), false)
    }

    // Find columns with type changes (in both but with different types)
    val commonColumns = currentFields.keySet.intersect(targetFields.keySet)
    commonColumns.foreach { columnName =>
      val currentField = currentFields(columnName)
      val targetField = targetFields(columnName)

      // If data types are different, add a type update change
      if (currentField.dataType != targetField.dataType) {
        changes += TableChange.updateColumnType(Array(columnName), targetField.dataType)
      }

      // If nullability is different, add a nullability update change
      if (currentField.nullable != targetField.nullable) {
        changes += TableChange.updateColumnNullability(Array(columnName), targetField.nullable)
      }

      // If comments are different, add a comment update change
      val currentComment = currentField.getComment().orNull
      val targetComment = targetField.getComment().orNull
      if (currentComment != targetComment) {
        changes += TableChange.updateColumnComment(Array(columnName), targetComment)
      }
    }

    changes.toSeq
  }
}
