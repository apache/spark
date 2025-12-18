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
   */
  def inferSchemaFromFlows(
      flows: Seq[ResolvedFlow],
      userSpecifiedSchema: Option[StructType]): StructType = {
    if (flows.isEmpty) {
      return userSpecifiedSchema.getOrElse(new StructType())
    }

    require(
      flows.forall(_.destinationIdentifier == flows.head.destinationIdentifier),
      "Expected all flows to have the same destination"
    )

    val inferredSchema = flows.map(_.schema).fold(new StructType()) { (schemaSoFar, schema) =>
      try {
        SchemaMergingUtils.mergeSchemas(schemaSoFar, schema)
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
      userSpecifiedSchema
    )
  }

  private def mergeInferredAndUserSchemasIfNeeded(
      tableIdentifier: TableIdentifier,
      datasetType: DatasetType,
      inferredSchema: StructType,
      userSpecifiedSchema: Option[StructType]): StructType = {
    userSpecifiedSchema match {
      case Some(userSpecifiedSchema) =>
        try {
          // Merge the inferred schema with the user-provided schema hint
          SchemaMergingUtils.mergeSchemas(userSpecifiedSchema, inferredSchema)
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
