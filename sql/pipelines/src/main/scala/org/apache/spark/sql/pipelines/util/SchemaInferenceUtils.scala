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
import org.apache.spark.sql.catalyst.analysis.{
  caseInsensitiveResolution,
  caseSensitiveResolution,
  Resolver
}
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.common.DatasetType
import org.apache.spark.sql.pipelines.graph.{
  Flow,
  GraphElementTypeUtils,
  GraphErrors,
  ResolvedFlow
}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}


object SchemaInferenceUtils {

  def resolverFor(caseSensitive: Boolean): Resolver = {
    if (caseSensitive) {
      caseSensitiveResolution
    } else {
      caseInsensitiveResolution
    }
  }

  /**
   * The effective `spark.sql.caseSensitive` for schema derivation on `tableIdentifier`, taken from
   * the flows writing to it rather than from the session.
   *
   * A pipeline can set `spark.sql.caseSensitive` for itself, and a `SET` in pipeline source does
   * not touch the session: [[org.apache.spark.sql.pipelines.graph.GraphRegistrationContext]] folds
   * it into each flow's `sqlConf`, and it is applied when the flow is analyzed and executed. Schema
   * derivation therefore has to read it from the same place, or evolution can disagree with the
   * flows whose schemas it is deriving from -- e.g. folding an incoming `Value` onto a persisted
   * `value` while the flow, resolving case-sensitively, expects `Value` to be its own column.
   *
   * All flows writing to a table must agree: the value decides whether names differing only in case
   * identify the same column, so a disagreement would make the resulting schema depend on the order
   * the flows are evaluated in. Throws
   * [[org.apache.spark.sql.pipelines.graph.GraphErrors.conflictingFlowConfigurationError]] if they
   * disagree. Flows that do not set it at all inherit the session's value.
   */
  def effectiveCaseSensitivity(
      tableIdentifier: TableIdentifier,
      flows: Seq[Flow],
      sessionCaseSensitive: Boolean): Boolean = {
    val declaredByFlow = flows.flatMap { flow =>
      flow.sqlConf.get(SQLConf.CASE_SENSITIVE.key).map(value => value -> flow.identifier)
    }
    if (declaredByFlow.isEmpty) {
      return sessionCaseSensitive
    }

    // Compare the parsed booleans, so that e.g. "TRUE" and "true" are not reported as a conflict,
    // but report the values as written to keep the error recognizable to the user.
    val byParsedValue = declaredByFlow.groupBy { case (value, _) => value.trim.toBoolean }
    // A flow that leaves the conf unset inherits the session value, which conflicts just as much as
    // an explicitly opposite value.
    val flowsWithoutDeclaration = flows.filterNot { flow =>
      flow.sqlConf.contains(SQLConf.CASE_SENSITIVE.key)
    }
    val effectiveValues = byParsedValue.keySet ++
      Option.when(flowsWithoutDeclaration.nonEmpty)(sessionCaseSensitive)
    if (effectiveValues.sizeIs > 1) {
      val valuesByFlow = declaredByFlow
        .groupBy { case (value, _) => value }
        .map { case (value, entries) => value -> entries.map { case (_, id) => id } } ++
        Option
          .when(flowsWithoutDeclaration.nonEmpty)(
            s"$sessionCaseSensitive (session default)" -> flowsWithoutDeclaration.map(_.identifier)
          )
          .toMap
      throw GraphErrors.conflictingFlowConfigurationError(
        tableIdentifier = tableIdentifier,
        configKey = SQLConf.CASE_SENSITIVE.key,
        valuesByFlow = valuesByFlow
      )
    }
    effectiveValues.head
  }

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
   * All merges honor the effective `spark.sql.caseSensitive` of the flows writing to
   * `tableIdentifier`, falling back to `sessionCaseSensitive` for flows that do not set it. Under
   * case-insensitive analysis, flows emitting column names that differ only in case contribute a
   * single column, and a declared column matches a flow column differing only in case -- consistent
   * with how the rest of the engine resolves those names.
   *
   * When flows differ only in column casing, the surviving spelling is the one from the flow with
   * the lowest identifier: `flows` is merged in sorted identifier order, not in the order given. We
   * sort on the identifier's parts (catalog, database, table) to avoid collisions for identifiers
   * whose parts contain dots.
   * Sorting here rather than at the call sites keeps every caller agreeing on the result, since the
   * schemas they derive are compared against each other -- the graph's inferred schema materializes
   * the table, while [[org.apache.spark.sql.pipelines.graph.VirtualTableInput]] produces the schema
   * downstream flows resolve against, and `diffSchemas` keys column identity on the exact name. Two
   * callers ordering the same flows differently would spell one column two ways, leaving a
   * downstream view disagreeing with its source and turning the next refresh into a drop-then-add.
   */
  def inferSchemaFromFlows(
      tableIdentifier: TableIdentifier,
      flows: Seq[ResolvedFlow],
      userSpecifiedSchema: Option[StructType],
      sessionCaseSensitive: Boolean): StructType = {
    if (flows.isEmpty) {
      return userSpecifiedSchema.getOrElse(new StructType())
    }

    require(
      flows.forall(_.destinationIdentifier == tableIdentifier),
      "Expected all flows to have the same destination"
    )

    val caseSensitive = effectiveCaseSensitivity(
      tableIdentifier = tableIdentifier,
      flows = flows,
      sessionCaseSensitive = sessionCaseSensitive
    )

    val inferredSchema = flows
      .sortBy(f => (f.identifier.catalog, f.identifier.database, f.identifier.table))
      .map(_.schema)
      .fold(new StructType()) { (schemaSoFar, schema) =>
        try {
          SchemaMergingUtils.mergeSchemas(schemaSoFar, schema, caseSensitive)
        } catch {
          case NonFatal(e) =>
            throw GraphErrors.unableToInferSchemaError(
              tableIdentifier,
              schemaSoFar,
              schema,
              cause = Option(e)
            )
        }
      }

    val datasetType = GraphElementTypeUtils.getDatasetTypeForMaterializedViewOrStreamingTable(flows)
    // We merge the inferred schema with the user-specified schema to pick up any schema metadata
    // that is provided by the user, e.g., comments or column masks.
    mergeInferredAndUserSchemasIfNeeded(
      tableIdentifier,
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
   * Produces the [[TableChange]] sequence needed to transform `currentSchema` into
   * `targetSchema`: additions, type updates, deletions, nullability and comment changes.
   * Recurses into structs, arrays, and maps so changes are emitted at the leaf level.
   * Similar to [[org.apache.spark.sql.catalyst.analysis.ResolveSchemaEvolution]], but
   * produces a full bidirectional sync (deletes, nullability, and comment changes) rather
   * than additive-only evolution.
   *
   * Column identity is keyed on the exact field name, not on a case-normalized one. On the
   * incremental streaming-table path, `targetSchema` is the merge of the current and desired
   * schemas, and [[SchemaMergingUtils.mergeSchemas]] has already folded an incoming
   * case-only-differing field onto the persisted one. On the non-merging paths (materialized views
   * and any full refresh), `targetSchema` is the run's declared schema as-is, so exact-name keying
   * keeps a case-only rename visible as an explicit drop-then-add.
   * Exact keying also avoids silently collapsing two genuinely distinct declared columns that
   * differ only in case (`value` and `Value`) into an arbitrary one of the two.
   *
   * @param currentSchema The current schema of the table
   * @param targetSchema The target schema that we want the table to have
   * @return A sequence of TableChange objects representing the necessary changes
   */
  def diffSchemas(currentSchema: StructType, targetSchema: StructType): Seq[TableChange] =
    diffStructs(
      currentStruct = currentSchema,
      targetStruct = targetSchema,
      // Root call: path is empty because current and target are the top-level schemas.
      pathToStruct = Seq.empty
    )

  /**
   * Diffs two structs field-by-field, matching fields by exact name.
   *
   * @param currentStruct The struct as it exists in the current schema.
   * @param targetStruct The struct as it should look in the target schema.
   * @param pathToStruct Path segments from the top-level schema to this
   *                     struct, if this is a nested struct. Empty for the
   *                     root call.
   */
  private def diffStructs(
      currentStruct: StructType,
      targetStruct: StructType,
      pathToStruct: Seq[String]): Seq[TableChange] = {
    val topLevelFieldsInCurrent = currentStruct.fields.map(field => field.name -> field).toMap
    val topLevelFieldsInTarget = targetStruct.fields.map(field => field.name -> field).toMap

    // Fields present in target but not in current are columns that need to be added.
    val columnsAdded = topLevelFieldsInTarget.values.toSeq
      .filterNot(fieldInTarget =>
        topLevelFieldsInCurrent.contains(fieldInTarget.name)
      )
      .map { fieldInTarget =>
        TableChange.addColumn(
          fieldNames = (pathToStruct :+ fieldInTarget.name).toArray,
          dataType = fieldInTarget.dataType,
          isNullable = fieldInTarget.nullable,
          comment = fieldInTarget.getComment().orNull
        )
      }

    // Fields present in current but not in target are columns that need to be removed.
    val columnsDeleted = topLevelFieldsInCurrent.values.toSeq
      .filterNot(fieldInCurrent =>
        topLevelFieldsInTarget.contains(fieldInCurrent.name)
      )
      .map(fieldInCurrent =>
        TableChange
          .deleteColumn(
            fieldNames = (pathToStruct :+ fieldInCurrent.name).toArray,
            ifExists = false
          )
      )

    // Fields in both current and target but vary in metadata or nested sub-fields represent
    // columns that need to be updated.
    val columnsUpdated = topLevelFieldsInCurrent.values.toSeq.flatMap {
      fieldInCurrent =>
        topLevelFieldsInTarget.get(fieldInCurrent.name).toSeq.flatMap {
          fieldInTarget =>
            diffField(
              currentField = fieldInCurrent,
              targetField = fieldInTarget,
              pathToField = pathToStruct :+ fieldInCurrent.name
            )
        }
    }

    columnsAdded ++ columnsDeleted ++ columnsUpdated
  }

  /** Diffs the type, nullability, and comment of one field present in both schemas. */
  private def diffField(
      currentField: StructField,
      targetField: StructField,
      pathToField: Seq[String]): Seq[TableChange] = {
    diffDataTypes(currentField.dataType, targetField.dataType, pathToField) ++
      diffNullability(currentField.nullable, targetField.nullable, pathToField) ++
      diffComment(currentField.getComment(), targetField.getComment(), pathToField)
  }

  private def diffNullability(
      currentNullable: Boolean,
      targetNullable: Boolean,
      pathToField: Seq[String]
  ): Option[TableChange] = {
    Option.when(currentNullable != targetNullable)(
      TableChange.updateColumnNullability(pathToField.toArray, targetNullable)
    )
  }

  private def diffComment(
      currentComment: Option[String],
      targetComment: Option[String],
      pathToField: Seq[String]
  ): Option[TableChange] = {
    Option.when(currentComment != targetComment)(
      TableChange.updateColumnComment(pathToField.toArray, targetComment.orNull)
    )
  }

  /** Diffs two data types at `path`, descending through matching complex types. */
  private def diffDataTypes(
      currentType: DataType,
      targetType: DataType,
      pathToField: Seq[String]
  ): Seq[TableChange] = (currentType, targetType) match {
    case (currentStruct: StructType, targetStruct: StructType) =>
      diffStructs(currentStruct, targetStruct, pathToField)

    case (currentArray: ArrayType, targetArray: ArrayType) =>
      val elementPath = pathToField :+ "element"

      diffDataTypes(currentArray.elementType, targetArray.elementType, elementPath) ++
        diffNullability(currentArray.containsNull, targetArray.containsNull, elementPath)

    case (currentMap: MapType, targetMap: MapType) =>
      val valuePath = pathToField :+ "value"
      val keyPath = pathToField :+ "key"

      diffDataTypes(currentMap.keyType, targetMap.keyType, keyPath) ++
        diffDataTypes(currentMap.valueType, targetMap.valueType, valuePath) ++
        diffNullability(currentMap.valueContainsNull, targetMap.valueContainsNull, valuePath)

    case _ if currentType == targetType =>
      Seq.empty

    case _ =>
      Seq(TableChange.updateColumnType(pathToField.toArray, targetType))
  }
}
