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

import java.util.Locale

import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

object SchemaMergingUtils {

  /**
   * Additively merges `dataSchema` into `tableSchema`, returning a schema that is the union of the
   * two (recursing into nested structs/arrays). On a field present in both, `tableSchema`'s name
   * and position win; `dataSchema` only contributes fields absent from `tableSchema`.
   *
   * @param caseSensitive whether two field names that differ only in case are considered distinct.
   *                      When `false` (mirroring a case-insensitive session), `dataSchema`'s field
   *                      is folded onto the matching `tableSchema` field rather than added as a
   *                      separate, case-differing column. Deliberately has no default: every caller
   *                      merges schemas that some pipeline will later resolve names against, so the
   *                      choice belongs to the caller and should be visible at the call site rather
   *                      than silently inherited. Callers should pass the effective
   *                      `spark.sql.caseSensitive` of the flows involved (see
   *                      [[SchemaInferenceUtils.effectiveCaseSensitivity]]).
   */
  def mergeSchemas(
      tableSchema: StructType,
      dataSchema: StructType,
      caseSensitive: Boolean): StructType = {
    StructType.merge(tableSchema, dataSchema, caseSensitive).asInstanceOf[StructType]
  }

  /**
   * Like [[mergeSchemas]], but on a field present in both schemas (at any nesting level), the
   * field's metadata (comment, default value, etc.) is taken from `dataSchema` rather than from
   * `tableSchema`. Fields present only in `tableSchema` keep their metadata.
   *
   * Used for additive schema evolution of an existing table, where `tableSchema` is the table's
   * current schema and `dataSchema` is the desired one: column metadata changes in the desired
   * schema (including a removed comment) must survive the merge so that they can be applied to
   * the table, the same way they are when the desired schema is used as-is.
   */
  def mergeSchemasWithDataMetadata(
      tableSchema: StructType,
      dataSchema: StructType,
      caseSensitive: Boolean): StructType = {
    val merged = mergeSchemas(tableSchema, dataSchema, caseSensitive)
    overlayFieldMetadata(merged, dataSchema, caseSensitive).asInstanceOf[StructType]
  }

  /**
   * Returns `target` with the metadata of every struct field that also exists (by name, honoring
   * `caseSensitive`) in `source` replaced by the metadata of the `source` field, recursing into
   * nested structs, arrays, and maps. Names, types, and nullability are taken from `target`.
   */
  private def overlayFieldMetadata(
      target: DataType,
      source: DataType,
      caseSensitive: Boolean): DataType = (target, source) match {
    case (targetStruct: StructType, sourceStruct: StructType) =>
      def normalize(name: String): String =
        if (caseSensitive) name else name.toLowerCase(Locale.ROOT)
      val sourceFields = sourceStruct.fields.map(f => normalize(f.name) -> f).toMap
      StructType(targetStruct.fields.map { targetField =>
        sourceFields.get(normalize(targetField.name)) match {
          case Some(sourceField) =>
            targetField.copy(
              dataType = overlayFieldMetadata(
                targetField.dataType, sourceField.dataType, caseSensitive),
              metadata = sourceField.metadata)
          case None => targetField
        }
      })
    case (targetArray: ArrayType, sourceArray: ArrayType) =>
      targetArray.copy(elementType = overlayFieldMetadata(
        targetArray.elementType, sourceArray.elementType, caseSensitive))
    case (targetMap: MapType, sourceMap: MapType) =>
      targetMap.copy(
        keyType = overlayFieldMetadata(targetMap.keyType, sourceMap.keyType, caseSensitive),
        valueType = overlayFieldMetadata(targetMap.valueType, sourceMap.valueType, caseSensitive))
    case _ => target
  }
}
