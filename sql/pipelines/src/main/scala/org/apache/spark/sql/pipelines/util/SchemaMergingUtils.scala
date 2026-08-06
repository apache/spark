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

import org.apache.spark.sql.types.StructType

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
}
