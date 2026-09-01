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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Names that AutoCDC reserves for its own use, both for internal columns it inserts during
 * reconciliation (e.g. `${prefix}metadata`, `${prefix}winning_row`) and for internal tables it
 * manages alongside user-defined targets (e.g. the per-target auxiliary state table).
 *
 * A single recognizable prefix gives a single auditable answer to "what does AutoCDC own", and
 * lets user-defined columns and tables be unambiguously distinguished from AutoCDC-managed ones.
 */
private[pipelines] object AutoCdcReservedNames {

  /** Common reserved-name prefix shared by AutoCDC internal columns and internal tables. */
  val prefix: String = "__spark_autocdc_"

  /**
   * Reserved name of the operational metadata column AutoCDC that is projected on every AutoCDC
   * microbatch, auxiliary table, and target table.
   *
   * Shared across all SCD strategies and across the flow resolution, batch-processor, and
   * streaming-write layers.
   *
   * Note that the schema of the CDC metadata column however can and does differ on the SCD-type.
   */
  val cdcMetadataColName: String = s"${prefix}metadata"

  // A field is engine-owned when its name carries the reserved prefix. Matching goes through the
  // caller's `resolver` so a user-declared column is still recognized as reserved under
  // case-insensitive analysis, and the length guard keeps `substring` safe for shorter names.
  private def isReservedFieldName(name: String, resolver: Resolver): Boolean =
    name.length >= prefix.length && resolver(name.substring(0, prefix.length), prefix)

  /** The engine-owned reserved AUTO CDC column(s) present in `schema`, if any. */
  private[pipelines] def reservedFields(schema: StructType, resolver: Resolver): Seq[StructField] =
    schema.fields.toSeq.filter(f => isReservedFieldName(f.name, resolver))

  /** `schema` with the engine-owned reserved AUTO CDC column(s) removed. */
  private[pipelines] def stripReservedFields(schema: StructType, resolver: Resolver): StructType =
    StructType(schema.fields.filterNot(f => isReservedFieldName(f.name, resolver)))

  /**
   * The schema a materialized AUTO CDC target carries when the user declares a schema: the user's
   * columns with the engine-owned type/nullability substituted into any declared reserved column,
   * followed by the engine-owned reserved column(s) the declaration omitted. `flowInferredSchema`
   * must be flow-derived (no user-declared columns merged in), so its reserved fields are exactly
   * the engine-produced ones and never a user column that happens to carry the prefix.
   *
   * A declared reserved field keeps its own spelling and position; only its type and nullability
   * are replaced with the engine's. That matters on the upgrade path: an already-materialized
   * target may carry the reserved column under a different casing (e.g. an older definition
   * declared it upper-cased under case-insensitive analysis), `evolveTable` merges that existing
   * spelling in, and this schema -- which also feeds the analysis-time read path -- must agree with
   * it so a downstream `SELECT *` plans against the column the target actually has.
   *
   * The engine-owned reserved fields are made nullable so a target created from a declared schema
   * matches one created from an omitted schema (that path materializes the inferred schema
   * `asNullable`), avoiding a spurious nullability diff when a declaration is added or removed.
   */
  private[pipelines] def appendEngineOwnedReservedFields(
      declaredSchema: StructType,
      flowInferredSchema: StructType,
      resolver: Resolver): StructType = {
    val engineReserved = StructType(reservedFields(flowInferredSchema, resolver)).asNullable.fields

    // A declaration may carry at most one field matching each engine-owned reserved column; more
    // than one (e.g. the metadata column declared twice under case-insensitive analysis) is
    // ambiguous and schema validation should have rejected it upstream.
    engineReserved.foreach { ef =>
      val matches = declaredSchema.fields.filter(df => resolver(df.name, ef.name))
      if (matches.length > 1) {
        throw SparkException.internalError(
          s"Ambiguous reserved AUTO CDC field: ${matches.map(_.name).mkString(", ")} all match " +
            s"the engine-owned column ${ef.name}.")
      }
    }

    // Rebuild the declared schema in place: a declared reserved field keeps its spelling and
    // position but takes the engine-owned type/nullability. A reserved-prefixed declared field with
    // no engine counterpart is left as-is (validation rejects it upstream for an AUTO CDC target).
    val usedEngineNames = scala.collection.mutable.Set.empty[String]
    val rebuilt = declaredSchema.fields.map { df =>
      if (isReservedFieldName(df.name, resolver)) {
        engineReserved.find(ef => resolver(df.name, ef.name)) match {
          case Some(ef) =>
            usedEngineNames += ef.name
            ef.copy(name = df.name)
          case None => df
        }
      } else {
        df
      }
    }

    // Append engine-owned reserved fields the declaration omitted, in canonical form.
    val appended = engineReserved.filterNot(ef => usedEngineNames.contains(ef.name))
    StructType(rebuilt ++ appended)
  }
}
