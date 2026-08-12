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
}
