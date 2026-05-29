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
}
