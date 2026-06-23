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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.autocdc.ScdType
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * A specification for an internal auxiliary table whose lifecycle follows a materialized
 * dataset of the [[DataflowGraph]]: it is created/evolved alongside that dataset during dataset
 * materialization and dropped when that dataset is fully refreshed.
 *
 * An [[AuxiliaryTableSpec]] describes a table that is deliberately NOT part of the logical
 * [[DataflowGraph]]: it is never resolved, connected, materialized as a user-facing dataset, nor
 * exposed as an [[Input]] to other flows, and it is intentionally outside the [[Output]]
 * hierarchy.
 *
 * An auxiliary table is owned by the dataset it accompanies and derived from the flows that write
 * to that dataset. This ownership coupling is intentional and part of the definition of an
 * auxiliary/companion table: it cannot exist independently of an owning dataset, which is itself an
 * artifact of the dataflow graph.
 */
sealed trait AuxiliaryTableSpec {
  /** The catalog identifier of the auxiliary table. */
  def identifier: TableIdentifier

  /** The schema the auxiliary table should be created with (and evolved towards). */
  def schema: StructType

  /** The table properties the auxiliary table should be created/altered with. */
  def properties: Map[String, String]
}

/**
 * An [[AuxiliaryTableSpec]] for the auxiliary state table owned by an AutoCDC target. Beyond the
 * create/evolve shape, it carries the metadata required to reject drift of an already-materialized
 * auxiliary table before it is evolved. The drift check itself is a stateless method on
 * [[AutoCdcAuxiliaryTable]]; this spec is pure data describing what that check expects.
 *
 * No flow name is carried here on purpose: a single auxiliary table is shared by every AutoCDC flow
 * writing to its target, so drift errors name the target table rather than any one flow.
 *
 * @param identifier           the catalog identifier of the auxiliary table.
 * @param schema               the schema the auxiliary table should be created with (and evolved
 *                             towards).
 * @param properties           the table properties the auxiliary table should be created/altered
 *                             with.
 * @param targetTableIdentifier the identifier of the AutoCDC target this auxiliary table belongs to,
 *                              used to name the target in drift error messages.
 * @param expectedKeyFields    the AutoCDC key fields the auxiliary table is expected to carry if
 *                             it already exists, in order (names and types).
 * @param expectedScdType      the SCD type the auxiliary table is expected to have recorded, if it
 *                             already exists.
 */
final case class AutoCdcAuxiliaryTableSpec(
    identifier: TableIdentifier,
    schema: StructType,
    properties: Map[String, String],
    targetTableIdentifier: TableIdentifier,
    expectedKeyFields: Seq[StructField],
    expectedScdType: ScdType) extends AuxiliaryTableSpec
