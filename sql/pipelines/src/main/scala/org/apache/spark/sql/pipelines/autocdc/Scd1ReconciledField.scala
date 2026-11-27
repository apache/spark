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

import org.apache.spark.sql.Column

/**
 * The reconciled value and sequencing clock of one [[Scd1FieldToReconcile]].
 *
 * Callers decide which authorship wins, and how the version map records it.
 *
 * Deletes and upserts can both author null, so a null value may be authored. An unauthored field,
 * whose sequence is null, must have a null value. Initializers are expected to respect this
 * relationship by construction to avoid the readers/dataclass paying a runtime cost to assert this
 * consistency, which can otherwise be expensive across all fields in all rows being reconciled.
 *
 * @param fieldToReconcile The field this reconciles.
 * @param reconciledValue The value selected for the field.
 * @param valueAuthoredAtSequence The latest sequence among the field's leaves, which orders the
 *                                field against competing authorship, or null if the field
 *                                remains unauthored. The leaves of a field authored by one event
 *                                share that sequence, but a target row can hold different ones
 *                                for the leaves of a field reconciled whole, such as when written
 *                                under an earlier ignore-null selection.
 */
private[autocdc] final case class Scd1ReconciledField(
    fieldToReconcile: Scd1FieldToReconcile,
    reconciledValue: Column,
    valueAuthoredAtSequence: Column) {

  /** Whether this field is a leaf of a column selected for ignore-null. */
  def isIgnoreNullLeaf: Boolean = fieldToReconcile.isIgnoreNullLeaf
}
