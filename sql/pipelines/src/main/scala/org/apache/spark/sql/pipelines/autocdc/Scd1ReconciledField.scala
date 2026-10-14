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

import org.apache.spark.sql.{functions => F, Column}
import org.apache.spark.sql.types.DataType

/**
 * The reconciled value and sequencing clock of one [[Scd1FieldToReconcile]].
 *
 * Callers decide which authorship wins. The companion then builds version maps independently of
 * how the winning value was selected.
 *
 * Deletes and upserts can both author null, so a null value may be authored. An unauthored field,
 * whose sequence is null, must have a null value, so readers can use the value without checking
 * its sequence.
 *
 * @param fieldToReconcile The field this reconciles.
 * @param reconciledValue The value selected for the field.
 * @param valueAuthoredAtSequence The sequence at which `reconciledValue` was authored, or null if
 *                                the field remains unauthored. The version map records it for
 *                                every leaf at or beneath the field.
 */
private[autocdc] final case class Scd1ReconciledField(
    fieldToReconcile: Scd1FieldToReconcile,
    reconciledValue: Column,
    valueAuthoredAtSequence: Column) {

  /** Whether this field is a leaf of a column selected for ignore-null. */
  def isIgnoreNullLeaf: Boolean = fieldToReconcile.isIgnoreNullLeaf
}

private[autocdc] object Scd1ReconciledField {

  /** Builds a version map containing one entry for every user-data leaf. */
  def versionMapFrom(
      reconciledFields: Seq[Scd1ReconciledField],
      sequencingType: DataType): Column = {
    val entries = reconciledFields.flatMap { reconciledField =>
      reconciledField.fieldToReconcile.leafPaths.flatMap { leafPath =>
        Seq(F.lit(Scd1VersionMap.serializeKey(leafPath)), reconciledField.valueAuthoredAtSequence)
      }
    }
    F.map(entries: _*).cast(Scd1VersionMap.mapType(sequencingType))
  }
}
