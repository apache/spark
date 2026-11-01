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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.DataFrame

/**
 * Shared target-table references and expressions used by SCD1 target merge strategies.
 *
 * @param changeArgs The CDC flow configuration.
 * @param reconciledMicrobatchDf The reconciled microbatch to merge, which must contain at most
 *                               one row per key. Its columns must include the keys and the CDC
 *                               metadata column, and be a subset of the target table's.
 * @param targetTableIdentifier The identifier of the target table to merge into.
 */
private[autocdc] abstract class Scd1TargetMergeStrategy(
    protected val changeArgs: ChangeArgs,
    reconciledMicrobatchDf: DataFrame,
    targetTableIdentifier: TableIdentifier) {

  protected final val cdcMetadataColName: String =
    AutoCdcReservedNames.cdcMetadataColName

  // The table the merge writes to. Its name also qualifies the target row's columns.
  protected final val targetTable: String = targetTableIdentifier.quotedString

  // The reconciled microbatch. Each strategy merges it, or a projection of it, into the target.
  protected final val microbatchDf: DataFrame = reconciledMicrobatchDf

  // Qualifies the incoming row's columns, which share names with the target row's. Each strategy
  // must alias the DataFrame it merges into the target with it, to use this class' helpers.
  protected final val incomingQualifier: String = "incoming"

  protected final val resolver: Resolver =
    microbatchDf.sparkSession.sessionState.conf.resolver

  protected final val keyNames: Seq[String] = changeArgs.keys.map(_.name)

  protected final def incomingColumn(columnName: String): Column =
    incomingColumn(Seq(columnName))

  protected final def incomingColumn(path: Seq[String]): Column =
    F.col(s"$incomingQualifier.${QuotingUtils.quoteNameParts(path)}")

  protected final def targetColumn(columnName: String): Column =
    targetColumn(Seq(columnName))

  protected final def targetColumn(path: Seq[String]): Column =
    F.col(s"$targetTable.${QuotingUtils.quoteNameParts(path)}")

  protected final def targetAssignment(columnName: String, value: Column): (String, Column) = {
    val quoted = QuotingUtils.quoteIdentifier(columnName)
    s"$targetTable.$quoted" -> value
  }

  protected final def assignmentFromIncoming(columnName: String): (String, Column) =
    targetAssignment(columnName, incomingColumn(columnName))

  protected final val incomingCdcMetadata: Column = incomingColumn(cdcMetadataColName)
  protected final val targetCdcMetadata: Column = targetColumn(cdcMetadataColName)
  protected final val incomingDeleteSequence: Column =
    Scd1BatchProcessor.deleteSequenceOf(incomingCdcMetadata)
  protected final val incomingUpsertSequence: Column =
    Scd1BatchProcessor.upsertSequenceOf(incomingCdcMetadata)
  protected final val targetUpsertSequence: Column =
    Scd1BatchProcessor.upsertSequenceOf(targetCdcMetadata)

  protected final val keysMatch: Column = changeArgs.keys
    .map(key => incomingColumn(key.name) === targetColumn(key.name))
    .reduce(_ && _)

  // Delete beats an existing row only if its sequence is strictly greater than the target upsert
  // sequence. Upserts therefore receive priority over deletes on tied sequences.
  protected final val incomingDeletesTargetRow: Column =
    incomingDeleteSequence.isNotNull && incomingDeleteSequence > targetUpsertSequence

  protected final val columnsToInsert: Map[String, Column] = microbatchDf.columns
    .map(assignmentFromIncoming)
    .toMap

  /** Executes the target-table merge. */
  def merge(): Unit
}

/** Executes an SCD1 target merge using row-level authorship. */
private[autocdc] final class Scd1RowLevelTargetMerge(
    changeArgs: ChangeArgs,
    reconciledMicrobatchDf: DataFrame,
    targetTableIdentifier: TableIdentifier)
    extends Scd1TargetMergeStrategy(
      changeArgs,
      reconciledMicrobatchDf,
      targetTableIdentifier) {

  override def merge(): Unit = {
    // Upsert beats an existing row if the incoming upsert sequence is greater than or equal to
    // the target upsert sequence.
    val incomingUpdatesTargetRow =
      incomingUpsertSequence.isNotNull && incomingUpsertSequence >= targetUpsertSequence

    // Join columns must not be mutated. Every other incoming column, including CDC metadata, is
    // overwritten when the incoming upsert wins.
    val columnsToUpdate = microbatchDf.columns
      .filterNot(columnName => keyNames.exists(resolver(_, columnName)))
      .map(assignmentFromIncoming)
      .toMap

    microbatchDf
      .as(incomingQualifier)
      .mergeInto(targetTable, keysMatch)
      .whenMatched(incomingDeletesTargetRow)
      .delete()
      .whenMatched(incomingUpdatesTargetRow)
      .update(columnsToUpdate)
      // Deletes for absent keys are no-ops for the target table. They are stored as tombstones in
      // the auxiliary table instead.
      .whenNotMatched(incomingDeleteSequence.isNull)
      .insert(columnsToInsert)
      .merge()
  }
}
