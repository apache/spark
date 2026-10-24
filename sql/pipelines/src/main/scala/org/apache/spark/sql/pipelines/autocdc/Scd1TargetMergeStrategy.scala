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
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.ArrayImplicits._

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

/**
 * Executes an SCD1 target merge using per-field authorship.
 *
 * Each [[Scd1FieldToReconcile]] takes its value from whichever of the incoming and target rows
 * authored it last. An incoming net delete authors null for every field at its sequence, and
 * deletes the target row only when it is newer than the row's upsert sequence.
 *
 * The reconciled microbatch must be aligned to the target schema, as returned by
 * [[Scd1LeafLevelReconciliation.reconcileMicrobatch]]. Only its leaves are reconciled, and the
 * version map is rewritten from them using its spelling, so a target leaf that it omits or spells
 * differently would lose its version-map entry.
 *
 * @param resolvedSequencingType The data type of the resolved sequencing column.
 */
private[autocdc] final class Scd1LeafLevelTargetMerge(
    changeArgs: ChangeArgs,
    resolvedSequencingType: DataType,
    reconciledMicrobatchDf: DataFrame,
    targetTableIdentifier: TableIdentifier)
    extends Scd1TargetMergeStrategy(
      changeArgs,
      reconciledMicrobatchDf,
      targetTableIdentifier) {
  import Scd1LeafLevelTargetMerge._

  private val userDataSchema: StructType = AutoCdcSchemaUtils.excludeColumns(
    schema = microbatchDf.schema,
    columnNamesToExclude = keyNames :+ cdcMetadataColName,
    resolver = resolver
  )

  private val fieldsToReconcile: Seq[Scd1FieldToReconcile] = Scd1FieldToReconcile.fromSchema(
    schema = userDataSchema,
    ignoreNullSelection = changeArgs.ignoreNullSelection,
    resolver = resolver
  )

  /**
   * The microbatch, with the authorship sequences that [[incomingAuthoredAt]] reads. They are
   * projected beforehand because the merge reads each one several times, and each read would
   * otherwise repeat its version-map lookups.
   */
  private val microbatchWithAuthorshipDf: DataFrame = {
    val microbatchCdcMetadata = F.col(QuotingUtils.quoteIdentifier(cdcMetadataColName))
    val authorshipColumns = fieldsToReconcile.map { fieldToReconcile =>
      incomingAuthoredAtColName(fieldToReconcile) ->
        Scd1BatchProcessor.lastAuthoredIn(microbatchCdcMetadata, fieldToReconcile.leafPaths)
    }.toMap
    microbatchDf
      .withColumns(authorshipColumns)
      .as(incomingQualifier)
  }

  override def merge(): Unit = {
    val mergedFields = fieldsToReconcile.map { fieldToReconcile =>
      val path = fieldToReconcile.path
      Scd1ReconciledField(
        fieldToReconcile = fieldToReconcile,
        reconciledValue = F.when(incomingWins(fieldToReconcile), incomingValue(fieldToReconcile))
          .otherwise(targetColumn(path)),
        // The winning authorship sequence is never less than the losing one.
        valueAuthoredAtSequence =
          F.greatest(incomingAuthoredAt(fieldToReconcile), targetAuthoredAt(fieldToReconcile)))
    }
    val mergedFieldsByColumn = mergedFields.groupBy(_.fieldToReconcile.path.head)

    val incomingAdvancesUpsertSequence =
      incomingUpsertSequence.isNotNull && incomingUpsertSequence > targetUpsertSequence
    // The upsert high-watermark advances even when every field keeps its target value. A delete
    // too old to remove the row still authors null for every target field it is newer than.
    val incomingUpdatesTargetRow =
      (incomingAdvancesUpsertSequence +: fieldsToReconcile.map(incomingWins)).reduce(_ || _)

    // A column outside the ignore-null selection is assigned its winning value whole, which MERGE
    // fits to the column as it does the inserted values. A column selected for ignore-null is
    // rebuilt from the winning value per leaf.
    val userDataAssignments = userDataSchema.fields.toImmutableArraySeq.map { field =>
      val assignedValue = mergedFieldsByColumn.getOrElse(field.name, Seq.empty) match {
        case Seq(column) if !column.isIgnoreNullLeaf => column.reconciledValue
        case leaves =>
          IgnoreNullColumnReconstruction.reconstructColumn(
            path = Seq(field.name),
            field = field,
            leafValues = leaves.map(leaf => leaf.fieldToReconcile.path -> leaf.reconciledValue))
      }
      targetAssignment(field.name, assignedValue)
    }
    val cdcMetadataAssignment = targetAssignment(
      cdcMetadataColName,
      Scd1BatchProcessor.constructCdcMetadataCol(
        deleteSequence = F.lit(null),
        upsertSequence = F.greatest(incomingUpsertSequence, targetUpsertSequence),
        versionMap = Scd1ReconciledField.versionMapFrom(mergedFields, resolvedSequencingType),
        sequencingType = resolvedSequencingType))
    val columnsToUpdate = (userDataAssignments :+ cdcMetadataAssignment).toMap

    // A row of a key absent from the target wins every field it authored, so it is inserted
    // unchanged.
    microbatchWithAuthorshipDf
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

  /** The sequence at which the incoming row authored `fieldToReconcile`, or null if it didn't. */
  private def incomingAuthoredAt(fieldToReconcile: Scd1FieldToReconcile): Column =
    incomingColumn(incomingAuthoredAtColName(fieldToReconcile))

  /**
   * The value the incoming row authored for `fieldToReconcile`. A net delete carries the values of
   * its latest delete event, but authored null.
   */
  private def incomingValue(fieldToReconcile: Scd1FieldToReconcile): Column =
    F.when(incomingDeleteSequence.isNull, incomingColumn(fieldToReconcile.path))

  /** The sequence at which the target row authored `fieldToReconcile`, or null if it didn't. */
  private def targetAuthoredAt(fieldToReconcile: Scd1FieldToReconcile): Column =
    Scd1BatchProcessor.lastAuthoredIn(targetCdcMetadata, fieldToReconcile.leafPaths)

  /**
   * Whether the incoming row's authorship of `fieldToReconcile` beats the target row's. The newer
   * authorship wins. At a tie, the incoming row wins only with a non-null value, so upserts win
   * ties against deletes and tombstones, which author null. Between two upserts with the same
   * sequence, the winner may be arbitrary.
   */
  private def incomingWins(fieldToReconcile: Scd1FieldToReconcile): Column = {
    val incomingSeq = incomingAuthoredAt(fieldToReconcile)
    val targetSeq = targetAuthoredAt(fieldToReconcile)

    F.coalesce(
      F.when(incomingSeq === targetSeq, incomingValue(fieldToReconcile).isNotNull)
        .otherwise(incomingSeq > targetSeq),
      // If the field is not explicitly authored in either the target or incoming rows, then
      // whichever one authored should always win. If neither authored, then the choice is
      // arbitrary and inconsequential.
      incomingSeq.isNotNull)
  }
}

private object Scd1LeafLevelTargetMerge {
  private val prefix = AutoCdcReservedNames.prefix

  private def incomingAuthoredAtColName(fieldToReconcile: Scd1FieldToReconcile): String =
    s"${prefix}incoming_authored_at_${QuotingUtils.quoteNameParts(fieldToReconcile.path)}"
}
