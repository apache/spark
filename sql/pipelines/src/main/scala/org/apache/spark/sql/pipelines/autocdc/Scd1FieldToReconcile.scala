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
import org.apache.spark.sql.{functions => F, Column}
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.ArrayImplicits._

/**
 * An SCD1 user-data field reconciled independently of all others: either a leaf of a column
 * selected for ignore-null, or a whole top-level column outside the selection. This is the first
 * state of the field lifecycle described in [[Scd1LeafLevelReconciliation]], and depends only on
 * the user-data schema and the ignore-null selection.
 */
private[autocdc] sealed trait Scd1FieldToReconcile {

  /** The field's name parts within the user-data schema. */
  def path: Seq[String]

  /** The field's schema field. */
  def field: StructField

  /** The name parts of every user-data leaf at or beneath [[path]]. */
  def leafPaths: Seq[Seq[String]]

  /**
   * When this field was last authored, according to a row's CDC metadata. A field was last
   * authored at the greatest sequence at which any of its leaves was authored. A delete authors
   * every leaf at its delete sequence.
   *
   * @param cdcMetadata The row's CDC metadata.
   * @return The sequence at which the row last authored this field, or null if the row leaves it
   *         unauthored.
   */
  final def lastAuthoredIn(cdcMetadata: Column): Column = {
    val deleteSequence = Scd1BatchProcessor.deleteSequenceOf(cdcMetadata)
    val upsertSequence = Scd1BatchProcessor.upsertSequenceOf(cdcMetadata)
    val versionMap = cdcMetadata.getField(Scd1BatchProcessor.versionMapFieldName)

    val greatestLeafSequence = leafPaths
      .map(leafPath => versionMap(Scd1VersionMap.serializeKey(leafPath)))
      .reduceOption(F.greatest(_, _))
      // A field without leaves, such as an empty struct, has no version-map entries.
      .getOrElse(upsertSequence)

    F.when(deleteSequence.isNotNull, deleteSequence)
      .when(versionMap.isNull, upsertSequence)
      .otherwise(greatestLeafSequence)
  }
}

private[autocdc] object Scd1FieldToReconcile {

  /**
   * A leaf of a column selected for ignore-null, which an upsert authors only when its version
   * map records the leaf as authored.
   */
  case class IgnoreNullLeaf(path: Seq[String], field: StructField) extends Scd1FieldToReconcile {
    override def leafPaths: Seq[Seq[String]] = Seq(path)
  }

  /**
   * A top-level column outside the ignore-null selection, which every upsert authors in full,
   * nulls included.
   */
  case class RespectNullColumn(field: StructField) extends Scd1FieldToReconcile {
    override def path: Seq[String] = Seq(field.name)

    override def leafPaths: Seq[Seq[String]] = field.dataType match {
      case struct: StructType => AutoCdcSchemaUtils.flattenStructFieldPaths(struct).map(path ++ _)
      case _ => Seq(path)
    }
  }

  /**
   * Returns the fields to reconcile independently of each other, preserving schema order.
   *
   * A column selected for ignore-null is reconciled leaf by leaf, since an upsert authors only the
   * leaves it provides as non-null. A null leaf in such a column means no live value was found to
   * inherit rather than an authored null, so two of its values are equivalent if every leaf reads
   * the same, where a leaf beneath a null struct reads as null. Every other column is reconciled
   * as a whole value: every upsert authors all of its leaves, so they share one winning event, and
   * keeping the winner's whole value preserves whether its structs were null.
   *
   * @param schema The user-data schema, excluding key and CDC metadata columns.
   * @param ignoreNullSelection The top-level columns selected for ignore-null, or None if
   *                            ignore-null is disabled.
   * @param resolver The resolver used for column-name matching.
   */
  def fromSchema(
      schema: StructType,
      ignoreNullSelection: Option[ColumnSelection],
      resolver: Resolver): Seq[Scd1FieldToReconcile] = {
    val ignoreNullColumnNames: Set[String] = ignoreNullSelection match {
      case None => Set.empty
      case Some(selection) =>
        ColumnSelection.applyToSchema(
          schemaName = "ignoreNullSelection",
          schema = schema,
          columnSelection = Some(selection),
          resolver = resolver
        ).fieldNames.toSet
    }

    schema.fields.toImmutableArraySeq.flatMap { field =>
      if (ignoreNullColumnNames.contains(field.name)) {
        leavesOf(new StructType(Array(field))).map { case (path, leafField) =>
          IgnoreNullLeaf(path, leafField)
        }
      } else {
        Seq(RespectNullColumn(field))
      }
    }
  }

  /** Returns every leaf path and its field, preserving schema order. */
  private def leavesOf(schema: StructType): Seq[(Seq[String], StructField)] =
    AutoCdcSchemaUtils.flattenStructFieldPaths(schema).map { path =>
      val field = schema.findNestedField(path).map(_._2).getOrElse {
        throw SparkException.internalError(
          s"Cannot resolve reconciled leaf ${QuotingUtils.quoteNameParts(path)}.")
      }
      path -> field
    }
}
