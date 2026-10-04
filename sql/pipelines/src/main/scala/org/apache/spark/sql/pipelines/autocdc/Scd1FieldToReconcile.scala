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
   * The sequence at which an upsert authors this field, or null if the upsert leaves it
   * unauthored.
   *
   * @param upsertSequence The event's upsert sequence, or null if the event isn't an upsert.
   * @param versionMap The upsert's version map, or null if the upsert authors every leaf.
   */
  def upsertAuthorshipSequence(upsertSequence: Column, versionMap: Column): Column
}

private[autocdc] object Scd1FieldToReconcile {

  /**
   * A leaf of a column selected for ignore-null, which an upsert authors only when its version
   * map records the leaf as authored.
   */
  case class IgnoreNullLeaf(path: Seq[String], field: StructField) extends Scd1FieldToReconcile {
    override def leafPaths: Seq[Seq[String]] = Seq(path)

    override def upsertAuthorshipSequence(upsertSequence: Column, versionMap: Column): Column =
      F.when(versionMap.isNull, upsertSequence)
        .otherwise(versionMap(Scd1VersionMap.serializeKey(path)))
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

    override def upsertAuthorshipSequence(upsertSequence: Column, versionMap: Column): Column =
      upsertSequence
  }

  /**
   * Returns the fields to reconcile independently of each other, preserving schema order.
   *
   * A column selected for ignore-null is reconciled leaf by leaf, since an upsert authors only the
   * leaves it provides as non-null. Every other column is reconciled as a whole value: every
   * upsert authors all of its leaves, so they share one winning event, and keeping the winner's
   * whole value preserves whether its structs were null. Reconstructing a selected struct's
   * nullness from its leaves relies on this split.
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
