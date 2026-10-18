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
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._

/**
 * An SCD1 user-data field reconciled independently of all others: either a leaf of a column
 * selected for ignore-null, or a whole top-level column outside the selection. It depends only on
 * the user-data schema and the ignore-null selection.
 *
 * @param path The field's name parts within the user-data schema.
 * @param leafPaths The name parts of every user-data leaf at or beneath `path`.
 * @param isIgnoreNullLeaf Whether the field is a leaf of a column selected for ignore-null, which
 *                         an upsert with a version map authors only when the map records the
 *                         leaf as authored. If not, the field is a whole column outside the
 *                         selection, which an upsert authors in full, nulls included, unless the
 *                         microbatch omits all of its leaves.
 */
private[autocdc] case class Scd1FieldToReconcile(
    path: Seq[String],
    leafPaths: Seq[Seq[String]],
    isIgnoreNullLeaf: Boolean)

private[autocdc] object Scd1FieldToReconcile {

  /**
   * Applies `ignoreNullSelection` to `schema` and returns a [[Scd1FieldToReconcile]] for each
   * field reconciled independently: every leaf of a selected column, and every other top-level
   * column as a whole.
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
    // Ignore null selection may only list top level columns, so ignoreNullColumnNames resolves to
    // only names of top level columns.
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
    val leafPathsByTopLevelColumnName =
      AutoCdcSchemaUtils.flattenStructFieldPaths(schema).groupBy(_.head)
    val topLevelColumns = schema.fields.toImmutableArraySeq

    topLevelColumns.flatMap { topLevelColumn =>
      val leafPaths = leafPathsByTopLevelColumnName.getOrElse(topLevelColumn.name, Seq.empty)
      if (ignoreNullColumnNames.contains(topLevelColumn.name)) {
        // For ignore-null columns, each of its leaves is independently reconciled.
        leafPaths.map { leafPath =>
          Scd1FieldToReconcile(
            path = leafPath, leafPaths = Seq(leafPath), isIgnoreNullLeaf = true)
        }
      } else {
        // A respect-null column is written whole rather than per leaf, but still lists its leaf
        // paths, since the version map is keyed by leaf.
        Seq(Scd1FieldToReconcile(
          path = Seq(topLevelColumn.name), leafPaths = leafPaths, isIgnoreNullLeaf = false))
      }
    }
  }
}
