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

package org.apache.spark.sql.connector.catalog

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, MetadataColumnHelper}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._

private[sql] object V2TableUtil extends SQLConfHelper {

  def toQualifiedName(catalog: CatalogPlugin, ident: Identifier): String = {
    s"${quoteIfNeeded(catalog.name)}.${ident.quoted}"
  }

  /**
   * Validates that captured data columns match the current table schema.
   *
   * @param table the current table metadata
   * @param relation the relation with captured columns
   * @return validation errors, or empty sequence if valid
   */
  def validateCapturedColumns(table: Table, relation: DataSourceV2Relation): Seq[String] = {
    validateCapturedColumns(table, relation.table.columns.toImmutableArraySeq)
  }

  /**
   * Validates that captured data columns match the current table schema.
   *
   * Checks for:
   *  - Column type or nullability changes
   *  - Removed columns (missing from the current table schema)
   *  - Added columns (new in the current table schema)
   *
   * @param table the current table metadata
   * @param originCols the originally captured columns
   * @return validation errors, or empty sequence if valid
   */
  def validateCapturedColumns(table: Table, originCols: Seq[Column]): Seq[String] = {
    val errors = mutable.ArrayBuffer[String]()
    val colsByNormalizedName = indexColumns(table.columns.toImmutableArraySeq)
    val originColsByNormalizedName = indexColumns(originCols)

    originColsByNormalizedName.foreach { case (normalizedName, originCol) =>
      colsByNormalizedName.get(normalizedName) match {
        case Some(col) =>
          if (originCol.dataType != col.dataType || originCol.nullable != col.nullable) {
            val oldType = formatType(originCol.dataType, originCol.nullable)
            val newType = formatType(col.dataType, col.nullable)
            errors += s"`${originCol.name}` type has changed from $oldType to $newType"
          }
        case None =>
          errors += s"${formatColumn(originCol)} has been removed"
      }
    }

    colsByNormalizedName.foreach { case (normalizedName, col) =>
      if (!originColsByNormalizedName.contains(normalizedName)) {
        errors += s"${formatColumn(col)} has been added"
      }
    }

    errors.toSeq
  }

  /**
   * Validates that captured metadata columns are consistent with the current table metadata.
   *
   * @param table the current table metadata
   * @param relation the relation with captured metadata columns
   * @return validation errors, or empty sequence if valid
   */
  def validateCapturedMetadataColumns(table: Table, relation: DataSourceV2Relation): Seq[String] = {
    validateCapturedMetadataColumns(table, extractMetadataColumns(relation))
  }

  // extracts original column info for all metadata attributes in relation
  def extractMetadataColumns(relation: DataSourceV2Relation): Seq[MetadataColumn] = {
    val metaAttrs = relation.output.filter(_.isMetadataCol)
    if (metaAttrs.nonEmpty) {
      val metaCols = metadataColumns(relation.table)
      val normalizedMetaAttrNames = metaAttrs.map(attr => normalize(attr.name)).toSet
      metaCols.filter(col => normalizedMetaAttrNames.contains(normalize(col.name)))
    } else {
      Seq.empty
    }
  }

  /**
   * Validates that captured metadata columns are consistent with the current table metadata.
   *
   * Checks for:
   *  - Metadata column type or nullability changes
   *  - Removed metadata columns (missing from current table)
   *
   * @param table the current table metadata
   * @param originMetaCols the originally captured metadata columns
   * @return validation errors, or empty sequence if valid
   */
  def validateCapturedMetadataColumns(
      table: Table,
      originMetaCols: Seq[MetadataColumn]): Seq[String] = {
    val errors = mutable.ArrayBuffer[String]()
    val metaCols = metadataColumns(table)
    val metaColsByNormalizedName = indexMetadataColumns(metaCols)

    originMetaCols.foreach { originMetaCol =>
      val normalizedName = normalize(originMetaCol.name)
      metaColsByNormalizedName.get(normalizedName) match {
        case Some(metaCol) =>
          if (originMetaCol.dataType != metaCol.dataType ||
              originMetaCol.isNullable != metaCol.isNullable) {
            val oldType = formatType(originMetaCol.dataType, originMetaCol.isNullable)
            val newType = formatType(metaCol.dataType, metaCol.isNullable)
            errors += s"`${originMetaCol.name}` type has changed from $oldType to $newType"
          }
        case None =>
          errors += s"${formatMetadataColumn(originMetaCol)} has been removed"
      }
    }

    errors.toSeq
  }

  private def formatColumn(col: Column): String = {
    s"`${col.name}` ${formatType(col.dataType, col.nullable)}"
  }

  private def formatMetadataColumn(col: MetadataColumn): String = {
    s"`${col.name}` ${formatType(col.dataType, col.isNullable)}"
  }

  private def formatType(dataType: DataType, nullable: Boolean): String = {
    if (nullable) dataType.sql else s"${dataType.sql} NOT NULL"
  }

  private def indexColumns(cols: Seq[Column]): Map[String, Column] = {
    index(cols)(_.name)
  }

  private def indexMetadataColumns(cols: Seq[MetadataColumn]): Map[String, MetadataColumn] = {
    index(cols)(_.name)
  }

  private def index[C](cols: Seq[C])(extractName: C => String): Map[String, C] = {
    SchemaUtils.checkColumnNameDuplication(cols.map(extractName), conf.caseSensitiveAnalysis)
    cols.map(col => normalize(extractName(col)) -> col).toMap
  }

  private def metadataColumns(table: Table): Seq[MetadataColumn] = table match {
    case hasMeta: SupportsMetadataColumns => hasMeta.metadataColumns.toImmutableArraySeq
    case _ => Seq.empty
  }

  private def normalize(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }
}
