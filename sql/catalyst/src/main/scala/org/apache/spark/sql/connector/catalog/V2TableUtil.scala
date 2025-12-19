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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, MetadataColumnHelper}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.sql.util.SchemaValidationMode
import org.apache.spark.sql.util.SchemaValidationMode.PROHIBIT_CHANGES
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
   * @param mode validation mode that defines what changes are acceptable
   * @return validation errors, or empty sequence if valid
   */
  def validateCapturedColumns(
      table: Table,
      relation: DataSourceV2Relation,
      mode: SchemaValidationMode): Seq[String] = {
    validateCapturedColumns(table, relation.table.columns.toImmutableArraySeq, mode)
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
   * @param mode validation mode that defines what changes are acceptable
   * @return validation errors, or empty sequence if valid
   */
  def validateCapturedColumns(
      table: Table,
      originCols: Seq[Column],
      mode: SchemaValidationMode = PROHIBIT_CHANGES): Seq[String] = {
    val originSchema = CatalogV2Util.v2ColumnsToStructType(originCols)
    val schema = CatalogV2Util.v2ColumnsToStructType(table.columns)
    SchemaUtils.validateSchemaCompatibility(originSchema, schema, resolver, mode)
  }

  /**
   * Validates that captured metadata columns are consistent with the current table metadata.
   *
   * @param table the current table metadata
   * @param relation the relation with captured metadata columns
   * @param mode validation mode that defines what changes are acceptable
   * @return validation errors, or empty sequence if valid
   */
  def validateCapturedMetadataColumns(
      table: Table,
      relation: DataSourceV2Relation,
      mode: SchemaValidationMode): Seq[String] = {
    validateCapturedMetadataColumns(table, extractMetadataColumns(relation), mode)
  }

  /**
   * Extracts original column info for all metadata attributes in the relation.
   *
   * @param relation the relation with captured metadata columns
   * @return metadata columns captured by the relation
   */
  def extractMetadataColumns(relation: DataSourceV2Relation): Seq[MetadataColumn] = {
    val metaAttrNames = relation.output.filter(_.isMetadataCol).map(_.name)
    filter(metaAttrNames, metadataColumns(relation.table))
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
   * @param mode validation mode that defines what changes are acceptable
   * @return validation errors, or empty sequence if valid
   */
  def validateCapturedMetadataColumns(
      table: Table,
      originMetaCols: Seq[MetadataColumn],
      mode: SchemaValidationMode = PROHIBIT_CHANGES): Seq[String] = {
    val originMetaColNames = originMetaCols.map(_.name)
    val originMetaSchema = CatalogV2Util.toStructType(originMetaCols)
    val metaCols = filter(originMetaColNames, metadataColumns(table))
    val metaSchema = CatalogV2Util.toStructType(metaCols)
    SchemaUtils.validateSchemaCompatibility(originMetaSchema, metaSchema, resolver, mode)
  }

  private def filter(colNames: Seq[String], cols: Seq[MetadataColumn]): Seq[MetadataColumn] = {
    val normalizedColNames = colNames.map(normalize).toSet
    cols.filter(col => normalizedColNames.contains(normalize(col.name)))
  }

  private def metadataColumns(table: Table): Seq[MetadataColumn] = table match {
    case hasMeta: SupportsMetadataColumns => hasMeta.metadataColumns.toImmutableArraySeq
    case _ => Seq.empty
  }

  private def normalize(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  private def resolver: Resolver = conf.resolver
}
