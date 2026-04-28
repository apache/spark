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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{escapeSingleQuotedString, quoteIfNeeded, ResolveDefaultColumns}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumnsUtils.CURRENT_DEFAULT_COLUMN_METADATA_KEY
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, ViewInfo}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Read-side v2 view execs. Each receives the typed [[ViewInfo]] resolved at analysis time
 * (carried on `ResolvedPersistentView.viewInfo`) and formats output rows directly from it --
 * matching the way v2 table inspection execs (e.g. `ShowCreateTableExec`, `DescribeTableExec`)
 * consume the [[org.apache.spark.sql.connector.catalog.Table]] attached to `ResolvedTable`.
 *
 * Only non-session v2 views land here; the session-catalog path is rewritten to v1 commands by
 * `ResolveSessionCatalog` before strategy fires. The catalog name and identifier are passed
 * alongside `viewInfo` for output formatting (qualified names, EXTENDED block headers).
 */

/**
 * Physical plan node for SHOW CREATE TABLE on a v2 view. Reconstructs the {@code CREATE VIEW}
 * statement directly from the typed [[ViewInfo]] -- the column list comes from
 * [[ViewInfo#schema]], the body from [[ViewInfo#queryText]], the binding mode from
 * [[ViewInfo#schemaMode]], and the user TBLPROPERTIES from [[ViewInfo#properties]] (with the
 * reserved-keys filter applied so internal entries don't leak into the rendered DDL).
 */
case class ShowCreateV2ViewExec(
    output: Seq[Attribute],
    quotedName: String,
    viewInfo: ViewInfo) extends LeafV2CommandExec with SQLConfHelper {

  override protected def run(): Seq[InternalRow] = {
    val builder = new StringBuilder
    builder ++= s"CREATE VIEW $quotedName "
    showViewDataColumns(builder)
    Option(viewInfo.properties.get(TableCatalog.PROP_COMMENT)).foreach { c =>
      builder ++= s"COMMENT '${escapeSingleQuotedString(c)}'\n"
    }
    Option(viewInfo.properties.get(TableCatalog.PROP_COLLATION)).foreach { c =>
      builder ++= s"DEFAULT COLLATION $c\n"
    }
    showViewProperties(builder)
    if (conf.viewSchemaBindingEnabled) {
      Option(viewInfo.schemaMode).foreach { sm =>
        builder ++= s"WITH SCHEMA $sm\n"
      }
    }
    builder ++= s"AS ${viewInfo.queryText}\n"
    Seq(toCatalystRow(builder.toString))
  }

  private def showViewDataColumns(builder: StringBuilder): Unit = {
    val schema = viewInfo.schema
    if (schema.nonEmpty) {
      val cols = schema.map { f =>
        val comment = f.getComment().map(c => s" COMMENT '${escapeSingleQuotedString(c)}'")
        s"${quoteIfNeeded(f.name)}${comment.getOrElse("")}"
      }
      builder ++= cols.mkString("(\n  ", ",\n  ", ")\n")
    }
  }

  private def showViewProperties(builder: StringBuilder): Unit = {
    // Drop the reserved keys that already appear as dedicated DDL clauses (PROP_COMMENT /
    // PROP_COLLATION) and the auto-injected `table_type`. Anything left is user TBLPROPERTIES.
    val reserved = Set(
      TableCatalog.PROP_COMMENT,
      TableCatalog.PROP_COLLATION,
      TableCatalog.PROP_TABLE_TYPE)
    val viewProps = viewInfo.properties.asScala
      .filter { case (k, _) => !reserved.contains(k) }
    if (viewProps.nonEmpty) {
      val props = viewProps.toSeq.sortBy(_._1).map { case (key, value) =>
        s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }
      builder ++= s"TBLPROPERTIES ${props.mkString("(\n  ", ",\n  ", ")\n")}"
    }
  }
}

/**
 * Physical plan node for SHOW TBLPROPERTIES on a v2 view. Returns the user-facing properties
 * from [[ViewInfo#properties]] -- the auto-injected `table_type` is filtered out so users see
 * only what they (or the catalog) explicitly set.
 */
case class ShowV2ViewPropertiesExec(
    output: Seq[Attribute],
    quotedName: String,
    viewInfo: ViewInfo,
    propertyKey: Option[String]) extends LeafV2CommandExec with SQLConfHelper {

  override protected def run(): Seq[InternalRow] = {
    val rawProps = viewInfo.properties.asScala.toMap
    val redacted = conf.redactOptions(rawProps)
    propertyKey match {
      case Some(p) =>
        val propValue = redacted.getOrElse(p,
          s"View $quotedName does not have property: $p")
        if (output.length == 1) {
          Seq(toCatalystRow(propValue))
        } else {
          Seq(toCatalystRow(p, propValue))
        }
      case None =>
        redacted
          .filter { case (k, _) => k != TableCatalog.PROP_TABLE_TYPE }
          .toSeq.sortBy(_._1).map(p => toCatalystRow(p._1, p._2))
    }
  }
}

/**
 * Physical plan node for SHOW COLUMNS on a v2 view. Returns one row per top-level field in
 * [[ViewInfo#schema]].
 */
case class ShowV2ViewColumnsExec(
    output: Seq[Attribute],
    viewInfo: ViewInfo) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    viewInfo.schema.map(c => toCatalystRow(c.name)).toSeq
  }
}

/**
 * Physical plan node for DESCRIBE TABLE on a v2 view. Schema rows first; when EXTENDED is
 * specified, an additional `# Detailed View Information` block emits the v2-native fields
 * (catalog, identifier, view text, captured creation context, schema-binding mode, query
 * column names, user TBLPROPERTIES). v2 views are unpartitioned by definition, so the
 * partition-spec branch from v1 `DescribeTableCommand` is unreachable here.
 */
case class DescribeV2ViewExec(
    output: Seq[Attribute],
    catalogName: String,
    identifier: Identifier,
    viewInfo: ViewInfo,
    isExtended: Boolean) extends LeafV2CommandExec with SQLConfHelper {

  override protected def run(): Seq[InternalRow] = {
    val result = new ArrayBuffer[InternalRow]
    viewInfo.schema.foreach { col =>
      result += toCatalystRow(col.name, col.dataType.simpleString, col.getComment().orNull)
    }
    if (isExtended) {
      result += toCatalystRow("", "", "")
      result += toCatalystRow("# Detailed View Information", "", "")
      result += toCatalystRow("Catalog", catalogName, "")
      val qualified = (identifier.namespace() :+ identifier.name())
        .map(quoteIfNeeded).mkString(".")
      result += toCatalystRow("Identifier", qualified, "")
      result += toCatalystRow("View Text", viewInfo.queryText, "")
      Option(viewInfo.currentCatalog).foreach { c =>
        result += toCatalystRow("View Current Catalog", c, "")
      }
      val ns = viewInfo.currentNamespace
      if (ns != null && ns.nonEmpty) {
        result += toCatalystRow(
          "View Current Namespace", ns.map(quoteIfNeeded).mkString("."), "")
      }
      Option(viewInfo.schemaMode).foreach { sm =>
        result += toCatalystRow("View Schema Mode", sm, "")
      }
      val queryColumns = viewInfo.queryColumnNames
      if (queryColumns != null && queryColumns.nonEmpty) {
        result += toCatalystRow(
          "View Query Output Columns", queryColumns.mkString("[", ", ", "]"), "")
      }
      val userProps = viewInfo.properties.asScala
        .filter { case (k, _) => k != TableCatalog.PROP_TABLE_TYPE }
      if (userProps.nonEmpty) {
        val props = conf.redactOptions(userProps.toMap).toSeq.sortBy(_._1).map {
          case (k, v) => s"$k=$v"
        }.mkString("[", ", ", "]")
        result += toCatalystRow("Properties", props, "")
      }
    }
    ResolveDefaultColumns.getDescribeMetadata(viewInfo.schema).foreach { row =>
      result += toCatalystRow(row._1, row._2, row._3)
    }
    result.toSeq
  }
}

/**
 * Physical plan node for DESCRIBE TABLE ... COLUMN on a v2 view. Mirrors the column-only
 * branch of v1 `DescribeColumnCommand`: emit `col_name`, `data_type`, `comment` for the
 * resolved field. v2 views do not carry column statistics, so the EXTENDED branch in v1 emits
 * `NULL` for every stat row -- we follow the same shape.
 */
case class DescribeV2ViewColumnExec(
    output: Seq[Attribute],
    viewInfo: ViewInfo,
    column: UnresolvedAttribute,
    isExtended: Boolean) extends LeafV2CommandExec with SQLConfHelper {

  override protected def run(): Seq[InternalRow] = {
    val resolver = conf.resolver
    val colNameParts = column.nameParts
    val colName = column.name
    if (colNameParts.length > 1) {
      throw QueryCompilationErrors.commandNotSupportNestedColumnError(
        "DESC TABLE COLUMN", colName)
    }
    val field = viewInfo.schema.fields
      .find(f => resolver(f.name, colNameParts.head))
      .getOrElse(throw QueryCompilationErrors.columnNotFoundError(colName))
    val dataType = field.dataType.catalogString
    val comment = field.getComment().orNull
    val rows = ArrayBuffer[InternalRow](
      toCatalystRow("col_name", field.name),
      toCatalystRow("data_type", dataType),
      toCatalystRow("comment", if (comment == null) "NULL" else comment)
    )
    if (isExtended) {
      // v2 views carry no column stats; emit NULL placeholders matching v1 output shape.
      Seq("min", "max", "num_nulls", "distinct_count", "avg_col_len", "max_col_len",
        "histogram").foreach { name =>
        rows += toCatalystRow(name, "NULL")
      }
      if (field.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
        rows += toCatalystRow("default",
          field.metadata.getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY))
      }
    }
    rows.toSeq
  }
}
