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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{escapeSingleQuotedString, quoteIfNeeded, ResolveDefaultColumns}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumnsUtils.CURRENT_DEFAULT_COLUMN_METADATA_KEY
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, TableCatalog, ViewInfo}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Read-side v2 view execs. Each receives the typed [[ViewInfo]] resolved at analysis time
 * (carried on `ResolvedPersistentView.info`) and formats output rows directly from it --
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
    // Drop the reserved keys that either already appear as dedicated DDL clauses
    // (PROP_COMMENT / PROP_COLLATION) or are otherwise managed outside user TBLPROPERTIES
    // (PROP_OWNER, PROP_TABLE_TYPE, etc.). Mirrors the v1 SHOW CREATE TABLE filter, which
    // hides the same first-class fields from the rendered TBLPROPERTIES clause.
    val viewProps = viewInfo.properties.asScala
      .filter { case (k, _) => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(k) }
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
 * from [[ViewInfo#properties]] -- reserved first-class keys (PROP_COMMENT, PROP_COLLATION,
 * PROP_OWNER, PROP_TABLE_TYPE, ...) are filtered out so users see only what they (or the
 * catalog) explicitly set, matching v1 `SHOW TBLPROPERTIES` on a session-catalog view (which
 * hides these because v1 stores them in typed `CatalogTable` fields rather than `properties`).
 * A directly-requested reserved key still returns its value so users can ask for it by name.
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
          .filter { case (k, _) => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(k) }
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
 * specified, an additional `# Detailed View Information` block emits the v2-native fields:
 * the resolved-identifier components via [[DescribeIdentifierRows#addIdentifierRows]] (which
 * also surfaces a v1-compat `Database` row for single-segment namespaces), followed by view
 * text, captured creation context, schema-binding mode, query column names, and user
 * TBLPROPERTIES. v2 views are unpartitioned by definition, so the partition-spec branch from
 * v1 `DescribeTableCommand` is unreachable here.
 */
case class DescribeV2ViewExec(
    output: Seq[Attribute],
    catalogName: String,
    identifier: Identifier,
    viewInfo: ViewInfo,
    isExtended: Boolean) extends DescribeIdentifierRows with SQLConfHelper {

  override protected def run(): Seq[InternalRow] = {
    val result = new ArrayBuffer[InternalRow]
    viewInfo.schema.foreach { col =>
      result += toCatalystRow(col.name, col.dataType.simpleString, col.getComment().orNull)
    }
    if (isExtended) {
      result += toCatalystRow("", "", "")
      result += toCatalystRow("# Detailed View Information", "", "")
      addIdentifierRows(result, catalogName, identifier, entityLabel = "View")
      // Promote first-class reserved fields (Owner / Comment / Collation) to top-level rows
      // before the EXTENDED Properties block, mirroring v1 `CatalogTable.toJsonLinkedHashMap`
      // which renders these as their own rows rather than burying them in `Table Properties`.
      Option(viewInfo.properties.get(TableCatalog.PROP_OWNER)).filter(_.nonEmpty).foreach { o =>
        result += toCatalystRow("Owner", o, "")
      }
      Option(viewInfo.properties.get(TableCatalog.PROP_COMMENT)).foreach { c =>
        result += toCatalystRow("Comment", c, "")
      }
      Option(viewInfo.properties.get(TableCatalog.PROP_COLLATION)).foreach { c =>
        result += toCatalystRow("Collation", c, "")
      }
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
      // Filter the same reserved set as `ShowV2ViewPropertiesExec` so the EXTENDED
      // `Properties` row mirrors `SHOW TBLPROPERTIES` and matches v1 (which hides these
      // first-class fields because they live in typed `CatalogTable` fields).
      val userProps = viewInfo.properties.asScala
        .filter { case (k, _) => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(k) }
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
 * Physical plan node for DESCRIBE TABLE ... COLUMN on a v2 view. The column nameParts are
 * extracted at strategy time from the (already-resolved) column expression on
 * `DescribeColumn`, so this exec doesn't have to deal with resolution. v2 views don't carry
 * column statistics, so the EXTENDED branch in v1 emits `NULL` for every stat row -- we
 * follow the same shape.
 */
case class DescribeV2ViewColumnExec(
    output: Seq[Attribute],
    viewInfo: ViewInfo,
    colNameParts: Seq[String],
    isExtended: Boolean) extends LeafV2CommandExec with SQLConfHelper {

  override protected def run(): Seq[InternalRow] = {
    val resolver = conf.resolver
    val colName = colNameParts.mkString(".")
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
