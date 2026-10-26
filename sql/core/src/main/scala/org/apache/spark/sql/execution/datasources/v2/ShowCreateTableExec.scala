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

import java.util

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, V1CommandUtils}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{escapeSingleQuotedString, CharVarcharUtils, WriteDistributionAndOrdering}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Table, TableCatalog, V1Table}
import org.apache.spark.sql.connector.expressions.{BucketTransform, Transform}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for show create table.
 */
case class ShowCreateTableExec(
    output: Seq[Attribute],
    resolvedTable: ResolvedTable) extends V2CommandExec with LeafExecNode {
  override protected def run(): Seq[InternalRow] = {
    val builder = new StringBuilder
    showCreateTable(resolvedTable, builder)
    Seq(InternalRow(UTF8String.fromString(builder.toString)))
  }

  private def showCreateTable(resolvedTable: ResolvedTable, builder: StringBuilder): Unit = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    val table = resolvedTable.table
    val quotedName = resolvedTable.name
    val schema = table.columns.asSchema
    val partitioning = table.partitioning
    val properties = table.properties
    val provider = Option(properties.get(TableCatalog.PROP_PROVIDER))
    builder ++= s"CREATE TABLE ${quotedName} "

    showTableDataColumns(table, schema, builder)
    showTableUsing(provider, builder)

    val tableOptions = properties.asScala
      .filter { case (k, _) => k.startsWith(TableCatalog.OPTION_PREFIX) }.map {
        case (k, v) => k.drop(TableCatalog.OPTION_PREFIX.length) -> v
      }.toMap
    showTableOptions(builder, tableOptions)
    showTablePartitioning(table, partitioning, builder)
    showTableWriteDistributionAndOrdering(resolvedTable, schema, partitioning, provider, builder)
    showTableComment(properties, builder)
    showTableCollation(properties, builder)
    showTableLocation(properties, builder)
    showTableProperties(properties, builder, tableOptions)
  }

  private def showTableDataColumns(
      table: Table,
      schema: StructType,
      builder: StringBuilder): Unit = {
    val rawSchema = CharVarcharUtils.getRawSchema(schema, conf)
    val schemaWithExplicitCollations = DataTypeUtils.replaceNonCollatedTypesWithExplicitUTF8Binary(
        rawSchema).asInstanceOf[StructType]
    val columns = schemaWithExplicitCollations.fields.map(_.toDDL)
    val constraints = table.constraints().map(_.toDDL)
    builder ++= concatByMultiLines(columns ++ constraints)
  }

  private def showTableUsing(provider: Option[String], builder: StringBuilder): Unit = {
    provider
      .map("USING " + escapeSingleQuotedString(_) + "\n")
      .foreach(builder.append)
  }

  private def showTableOptions(
      builder: StringBuilder,
      tableOptions: Map[String, String]): Unit = {
    if (tableOptions.nonEmpty) {
      val props = conf.redactOptions(tableOptions).toSeq.sortBy(_._1).map {
        case (key, value) =>
          s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }
      builder ++= "OPTIONS "
      builder ++= concatByMultiLines(props)
    }
  }

  private def showTablePartitioning(
      table: Table,
      partitioning: Array[Transform],
      builder: StringBuilder): Unit = {
    if (!partitioning.isEmpty) {
      val transforms = new ArrayBuffer[String]
      var bucketSpec = Option.empty[BucketSpec]
      partitioning.map {
        case BucketTransform(numBuckets, col, sortCol) if table.isInstanceOf[V1Table] =>
          require(bucketSpec.isEmpty, "V1Table can not define multiple bucket transforms")
          if (sortCol.isEmpty) {
            bucketSpec = Some(BucketSpec(numBuckets, col.map(_.fieldNames.mkString(".")), Nil))
          } else {
            bucketSpec = Some(BucketSpec(numBuckets, col.map(_.fieldNames.mkString(".")),
              sortCol.map(_.fieldNames.mkString("."))))
          }
        case t =>
          transforms += t.describe()
      }
      if (transforms.nonEmpty) {
        builder ++= s"PARTITIONED BY ${transforms.mkString("(", ", ", ")")}\n"
      }

      // compatible with v1
      bucketSpec.map { bucket =>
        assert(bucket.bucketColumnNames.nonEmpty)
        builder ++= s"CLUSTERED BY ${bucket.bucketColumnNames.mkString("(", ", ", ")")}\n"
        if (bucket.sortColumnNames.nonEmpty) {
          builder ++= s"SORTED BY ${bucket.sortColumnNames.mkString("(", ", ", ")")}\n"
        }
        builder ++= s"INTO ${bucket.numBuckets} BUCKETS\n"
      }
    }
  }

  /**
   * Emits the table's declared write distribution and ordering as clauses when parsing them in
   * this session declares the same pair. Otherwise the pair is omitted: it has no clause form, or
   * the catalog does not accept the clauses, or the statement would create a v1 table, or a sort
   * key references a column the table does not have or does not parse back to the same key.
   */
  private def showTableWriteDistributionAndOrdering(
      resolvedTable: ResolvedTable,
      schema: StructType,
      partitioning: Array[Transform],
      provider: Option[String],
      builder: StringBuilder): Unit = {
    val table = resolvedTable.table
    if (WriteDistributionAndOrdering.supportsWriteDistributionAndOrdering(resolvedTable.catalog)) {
      val writeDistributionMode = table.writeDistributionMode()
      val writeOrdering = WriteDistributionAndOrdering.writeOrderingOf(table)
      if (WriteDistributionAndOrdering.isRequested(writeDistributionMode, writeOrdering)) {
        WriteDistributionAndOrdering.writeClausesSQL(
          writeDistributionMode,
          writeOrdering,
          partitioning.toImmutableArraySeq,
          schema,
          WriteDistributionAndOrdering.replayWith(session.sessionState.sqlParser)
        ).filter(_ => !createsV1Table(resolvedTable.catalog, provider))
          .foreach(clauses => builder ++= s"$clauses\n")
      }
    }
  }

  // Whether the printed CREATE TABLE would create a v1 table, which cannot record the clauses, so
  // the replay would be rejected. It has the table's provider, if any, as USING and no STORED AS
  // or ROW FORMAT, so CREATE TABLE picks its provider as `createTableProvider` does. A provider
  // that cannot be looked up or linked fails the replay too. The lookup is costly, so it runs
  // only for clauses that are emitted.
  private def createsV1Table(catalog: TableCatalog, provider: Option[String]): Boolean = {
    try {
      V1CommandUtils.supportsV1Command(catalog) && {
        val tableProvider = DataSourceV2Utils.createTableProvider(
          provider, hasSerde = false, ctas = false, conf)
        !DataSourceV2Utils.isV2Provider(tableProvider, conf)
      }
    } catch {
      case NonFatal(_) | _: LinkageError => true
    }
  }

  private def showTableLocation(
      properties: util.Map[String, String],
      builder: StringBuilder): Unit = {
    val isManagedOption = Option(properties.get(TableCatalog.PROP_IS_MANAGED_LOCATION))
    // Only generate LOCATION clause if it's not managed.
    if (isManagedOption.isEmpty || !isManagedOption.get.equalsIgnoreCase("true")) {
      Option(properties.get(TableCatalog.PROP_LOCATION))
        .map("LOCATION '" + escapeSingleQuotedString(_) + "'\n")
        .foreach(builder.append)
    }
  }

  private def showTableProperties(
      properties: util.Map[String, String],
      builder: StringBuilder,
      tableOptions: Map[String, String]): Unit = {

    val showProps = properties.asScala
      .filter { case (key, _) =>
        !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(key) &&
        !key.startsWith(TableCatalog.OPTION_PREFIX) &&
        !tableOptions.contains(key)
      }
    if (showProps.nonEmpty) {
      val props = conf.redactOptions(showProps.toMap).toSeq.sortBy(_._1).map {
        case (key, value) =>
          s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= "TBLPROPERTIES "
      builder ++= concatByMultiLines(props)
    }
  }

  private def showTableComment(
      properties: util.Map[String, String],
      builder: StringBuilder): Unit = {
    Option(properties.get(TableCatalog.PROP_COMMENT))
      .map("COMMENT '" + escapeSingleQuotedString(_) + "'\n")
      .foreach(builder.append)
  }

  private def showTableCollation(
      properties: util.Map[String, String],
      builder: StringBuilder): Unit = {
    Option(properties.get(TableCatalog.PROP_COLLATION))
      .map("DEFAULT COLLATION " + _ + "\n").foreach(builder.append)
  }

  private def concatByMultiLines(iter: Iterable[String]): String = {
    iter.mkString("(\n  ", ",\n  ", ")\n")
  }
}
