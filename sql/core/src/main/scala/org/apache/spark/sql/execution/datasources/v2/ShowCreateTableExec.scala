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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{escapeSingleQuotedString, CharVarcharUtils}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.BucketTransform
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.unsafe.types.UTF8String

/**
 * Physical plan node for show create table.
 */
case class ShowCreateTableExec(
    output: Seq[Attribute],
    table: Table) extends V2CommandExec with LeafExecNode {
  override protected def run(): Seq[InternalRow] = {
    val builder = StringBuilder.newBuilder
    showCreateTable(table, builder)
    Seq(InternalRow(UTF8String.fromString(builder.toString)))
  }

  private def showCreateTable(table: Table, builder: StringBuilder): Unit = {
    builder ++= s"CREATE TABLE ${table.name()} "

    showTableDataColumns(table, builder)
    showTableUsing(table, builder)

    val tableOptions = table.properties.asScala
      .filterKeys(_.startsWith(TableCatalog.OPTION_PREFIX)).map {
      case (k, v) => k.drop(TableCatalog.OPTION_PREFIX.length) -> v
    }.toMap
    showTableOptions(builder, tableOptions)
    showTablePartitioning(table, builder)
    showTableComment(table, builder)
    showTableLocation(table, builder)
    showTableProperties(table, builder, tableOptions)
  }

  private def showTableDataColumns(table: Table, builder: StringBuilder): Unit = {
    val columns = CharVarcharUtils.getRawSchema(table.schema(), conf).fields.map(_.toDDL)
    builder ++= concatByMultiLines(columns)
  }

  private def showTableUsing(table: Table, builder: StringBuilder): Unit = {
    Option(table.properties.get(TableCatalog.PROP_PROVIDER))
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

  private def showTablePartitioning(table: Table, builder: StringBuilder): Unit = {
    if (!table.partitioning.isEmpty) {
      val transforms = new ArrayBuffer[String]
      var bucketSpec = Option.empty[BucketSpec]
      table.partitioning.map {
        case BucketTransform(numBuckets, col, sortCol) =>
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

  private def showTableLocation(table: Table, builder: StringBuilder): Unit = {
    Option(table.properties.get(TableCatalog.PROP_LOCATION))
      .map("LOCATION '" + escapeSingleQuotedString(_) + "'\n")
      .foreach(builder.append)
  }

  private def showTableProperties(
      table: Table,
      builder: StringBuilder,
      tableOptions: Map[String, String]): Unit = {

    val showProps = table.properties.asScala
      .filterKeys(key => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(key)
        && !key.startsWith(TableCatalog.OPTION_PREFIX)
        && !tableOptions.contains(key)
        && !key.equals(TableCatalog.PROP_EXTERNAL)
      )
    if (showProps.nonEmpty) {
      val props = showProps.toSeq.sortBy(_._1).map {
        case (key, value) =>
          s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= "TBLPROPERTIES "
      builder ++= concatByMultiLines(props)
    }
  }

  private def showTableComment(table: Table, builder: StringBuilder): Unit = {
    Option(table.properties.get(TableCatalog.PROP_COMMENT))
      .map("COMMENT '" + escapeSingleQuotedString(_) + "'\n")
      .foreach(builder.append)
  }

  private def concatByMultiLines(iter: Iterable[String]): String = {
    iter.mkString("(\n  ", ",\n  ", ")\n")
  }
}
