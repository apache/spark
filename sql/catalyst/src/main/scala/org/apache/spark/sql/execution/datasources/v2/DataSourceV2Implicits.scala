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

import org.apache.spark.sql.catalyst.analysis.{PartitionSpec, ResolvedPartitionSpec, UnresolvedPartitionSpec}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.connector.catalog.{MetadataColumn, SupportsAtomicPartitionManagement, SupportsDelete, SupportsPartitionManagement, SupportsRead, SupportsWrite, Table, TableCapability, TruncatableTable}
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object DataSourceV2Implicits {
  implicit class TableHelper(table: Table) {
    def asReadable: SupportsRead = {
      table match {
        case support: SupportsRead =>
          support
        case _ =>
          throw QueryCompilationErrors.tableDoesNotSupportReadsError(table)
      }
    }

    def asWritable: SupportsWrite = {
      table match {
        case support: SupportsWrite =>
          support
        case _ =>
          throw QueryCompilationErrors.tableDoesNotSupportWritesError(table)
      }
    }

    def asDeletable: SupportsDelete = {
      table match {
        case support: SupportsDelete =>
          support
        case _ =>
          throw QueryCompilationErrors.tableDoesNotSupportDeletesError(table)
      }
    }

    def asTruncatable: TruncatableTable = {
      table match {
        case t: TruncatableTable => t
        case _ =>
          throw QueryCompilationErrors.tableDoesNotSupportTruncatesError(table)
      }
    }

    def asPartitionable: SupportsPartitionManagement = {
      table match {
        case support: SupportsPartitionManagement =>
          support
        case _ =>
          throw QueryCompilationErrors.tableDoesNotSupportPartitionManagementError(table)
      }
    }

    def asAtomicPartitionable: SupportsAtomicPartitionManagement = {
      table match {
        case support: SupportsAtomicPartitionManagement =>
          support
        case _ =>
          throw QueryCompilationErrors.tableDoesNotSupportAtomicPartitionManagementError(table)
      }
    }

    def asRowLevelOperationTable: RowLevelOperationTable = {
      table match {
        case rowLevelOperationTable: RowLevelOperationTable =>
          rowLevelOperationTable
        case _ =>
          throw QueryCompilationErrors.tableIsNotRowLevelOperationTableError(table)
      }
    }

    def supports(capability: TableCapability): Boolean = table.capabilities.contains(capability)

    def supportsAny(capabilities: TableCapability*): Boolean = capabilities.exists(supports)
  }

  implicit class MetadataColumnsHelper(metadata: Array[MetadataColumn]) {
    def asStruct: StructType = {
      val fields = metadata.map { metaCol =>
        val fieldMeta = new MetadataBuilder().putBoolean(METADATA_COL_ATTR_KEY, true).build()
        val field = StructField(metaCol.name, metaCol.dataType, metaCol.isNullable, fieldMeta)
        Option(metaCol.comment).map(field.withComment).getOrElse(field)
      }
      StructType(fields)
    }

    def toAttributes: Seq[AttributeReference] = asStruct.toAttributes
  }

  implicit class OptionsHelper(options: Map[String, String]) {
    def asOptions: CaseInsensitiveStringMap = {
      new CaseInsensitiveStringMap(options.asJava)
    }
  }

  implicit class PartitionSpecsHelper(partSpecs: Seq[PartitionSpec]) {
    def asUnresolvedPartitionSpecs: Seq[UnresolvedPartitionSpec] =
      partSpecs.map(_.asInstanceOf[UnresolvedPartitionSpec])

    def asResolvedPartitionSpecs: Seq[ResolvedPartitionSpec] =
      partSpecs.map(_.asInstanceOf[ResolvedPartitionSpec])
  }
}
