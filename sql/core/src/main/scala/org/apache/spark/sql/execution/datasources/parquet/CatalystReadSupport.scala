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

package org.apache.spark.sql.execution.datasources.parquet

import java.util.{Map => JMap}

import scala.collection.JavaConversions.{iterableAsScalaIterable, mapAsJavaMap, mapAsScalaMap}

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

private[parquet] class CatalystReadSupport extends ReadSupport[InternalRow] with Logging {
  // Called after `init()` when initializing Parquet record reader.
  override def prepareForRead(
      conf: Configuration,
      keyValueMetaData: JMap[String, String],
      fileSchema: MessageType,
      readContext: ReadContext): RecordMaterializer[InternalRow] = {
    log.debug(s"Preparing for read Parquet file with message type: $fileSchema")

    val toCatalyst = new CatalystSchemaConverter(conf)
    val parquetRequestedSchema = readContext.getRequestedSchema

    val catalystRequestedSchema =
      Option(readContext.getReadSupportMetadata).map(_.toMap).flatMap { metadata =>
        metadata
          // First tries to read requested schema, which may result from projections
          .get(CatalystReadSupport.SPARK_ROW_REQUESTED_SCHEMA)
          // If not available, tries to read Catalyst schema from file metadata.  It's only
          // available if the target file is written by Spark SQL.
          .orElse(metadata.get(CatalystReadSupport.SPARK_METADATA_KEY))
      }.map(StructType.fromString).getOrElse {
        logInfo("Catalyst schema not available, falling back to Parquet schema")
        toCatalyst.convert(parquetRequestedSchema)
      }

    logInfo {
      s"""Going to read the following fields from the Parquet file:
         |
         |Parquet form:
         |$parquetRequestedSchema
         |Catalyst form:
         |$catalystRequestedSchema
       """.stripMargin
    }

    new CatalystRecordMaterializer(parquetRequestedSchema, catalystRequestedSchema)
  }

  // Called before `prepareForRead()` when initializing Parquet record reader.
  override def init(context: InitContext): ReadContext = {
    val conf = context.getConfiguration

    // If the target file was written by Spark SQL, we should be able to find a serialized Catalyst
    // schema of this file from its metadata.
    val maybeRowSchema = Option(conf.get(RowWriteSupport.SPARK_ROW_SCHEMA))

    // Optional schema of requested columns, in the form of a string serialized from a Catalyst
    // `StructType` containing all requested columns.
    val maybeRequestedSchema = Option(conf.get(CatalystReadSupport.SPARK_ROW_REQUESTED_SCHEMA))

    // Below we construct a Parquet schema containing all requested columns.  This schema tells
    // Parquet which columns to read.
    //
    // If `maybeRequestedSchema` is defined, we assemble an equivalent Parquet schema.  Otherwise,
    // we have to fallback to the full file schema which contains all columns in the file.
    // Obviously this may waste IO bandwidth since it may read more columns than requested.
    //
    // Two things to note:
    //
    // 1. It's possible that some requested columns don't exist in the target Parquet file.  For
    //    example, in the case of schema merging, the globally merged schema may contain extra
    //    columns gathered from other Parquet files.  These columns will be simply filled with nulls
    //    when actually reading the target Parquet file.
    //
    // 2. When `maybeRequestedSchema` is available, we can't simply convert the Catalyst schema to
    //    Parquet schema using `CatalystSchemaConverter`, because the mapping is not unique due to
    //    non-standard behaviors of some Parquet libraries/tools.  For example, a Parquet file
    //    containing a single integer array field `f1` may have the following legacy 2-level
    //    structure:
    //
    //      message root {
    //        optional group f1 (LIST) {
    //          required INT32 element;
    //        }
    //      }
    //
    //    while `CatalystSchemaConverter` may generate a standard 3-level structure:
    //
    //      message root {
    //        optional group f1 (LIST) {
    //          repeated group list {
    //            required INT32 element;
    //          }
    //        }
    //      }
    //
    //    Apparently, we can't use the 2nd schema to read the target Parquet file as they have
    //    different physical structures.
    val parquetRequestedSchema =
      maybeRequestedSchema.fold(context.getFileSchema) { schemaString =>
        val toParquet = new CatalystSchemaConverter(conf)
        val fileSchema = context.getFileSchema.asGroupType()
        val fileFieldNames = fileSchema.getFields.map(_.getName).toSet

        StructType
          // Deserializes the Catalyst schema of requested columns
          .fromString(schemaString)
          .map { field =>
            if (fileFieldNames.contains(field.name)) {
              // If the field exists in the target Parquet file, extracts the field type from the
              // full file schema and makes a single-field Parquet schema
              new MessageType("root", fileSchema.getType(field.name))
            } else {
              // Otherwise, just resorts to `CatalystSchemaConverter`
              toParquet.convert(StructType(Array(field)))
            }
          }
          // Merges all single-field Parquet schemas to form a complete schema for all requested
          // columns.  Note that it's possible that no columns are requested at all (e.g., count
          // some partition column of a partitioned Parquet table). That's why `fold` is used here
          // and always fallback to an empty Parquet schema.
          .fold(new MessageType("root")) {
            _ union _
          }
      }

    val metadata =
      Map.empty[String, String] ++
        maybeRequestedSchema.map(CatalystReadSupport.SPARK_ROW_REQUESTED_SCHEMA -> _) ++
        maybeRowSchema.map(RowWriteSupport.SPARK_ROW_SCHEMA -> _)

    new ReadContext(parquetRequestedSchema, metadata)
  }
}

private[parquet] object CatalystReadSupport {
  val SPARK_ROW_REQUESTED_SCHEMA = "org.apache.spark.sql.parquet.row.requested_schema"

  val SPARK_METADATA_KEY = "org.apache.spark.sql.parquet.row.metadata"
}
