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
package org.apache.spark.sql.execution.datasources.parquet.types.ops

import java.time.ZoneId

import org.apache.parquet.io.api.{Converter, RecordConsumer}
import org.apache.parquet.schema.{Type, Types}
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.datasources.parquet.{HasParentContainerUpdater, ParentContainerUpdater, ParquetRowConverter, ParquetToSparkSchemaConverter, SparkToParquetSchemaConverter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, FileType, StructField, StructType}

/**
 * Parquet operations for [[FileType]].
 *
 * A FILE is stored as a Parquet group whose fields are [[FileType.FIELDS]] (uri, offset, size,
 * content_type, checksum, inline). Every Parquet concern delegates to the struct support over
 * that field list, so the layout on disk is that of a plain struct of the same fields, which any
 * Parquet reader can read:
 *
 *   - Schema conversion: the group of `FileType.FIELDS`, built with the write-path converter.
 *   - Value write: the struct value writer, through the `makeFieldWriter` callback.
 *   - Row-based read: a nested [[ParquetRowConverter]] over `FileType.asStruct`, built with the
 *     shared [[ParquetRowConverter.newStructConverter]], so that a FILE nested in an array or map
 *     is copied like a struct.
 *   - Column pruning: `FileType.asStruct` as `parquetStructSchema`, so that
 *     `ParquetReadSupport` clips the group to the FILE fields.
 *   - Type gate: Parquet support requires the FILE type to be enabled.
 *
 * The mode does not change the layout, so it is not used here. The read direction in
 * `ParquetToSparkSchemaConverter.convertField` keeps the requested FileType, with its mode, and
 * the vectorized reader assembles a FILE like a struct (`ParquetColumnVector`), as its row
 * representation is a struct (see `TypeOps.rowRepresentation`).
 *
 * The group carries no FILE logical type annotation yet.
 *
 * @param fileType the FileType
 * @since 4.5.0
 */
case class FileTypeParquetOps(fileType: FileType) extends StructBackedParquetTypeOps {

  override def structSchema: StructType = FileType.asStruct

  // ==================== Schema Conversion ====================

  /**
   * The Parquet type of a FILE: a group with the given repetition, named after the column, whose
   * fields are the converted `FileType.FIELDS`. This is the layout of a struct of the same fields.
   *
   * The FILE fields are STRING, BIGINT and BINARY, whose conversion does not depend on the
   * legacy-format, timestamp or field-id options, and they carry no field id. So a shared
   * converter with default options converts them as any configured converter would. The field id
   * of the FILE column itself is applied by the enclosing `convertField`.
   */
  override def convertToParquetType(
      fieldName: String, repetition: Repetition, inShredded: Boolean): Type =
    FileType.FIELDS
      .foldLeft(Types.buildGroup(repetition)) { (builder, subField: StructField) =>
        builder.addField(FileTypeParquetOps.writeSchemaConverter.convertField(subField, inShredded))
      }
      .named(fieldName)

  // ==================== Value Write ====================

  /**
   * Writes a FILE with the struct value writer: `makeFieldWriter` goes back through
   * `ParquetWriteSupport.makeWriter`, which writes the group and its `FileType.FIELDS`.
   */
  override def makeWriter(
      recordConsumer: () => RecordConsumer,
      makeFieldWriter: DataType => (SpecializedGetters, Int) => Unit
  ): (SpecializedGetters, Int) => Unit =
    makeFieldWriter(FileType.asStruct)

  // ==================== Row-Based Read ====================

  override def newConverter(
      parquetType: Type,
      updater: ParentContainerUpdater,
      schemaConverter: ParquetToSparkSchemaConverter,
      convertTz: Option[ZoneId],
      datetimeRebaseSpec: RebaseSpec,
      int96RebaseSpec: RebaseSpec): Converter with HasParentContainerUpdater =
    ParquetRowConverter.newStructConverter(
      schemaConverter,
      parquetType.asGroupType(),
      FileType.asStruct,
      convertTz,
      datetimeRebaseSpec,
      int96RebaseSpec,
      updater)

  // ==================== Type Gates ====================

  override def supportDataType: Boolean = SQLConf.get.isFileTypeEnabled

  // ==================== Vectorized Read ====================

  // A FILE is read as a nested group of atomic fields, which the built-in vectorized updaters
  // read, so it needs no updater of its own. This is the StructType case of
  // `ParquetUtils.isBatchReadSupported` for the fixed, all-atomic FILE fields.
  override def isBatchReadSupported(sqlConf: SQLConf): Boolean =
    sqlConf.parquetVectorizedReaderNestedColumnEnabled
}

private[ops] object FileTypeParquetOps {

  /** The write-path schema converter of [[FileTypeParquetOps.convertToParquetType]]. */
  private val writeSchemaConverter = new SparkToParquetSchemaConverter()
}
