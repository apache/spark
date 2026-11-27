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

import org.apache.parquet.io.api.Converter
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT64}
import org.apache.parquet.schema.Type.Repetition.{OPTIONAL, REPEATED, REQUIRED}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.datasources.parquet.{HasParentContainerUpdater, NoopUpdater, ParentContainerUpdater, ParquetRowConverter, ParquetToSparkSchemaConverter, SparkToParquetSchemaConverter, StructFieldUpdater}
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types.{ArrayType, BinaryType, FileType, LongType, StringType, StructField, StructType}

/**
 * Unit tests for [[FileTypeParquetOps]], a struct-backed [[ParquetTypeOps]].
 *
 * A FILE is stored as a Parquet group whose fields are [[FileType.FIELDS]]. These tests cover the
 * write-path schema conversion, the struct schema exposed for column pruning, the type gates, the
 * registration in [[ParquetTypeOps.apply]] for every mode, and the row-read path, including the
 * SPARK-30338 copy guard. The read and write round trip is covered by `ParquetFileTypeSuite`.
 */
class FileTypeParquetOpsSuite extends SparkFunSuite with SQLHelper {

  private val ops = FileTypeParquetOps(FileType.external)

  private def newConverter(
      group: GroupType,
      updater: ParentContainerUpdater): Converter with HasParentContainerUpdater =
    ops.newConverter(
      group,
      updater,
      new ParquetToSparkSchemaConverter(),
      convertTz = None,
      datetimeRebaseSpec = RebaseSpec(LegacyBehaviorPolicy.CORRECTED),
      int96RebaseSpec = RebaseSpec(LegacyBehaviorPolicy.CORRECTED))

  test("registered in ParquetTypeOps.apply for every mode") {
    Seq(FileType(), FileType.external).foreach { ft =>
      ParquetTypeOps(ft) match {
        case Some(o: FileTypeParquetOps) => assert(o.fileType === ft)
        case other => fail(s"expected FileTypeParquetOps for $ft, got $other")
      }
    }
  }

  test("convertToParquetType builds a group of FileType.FIELDS in order") {
    val group = ops.convertToParquetType("file_data", OPTIONAL).asGroupType()
    assert(group.getName === "file_data")
    assert(group.getRepetition === OPTIONAL)
    assert(group.getLogicalTypeAnnotation === null)
    assert(group.getFieldCount === FileType.FIELDS.length)
    val names = (0 until group.getFieldCount).map(group.getType(_).getName)
    assert(names === Seq("uri", "offset", "size", "content_type", "checksum", "inline"))
    (0 until group.getFieldCount).foreach { i =>
      assert(group.getType(i).getRepetition === OPTIONAL)
    }

    def primName(idx: Int): PrimitiveTypeName =
      group.getType(idx).asPrimitiveType.getPrimitiveTypeName
    assert(primName(FileType.URI_INDEX) === BINARY)
    assert(primName(FileType.OFFSET_INDEX) === INT64)
    assert(primName(FileType.SIZE_INDEX) === INT64)
    assert(primName(FileType.CONTENT_TYPE_INDEX) === BINARY)
    assert(primName(FileType.CHECKSUM_INDEX) === BINARY)
    assert(primName(FileType.INLINE_INDEX) === BINARY)
    // The string fields are annotated as strings, inline is plain binary.
    Seq(FileType.URI_INDEX, FileType.CONTENT_TYPE_INDEX, FileType.CHECKSUM_INDEX).foreach { i =>
      assert(group.getType(i).getLogicalTypeAnnotation === LogicalTypeAnnotation.stringType())
    }
    assert(group.getType(FileType.INLINE_INDEX).getLogicalTypeAnnotation === null)
  }

  test("convertToParquetType honors the requested repetition") {
    assert(ops.convertToParquetType("f", REQUIRED).getRepetition === REQUIRED)
    assert(ops.convertToParquetType("f", OPTIONAL).getRepetition === OPTIONAL)
  }

  test("parquetStructSchema exposes FileType.asStruct for column pruning") {
    assert(ops.parquetStructSchema === Some(FileType.asStruct))
    val fields = ops.parquetStructSchema.get.fields
    assert(fields.map(_.name).toSeq ===
      Seq("uri", "offset", "size", "content_type", "checksum", "inline"))
    assert(fields.map(_.dataType).toSeq ===
      Seq(StringType, LongType, LongType, StringType, StringType, BinaryType))
  }

  test("mode does not affect the physical schema") {
    val external = FileTypeParquetOps(FileType.external).convertToParquetType("f", OPTIONAL)
    val unknown = FileTypeParquetOps(FileType()).convertToParquetType("f", OPTIONAL)
    assert(external === unknown)
  }

  test("the layout is that of a struct of the same fields") {
    val converter = new SparkToParquetSchemaConverter()
    val asFile = converter.convert(StructType(Seq(StructField("f", FileType.external))))
    val asStruct = converter.convert(StructType(Seq(StructField("f", FileType.asStruct))))
    assert(asFile === asStruct)
  }

  test("legacy two-level list of non-nullable FILE elements keeps the element REPEATED") {
    // The legacy two-level list encoding (writeLegacyFormat = true, containsNull = false) passes
    // a REPEATED repetition that does not derive from the nullability of the element.
    val schema = StructType(Seq(
      StructField("files", ArrayType(FileType.external, containsNull = false), nullable = true)))
    val converter = new SparkToParquetSchemaConverter(writeLegacyParquetFormat = true)
    val message = converter.convert(schema)

    val listGroup = message.getType(0).asGroupType()
    assert(listGroup.getName === "files")
    assert(listGroup.getLogicalTypeAnnotation === LogicalTypeAnnotation.listType())
    val element = listGroup.getType(0)
    assert(element.getRepetition === REPEATED,
      s"legacy two-level list element must be REPEATED, got ${element.getRepetition}")
    assert(element.asGroupType().getFieldCount === FileType.FIELDS.length)
  }

  test("the simple newConverter overload is unsupported for a struct-backed type") {
    intercept[UnsupportedOperationException] {
      ops.newConverter(ops.convertToParquetType("f", OPTIONAL), null)
    }
  }

  test("extended newConverter builds a row from the FILE fields") {
    val group = ops.convertToParquetType("file_data", OPTIONAL).asGroupType()
    val rowConverter = newConverter(group, NoopUpdater).asInstanceOf[ParquetRowConverter]

    val uriConverter = rowConverter.getConverter(FileType.URI_INDEX)
    assert(uriConverter.isPrimitive, "uri (STRING) must decode via a primitive converter")
    // Bound to a field of the row, not to the NoopUpdater of an unmatched field.
    assert(uriConverter.asInstanceOf[HasParentContainerUpdater].updater
      .isInstanceOf[StructFieldUpdater], "uri must be bound to a field of the row")
    assert(rowConverter.getConverter(FileType.INLINE_INDEX).isPrimitive,
      "inline (BINARY) must decode via a primitive converter")
  }

  test("extended newConverter copies rows under an array or map parent (SPARK-30338)") {
    // The converter reuses one mutable row across records, so under an array or map element
    // parent each produced row must be copied, or every element would alias the last row.
    val group = ops.convertToParquetType("file_data", OPTIONAL).asGroupType()

    val arrayUpdater = new CapturingUpdater
    val arrayConv = newConverter(group, arrayUpdater).asInstanceOf[ParquetRowConverter]
    arrayConv.start(); arrayConv.end()
    arrayConv.start(); arrayConv.end()
    assert(arrayUpdater.captured.length === 2)
    assert(!(arrayUpdater.captured(0) eq arrayUpdater.captured(1)),
      "an array or map parent must receive distinct copied rows")

    // Under a struct parent the row is not copied.
    val structUpdater = new CapturingUpdater with StructFieldUpdater
    val structConv = newConverter(group, structUpdater).asInstanceOf[ParquetRowConverter]
    structConv.start(); structConv.end()
    structConv.start(); structConv.end()
    assert(structUpdater.captured.length === 2)
    assert(structUpdater.captured(0) eq structUpdater.captured(1),
      "a struct parent should receive the same, uncopied row")
  }

  test("supportDataType is gated on the FILE type flag") {
    withSQLConf(SQLConf.FILE_TYPE_ENABLED.key -> "true") {
      assert(ops.supportDataType)
    }
    withSQLConf(SQLConf.FILE_TYPE_ENABLED.key -> "false") {
      assert(!ops.supportDataType)
    }
  }

  test("isBatchReadSupported is gated on the nested-column vectorization flag") {
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true") {
      assert(ops.isBatchReadSupported(SQLConf.get))
    }
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "false") {
      assert(!ops.isBatchReadSupported(SQLConf.get))
    }
  }

  // A ParentContainerUpdater that records every value handed to set().
  private class CapturingUpdater extends ParentContainerUpdater {
    // AnyRef, not Any, so that the tests can compare captured rows by reference (`eq`).
    val captured = scala.collection.mutable.ArrayBuffer.empty[AnyRef]
    override def set(value: Any): Unit = captured += value.asInstanceOf[AnyRef]
  }
}
