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
package org.apache.spark.sql.catalyst.types.ops

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, Literal, MutableAny, MutableValue}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalStructType}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{FileRef, FileRefInternals, FileType, ObjectType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Server-side (catalyst) operations for FileType.
 *
 * This class implements the TypeOps methods for the FILE data type, providing:
 *   - Physical type: PhysicalStructType of [[FileType.FIELDS]], so that the row infrastructure
 *     handles a value as a struct (see [[TypeOps.rowRepresentationType]])
 *   - Literals: the default is a reference with an empty uri and no other field set
 *   - External conversion: [[FileRef]] <-> InternalRow
 *
 * It also inherits client-side operations from FileTypeApiOps:
 *   - String formatting (the canonical JSON of a value)
 *   - Row encoding (FileRefEncoder)
 *
 * INTERNAL REPRESENTATION:
 *   - An InternalRow with the fields of [[FileType.FIELDS]], at the `*_INDEX` positions of
 *     [[FileType]]: uri (UTF8String), offset (Long), size (Long), content_type (UTF8String),
 *     checksum (UTF8String) and inline (Array[Byte]), each nullable
 *   - External type: [[FileRef]], which carries every field, including inline
 *
 * @param ft
 *   The FileType with its mode
 * @since 4.5.0
 */
case class FileTypeOps(override val ft: FileType) extends FileTypeApiOps(ft) with TypeOps {

  // ==================== String Formatting ====================

  override def format(v: Any): String = {
    val row = v.asInstanceOf[InternalRow]
    FileTypeApiOps.formatJson(
      FileTypeOps.getString(row, FileType.URI_INDEX),
      FileTypeOps.getLong(row, FileType.OFFSET_INDEX),
      FileTypeOps.getLong(row, FileType.SIZE_INDEX),
      FileTypeOps.getString(row, FileType.CONTENT_TYPE_INDEX),
      FileTypeOps.getString(row, FileType.CHECKSUM_INDEX),
      FileTypeOps.getBinary(row, FileType.INLINE_INDEX))
  }

  // ==================== Physical Type Representation ====================

  override def getPhysicalType: PhysicalDataType = PhysicalStructType(FileType.FIELDS)

  override def getJavaClass: Class[_] = classOf[InternalRow]

  override def getMutableValue: MutableValue = new MutableAny

  override def getRowWriter(ordinal: Int): (InternalRow, Any) => Unit =
    (input, v) => input.update(ordinal, v)

  // ==================== Literal Creation ====================

  override def getDefaultLiteral: Literal = Literal.create(
    new GenericInternalRow(
      Array[Any](UTF8String.fromString(""), null, null, null, null, null)),
    ft)

  // A struct-backed value has no Java literal: code generation reaches a FILE literal through
  // its row representation, as for a struct.
  override def getJavaLiteral(v: Any): String =
    throw SparkException.internalError(s"FILE has no Java literal: ${format(v)}")

  // ==================== External Type Conversion ====================

  override def toCatalystImpl(scalaValue: Any): Any = {
    val f = scalaValue.asInstanceOf[FileRef]
    new GenericInternalRow(Array[Any](
      UTF8String.fromString(f.uri),
      if (f.offset == null) null else f.offset.longValue(),
      if (f.size == null) null else f.size.longValue(),
      UTF8String.fromString(f.contentType),
      UTF8String.fromString(f.checksum),
      FileRefInternals.inline(f)))
  }

  override def toScala(catalystValue: Any): Any = {
    if (catalystValue == null) {
      null
    } else {
      val row = catalystValue.asInstanceOf[InternalRow]
      FileRefInternals.create(
        FileTypeOps.getString(row, FileType.URI_INDEX),
        FileTypeOps.getLong(row, FileType.OFFSET_INDEX),
        FileTypeOps.getLong(row, FileType.SIZE_INDEX),
        FileTypeOps.getString(row, FileType.CONTENT_TYPE_INDEX),
        FileTypeOps.getString(row, FileType.CHECKSUM_INDEX),
        FileTypeOps.getBinary(row, FileType.INLINE_INDEX))
    }
  }

  override def toScalaImpl(row: InternalRow, column: Int): Any =
    toScala(row.getStruct(column, FileType.FIELDS.length))

  // ==================== Optional Operations ====================

  // Building a FILE serializer/deserializer requires the FILE type to be enabled. The framework
  // dispatch at the head of Serializer/DeserializerBuildHelper routes FileRefEncoder through
  // these methods, so this is the single gate on the encoder path. Throw rather than return None
  // so the dispatch surfaces UNSUPPORTED_FILE_TYPE instead of falling through to the default
  // match, which does not handle FileRefEncoder.
  private def checkFileTypeEnabled(): Unit = {
    if (!SQLConf.get.isFileTypeEnabled) {
      throw QueryCompilationErrors.unsupportedFileTypeError()
    }
  }

  override def createSerializer(input: Expression): Option[Expression] = {
    checkFileTypeEnabled()
    Some(StaticInvoke(
      FileTypeOps.getClass,
      ft,
      "fromFileRef",
      input :: Nil,
      returnNullable = false))
  }

  override def createDeserializer(path: Expression): Option[Expression] = {
    checkFileTypeEnabled()
    Some(StaticInvoke(
      FileTypeOps.getClass,
      ObjectType(classOf[FileRef]),
      "toFileRef",
      path :: Nil,
      returnNullable = false))
  }
}

object FileTypeOps {
  // The conversions do not depend on the mode.
  private val ops = FileTypeOps(FileType())

  /** Converts a non-null FileRef to its InternalRow, for the serializer of FileRefEncoder. */
  def fromFileRef(f: FileRef): InternalRow = ops.toCatalystImpl(f).asInstanceOf[InternalRow]

  /** Converts a non-null InternalRow to its FileRef, for the deserializer of FileRefEncoder. */
  def toFileRef(row: InternalRow): FileRef = ops.toScala(row).asInstanceOf[FileRef]

  private def getString(row: InternalRow, ordinal: Int): String =
    if (row.isNullAt(ordinal)) null else row.getUTF8String(ordinal).toString

  private def getLong(row: InternalRow, ordinal: Int): java.lang.Long =
    if (row.isNullAt(ordinal)) null else java.lang.Long.valueOf(row.getLong(ordinal))

  private def getBinary(row: InternalRow, ordinal: Int): Array[Byte] =
    if (row.isNullAt(ordinal)) null else row.getBinary(ordinal)
}
