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
package org.apache.spark.sql.types

import java.util.Locale

import org.json4s.JsonAST.{JString, JValue}

import org.apache.spark.annotation.Unstable

/**
 * The data type representing a reference to a file, with metadata describing the file.
 *
 * In SQL the type is named `FILE`. A table column must be declared with the `EXTERNAL` keyword,
 * as in `CREATE TABLE t (f FILE EXTERNAL)`, while the signature of a function declares a bare
 * `FILE`. Its JSON representation is `"file external"` for
 * `FileType.external` and `"file unknown"` for `FileType()`, except that an external file in a
 * struct field is the plain `"file"`. A bare `"file"` is read as external inside a struct field
 * and as unknown elsewhere.
 *
 * FileType is planned to be physically stored as a struct with the following fields (see
 * `FileType.FIELDS`), but it deliberately does not extend [[StructType]]:
 *   - uri (STRING): the opaque URI to the file
 *   - offset (BIGINT): the start of the referenced byte range
 *   - size (BIGINT): the size of the file in bytes
 *   - content_type (STRING): the MIME/content type of the file
 *   - checksum (STRING): an algorithm-tagged integrity token for the file, e.g. "ETAG:{etag}"
 *   - inline (BINARY): the referenced bytes stored inline in the value
 *
 * FileType may be EXTERNAL (simple references, lifecycle independent of the table) or UNKNOWN
 * (mode not yet determined, such as the result of a cast or of a UDF). Mode is part of type
 * identity - files with different modes are distinct under `equals`.
 *
 * FileType is not comparable -- it cannot be used in a map key, join expressions, grouping
 * expressions, set operations, or as a partition/clustering column. Users can extract the uri
 * field for such operations. The physical representation is added by a later part.
 *
 * @param mode
 *   the mode for this FileType column
 * @since 4.4.0
 */
@Unstable
case class FileType(mode: FileTypeMode = FileTypeMode.UNKNOWN) extends DataType {

  override def defaultSize: Int = FileType.asStruct.defaultSize

  override def typeName: String = mode match {
    case FileTypeMode.EXTERNAL => "file external"
    case _ => "file"
  }

  // The JSON form carries the mode, as `"file external"` or `"file unknown"`. `StructField` writes
  // an external file as the plain `"file"`.
  override private[sql] def jsonValue: JValue =
    JString(s"file ${mode.name.toLowerCase(Locale.ROOT)}")

  /**
   * `acceptsType` ignores mode, so an expression that declares `inputTypes = Seq(FileType())`
   * accepts FileType(EXTERNAL) as well. `sameType` / `equals` still distinguish modes.
   */
  override private[sql] def acceptsType(other: DataType): Boolean = other.isInstanceOf[FileType]

  private[spark] override def asNullable: FileType = this
}

/**
 * @since 4.4.0
 */
@Unstable
object FileType {

  /** Creates an external FileType. */
  def external: FileType = FileType(FileTypeMode.EXTERNAL)

  /** Field index for the uri field. */
  val URI_INDEX = 0

  /** Field index for the offset field. */
  val OFFSET_INDEX = 1

  /** Field index for the size field. */
  val SIZE_INDEX = 2

  /** Field index for the content_type field. */
  val CONTENT_TYPE_INDEX = 3

  /** Field index for the checksum field. */
  val CHECKSUM_INDEX = 4

  /** Field index for the inline field. */
  val INLINE_INDEX = 5

  /**
   * The struct fields that define the physical layout of FileType. Positional consumers (the
   * `*_INDEX` constants) depend on this order; do not reorder without updating every layer.
   * `inline` is internal: it is not exposed as a user-facing field.
   */
  val FIELDS: Array[StructField] = Array(
    StructField("uri", StringType, nullable = true),
    StructField("offset", LongType, nullable = true),
    StructField("size", LongType, nullable = true),
    StructField("content_type", StringType, nullable = true),
    StructField("checksum", StringType, nullable = true),
    StructField("inline", BinaryType, nullable = true))

  /** The user-facing subset of `FIELDS`: everything except `inline`. */
  val PUBLIC_FIELDS: Array[StructField] = FIELDS.take(INLINE_INDEX)

  /** Returns the FileType fields as a StructType, for use in expressions. */
  val asStruct: StructType = StructType(FIELDS)
}
