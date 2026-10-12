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

import org.apache.spark.annotation.Unstable

/**
 * The data type representing a reference to an external file, such as a PDF, image, audio or
 * video. A value holds the location of the file and a small amount of metadata describing it
 * (size, content type and checksum); the bytes of the file are never stored in the value.
 *
 * In SQL the type is named `FILE`, and the `EXTERNAL` keyword is required when declaring a
 * column, as in `CREATE TABLE t (f FILE EXTERNAL)`. Its JSON representation is `"file"`. OSS has
 * a single kind of file, which behaves like an external file: the lifecycle of the referenced
 * file is independent of the table.
 *
 * The type is planned to be physically represented as a struct (see [[FileType.FIELDS]]) but
 * deliberately does not extend [[StructType]]. It is planned to have no ordering, so it is not
 * to be usable in the key of a map, a join, a grouping expression, or as a partition or
 * clustering column. Of these, this type currently only guarantees the schema-level pieces (DDL
 * and JSON); the physical representation and most of the restrictions are added by later parts.
 *
 * @since 4.4.0
 */
@Unstable
class FileType private () extends DataType {

  override def defaultSize: Int = FileType.asStruct.defaultSize

  override def typeName: String = "file"

  override def equals(obj: Any): Boolean = obj.isInstanceOf[FileType]

  override def hashCode(): Int = classOf[FileType].getSimpleName.hashCode

  private[spark] override def asNullable: FileType = this
}

/**
 * @since 4.4.0
 */
@Unstable
case object FileType extends FileType {

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
    StructField("inline", BinaryType, nullable = true)
  )

  /** The user-facing subset of `FIELDS`: everything except `inline`. */
  val PUBLIC_FIELDS: Array[StructField] = FIELDS.take(INLINE_INDEX)

  /** Returns the FileType fields as a StructType, for use in expressions. */
  val asStruct: StructType = StructType(FIELDS)
}
