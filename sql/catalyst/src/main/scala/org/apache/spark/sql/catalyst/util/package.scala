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

package org.apache.spark.sql.catalyst

import java.io._
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.io.ByteStreams

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.catalog.MetadataColumn
import org.apache.spark.sql.types.{MetadataBuilder, NumericType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SparkErrorUtils, Utils}

package object util extends Logging {

  /** Silences output to stderr or stdout for the duration of f */
  def quietly[A](f: => A): A = {
    val origErr = System.err
    val origOut = System.out
    try {
      System.setErr(new PrintStream((_: Int) => {}))
      System.setOut(new PrintStream((_: Int) => {}))

      f
    } finally {
      System.setErr(origErr)
      System.setOut(origOut)
    }
  }

  def fileToString(file: File, encoding: Charset = UTF_8): String = {
    val inStream = new FileInputStream(file)
    try {
      new String(ByteStreams.toByteArray(inStream), encoding)
    } finally {
      inStream.close()
    }
  }

  def resourceToBytes(
      resource: String,
      classLoader: ClassLoader = Utils.getSparkClassLoader): Array[Byte] = {
    val inStream = classLoader.getResourceAsStream(resource)
    try {
      ByteStreams.toByteArray(inStream)
    } finally {
      inStream.close()
    }
  }

  def resourceToString(
      resource: String,
      encoding: String = UTF_8.name(),
      classLoader: ClassLoader = Utils.getSparkClassLoader): String = {
    new String(resourceToBytes(resource, classLoader), encoding)
  }

  def stringToFile(file: File, str: String): File = {
    Utils.tryWithResource(new PrintWriter(file)) { out =>
      out.write(str)
    }
    file
  }

  def sideBySide(left: String, right: String): Seq[String] = {
    SparkStringUtils.sideBySide(left, right)
  }

  def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
    SparkStringUtils.sideBySide(left, right)
  }

  def stackTraceToString(t: Throwable): String = SparkErrorUtils.stackTraceToString(t)

  // Replaces attributes, string literals, complex type extractors with their pretty form so that
  // generated column names don't contain back-ticks or double-quotes.
  def usePrettyExpression(e: Expression): Expression = e transform {
    case a: Attribute => new PrettyAttribute(a)
    case Literal(s: UTF8String, StringType) => PrettyAttribute(s.toString, StringType)
    case Literal(v, t: NumericType) if v != null => PrettyAttribute(v.toString, t)
    case Literal(null, dataType) => PrettyAttribute("NULL", dataType)
    case e: GetStructField =>
      val name = e.name.getOrElse(e.childSchema(e.ordinal).name)
      PrettyAttribute(usePrettyExpression(e.child).sql + "." + name, e.dataType)
    case e: GetArrayStructFields =>
      PrettyAttribute(s"${usePrettyExpression(e.child)}.${e.field.name}", e.dataType)
    case r: InheritAnalysisRules =>
      PrettyAttribute(r.makeSQLString(r.parameters.map(toPrettySQL)), r.dataType)
    case c: Cast if c.getTagValue(Cast.USER_SPECIFIED_CAST).isEmpty =>
      PrettyAttribute(usePrettyExpression(c.child).sql, c.dataType)
    case p: PythonFuncExpression => PrettyPythonUDF(p.name, p.dataType, p.children)
  }

  def quoteIdentifier(name: String): String = {
    QuotingUtils.quoteIdentifier(name)
  }

  def quoteNameParts(name: Seq[String]): String = {
    QuotingUtils.quoteNameParts(name)
  }

  def quoteIfNeeded(part: String): String = {
    QuotingUtils.quoteIfNeeded(part)
  }

  def toPrettySQL(e: Expression): String = usePrettyExpression(e).sql

  def escapeSingleQuotedString(str: String): String = {
    QuotingUtils.escapeSingleQuotedString(str)
  }

  /**
   * Format a sequence with semantics similar to calling .mkString(). Any elements beyond
   * maxNumToStringFields will be dropped and replaced by a "... N more fields" placeholder.
   *
   * @return the trimmed and formatted string.
   */
  def truncatedString[T](
      seq: Seq[T],
      start: String,
      sep: String,
      end: String,
      maxFields: Int): String = {
    SparkStringUtils.truncatedString(seq, start, sep, end, maxFields)
  }

  /** Shorthand for calling truncatedString() without start or end strings. */
  def truncatedString[T](seq: Seq[T], sep: String, maxFields: Int): String = {
    SparkStringUtils.truncatedString(seq, "", sep, "", maxFields)
  }

  val METADATA_COL_ATTR_KEY = "__metadata_col"

  /**
   * If set, this metadata column can only be accessed with qualifiers, e.g. `qualifiers.col` or
   * `qualifiers.*`. If not set, metadata columns cannot be accessed via star.
   */
  val QUALIFIED_ACCESS_ONLY = "__qualified_access_only"

  implicit class MetadataColumnHelper(attr: Attribute) {

    def isMetadataCol: Boolean = MetadataAttribute.isValid(attr.metadata)

    def qualifiedAccessOnly: Boolean = attr.isMetadataCol &&
      attr.metadata.contains(QUALIFIED_ACCESS_ONLY) &&
      attr.metadata.getBoolean(QUALIFIED_ACCESS_ONLY)

    def markAsQualifiedAccessOnly(): Attribute = attr.withMetadata(
      new MetadataBuilder()
        .withMetadata(attr.metadata)
        .putString(METADATA_COL_ATTR_KEY, attr.name)
        .putBoolean(QUALIFIED_ACCESS_ONLY, true)
        .build()
    )

    def markAsAllowAnyAccess(): Attribute = {
      if (qualifiedAccessOnly) {
        attr.withMetadata(
          new MetadataBuilder()
            .withMetadata(attr.metadata)
            .remove(QUALIFIED_ACCESS_ONLY)
            .build()
        )
      } else {
        attr
      }
    }
  }

  val AUTO_GENERATED_ALIAS = "__autoGeneratedAlias"

  val INTERNAL_METADATA_KEYS = Seq(
    AUTO_GENERATED_ALIAS,
    METADATA_COL_ATTR_KEY,
    QUALIFIED_ACCESS_ONLY,
    FileSourceMetadataAttribute.FILE_SOURCE_METADATA_COL_ATTR_KEY,
    FileSourceConstantMetadataStructField.FILE_SOURCE_CONSTANT_METADATA_COL_ATTR_KEY,
    FileSourceGeneratedMetadataStructField.FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY,
    MetadataColumn.PRESERVE_ON_DELETE,
    MetadataColumn.PRESERVE_ON_UPDATE,
    MetadataColumn.PRESERVE_ON_REINSERT
  )

  def removeInternalMetadata(schema: StructType): StructType = {
    StructType(schema.map { field =>
      var builder = new MetadataBuilder().withMetadata(field.metadata)
      INTERNAL_METADATA_KEYS.foreach { key =>
        builder = builder.remove(key)
      }
      field.copy(metadata = builder.build())
    })
  }
}
