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

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.util.{escapeSingleQuotedString, quoteIdentifier}
import org.apache.spark.sql.catalyst.util.StringUtils.StringConcat

/**
 * A field inside a StructType.
 * @param name The name of this field.
 * @param dataType The data type of this field.
 * @param nullable Indicates if values of this field can be `null` values.
 * @param metadata The metadata of this field. The metadata should be preserved during
 *                 transformation if the content of the column is not modified, e.g, in selection.
 *
 * @since 1.3.0
 */
@Stable
case class StructField(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    metadata: Metadata = Metadata.empty) {

  /** No-arg constructor for kryo. */
  protected def this() = this(null, null)

  private[sql] def buildFormattedString(
      prefix: String,
      stringConcat: StringConcat,
      maxDepth: Int): Unit = {
    if (maxDepth > 0) {
      stringConcat.append(s"$prefix-- $name: ${dataType.typeName} (nullable = $nullable)\n")
      DataType.buildFormattedString(dataType, s"$prefix    |", stringConcat, maxDepth)
    }
  }

  // override the default toString to be compatible with legacy parquet files.
  override def toString: String = s"StructField($name,$dataType,$nullable)"

  private[sql] def jsonValue: JValue = {
    ("name" -> name) ~
      ("type" -> dataType.jsonValue) ~
      ("nullable" -> nullable) ~
      ("metadata" -> metadata.jsonValue)
  }

  /**
   * Updates the StructField with a new comment value.
   */
  def withComment(comment: String): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(metadata)
      .putString("comment", comment)
      .build()
    copy(metadata = newMetadata)
  }

  /**
   * Return the comment of this StructField.
   */
  def getComment(): Option[String] = {
    if (metadata.contains("comment")) Option(metadata.getString("comment")) else None
  }

  private def getDDLComment = getComment()
    .map(escapeSingleQuotedString)
    .map(" COMMENT '" + _ + "'")
    .getOrElse("")

  /**
   * Returns a string containing a schema in SQL format. For example the following value:
   * `StructField("eventId", IntegerType)` will be converted to `eventId`: INT.
   */
  private[sql] def sql = s"${quoteIdentifier(name)}: ${dataType.sql}$getDDLComment"

  /**
   * Returns a string containing a schema in DDL format. For example, the following value:
   * `StructField("eventId", IntegerType)` will be converted to `eventId` INT.
   *
   * @since 2.4.0
   */
  def toDDL: String = s"${quoteIdentifier(name)} ${dataType.sql}$getDDLComment"
}
