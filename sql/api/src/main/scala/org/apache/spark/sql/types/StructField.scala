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

import scala.collection.mutable

import org.json4s.{JObject, JString}
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

import org.apache.spark.SparkException
import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.collation.CollationFactory
import org.apache.spark.sql.catalyst.util.{QuotingUtils, StringConcat}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumnsUtils.{CURRENT_DEFAULT_COLUMN_METADATA_KEY, EXISTS_DEFAULT_COLUMN_METADATA_KEY}
import org.apache.spark.util.SparkSchemaUtils

/**
 * A field inside a StructType.
 * @param name
 *   The name of this field.
 * @param dataType
 *   The data type of this field.
 * @param nullable
 *   Indicates if values of this field can be `null` values.
 * @param metadata
 *   The metadata of this field. The metadata should be preserved during transformation if the
 *   content of the column is not modified, e.g, in selection.
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
      stringConcat.append(
        s"$prefix-- ${SparkSchemaUtils.escapeMetaCharacters(name)}: " +
          s"${dataType.typeName} (nullable = $nullable)\n")
      DataType.buildFormattedString(dataType, s"$prefix    |", stringConcat, maxDepth)
    }
  }

  // override the default toString to be compatible with legacy parquet files.
  override def toString: String = s"StructField($name,$dataType,$nullable)"

  private[sql] def jsonValue: JValue = {
    ("name" -> name) ~
      ("type" -> dataType.jsonValue) ~
      ("nullable" -> nullable) ~
      ("metadata" -> metadataJson)
  }

  private def metadataJson: JValue = {
    val metadataJsonValue = metadata.jsonValue
    metadataJsonValue match {
      case JObject(fields) if collationMetadata.nonEmpty =>
        val collationFields = collationMetadata.map(kv => kv._1 -> JString(kv._2)).toList
        JObject(fields :+ (DataType.COLLATIONS_METADATA_KEY -> JObject(collationFields)))

      case _ => metadataJsonValue
    }
  }

  /** Map of field path to collation name. */
  private lazy val collationMetadata: Map[String, String] = {
    val fieldToCollationMap = mutable.Map[String, String]()

    def visitRecursively(dt: DataType, path: String): Unit = dt match {
      case at: ArrayType =>
        processDataType(at.elementType, path + ".element")

      case mt: MapType =>
        processDataType(mt.keyType, path + ".key")
        processDataType(mt.valueType, path + ".value")

      case st: StringType if isCollatedString(st) =>
        fieldToCollationMap(path) = schemaCollationValue(st)

      case _ =>
    }

    def processDataType(dt: DataType, path: String): Unit = {
      if (isCollatedString(dt)) {
        fieldToCollationMap(path) = schemaCollationValue(dt)
      } else {
        visitRecursively(dt, path)
      }
    }

    visitRecursively(dataType, name)
    fieldToCollationMap.toMap
  }

  private def isCollatedString(dt: DataType): Boolean = dt match {
    case st: StringType => !st.isUTF8BinaryCollation
    case _ => false
  }

  private def schemaCollationValue(dt: DataType): String = dt match {
    case st: StringType =>
      val collation = CollationFactory.fetchCollation(st.collationId)
      collation.identifier().toStringWithoutVersion()
    case _ =>
      throw SparkException.internalError(s"Unexpected data type $dt")
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

  /**
   * Updates the StructField with a new current default value.
   */
  def withCurrentDefaultValue(value: String): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(metadata)
      .putString(CURRENT_DEFAULT_COLUMN_METADATA_KEY, value)
      .build()
    copy(metadata = newMetadata)
  }

  /**
   * Clears the StructField of its current default value, if any.
   */
  def clearCurrentDefaultValue(): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(metadata)
      .remove(CURRENT_DEFAULT_COLUMN_METADATA_KEY)
      .build()
    copy(metadata = newMetadata)
  }

  /**
   * Return the current default value of this StructField.
   */
  def getCurrentDefaultValue(): Option[String] = {
    if (metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
      Option(metadata.getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY))
    } else {
      None
    }
  }

  /**
   * Updates the StructField with a new existence default value.
   */
  def withExistenceDefaultValue(value: String): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(metadata)
      .putString(EXISTS_DEFAULT_COLUMN_METADATA_KEY, value)
      .build()
    copy(metadata = newMetadata)
  }

  /**
   * Return the existence default value of this StructField.
   */
  private[sql] def getExistenceDefaultValue(): Option[String] = {
    if (metadata.contains(EXISTS_DEFAULT_COLUMN_METADATA_KEY)) {
      Option(metadata.getString(EXISTS_DEFAULT_COLUMN_METADATA_KEY))
    } else {
      None
    }
  }

  private def getDDLDefault = getCurrentDefaultValue()
    .map(" DEFAULT " + _)
    .getOrElse("")

  private def getDDLComment = getComment()
    .map(QuotingUtils.escapeSingleQuotedString)
    .map(" COMMENT '" + _ + "'")
    .getOrElse("")

  private lazy val nullDDL = if (nullable) "" else " NOT NULL"

  /**
   * Returns a string containing a schema in SQL format. For example the following value:
   * `StructField("eventId", IntegerType)` will be converted to `eventId`: INT.
   */
  private[sql] def sql =
    s"${QuotingUtils.quoteIfNeeded(name)}: ${dataType.sql}$nullDDL$getDDLComment"

  /**
   * Returns a string containing a schema in DDL format. For example, the following value:
   * `StructField("eventId", IntegerType, false)` will be converted to `eventId` INT NOT NULL.
   * `StructField("eventId", IntegerType, true)` will be converted to `eventId` INT.
   * @since 2.4.0
   */
  def toDDL: String = {
    s"${QuotingUtils.quoteIfNeeded(name)} ${dataType.sql}$nullDDL" +
      s"$getDDLDefault$getDDLComment"
  }
}
