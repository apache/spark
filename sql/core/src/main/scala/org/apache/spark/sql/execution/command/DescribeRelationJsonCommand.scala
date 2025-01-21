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

package org.apache.spark.sql.execution.command

import java.time.ZoneId

import scala.collection.mutable

import org.json4s._
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{ResolvedPersistentView, ResolvedTable, ResolvedTempView}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.{
  quoteIfNeeded,
  DateFormatter,
  DateTimeUtils,
  Iso8601TimestampFormatter,
  LegacyDateFormats
}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.PartitioningUtils

/**
 * The command for `DESCRIBE ... AS JSON`.
 */
case class DescribeRelationJsonCommand(
    child: LogicalPlan,
    partitionSpec: TablePartitionSpec,
    isExtended: Boolean,
    override val output: Seq[Attribute] = Seq(
      AttributeReference(
        "json_metadata",
        StringType,
        nullable = false,
        new MetadataBuilder().putString("comment", "JSON metadata of the table").build())()
    )) extends UnaryRunnableCommand {
  private lazy val timestampFormatter = new Iso8601TimestampFormatter(
    pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'",
    zoneId = ZoneId.of("UTC"),
    locale = DateFormatter.defaultLocale,
    legacyFormat = LegacyDateFormats.LENIENT_SIMPLE_DATE_FORMAT,
    isParsing = true
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val jsonMap = mutable.LinkedHashMap[String, JValue]()
    child match {
      case v: ResolvedTempView =>
        if (partitionSpec.nonEmpty) {
          throw QueryCompilationErrors.descPartitionNotAllowedOnTempView(v.identifier.name())
        }
        describeIdentifier(Seq("system", "session", v.identifier.name()), jsonMap)
        describeColsJson(v.metadata.schema, jsonMap)
        describeFormattedTableInfoJson(v.metadata, jsonMap)

      case v: ResolvedPersistentView =>
        if (partitionSpec.nonEmpty) {
          throw QueryCompilationErrors.descPartitionNotAllowedOnView(v.identifier.name())
        }
        describeIdentifier(v.identifier.toQualifiedNameParts(v.catalog), jsonMap)
        describeColsJson(v.metadata.schema, jsonMap)
        describeFormattedTableInfoJson(v.metadata, jsonMap)

      case ResolvedTable(catalog, identifier, V1Table(metadata), _) =>
        describeIdentifier(identifier.toQualifiedNameParts(catalog), jsonMap)
        val schema = if (metadata.schema.isEmpty) {
          // In older versions of Spark,
          // the table schema can be empty and should be inferred at runtime.
          sparkSession.table(metadata.identifier).schema
        } else {
          metadata.schema
        }
        describeColsJson(schema, jsonMap)
        describeClusteringInfoJson(metadata, jsonMap)
        if (partitionSpec.nonEmpty) {
          // Outputs the partition-specific info for the DDL command:
          // "DESCRIBE [EXTENDED|FORMATTED] table_name PARTITION (partitionVal*)"
          describePartitionInfoJson(
            sparkSession, sparkSession.sessionState.catalog, metadata, jsonMap)
        } else {
          describeFormattedTableInfoJson(metadata, jsonMap)
        }

      case _ => throw QueryCompilationErrors.describeAsJsonNotSupportedForV2TablesError()
    }

    Seq(Row(compact(render(JObject(jsonMap.toList)))))
  }

  private def addKeyValueToMap(
      key: String,
      value: JValue,
      jsonMap: mutable.LinkedHashMap[String, JValue]): Unit = {
    // Rename some JSON keys that are pre-named in describe table implementation
    val renames = Map(
      "inputformat" -> "input_format",
      "outputformat" -> "output_format"
    )

    val timestampKeys = Set("created_time", "last_access")

    val normalizedKey = key.toLowerCase().replace(" ", "_")
    val renamedKey = renames.getOrElse(normalizedKey, normalizedKey)

    if (!jsonMap.contains(renamedKey) && !excludedKeys.contains(renamedKey)) {
      val formattedValue = if (timestampKeys.contains(renamedKey)) {
        value match {
          case JLong(timestamp) =>
            JString(timestampFormatter.format(DateTimeUtils.millisToMicros(timestamp)))
          case _ => value
        }
      } else {
        value
      }
      jsonMap += renamedKey -> formattedValue
    }
  }

  private def describeIdentifier(
      ident: Seq[String],
      jsonMap: mutable.LinkedHashMap[String, JValue]): Unit = {
    addKeyValueToMap("table_name", JString(ident.last), jsonMap)
    addKeyValueToMap("catalog_name", JString(ident.head), jsonMap)
    val namespace = ident.init.tail
    addKeyValueToMap("namespace", JArray(namespace.map(JString).toList), jsonMap)
    if (namespace.nonEmpty) {
      addKeyValueToMap("schema_name", JString(namespace.last), jsonMap)
    }
  }

  /**
   * Util to recursively form JSON string representation of data type, used for DESCRIBE AS JSON.
   * Differs from `json` in DataType.scala by providing additional fields for some types.
   */
  private def jsonType(dataType: DataType): JValue = {
    dataType match {
      case arrayType: ArrayType =>
        JObject(
          "name" -> JString("array"),
          "element_type" -> jsonType(arrayType.elementType),
          "element_nullable" -> JBool(arrayType.containsNull)
        )

      case mapType: MapType =>
        JObject(
          "name" -> JString("map"),
          "key_type" -> jsonType(mapType.keyType),
          "value_type" -> jsonType(mapType.valueType),
          "value_nullable" -> JBool(mapType.valueContainsNull)
        )

      case structType: StructType =>
        val fieldsJson = structType.fields.map { field =>
          val baseJson = List(
            "name" -> JString(field.name),
            "type" -> jsonType(field.dataType),
            "nullable" -> JBool(field.nullable)
          )
          val commentJson = field.getComment().map(comment => "comment" -> JString(comment)).toList
          val defaultJson =
            field.getCurrentDefaultValue().map(default => "default" -> JString(default)).toList

          JObject(baseJson ++ commentJson ++ defaultJson: _*)
        }.toList

        JObject(
          "name" -> JString("struct"),
          "fields" -> JArray(fieldsJson)
        )

      case decimalType: DecimalType =>
        JObject(
          "name" -> JString("decimal"),
          "precision" -> JInt(decimalType.precision),
          "scale" -> JInt(decimalType.scale)
        )

      case varcharType: VarcharType =>
        JObject(
          "name" -> JString("varchar"),
          "length" -> JInt(varcharType.length)
        )

      case charType: CharType =>
        JObject(
          "name" -> JString("char"),
          "length" -> JInt(charType.length)
        )

      // Only override TimestampType; TimestampType_NTZ type is already timestamp_ntz
      case _: TimestampType =>
        JObject("name" -> JString("timestamp_ltz"))

      case yearMonthIntervalType: YearMonthIntervalType =>
        def getFieldName(field: Byte): String = YearMonthIntervalType.fieldToString(field)

        JObject(
          "name" -> JString("interval"),
          "start_unit" -> JString(getFieldName(yearMonthIntervalType.startField)),
          "end_unit" -> JString(getFieldName(yearMonthIntervalType.endField))
        )

      case dayTimeIntervalType: DayTimeIntervalType =>
        def getFieldName(field: Byte): String = DayTimeIntervalType.fieldToString(field)

        JObject(
          "name" -> JString("interval"),
          "start_unit" -> JString(getFieldName(dayTimeIntervalType.startField)),
          "end_unit" -> JString(getFieldName(dayTimeIntervalType.endField))
        )

      case _ =>
        JObject("name" -> JString(dataType.simpleString))
    }
  }

  private def describeColsJson(
      schema: StructType,
      jsonMap: mutable.LinkedHashMap[String, JValue]): Unit = {
    val columnsJson = jsonType(StructType(schema.fields))
      .asInstanceOf[JObject].find(_.isInstanceOf[JArray]).get
    addKeyValueToMap("columns", columnsJson, jsonMap)
  }

  private def describeClusteringInfoJson(
      table: CatalogTable, jsonMap: mutable.LinkedHashMap[String, JValue]): Unit = {
    table.clusterBySpec.foreach { clusterBySpec =>
      val clusteringColumnsJson: JValue = JArray(
        clusterBySpec.columnNames.map { fieldNames =>
          val nestedFieldOpt = table.schema.findNestedField(fieldNames.fieldNames.toIndexedSeq)
          assert(nestedFieldOpt.isDefined,
            "The clustering column " +
              s"${fieldNames.fieldNames.map(quoteIfNeeded).mkString(".")} " +
              s"was not found in the table schema ${table.schema.catalogString}."
          )
          val (path, field) = nestedFieldOpt.get
          JObject(
            "name" -> JString((path :+ field.name).map(quoteIfNeeded).mkString(".")),
            "type" -> jsonType(field.dataType),
            "comment" -> field.getComment().map(JString).getOrElse(JNull)
          )
        }.toList
      )
      addKeyValueToMap("clustering_information", clusteringColumnsJson, jsonMap)
    }
  }

  private def describeFormattedTableInfoJson(
      table: CatalogTable, jsonMap: mutable.LinkedHashMap[String, JValue]): Unit = {
    table.bucketSpec match {
      case Some(spec) =>
        spec.toJsonLinkedHashMap.foreach { case (key, value) =>
          addKeyValueToMap(key, value, jsonMap)
        }
      case _ =>
    }
    table.storage.toJsonLinkedHashMap.foreach { case (key, value) =>
      addKeyValueToMap(key, value, jsonMap)
    }

    val filteredTableInfo = table.toJsonLinkedHashMap

    filteredTableInfo.map { case (key, value) =>
      addKeyValueToMap(key, value, jsonMap)
    }
  }

  private def describePartitionInfoJson(
      spark: SparkSession,
      catalog: SessionCatalog,
      metadata: CatalogTable,
      jsonMap: mutable.LinkedHashMap[String, JValue]): Unit = {
    if (metadata.tableType == CatalogTableType.VIEW) {
      throw QueryCompilationErrors.descPartitionNotAllowedOnView(metadata.identifier.identifier)
    }

    DDLUtils.verifyPartitionProviderIsHive(spark, metadata, "DESC PARTITION")
    val normalizedPartSpec = PartitioningUtils.normalizePartitionSpec(
      partitionSpec,
      metadata.partitionSchema,
      metadata.identifier.quotedString,
      spark.sessionState.conf.resolver)
    val partition = catalog.getPartition(metadata.identifier, normalizedPartSpec)

    // First add partition details to jsonMap.
    // `addKeyValueToMap` only adds unique keys, so this ensures the
    // more detailed partition information is added
    // in the case of duplicated key names (e.g. storage_information).
    partition.toJsonLinkedHashMap.foreach { case (key, value) =>
      addKeyValueToMap(key, value, jsonMap)
    }

    metadata.toJsonLinkedHashMap.foreach { case (key, value) =>
      addKeyValueToMap(key, value, jsonMap)
    }

    metadata.bucketSpec match {
      case Some(spec) =>
        spec.toJsonLinkedHashMap.foreach { case (key, value) =>
          addKeyValueToMap(key, value, jsonMap)
        }
      case _ =>
    }
    metadata.storage.toJsonLinkedHashMap.foreach { case (key, value) =>
      addKeyValueToMap(key, value, jsonMap)
    }
  }

  // Already added to jsonMap in DescribeTableJsonCommand
  private val excludedKeys = Set("catalog", "schema", "database", "table")

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}
