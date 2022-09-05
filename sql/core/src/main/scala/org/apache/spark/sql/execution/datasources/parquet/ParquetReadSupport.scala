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

import java.time.ZoneId
import java.util
import java.util.{Locale, Map => JMap, UUID}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema._
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types._

/**
 * A Parquet [[ReadSupport]] implementation for reading Parquet records as Catalyst
 * [[InternalRow]]s.
 *
 * The API interface of [[ReadSupport]] is a little bit over complicated because of historical
 * reasons.  In older versions of parquet-mr (say 1.6.0rc3 and prior), [[ReadSupport]] need to be
 * instantiated and initialized twice on both driver side and executor side.  The [[init()]] method
 * is for driver side initialization, while [[prepareForRead()]] is for executor side.  However,
 * starting from parquet-mr 1.6.0, it's no longer the case, and [[ReadSupport]] is only instantiated
 * and initialized on executor side.  So, theoretically, now it's totally fine to combine these two
 * methods into a single initialization method.  The only reason (I could think of) to still have
 * them here is for parquet-mr API backwards-compatibility.
 *
 * Due to this reason, we no longer rely on [[ReadContext]] to pass requested schema from [[init()]]
 * to [[prepareForRead()]], but use a private `var` for simplicity.
 */
class ParquetReadSupport(
    val convertTz: Option[ZoneId],
    enableVectorizedReader: Boolean,
    datetimeRebaseSpec: RebaseSpec,
    int96RebaseSpec: RebaseSpec)
  extends ReadSupport[InternalRow] with Logging {
  private var catalystRequestedSchema: StructType = _

  def this() = {
    // We need a zero-arg constructor for SpecificParquetRecordReaderBase.  But that is only
    // used in the vectorized reader, where we get the convertTz/rebaseDateTime value directly,
    // and the values here are ignored.
    this(
      None,
      enableVectorizedReader = true,
      datetimeRebaseSpec = RebaseSpec(LegacyBehaviorPolicy.CORRECTED),
      int96RebaseSpec = RebaseSpec(LegacyBehaviorPolicy.LEGACY))
  }

  /**
   * Called on executor side before [[prepareForRead()]] and instantiating actual Parquet record
   * readers.  Responsible for figuring out Parquet requested schema used for column pruning.
   */
  override def init(context: InitContext): ReadContext = {
    val conf = context.getConfiguration
    catalystRequestedSchema = {
      val schemaString = conf.get(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA)
      assert(schemaString != null, "Parquet requested schema not set.")
      StructType.fromString(schemaString)
    }

    val parquetRequestedSchema = ParquetReadSupport.getRequestedSchema(
      context.getFileSchema, catalystRequestedSchema, conf, enableVectorizedReader)
    new ReadContext(parquetRequestedSchema, new util.HashMap[String, String]())
  }

  /**
   * Called on executor side after [[init()]], before instantiating actual Parquet record readers.
   * Responsible for instantiating [[RecordMaterializer]], which is used for converting Parquet
   * records to Catalyst [[InternalRow]]s.
   */
  override def prepareForRead(
      conf: Configuration,
      keyValueMetaData: JMap[String, String],
      fileSchema: MessageType,
      readContext: ReadContext): RecordMaterializer[InternalRow] = {
    val parquetRequestedSchema = readContext.getRequestedSchema
    new ParquetRecordMaterializer(
      parquetRequestedSchema,
      ParquetReadSupport.expandUDT(catalystRequestedSchema),
      new ParquetToSparkSchemaConverter(conf),
      convertTz,
      datetimeRebaseSpec,
      int96RebaseSpec)
  }
}

object ParquetReadSupport extends Logging {
  val SPARK_ROW_REQUESTED_SCHEMA = "org.apache.spark.sql.parquet.row.requested_schema"

  val SPARK_METADATA_KEY = "org.apache.spark.sql.parquet.row.metadata"

  def generateFakeColumnName: String = s"_fake_name_${UUID.randomUUID()}"

  def getRequestedSchema(
      parquetFileSchema: MessageType,
      catalystRequestedSchema: StructType,
      conf: Configuration,
      enableVectorizedReader: Boolean): MessageType = {
    val caseSensitive = conf.getBoolean(SQLConf.CASE_SENSITIVE.key,
      SQLConf.CASE_SENSITIVE.defaultValue.get)
    val schemaPruningEnabled = conf.getBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.defaultValue.get)
    val useFieldId = conf.getBoolean(SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key,
      SQLConf.PARQUET_FIELD_ID_READ_ENABLED.defaultValue.get)
    val timestampNTZEnabled = conf.getBoolean(SQLConf.PARQUET_TIMESTAMP_NTZ_ENABLED.key,
      SQLConf.PARQUET_TIMESTAMP_NTZ_ENABLED.defaultValue.get)
    val ignoreMissingIds = conf.getBoolean(SQLConf.IGNORE_MISSING_PARQUET_FIELD_ID.key,
      SQLConf.IGNORE_MISSING_PARQUET_FIELD_ID.defaultValue.get)

    if (!ignoreMissingIds &&
        !containsFieldIds(parquetFileSchema) &&
        ParquetUtils.hasFieldIds(catalystRequestedSchema)) {
      throw new RuntimeException(
        "Spark read schema expects field Ids, " +
          "but Parquet file schema doesn't contain any field Ids.\n" +
        "Please remove the field ids from Spark schema or ignore missing ids by " +
          s"setting `${SQLConf.IGNORE_MISSING_PARQUET_FIELD_ID.key} = true`\n" +
        s"""
           |Spark read schema:
           |${catalystRequestedSchema.prettyJson}
           |
           |Parquet file schema:
           |${parquetFileSchema.toString}
           |""".stripMargin)
    }
    val parquetClippedSchema = ParquetReadSupport.clipParquetSchema(parquetFileSchema,
      catalystRequestedSchema, caseSensitive, useFieldId, timestampNTZEnabled)

    // We pass two schema to ParquetRecordMaterializer:
    // - parquetRequestedSchema: the schema of the file data we want to read
    // - catalystRequestedSchema: the schema of the rows we want to return
    // The reader is responsible for reconciling the differences between the two.
    val parquetRequestedSchema = if (schemaPruningEnabled && !enableVectorizedReader) {
      // Parquet-MR reader requires that parquetRequestedSchema include only those fields present
      // in the underlying parquetFileSchema. Therefore, we intersect the parquetClippedSchema
      // with the parquetFileSchema
      ParquetReadSupport.intersectParquetGroups(parquetClippedSchema, parquetFileSchema)
        .map(groupType => new MessageType(groupType.getName, groupType.getFields))
        .getOrElse(ParquetSchemaConverter.EMPTY_MESSAGE)
    } else {
      // Spark's vectorized reader only support atomic types currently. It also skip fields
      // in parquetRequestedSchema which are not present in the file.
      parquetClippedSchema
    }

    logDebug(
      s"""Going to read the following fields from the Parquet file with the following schema:
         |Parquet file schema:
         |$parquetFileSchema
         |Parquet clipped schema:
         |$parquetClippedSchema
         |Parquet requested schema:
         |$parquetRequestedSchema
         |Catalyst requested schema:
         |${catalystRequestedSchema.treeString}
       """.stripMargin)

    parquetRequestedSchema
  }

  /**
   * Tailors `parquetSchema` according to `catalystSchema` by removing column paths don't exist
   * in `catalystSchema`, and adding those only exist in `catalystSchema`.
   */
  def clipParquetSchema(
      parquetSchema: MessageType,
      catalystSchema: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean,
      timestampNTZEnabled: Boolean): MessageType = {
    val clippedParquetFields = clipParquetGroupFields(
      parquetSchema.asGroupType(), catalystSchema, caseSensitive, useFieldId, timestampNTZEnabled)
    if (clippedParquetFields.isEmpty) {
      ParquetSchemaConverter.EMPTY_MESSAGE
    } else {
      Types
        .buildMessage()
        .addFields(clippedParquetFields: _*)
        .named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)
    }
  }

  private def clipParquetType(
      parquetType: Type,
      catalystType: DataType,
      caseSensitive: Boolean,
      useFieldId: Boolean,
      timestampNTZEnabled: Boolean): Type = {
    val newParquetType = catalystType match {
      case t: ArrayType if !isPrimitiveCatalystType(t.elementType) =>
        // Only clips array types with nested type as element type.
        clipParquetListType(parquetType.asGroupType(), t.elementType, caseSensitive, useFieldId,
          timestampNTZEnabled)

      case t: MapType
        if !isPrimitiveCatalystType(t.keyType) ||
           !isPrimitiveCatalystType(t.valueType) =>
        // Only clips map types with nested key type or value type
        clipParquetMapType(
          parquetType.asGroupType(), t.keyType, t.valueType, caseSensitive, useFieldId,
          timestampNTZEnabled)

      case t: StructType =>
        clipParquetGroup(
          parquetType.asGroupType(), t, caseSensitive, useFieldId, timestampNTZEnabled)

      case _ =>
        // UDTs and primitive types are not clipped.  For UDTs, a clipped version might not be able
        // to be mapped to desired user-space types.  So UDTs shouldn't participate schema merging.
        parquetType
    }

    if (useFieldId && parquetType.getId != null) {
      newParquetType.withId(parquetType.getId.intValue())
    } else {
      newParquetType
    }
  }

  /**
   * Whether a Catalyst [[DataType]] is primitive.  Primitive [[DataType]] is not equivalent to
   * [[AtomicType]].  For example, [[CalendarIntervalType]] is primitive, but it's not an
   * [[AtomicType]].
   */
  private def isPrimitiveCatalystType(dataType: DataType): Boolean = {
    dataType match {
      case _: ArrayType | _: MapType | _: StructType => false
      case _ => true
    }
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[ArrayType]].  The element type
   * of the [[ArrayType]] should also be a nested type, namely an [[ArrayType]], a [[MapType]], or a
   * [[StructType]].
   */
  private def clipParquetListType(
      parquetList: GroupType,
      elementType: DataType,
      caseSensitive: Boolean,
      useFieldId: Boolean,
      timestampNTZEnabled: Boolean): Type = {
    // Precondition of this method, should only be called for lists with nested element types.
    assert(!isPrimitiveCatalystType(elementType))

    // Unannotated repeated group should be interpreted as required list of required element, so
    // list element type is just the group itself.  Clip it.
    if (parquetList.getLogicalTypeAnnotation == null &&
      parquetList.isRepetition(Repetition.REPEATED)) {
      clipParquetType(parquetList, elementType, caseSensitive, useFieldId, timestampNTZEnabled)
    } else {
      assert(
        parquetList.getLogicalTypeAnnotation.isInstanceOf[ListLogicalTypeAnnotation],
        "Invalid Parquet schema. " +
          "Logical type annotation of annotated Parquet lists must be ListLogicalTypeAnnotation: " +
          parquetList.toString)

      assert(
        parquetList.getFieldCount == 1 && parquetList.getType(0).isRepetition(Repetition.REPEATED),
        "Invalid Parquet schema. " +
          "LIST-annotated group should only have exactly one repeated field: " +
          parquetList)

      // Precondition of this method, should only be called for lists with nested element types.
      assert(!parquetList.getType(0).isPrimitive)

      val repeatedGroup = parquetList.getType(0).asGroupType()

      // If the repeated field is a group with multiple fields, or the repeated field is a group
      // with one field and is named either "array" or uses the LIST-annotated group's name with
      // "_tuple" appended then the repeated type is the element type and elements are required.
      // Build a new LIST-annotated group with clipped `repeatedGroup` as element type and the
      // only field.
      if (
        repeatedGroup.getFieldCount > 1 ||
        repeatedGroup.getName == "array" ||
        repeatedGroup.getName == parquetList.getName + "_tuple"
      ) {
        Types
          .buildGroup(parquetList.getRepetition)
          .as(LogicalTypeAnnotation.listType())
          .addField(
            clipParquetType(
              repeatedGroup, elementType, caseSensitive, useFieldId, timestampNTZEnabled))
          .named(parquetList.getName)
      } else {
        val newRepeatedGroup = Types
          .repeatedGroup()
          .addField(
            clipParquetType(
              repeatedGroup.getType(0), elementType, caseSensitive, useFieldId,
              timestampNTZEnabled))
          .named(repeatedGroup.getName)

        val newElementType = if (useFieldId && repeatedGroup.getId != null) {
          newRepeatedGroup.withId(repeatedGroup.getId.intValue())
        } else {
          newRepeatedGroup
        }

        // Otherwise, the repeated field's type is the element type with the repeated field's
        // repetition.
        Types
          .buildGroup(parquetList.getRepetition)
          .as(LogicalTypeAnnotation.listType())
          .addField(newElementType)
          .named(parquetList.getName)
      }
    }
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[MapType]].  Either key type or
   * value type of the [[MapType]] must be a nested type, namely an [[ArrayType]], a [[MapType]], or
   * a [[StructType]].
   */
  private def clipParquetMapType(
      parquetMap: GroupType,
      keyType: DataType,
      valueType: DataType,
      caseSensitive: Boolean,
      useFieldId: Boolean,
      timestampNTZEnabled: Boolean): GroupType = {
    // Precondition of this method, only handles maps with nested key types or value types.
    assert(!isPrimitiveCatalystType(keyType) || !isPrimitiveCatalystType(valueType))

    val repeatedGroup = parquetMap.getType(0).asGroupType()
    val parquetKeyType = repeatedGroup.getType(0)
    val parquetValueType = repeatedGroup.getType(1)

    val clippedRepeatedGroup = {
      val newRepeatedGroup = Types
        .repeatedGroup()
        .as(repeatedGroup.getLogicalTypeAnnotation)
        .addField(
          clipParquetType(
            parquetKeyType, keyType, caseSensitive, useFieldId, timestampNTZEnabled))
        .addField(
          clipParquetType(
            parquetValueType, valueType, caseSensitive, useFieldId, timestampNTZEnabled))
        .named(repeatedGroup.getName)
      if (useFieldId && repeatedGroup.getId != null) {
        newRepeatedGroup.withId(repeatedGroup.getId.intValue())
      } else {
        newRepeatedGroup
      }
    }

    Types
      .buildGroup(parquetMap.getRepetition)
      .as(parquetMap.getLogicalTypeAnnotation)
      .addField(clippedRepeatedGroup)
      .named(parquetMap.getName)
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[StructType]].
   *
   * @return A clipped [[GroupType]], which has at least one field.
   * @note Parquet doesn't allow creating empty [[GroupType]] instances except for empty
   *       [[MessageType]].  Because it's legal to construct an empty requested schema for column
   *       pruning.
   */
  private def clipParquetGroup(
      parquetRecord: GroupType,
      structType: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean,
      timestampNTZEnabled: Boolean): GroupType = {
    val clippedParquetFields =
      clipParquetGroupFields(parquetRecord, structType, caseSensitive, useFieldId,
        timestampNTZEnabled)
    Types
      .buildGroup(parquetRecord.getRepetition)
      .as(parquetRecord.getLogicalTypeAnnotation)
      .addFields(clippedParquetFields: _*)
      .named(parquetRecord.getName)
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[StructType]].
   *
   * @return A list of clipped [[GroupType]] fields, which can be empty.
   */
  private def clipParquetGroupFields(
      parquetRecord: GroupType,
      structType: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean,
      timestampNTZEnabled: Boolean): Seq[Type] = {
    val toParquet = new SparkToParquetSchemaConverter(
      writeLegacyParquetFormat = false,
      useFieldId = useFieldId,
      timestampNTZEnabled = timestampNTZEnabled)
    lazy val caseSensitiveParquetFieldMap =
        parquetRecord.getFields.asScala.map(f => f.getName -> f).toMap
    lazy val caseInsensitiveParquetFieldMap =
        parquetRecord.getFields.asScala.groupBy(_.getName.toLowerCase(Locale.ROOT))
    lazy val idToParquetFieldMap =
        parquetRecord.getFields.asScala.filter(_.getId != null).groupBy(f => f.getId.intValue())

    def matchCaseSensitiveField(f: StructField): Type = {
      caseSensitiveParquetFieldMap
          .get(f.name)
          .map(clipParquetType(_, f.dataType, caseSensitive, useFieldId, timestampNTZEnabled))
          .getOrElse(toParquet.convertField(f))
    }

    def matchCaseInsensitiveField(f: StructField): Type = {
      // Do case-insensitive resolution only if in case-insensitive mode
      caseInsensitiveParquetFieldMap
          .get(f.name.toLowerCase(Locale.ROOT))
          .map { parquetTypes =>
            if (parquetTypes.size > 1) {
              // Need to fail if there is ambiguity, i.e. more than one field is matched
              val parquetTypesString = parquetTypes.map(_.getName).mkString("[", ", ", "]")
              throw QueryExecutionErrors.foundDuplicateFieldInCaseInsensitiveModeError(
                f.name, parquetTypesString)
            } else {
              clipParquetType(
                parquetTypes.head, f.dataType, caseSensitive, useFieldId, timestampNTZEnabled)
            }
          }.getOrElse(toParquet.convertField(f))
    }

    def matchIdField(f: StructField): Type = {
      val fieldId = ParquetUtils.getFieldId(f)
      idToParquetFieldMap
        .get(fieldId)
        .map { parquetTypes =>
          if (parquetTypes.size > 1) {
            // Need to fail if there is ambiguity, i.e. more than one field is matched
            val parquetTypesString = parquetTypes.map(_.getName).mkString("[", ", ", "]")
            throw QueryExecutionErrors.foundDuplicateFieldInFieldIdLookupModeError(
              fieldId, parquetTypesString)
          } else {
            clipParquetType(
              parquetTypes.head, f.dataType, caseSensitive, useFieldId, timestampNTZEnabled)
          }
        }.getOrElse {
          // When there is no ID match, we use a fake name to avoid a name match by accident
          // We need this name to be unique as well, otherwise there will be type conflicts
          toParquet.convertField(f.copy(name = generateFakeColumnName))
        }
    }

    val shouldMatchById = useFieldId && ParquetUtils.hasFieldIds(structType)
    structType.map { f =>
      if (shouldMatchById && ParquetUtils.hasFieldId(f)) {
        matchIdField(f)
      } else if (caseSensitive) {
        matchCaseSensitiveField(f)
      } else {
        matchCaseInsensitiveField(f)
      }
    }
  }

  /**
   * Computes the structural intersection between two Parquet group types.
   * This is used to create a requestedSchema for ReadContext of Parquet-MR reader.
   * Parquet-MR reader does not support the nested field access to non-existent field
   * while parquet library does support to read the non-existent field by regular field access.
   */
  private def intersectParquetGroups(
      groupType1: GroupType, groupType2: GroupType): Option[GroupType] = {
    val fields =
      groupType1.getFields.asScala
        .filter(field => groupType2.containsField(field.getName))
        .flatMap {
          case field1: GroupType =>
            val field2 = groupType2.getType(field1.getName)
            if (field2.isPrimitive) {
              None
            } else {
              intersectParquetGroups(field1, field2.asGroupType)
            }
          case field1 => Some(field1)
        }

    if (fields.nonEmpty) {
      Some(groupType1.withNewFields(fields.asJava))
    } else {
      None
    }
  }

  def expandUDT(schema: StructType): StructType = {
    def expand(dataType: DataType): DataType = {
      dataType match {
        case t: ArrayType =>
          t.copy(elementType = expand(t.elementType))

        case t: MapType =>
          t.copy(
            keyType = expand(t.keyType),
            valueType = expand(t.valueType))

        case t: StructType =>
          val expandedFields = t.fields.map(f => f.copy(dataType = expand(f.dataType)))
          t.copy(fields = expandedFields)

        case t: UserDefinedType[_] =>
          t.sqlType

        case t =>
          t
      }
    }

    expand(schema).asInstanceOf[StructType]
  }

  /**
   * Whether the parquet schema contains any field IDs.
   */
  def containsFieldIds(schema: Type): Boolean = schema match {
    case p: PrimitiveType => p.getId != null
    // We don't require all fields to have IDs, so we use `exists` here.
    case g: GroupType => g.getId != null || g.getFields.asScala.exists(containsFieldIds)
  }
}
