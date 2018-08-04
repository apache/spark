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

package org.apache.spark.sql.avro

import scala.collection.JavaConverters._

import org.apache.avro.{LogicalType, LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type._

import org.apache.spark.sql.internal.SQLConf.AvroOutputTimestampType
import org.apache.spark.sql.types._

/**
 * This object contains method that are used to convert sparkSQL schemas to avro schemas and vice
 * versa.
 */
object SchemaConverters {
  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
   * This function takes an avro schema and returns a sql schema.
   */
  def toSqlType(avroSchema: Schema): SchemaType = {
    avroSchema.getType match {
      case INT => SchemaType(IntegerType, nullable = false)
      case STRING => SchemaType(StringType, nullable = false)
      case BOOLEAN => SchemaType(BooleanType, nullable = false)
      case BYTES => SchemaType(BinaryType, nullable = false)
      case DOUBLE => SchemaType(DoubleType, nullable = false)
      case FLOAT => SchemaType(FloatType, nullable = false)
      case LONG => avroSchema.getLogicalType match {
        case _: TimestampMillis | _: TimestampMicros => SchemaType(TimestampType, nullable = false)
        case _ => SchemaType(LongType, nullable = false)
      }
      case FIXED => SchemaType(BinaryType, nullable = false)
      case ENUM => SchemaType(StringType, nullable = false)

      case RECORD =>
        val fields = avroSchema.getFields.asScala.map { f =>
          val schemaType = toSqlType(f.schema())
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }

        SchemaType(StructType(fields), nullable = false)

      case ARRAY =>
        val schemaType = toSqlType(avroSchema.getElementType)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false)

      case MAP =>
        val schemaType = toSqlType(avroSchema.getValueType)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false)

      case UNION =>
        if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            toSqlType(remainingUnionTypes.head).copy(nullable = true)
          } else {
            toSqlType(Schema.createUnion(remainingUnionTypes.asJava)).copy(nullable = true)
          }
        } else avroSchema.getTypes.asScala.map(_.getType) match {
          case Seq(t1) =>
            toSqlType(avroSchema.getTypes.get(0))
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            SchemaType(LongType, nullable = false)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false)
          case _ =>
            // Convert complex unions to struct types where field names are member0, member1, etc.
            // This is consistent with the behavior when converting between Avro and Parquet.
            val fields = avroSchema.getTypes.asScala.zipWithIndex.map {
              case (s, i) =>
                val schemaType = toSqlType(s)
                // All fields are nullable because only one of them is set at a time
                StructField(s"member$i", schemaType.dataType, nullable = true)
            }

            SchemaType(StructType(fields), nullable = false)
        }

      case other => throw new IncompatibleSchemaException(s"Unsupported type $other")
    }
  }

  def toAvroType(
      catalystType: DataType,
      nullable: Boolean = false,
      recordName: String = "topLevelRecord",
      prevNameSpace: String = "",
      outputTimestampType: AvroOutputTimestampType.Value = AvroOutputTimestampType.TIMESTAMP_MICROS)
    : Schema = {
    val builder = if (nullable) {
      SchemaBuilder.builder().nullable()
    } else {
      SchemaBuilder.builder()
    }

    catalystType match {
      case BooleanType => builder.booleanType()
      case ByteType | ShortType | IntegerType => builder.intType()
      case LongType => builder.longType()
      case DateType => builder.longType()
      case TimestampType =>
        val timestampType = outputTimestampType match {
          case AvroOutputTimestampType.TIMESTAMP_MILLIS => LogicalTypes.timestampMillis()
          case AvroOutputTimestampType.TIMESTAMP_MICROS => LogicalTypes.timestampMicros()
          case other =>
            throw new IncompatibleSchemaException(s"Unexpected output timestamp type $other.")
        }
        builder.longBuilder().prop(LogicalType.LOGICAL_TYPE_PROP, timestampType.getName).endLong()

      case FloatType => builder.floatType()
      case DoubleType => builder.doubleType()
      case _: DecimalType | StringType => builder.stringType()
      case BinaryType => builder.bytesType()
      case ArrayType(et, containsNull) =>
        builder.array()
          .items(toAvroType(et, containsNull, recordName, prevNameSpace, outputTimestampType))
      case MapType(StringType, vt, valueContainsNull) =>
        builder.map()
          .values(toAvroType(vt, valueContainsNull, recordName, prevNameSpace, outputTimestampType))
      case st: StructType =>
        val nameSpace = prevNameSpace match {
          case "" => recordName
          case _ => s"$prevNameSpace.$recordName"
        }

        val fieldsAssembler = builder.record(recordName).namespace(nameSpace).fields()
        st.foreach { f =>
          val fieldAvroType =
            toAvroType(f.dataType, f.nullable, f.name, nameSpace, outputTimestampType)
          fieldsAssembler.name(f.name).`type`(fieldAvroType).noDefault()
        }
        fieldsAssembler.endRecord()

      // This should never happen.
      case other => throw new IncompatibleSchemaException(s"Unexpected type $other.")
    }
  }
}

class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)
