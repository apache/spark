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
package org.apache.spark.sql.protobuf.utils

import scala.collection.JavaConverters._

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.protobuf.ScalaReflectionLock
import org.apache.spark.sql.types._

@DeveloperApi
object SchemaConverters {

  /**
   * Internal wrapper for SQL data type and nullability.
   *
   * @since 3.4.0
   */
  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
   * Converts an Protobuf schema to a corresponding Spark SQL schema.
   *
   * @since 3.4.0
   */
  def toSqlType(
      descriptor: Descriptor,
      protobufOptions: ProtobufOptions = ProtobufOptions(Map.empty)): SchemaType = {
    toSqlTypeHelper(descriptor, protobufOptions)
  }

  def toSqlTypeHelper(
      descriptor: Descriptor,
      protobufOptions: ProtobufOptions): SchemaType = ScalaReflectionLock.synchronized {
    SchemaType(
      StructType(descriptor.getFields.asScala.flatMap(
        structFieldFor(_, Map.empty, Map.empty, protobufOptions: ProtobufOptions)).toArray),
      nullable = true)
  }

  def structFieldFor(
      fd: FieldDescriptor,
      existingRecordNames: Map[String, Int],
      existingRecordTypes: Map[String, Int],
      protobufOptions: ProtobufOptions): Option[StructField] = {
    import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
    val dataType = fd.getJavaType match {
      case INT => Some(IntegerType)
      case LONG => Some(LongType)
      case FLOAT => Some(FloatType)
      case DOUBLE => Some(DoubleType)
      case BOOLEAN => Some(BooleanType)
      case STRING => Some(StringType)
      case BYTE_STRING => Some(BinaryType)
      case ENUM => Some(StringType)
      case MESSAGE
        if (fd.getMessageType.getName == "Duration" &&
          fd.getMessageType.getFields.size() == 2 &&
          fd.getMessageType.getFields.get(0).getName.equals("seconds") &&
          fd.getMessageType.getFields.get(1).getName.equals("nanos")) =>
        Some(DayTimeIntervalType.defaultConcreteType)
      case MESSAGE
        if (fd.getMessageType.getName == "Timestamp" &&
          fd.getMessageType.getFields.size() == 2 &&
          fd.getMessageType.getFields.get(0).getName.equals("seconds") &&
          fd.getMessageType.getFields.get(1).getName.equals("nanos")) =>
          Some(TimestampType)
      case MESSAGE if fd.isRepeated && fd.getMessageType.getOptions.hasMapEntry =>
        var keyType: DataType = NullType
        var valueType: DataType = NullType
        fd.getMessageType.getFields.forEach { field =>
          field.getName match {
            case "key" =>
              keyType =
                structFieldFor(
                  field,
                  existingRecordNames,
                  existingRecordTypes,
                  protobufOptions).get.dataType
            case "value" =>
              valueType =
                structFieldFor(
                  field,
                  existingRecordNames,
                  existingRecordTypes,
                  protobufOptions).get.dataType
          }
        }
        return Option(
          StructField(
            fd.getName,
            MapType(keyType, valueType, valueContainsNull = false).defaultConcreteType,
            nullable = false))
      case MESSAGE =>
        // User can set circularReferenceDepth of 0 or 1 or 2.
        // Going beyond 3 levels of recursion is not allowed.
        if (protobufOptions.circularReferenceType.equals("FIELD_TYPE")) {
          if (existingRecordTypes.contains(fd.getType.name()) &&
            (protobufOptions.circularReferenceDepth < 0 ||
              protobufOptions.circularReferenceDepth >= 3)) {
            throw QueryCompilationErrors.foundRecursionInProtobufSchema(fd.toString())
          } else if (existingRecordTypes.contains(fd.getType.name()) &&
            (existingRecordTypes.getOrElse(fd.getType.name(), 0)
              <= protobufOptions.circularReferenceDepth)) {
            return Some(StructField(fd.getName, NullType, nullable = false))
          }
        } else {
          if (existingRecordNames.contains(fd.getFullName) &&
            (protobufOptions.circularReferenceDepth < 0 ||
              protobufOptions.circularReferenceDepth >= 3)) {
            throw QueryCompilationErrors.foundRecursionInProtobufSchema(fd.toString())
          } else if (existingRecordNames.contains(fd.getFullName) &&
            existingRecordNames.getOrElse(fd.getFullName, 0)
              <= protobufOptions.circularReferenceDepth) {
            return Some(StructField(fd.getName, NullType, nullable = false))
          }
        }

        val newRecordNames = existingRecordNames +
          (fd.getFullName -> (existingRecordNames.getOrElse(fd.getFullName, 0) + 1))
        val newRecordTypes = existingRecordTypes +
          (fd.getType.name() -> (existingRecordTypes.getOrElse(fd.getType.name(), 0) + 1))

        Option(
          fd.getMessageType.getFields.asScala
            .flatMap(structFieldFor(_, newRecordNames, newRecordTypes, protobufOptions))
            .toSeq)
          .filter(_.nonEmpty)
          .map(StructType.apply)
      case other =>
        throw QueryCompilationErrors.protobufTypeUnsupportedYetError(other.toString)
    }
    dataType.map(dt =>
      StructField(
        fd.getName,
        if (fd.isRepeated) ArrayType(dt, containsNull = false) else dt,
        nullable = !fd.isRequired && !fd.isRepeated))
  }
}
