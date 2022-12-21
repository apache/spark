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
        structFieldFor(_,
          Map(descriptor.getFullName -> 1),
          protobufOptions: ProtobufOptions)).toArray),
      nullable = true)
  }

  // existingRecordNames: Map[String, Int] used to track the depth of recursive fields and to
  // ensure that the conversion of the protobuf message to a Spark SQL StructType object does not
  // exceed the maximum recursive depth specified by the recursiveFieldMaxDepth option.
  def structFieldFor(
      fd: FieldDescriptor,
      existingRecordNames: Map[String, Int],
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
                  protobufOptions).get.dataType
            case "value" =>
              valueType =
                structFieldFor(
                  field,
                  existingRecordNames,
                  protobufOptions).get.dataType
          }
        }
        return Option(
          StructField(
            fd.getName,
            MapType(keyType, valueType, valueContainsNull = false).defaultConcreteType,
            nullable = false))
      case MESSAGE =>
        // If the `recursive.fields.max.depth` value is not specified, it will default to -1;
        // recursive fields are not permitted. Setting it to 0 drops all recursive fields,
        // 1 allows it to be recursed once, and 2 allows it to be recursed twice and so on.
        // A value greater than 10 is not allowed, and if a protobuf record has more depth for
        // recursive fields than the allowed value, it will be truncated and some fields may be
        // discarded.
        // SQL Schema for the protobuf message `message Person { string name = 1; Person bff = 2}`
        // will vary based on the value of "recursive.fields.max.depth".
        // 0: struct<name: string, bff: null>
        // 1: struct<name string, bff: <name: string, bff: null>>
        // 2: struct<name string, bff: <name: string, bff: struct<name: string, bff: null>>> ...
        val recordName = fd.getMessageType.getFullName
        val recursiveDepth = existingRecordNames.getOrElse(recordName, 0)
        val recursiveFieldMaxDepth = protobufOptions.recursiveFieldMaxDepth
        if (existingRecordNames.contains(recordName) && (recursiveFieldMaxDepth < 0 ||
          recursiveFieldMaxDepth > 10)) {
          throw QueryCompilationErrors.foundRecursionInProtobufSchema(fd.toString())
        } else if (existingRecordNames.contains(recordName) &&
          recursiveDepth > recursiveFieldMaxDepth) {
          Some(NullType)
        } else {
          val newRecordNames = existingRecordNames + (recordName -> (recursiveDepth + 1))
          Option(
            fd.getMessageType.getFields.asScala
              .flatMap(structFieldFor(_, newRecordNames, protobufOptions))
              .toSeq)
            .filter(_.nonEmpty)
            .map(StructType.apply)
        }
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
