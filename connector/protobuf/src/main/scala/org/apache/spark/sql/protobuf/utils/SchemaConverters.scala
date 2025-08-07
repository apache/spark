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

import scala.jdk.CollectionConverters._

import com.google.protobuf.{BoolValue, BytesValue, DoubleValue, FloatValue, Int32Value, Int64Value, StringValue, UInt32Value, UInt64Value}
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.WireFormat

import org.apache.spark.internal.Logging
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._

object SchemaConverters extends Logging {

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
  private[protobuf] def toSqlType(
      descriptor: Descriptor,
      protobufOptions: ProtobufOptions = ProtobufOptions(Map.empty)): SchemaType = {
    toSqlTypeHelper(descriptor, protobufOptions)
  }

  private[protobuf] def toSqlTypeHelper(
      descriptor: Descriptor,
      protobufOptions: ProtobufOptions): SchemaType = {
    val fields = descriptor.getFields.asScala.flatMap(
      structFieldFor(_,
        Map(descriptor.getFullName -> 1),
        protobufOptions: ProtobufOptions)).toSeq
    if (fields.isEmpty && protobufOptions.retainEmptyMessage) {
      SchemaType(convertEmptyProtoToStructWithDummyField(descriptor.getFullName), nullable = true)
    } else SchemaType(StructType(fields), nullable = true)
  }

  // existingRecordNames: Map[String, Int] used to track the depth of recursive fields and to
  // ensure that the conversion of the protobuf message to a Spark SQL StructType object does not
  // exceed the maximum recursive depth specified by the recursiveFieldMaxDepth option.
  // A return of None implies the field has reached the maximum allowed recursive depth and
  // should be dropped.
  private def structFieldFor(
      fd: FieldDescriptor,
      existingRecordNames: Map[String, Int],
      protobufOptions: ProtobufOptions): Option[StructField] = {
    import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._

    val dataType = fd.getJavaType match {
      // When the protobuf type is unsigned and upcastUnsignedIntegers has been set,
      // use a larger type (LongType and Decimal(20,0) for uint32 and uint64).
      case INT =>
        if (fd.getLiteType == WireFormat.FieldType.UINT32 && protobufOptions.upcastUnsignedInts) {
          Some(LongType)
        } else {
          Some(IntegerType)
        }
      case LONG => if (fd.getLiteType == WireFormat.FieldType.UINT64
          && protobufOptions.upcastUnsignedInts) {
        Some(DecimalType.LongDecimal)
      } else {
        Some(LongType)
      }
      case FLOAT => Some(FloatType)
      case DOUBLE => Some(DoubleType)
      case BOOLEAN => Some(BooleanType)
      case STRING => Some(StringType)
      case BYTE_STRING => Some(BinaryType)
      case ENUM => if (protobufOptions.enumsAsInts) Some(IntegerType) else Some(StringType)
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
      case MESSAGE if protobufOptions.convertAnyFieldsToJson &&
        fd.getMessageType.getFullName == "google.protobuf.Any" =>
        Some(StringType) // Any protobuf will be parsed and converted to json string.

      // Unwrap well known primitive wrapper types if the option has been set.
      case MESSAGE if fd.getMessageType.getFullName == BoolValue.getDescriptor.getFullName
        && protobufOptions.unwrapWellKnownTypes =>
        Some(BooleanType)
      case MESSAGE if fd.getMessageType.getFullName == Int32Value.getDescriptor.getFullName
        && protobufOptions.unwrapWellKnownTypes =>
        Some(IntegerType)
      case MESSAGE if fd.getMessageType.getFullName == UInt32Value.getDescriptor.getFullName
        && protobufOptions.unwrapWellKnownTypes =>
        if (protobufOptions.upcastUnsignedInts) {
          Some(LongType)
        } else {
          Some(IntegerType)
        }
      case MESSAGE if fd.getMessageType.getFullName == Int64Value.getDescriptor.getFullName
        && protobufOptions.unwrapWellKnownTypes =>
        Some(LongType)
      case MESSAGE if fd.getMessageType.getFullName == UInt64Value.getDescriptor.getFullName
        && protobufOptions.unwrapWellKnownTypes =>
        if (protobufOptions.upcastUnsignedInts) {
          Some(DecimalType.LongDecimal)
        } else {
          Some(LongType)
        }
      case MESSAGE if fd.getMessageType.getFullName == StringValue.getDescriptor.getFullName
        && protobufOptions.unwrapWellKnownTypes =>
        Some(StringType)
      case MESSAGE if fd.getMessageType.getFullName == BytesValue.getDescriptor.getFullName
        && protobufOptions.unwrapWellKnownTypes =>
        Some(BinaryType)
      case MESSAGE if fd.getMessageType.getFullName == FloatValue.getDescriptor.getFullName
        && protobufOptions.unwrapWellKnownTypes =>
        Some(FloatType)
      case MESSAGE if fd.getMessageType.getFullName == DoubleValue.getDescriptor.getFullName
        && protobufOptions.unwrapWellKnownTypes =>
        Some(DoubleType)

      case MESSAGE if fd.isRepeated && fd.getMessageType.getOptions.hasMapEntry =>
        var keyType: Option[DataType] = None
        var valueType: Option[DataType] = None
        fd.getMessageType.getFields.forEach { field =>
          field.getName match {
            case "key" =>
              keyType =
                structFieldFor(
                  field,
                  existingRecordNames,
                  protobufOptions).map(_.dataType)
            case "value" =>
              valueType =
                structFieldFor(
                  field,
                  existingRecordNames,
                  protobufOptions).map(_.dataType)
          }
        }
        (keyType, valueType) match {
          case (None, _) =>
            // This is probably never expected. Protobuf does not allow complex types for keys.
            log.info(s"Dropping map field ${fd.getFullName}. Key reached max recursive depth.")
            None
          case (_, None) =>
            log.info(s"Dropping map field ${fd.getFullName}. Value reached max recursive depth.")
            None
          case (Some(kt), Some(vt)) => Some(MapType(kt, vt, valueContainsNull = false))
        }
      case MESSAGE =>
        // If the `recursive.fields.max.depth` value is not specified, it will default to -1,
        // and recursive fields are not permitted. Setting it to 1 drops all recursive fields,
        // 2 allows it to be recursed once, and 3 allows it to be recursed twice and so on.
        // A value less than or equal to 0 or greater than 10 is not allowed, and if a protobuf
        // record has more depth for recursive fields than the allowed value, it will be truncated
        // and some fields may be discarded.
        // SQL Schema for protob2uf `message Person { string name = 1; Person bff = 2;}`
        // will vary based on the value of "recursive.fields.max.depth".
        // 1: struct<name: string>
        // 2: struct<name: string, bff: struct<name: string>>
        // 3: struct<name: string, bff: struct<name: string, bff: struct<name: string>>>
        // and so on.
        // TODO(rangadi): A better way to terminate would be replace the remaining recursive struct
        //      with the byte array of corresponding protobuf. This way no information is lost.
        //      i.e. with max depth 2, the above looks like this:
        //      struct<name: string, bff: struct<name: string, _serialized_bff: bytes>>
        val recordName = fd.getMessageType.getFullName
        val recursiveDepth = existingRecordNames.getOrElse(recordName, 0)
        val recursiveFieldMaxDepth = protobufOptions.recursiveFieldMaxDepth
        if (existingRecordNames.contains(recordName) && (recursiveFieldMaxDepth <= 0 ||
          recursiveFieldMaxDepth > 10)) {
          throw QueryCompilationErrors.foundRecursionInProtobufSchema(fd.toString())
        } else if (existingRecordNames.contains(recordName) &&
          recursiveDepth >= recursiveFieldMaxDepth) {
          // Recursive depth limit is reached. This field is dropped.
          // If it is inside a container like map or array, the containing field is dropped.
          log.info(
            s"The field ${fd.getFullName} of type $recordName is dropped " +
              s"at recursive depth $recursiveDepth"
          )
          None
        } else {
          val newRecordNames = existingRecordNames + (recordName -> (recursiveDepth + 1))
          val fields = fd.getMessageType.getFields.asScala.flatMap(
            structFieldFor(_, newRecordNames, protobufOptions)
          ).toSeq
          fields match {
            case Nil =>
              if (protobufOptions.retainEmptyMessage) {
                Some(convertEmptyProtoToStructWithDummyField(fd.getFullName))
              } else {
                log.info(
                  s"Dropping ${fd.getFullName} as it does not have any fields left " +
                    "likely due to recursive depth limit."
                )
                None
              }
            case fds => Some(StructType(fds))
          }
        }
      case other =>
        throw QueryCompilationErrors.protobufTypeUnsupportedYetError(other.toString)
    }
    dataType.map {
      case dt: MapType => StructField(fd.getName, dt)
      case dt if fd.isRepeated =>
        StructField(fd.getName, ArrayType(dt, containsNull = false))
      case dt => StructField(fd.getName, dt, nullable = !fd.isRequired)
    }
  }

  // Insert a dummy column to retain the empty message because
  // spark doesn't allow empty struct type.
  private def convertEmptyProtoToStructWithDummyField(desc: String): StructType = {
    log.info(s"Keep $desc which is empty struct by inserting a dummy field.")
    StructType(StructField("__dummy_field_in_empty_struct", StringType) :: Nil)
  }
}
