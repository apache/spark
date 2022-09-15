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
package org.apache.spark.sql.proto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types._

/*
 * This object contains method that are used to convert sparkSQL
 * schemas to proto schemas and vice versa.
 */

@DeveloperApi
object SchemaConverters {
  /**
    * Internal wrapper for SQL data type and nullability.
    * @since 3.4.0
    */
  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
    * Converts an Proto schema to a corresponding Spark SQL schema.
    * @since 3.4.0
    */
  def toSqlType(protoSchema: Descriptor): SchemaType = {
    toSqlTypeHelper(protoSchema)
  }

  def toSqlTypeHelper(descriptor: Descriptor): SchemaType = ScalaReflectionLock.synchronized {
    import scala.collection.JavaConverters._
    SchemaType(StructType(descriptor.getFields.asScala.flatMap(structFieldFor)), nullable = true)
  }

  def structFieldFor(fd: FieldDescriptor): Option[StructField] = {
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
      case MESSAGE =>
        import collection.JavaConverters._
        Option(fd.getMessageType.getFields.asScala.flatMap(structFieldFor))
          .filter(_.nonEmpty)
          .map(StructType.apply)

    }
    dataType.map( dt => StructField(
      fd.getName,
      if (fd.isRepeated) ArrayType(dt, containsNull = false) else dt,
      nullable = !fd.isRequired && !fd.isRepeated
    ))
  }

  private[proto] class IncompatibleSchemaException(
                                                   msg: String,
                                                   ex: Throwable = null) extends Exception(msg, ex)

  private[proto] class UnsupportedProtoTypeException(msg: String) extends Exception(msg)
  private[proto] class UnsupportedProtoValueException(msg: String) extends Exception(msg)
}
