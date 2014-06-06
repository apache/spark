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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.types.{StructField, DataType, ArrayType, StructType}

/**
 * A a collection of common abstractions for query plans as well as
 * a base logical plan representation.
 */
package object plans {
  def generateSchemaTreeString(schema: Seq[Attribute]): String = {
    val builder = new StringBuilder
    builder.append("root\n")
    val prefix = " |"
    schema.foreach {
      attribute => {
        val name = attribute.name
        val dataType = attribute.dataType
        dataType match {
          case fields: StructType =>
            builder.append(s"$prefix-- $name: $StructType\n")
            generateSchemaTreeString(fields, s"$prefix    |", builder)
          case ArrayType(fields: StructType) =>
            builder.append(s"$prefix-- $name: $ArrayType[$StructType]\n")
            generateSchemaTreeString(fields, s"$prefix    |", builder)
          case ArrayType(elementType: DataType) =>
            builder.append(s"$prefix-- $name: $ArrayType[$elementType]\n")
          case _ => builder.append(s"$prefix-- $name: $dataType\n")
        }
      }
    }

    builder.toString()
  }

  def generateSchemaTreeString(
      schema: StructType,
      prefix: String,
      builder: StringBuilder): StringBuilder = {
    schema.fields.foreach {
      case StructField(name, fields: StructType, _) =>
        builder.append(s"$prefix-- $name: $StructType\n")
        generateSchemaTreeString(fields, s"$prefix    |", builder)
      case StructField(name, ArrayType(fields: StructType), _) =>
        builder.append(s"$prefix-- $name: $ArrayType[$StructType]\n")
        generateSchemaTreeString(fields, s"$prefix    |", builder)
      case StructField(name, ArrayType(elementType: DataType), _) =>
        builder.append(s"$prefix-- $name: $ArrayType[$elementType]\n")
      case StructField(name, fieldType: DataType, _) =>
        builder.append(s"$prefix-- $name: $fieldType\n")
    }

    builder
  }
}
