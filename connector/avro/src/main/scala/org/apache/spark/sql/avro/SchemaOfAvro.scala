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

import org.apache.avro.Schema

import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, Literal, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.util.{FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{DataType, ObjectType, StringType}

case class SchemaOfAvro(
    jsonFormatSchema: String,
    options: Map[String, String])
  extends LeafExpression with RuntimeReplaceable {

  override def dataType: DataType = StringType

  override def nullable: Boolean = false

  @transient private lazy val avroOptions = AvroOptions(options)

  @transient private lazy val actualSchema =
    new Schema.Parser().setValidateDefaults(false).parse(jsonFormatSchema)

  @transient private lazy val expectedSchema = avroOptions.schema.getOrElse(actualSchema)

  @transient private lazy val parseMode: ParseMode = {
    val mode = avroOptions.parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw QueryCompilationErrors.parseModeUnsupportedError(
        prettyName, mode
      )
    }
    mode
  }

  override def prettyName: String = "schema_of_avro"

  @transient private lazy val avroOptionsObjectType = ObjectType(classOf[AvroOptions])
  @transient private lazy val parseModeObjectType = ObjectType(classOf[ParseMode])
  @transient private lazy val schemaObjectType = ObjectType(classOf[Schema])

  override def replacement: Expression = StaticInvoke(
    AvroExpressionEvalUtils.getClass,
    dataType,
    "schemaOfAvro",
    Seq(
      Literal(avroOptions, avroOptionsObjectType),
      Literal(parseMode, parseModeObjectType),
      Literal(expectedSchema, schemaObjectType)),
    Seq(avroOptionsObjectType, parseModeObjectType, schemaObjectType)
  )
}
