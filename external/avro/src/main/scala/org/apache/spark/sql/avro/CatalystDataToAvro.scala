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

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BinaryType, DataType}

private[avro] case class CatalystDataToAvro(
    child: Expression,
    jsonFormatSchema: Option[String]) extends UnaryExpression {

  override def dataType: DataType = BinaryType

  @transient private lazy val avroType =
    jsonFormatSchema
      .map(new Schema.Parser().parse)
      .getOrElse(SchemaConverters.toAvroType(child.dataType, child.nullable))

  @transient private lazy val serializer =
    new AvroSerializer(child.dataType, avroType, child.nullable)

  @transient private lazy val writer =
    new GenericDatumWriter[Any](avroType)

  @transient private var encoder: BinaryEncoder = _

  @transient private lazy val out = new ByteArrayOutputStream

  override def nullSafeEval(input: Any): Any = {
    out.reset()
    encoder = EncoderFactory.get().directBinaryEncoder(out, encoder)
    val avroData = serializer.serialize(input)
    writer.write(avroData, encoder)
    encoder.flush()
    out.toByteArray
  }

  override def prettyName: String = "to_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(byte[]) $expr.nullSafeEval($input)")
  }
}
