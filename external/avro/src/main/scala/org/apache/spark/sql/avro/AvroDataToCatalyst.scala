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

package org.apache.spark.sql

import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}

import org.apache.spark.sql.avro.{AvroDeserializer, SchemaConverters, SerializableSchema}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

case class AvroDataToCatalyst(child: Expression, avroType: SerializableSchema)
  extends UnaryExpression with CodegenFallback with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override lazy val dataType: DataType =
    SchemaConverters.toSqlType(avroType.value).dataType

  override def nullable: Boolean = true

  @transient private lazy val reader = new GenericDatumReader[Any](avroType.value)

  @transient private lazy val deserializer = new AvroDeserializer(avroType.value, dataType)

  @transient private var decoder: BinaryDecoder = _

  @transient private var result: Any = _

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    decoder = DecoderFactory.get().binaryDecoder(binary, 0, binary.length, decoder)
    result = reader.read(result, decoder)
    deserializer.deserialize(result)
  }

  override def simpleString: String = {
    s"from_avro(${child.sql}, ${dataType.simpleString})"
  }

  override def sql: String = simpleString
}
