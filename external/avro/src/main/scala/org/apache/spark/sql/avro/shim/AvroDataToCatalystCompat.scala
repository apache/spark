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

package org.apache.spark.sql.avro.shim

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}

import org.apache.spark.sql.avro._
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}


/**
 * This trait does the actual decoding logic for a given schema and should be created by a given AvroDecoderFactory
 */
trait AvroDecoder {
    def decode(input: Array[Byte]): Any
}

/**
 * Implementing this trait allows the implementer to define how to decode avro records for a particular schema.
 */
trait AvroDecoderFactory {
    def create(schema: Schema): AvroDecoder
}

class DirectBinaryAvroDecoder(avroSchema: Schema) extends AvroDecoder {
    @transient lazy val reader = new GenericDatumReader[Any](avroSchema)
    
    @transient private var decoder: BinaryDecoder = _
    
    @transient private var result: Any = _
    
    override def decode(binary: Array[Byte]): Any = {
        decoder = DecoderFactory.get().binaryDecoder(binary, 0, binary.length, decoder)
        result = reader.read(result, decoder)
    }
    
}


class DirectBinaryAvroDecoderFactory extends AvroDecoderFactory {
    override def create(schema: Schema) : AvroDecoder = {
        return new DirectBinaryAvroDecoder(schema)
    }
}


/**
* This is a modified version of AvroDataToCatalyst from version 2.4 which replaces the built in avro decoding functionality.
*/
case class AvroDataToCatalystCompat(child: Expression, jsonFormatSchema: String, options: Map[String, String], decoderFactory: AvroDecoderFactory = new DirectBinaryAvroDecoderFactory())
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override lazy val dataType: DataType = SchemaConverters.toSqlType(avroSchema).dataType

  override def nullable: Boolean = true

  @transient private lazy val avroSchema = new Schema.Parser().parse(jsonFormatSchema)

  @transient private lazy val avroDecoder = decoderFactory.create(avroSchema)

  //@transient private lazy val reader = new GenericDatumReader[Any](avroSchema)  //Replaced with decoderFactory

  @transient private lazy val deserializer = new AvroDeserializer(avroSchema, dataType)

  //@transient private var decoder: BinaryDecoder = _ //Replaced with decoderFactory

  //@transient private var result: Any = _  //Replaced with decoderFactory

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    
    deserializer.deserialize(avroDecoder.decode(binary))
  }

  override def prettyName: String = "from_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(${CodeGenerator.boxedType(dataType)})$expr.nullSafeEval($input)")
  }
}
