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

import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}


private[avro] case class ConfluentSRToCatalyst(
  child: Expression,
  schemaRegistryAddr: String,
  subject: String) extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  var dataType: DataType = _

  override def nullable: Boolean = true

  @transient private lazy val valueField = "payload"

  @transient private lazy val avroDeserializer = new KafkaAvroDeserializer()

  @transient private lazy val avroDeserializerConfig = {
    Map("schema.registry.url" -> schemaRegistryAddr)
  }

  @transient private lazy val isKey = {
    if (subject.contains("-key")) {
        true
    } else if (subject.contains("-value")) {
      false
    } else {
      throw new SparkException("Unacceptable subject. " +
        "it should be like 'topic-key' or 'topic-value'")
    }
  }

  avroDeserializer.configure(avroDeserializerConfig.asJava, isKey)

  override def prettyName: String = "from_avro"

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    try {
      val data = avroDeserializer.deserialize(subject, binary).asInstanceOf[GenericRecord]

      dataType = SchemaConverters.toSqlType(data.getSchema).dataType
      AvroSchemaUtils.toJson(data.get(valueField))
    } catch {
      case _ : Throwable =>
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(ctx, ev, eval => {
      val result = ctx.freshName("result")
      val dt = CodeGenerator.boxedType(dataType)
      s"""
        $dt $result = ($dt) $expr.nullSafeEval($eval);
        if ($result == null) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = $result;
        }
      """
    })
  }

  override protected def withNewChildInternal(newChild: Expression): ConfluentSRToCatalyst =
    copy(child = newChild)
}

