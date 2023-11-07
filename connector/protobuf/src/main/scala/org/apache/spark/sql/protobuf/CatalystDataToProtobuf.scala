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
package org.apache.spark.sql.protobuf

import com.google.protobuf.DynamicMessage

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.types.{BinaryType, DataType}

private[sql] case class CatalystDataToProtobuf(
    child: Expression,
    messageName: String,
    binaryFileDescriptorSet: Option[Array[Byte]] = None,
    options: Map[String, String] = Map.empty)
    extends UnaryExpression {

  // TODO(SPARK-43578): binaryFileDescriptorSet could be very large in some cases. It is better
  //                    to broadcast it so that it is not transferred with each task.

  override def dataType: DataType = BinaryType

  @transient private lazy val protoDescriptor =
    ProtobufUtils.buildDescriptor(messageName, binaryFileDescriptorSet)

  @transient private lazy val serializer =
    new ProtobufSerializer(child.dataType, protoDescriptor, child.nullable)

  override def nullSafeEval(input: Any): Any = {
    val dynamicMessage = serializer.serialize(input).asInstanceOf[DynamicMessage]
    dynamicMessage.toByteArray
  }

  override def prettyName: String = "to_protobuf"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input => s"(byte[]) $expr.nullSafeEval($input)")
  }

  override protected def withNewChildInternal(newChild: Expression): CatalystDataToProtobuf =
    copy(child = newChild)
}
