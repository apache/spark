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

import com.google.protobuf.DynamicMessage

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BinaryType, DataType}

private[proto] case class CatalystDataToProto(
                                             child: Expression,
                                             descFilePath: String, messageName: String) extends UnaryExpression {

  override def dataType: DataType = BinaryType

  @transient private lazy val protoType = ProtoUtils.buildDescriptor(descFilePath, messageName)

  @transient private lazy val serializer = new ProtoSerializer(child.dataType, protoType, child.nullable)

  override def nullSafeEval(input: Any): Any = {

    var ret: Array[Byte] = Array.empty

    try {
      val dynamicMessage = serializer.serialize(input).asInstanceOf[DynamicMessage]
      ret = dynamicMessage.toByteArray
    } catch {
      case es: Exception => println("=-=-=-=Exception caught ", es.printStackTrace())
    }
    ret
  }

  override def prettyName: String = "to_proto"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(byte[]) $expr.nullSafeEval($input)")
  }

  override protected def withNewChildInternal(newChild: Expression): CatalystDataToProto =
    copy(child = newChild)
}

