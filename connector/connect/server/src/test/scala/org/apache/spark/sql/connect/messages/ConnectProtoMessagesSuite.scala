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
package org.apache.spark.sql.connect.messages

import com.google.protobuf.ByteString

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types.IntegerType

class ConnectProtoMessagesSuite extends SparkFunSuite {
  test("UserContext can deal with extensions") {
    // Create the builder.
    val builder = proto.UserContext.newBuilder().setUserId("1").setUserName("Martin")

    // Create the extension value.
    val lit = proto.Expression
      .newBuilder()
      .setLiteral(proto.Expression.Literal.newBuilder().setInteger(32).build())
    // Pack the extension into Any.
    val aval = com.google.protobuf.Any.pack(lit.build())
    // Add Any to the repeated field list.
    builder.addExtensions(aval)
    // Create serialized value.
    val serialized = builder.build().toByteArray

    // Now, read the serialized value.
    val result = proto.UserContext.parseFrom(serialized)
    assert(result.getUserId.equals("1"))
    assert(result.getUserName.equals("Martin"))
    assert(result.getExtensionsCount == 1)

    val ext = result.getExtensions(0)
    assert(ext.is(classOf[proto.Expression]))
    val extLit = ext.unpack(classOf[proto.Expression])
    assert(extLit.hasLiteral)
    assert(extLit.getLiteral.hasInteger)
    assert(extLit.getLiteral.getInteger == 32)
  }

  test("CommonInlineUserDefinedFunction") {
    val arguments = proto.Expression
      .newBuilder()
      .setUnresolvedAttribute(
        proto.Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("id"))
      .build()

    val pythonUdf = proto.PythonUDF
      .newBuilder()
      .setEvalType(100)
      .setOutputType(DataTypeProtoConverter.toConnectProtoType(IntegerType))
      .setCommand(ByteString.copyFrom("command".getBytes()))
      .setPythonVer("3.10")
      .build()

    val commonInlineUserDefinedFunctionExpr = proto.Expression
      .newBuilder()
      .setCommonInlineUserDefinedFunction(
        proto.CommonInlineUserDefinedFunction
          .newBuilder()
          .setFunctionName("f")
          .setDeterministic(true)
          .addArguments(arguments)
          .setPythonUdf(pythonUdf))
      .build()

    val fun = commonInlineUserDefinedFunctionExpr.getCommonInlineUserDefinedFunction()
    assert(fun.getFunctionName == "f")
    assert(fun.getDeterministic == true)
    assert(fun.getArgumentsCount == 1)
    assert(fun.hasPythonUdf == true)
    assert(pythonUdf.getPythonVer == "3.10")
  }
}
