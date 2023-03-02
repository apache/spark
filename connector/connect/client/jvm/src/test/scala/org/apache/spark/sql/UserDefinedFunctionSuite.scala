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

import scala.reflect.runtime.universe.typeTag

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.connect.client.util.ConnectFunSuite
import org.apache.spark.sql.connect.common.UdfPacket
import org.apache.spark.sql.functions.udf
import org.apache.spark.util.Utils

class UserDefinedFunctionSuite extends ConnectFunSuite with BeforeAndAfterEach {

  test("udf and encoder serialization") {
    def func(x: Int): Int = x + 1

    val myUdf = udf(func _)
    val colWithUdf = myUdf(Column("dummy"))

    val udfExpr = colWithUdf.expr.getCommonInlineUserDefinedFunction
    assert(udfExpr.getDeterministic)
    assert(udfExpr.getArgumentsCount == 1)
    assert(udfExpr.getArguments(0) == Column("dummy").expr)
    val udfObj = udfExpr.getScalarScalaUdf

    assert(udfObj.getNullable)

    val deSer = Utils.deserialize[UdfPacket](udfObj.getPayload.toByteArray)

    assert(deSer.function.asInstanceOf[Int => Int](5) == func(5))
    assert(deSer.outputEncoder == ScalaReflection.encoderFor(typeTag[Int]))
    assert(deSer.inputEncoders == Seq(ScalaReflection.encoderFor(typeTag[Int])))
  }
}
