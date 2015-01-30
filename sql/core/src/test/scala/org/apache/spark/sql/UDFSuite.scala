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

import org.apache.spark.sql.Dsl.StringToColumn
import org.apache.spark.sql.test._

/* Implicits */
import TestSQLContext._

case class FunctionResult(f1: String, f2: String)

class UDFSuite extends QueryTest {

  test("Simple UDF") {
    udf.register("strLenScala", (_: String).length)
    assert(sql("SELECT strLenScala('test')").head().getInt(0) === 4)
  }

  test("ZeroArgument UDF") {
    udf.register("random0", () => { Math.random()})
    assert(sql("SELECT random0()").head().getDouble(0) >= 0.0)
  }

  test("TwoArgument UDF") {
    udf.register("strLenScala", (_: String).length + (_:Int))
    assert(sql("SELECT strLenScala('test', 1)").head().getInt(0) === 5)
  }

  test("struct UDF") {
    udf.register("returnStruct", (f1: String, f2: String) => FunctionResult(f1, f2))

    val result =
      sql("SELECT returnStruct('test', 'test2') as ret")
        .select($"ret.f1").head().getString(0)
    assert(result === "test")
  }
}
