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

package org.apache.spark.sql.hive

import org.apache.spark.sql.QueryTest

case class FunctionResult(f1: String, f2: String)

class UDFSuite extends QueryTest {
  private lazy val ctx = org.apache.spark.sql.hive.test.TestHive

  test("UDF case insensitive") {
    ctx.udf.register("random0", () => { Math.random() })
    ctx.udf.register("RANDOM1", () => { Math.random() })
    ctx.udf.register("strlenScala", (_: String).length + (_: Int))
    assert(ctx.sql("SELECT RANDOM0() FROM src LIMIT 1").head().getDouble(0) >= 0.0)
    assert(ctx.sql("SELECT RANDOm1() FROM src LIMIT 1").head().getDouble(0) >= 0.0)
    assert(ctx.sql("SELECT strlenscala('test', 1) FROM src LIMIT 1").head().getInt(0) === 5)
  }
}
