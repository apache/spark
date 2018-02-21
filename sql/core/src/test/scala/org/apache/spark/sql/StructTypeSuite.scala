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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.StructType

class StructTypeSuite extends SparkFunSuite{

  test("SPARK-23462 lookup a single missing field should output existing fields") {
    val s = StructType.fromDDL("a INT")
    val e = intercept[IllegalArgumentException](s("b")).getMessage
    assert(e.contains("Available fields: a"))
  }

  test("SPARK-23462 lookup a set of missing fields should output existing fields") {
    val s = StructType.fromDDL("a INT")
    val e = intercept[IllegalArgumentException](s(Set("a", "b"))).getMessage
    assert(e.contains("Available fields: a"))
  }

  test("SPARK-23462 lookup fieldIndex for missing field should output existing fields") {
    val s = StructType.fromDDL("a INT")
    val e = intercept[IllegalArgumentException](s.fieldIndex("b")).getMessage
    assert(e.contains("Available fields: a"))
  }

}
