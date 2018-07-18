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

package org.apache.spark.sql.types

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.StructType.{fromDDL, toDDL}

class StructTypeSuite extends SparkFunSuite {

  val s = StructType.fromDDL("a INT, b STRING")

  test("lookup a single missing field should output existing fields") {
    val e = intercept[IllegalArgumentException](s("c")).getMessage
    assert(e.contains("Available fields: a, b"))
  }

  test("lookup a set of missing fields should output existing fields") {
    val e = intercept[IllegalArgumentException](s(Set("a", "c"))).getMessage
    assert(e.contains("Available fields: a, b"))
  }

  test("lookup fieldIndex for missing field should output existing fields") {
    val e = intercept[IllegalArgumentException](s.fieldIndex("c")).getMessage
    assert(e.contains("Available fields: a, b"))
  }

  test("SPARK-24849: toDDL - simple struct") {
    val struct = StructType(Seq(StructField("a", IntegerType)))

    assert(toDDL(struct) == "a int")
  }

  test("SPARK-24849: round trip toDDL - fromDDL") {
    val struct = new StructType().add("a", IntegerType).add("b", StringType)

    assert(fromDDL(toDDL(struct)) === struct)
  }

  test("SPARK-24849: round trip fromDDL - toDDL") {
    val struct = "a map<int,string>,b int"

    assert(toDDL(fromDDL(struct)) === struct)
  }

  test("SPARK-24849: toDDL must take into account case") {
    val struct = new StructType()
      .add("metaData", new StructType().add("eventId", StringType))

    assert(toDDL(struct) == "metaData struct<eventId:string>")
  }
}
