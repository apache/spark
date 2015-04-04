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

import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.FunSuite

import org.apache.spark.sql.test.ExamplePointUDT
import org.apache.spark.sql.types.StructType

class HiveMetastoreCatalogSuite extends FunSuite {

  test("struct field should accept underscore in sub-column name") {
    val metastr = "struct<a: int, b_1: string, c: string>"

    val datatype = HiveMetastoreTypes.toDataType(metastr)
    assert(datatype.isInstanceOf[StructType])
  }

  test("udt to metastore type conversion") {
    val udt = new ExamplePointUDT
    assert(HiveMetastoreTypes.toMetastoreType(udt) ===
      HiveMetastoreTypes.toMetastoreType(udt.sqlType))
  }

  test("duplicated metastore relations") {
    import TestHive.implicits._
    val df = TestHive.sql("SELECT * FROM src")
    println(df.queryExecution)
    df.as('a).join(df.as('b), $"a.key" === $"b.key")
  }
}
