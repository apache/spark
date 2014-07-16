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

import org.scalatest.FunSuite

class SchemaSuite extends FunSuite {

  test("constructing an ArrayType") {
    val array = ArrayType(StringType)

    assert(ArrayType(StringType, false) === array)
  }

  test("extracting fields from a StructType") {
    val struct = StructType(
      StructField("a", IntegerType, true) ::
      StructField("b", LongType, false) ::
      StructField("c", StringType, true) ::
      StructField("d", FloatType, true) :: Nil)

    assert(StructField("b", LongType, false) === struct("b"))

    assert(struct("e") === null)

    val expectedStruct = StructType(
      StructField("b", LongType, false) ::
      StructField("d", FloatType, true) :: Nil)

    assert(expectedStruct === struct(Set("b", "d")))
    // struct does not have a field called e. So e is ignored.
    assert(expectedStruct === struct(Set("b", "d", "e")))
  }
}
