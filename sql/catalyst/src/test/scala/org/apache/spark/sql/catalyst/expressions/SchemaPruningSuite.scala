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

package org.apache.spark.sql.catalyst.expressions

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class SchemaPruningSuite extends SparkFunSuite {

  test("collect struct types") {
    val dataTypes = Seq(
      IntegerType,
      ArrayType(IntegerType),
      StructType.fromDDL("a int, b int"),
      ArrayType(StructType.fromDDL("a int, b int, c string")),
      StructType.fromDDL("a struct<a:int, b:int>, b int")
    )

    val expectedTypes = Seq(
      Seq.empty[StructType],
      Seq.empty[StructType],
      Seq(StructType.fromDDL("a int, b int")),
      Seq(StructType.fromDDL("a int, b int, c string")),
      Seq(StructType.fromDDL("a struct<a:int, b:int>, b int"),
        StructType.fromDDL("a int, b int"))
    )

    dataTypes.zipWithIndex.foreach { case (dt, idx) =>
      val structs = SchemaPruning.collectStructType(dt, ArrayBuffer.empty[StructType])
      assert(structs === expectedTypes(idx))
    }
  }
}
