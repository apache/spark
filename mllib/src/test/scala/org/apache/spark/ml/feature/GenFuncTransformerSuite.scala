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

package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


class GenFuncTransformerSuite 
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {
  
  import testImplicits._
  
  test("params") {
    ParamsSuite.checkParams(new GenFuncTransformer)
  }
  
  test("execute simple add function") {
    val function = "function(a, b) { return a + b;}"
    val original = Seq((1.0, 2.0), (3.0, 4.0)).toDF("v1", "v2")
    val transformer = new GenFuncTransformer().setInputCols(Array("v1", "v2")).setOutputCol("result").setFunction(function)
    val result = transformer.transform(original)
    val resultSchema = transformer.transformSchema(original.schema)
    val expected = Seq((1.0, 2.0, 3.0), (3.0, 4.0, 7.0)).toDF("v1", "v2", "result")
    val expectedSchema = StructType(original.schema.fields :+ StructField("result", DoubleType, true))
    assert(result.schema.toString == resultSchema.toString)
    assert(resultSchema == expectedSchema)
    assert(result.collect().toSeq == expected.collect().toSeq)
    assert(original.sparkSession.catalog.listTables().count() == 0)
  }
  
  test("execute function when input column is non numeric") {
    val function = "function(a) { return a.length; }"
    val original = Seq((1, "hello"), (2, "sparkml")).toDF("id", "message")
    val transformer = new GenFuncTransformer().setInputCols(Array("message")).setOutputCol("length").setFunction(function)
    val result = transformer.transform(original)
    val expected = Seq((1, "hello", 5.0), (2, "sparkml", 7.0)).toDF("id", "message", "length")
    assert(result.collect().toSeq == expected.collect().toSeq)
  }
  
  test("read/write") {
    val t = new GenFuncTransformer()
              .setInputCols(Array("v1", "v2"))
              .setOutputCol("result")
              .setFunction("function(a, b) { return a + b;}")
    testDefaultReadWrite(t)
  }
}