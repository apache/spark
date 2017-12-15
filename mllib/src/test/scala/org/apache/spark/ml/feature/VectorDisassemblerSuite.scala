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
import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.functions.col

class VectorDisassemblerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new VectorDisassembler())
  }

  test("VectorDisassembler") {
    val df = Seq(
      (0, 0.0, Vectors.dense(1.0, 2.0), "a", Vectors.sparse(2, Array(1), Array(3.0)), 10L)
    ).toDF("id", "x", "y", "name", "z", "n")
    val assembler = new VectorAssembler()
      .setInputCols(Array("x", "y", "z", "n"))
      .setOutputCol("features")
      .transform(df)
      .drop("x", "y", "z", "n")
    assert(assembler.schema.length == 3)

    val vecDis = new VectorDisassembler()
      .setInputCol("features")
      .transform(assembler)
    assert(vecDis.schema.length == 9)
    vecDis.printSchema()

    val line = vecDis.first()
    assert(line(0) == 0)
    assert(line(1) == "a")
    assert(line(2) === Vectors.sparse(6, Array(1, 2, 4, 5), Array(1.0, 2.0, 3.0, 10.0)))
    assert(line(3).equals(0.0))
    assert(line(4).equals(1.0))
    assert(line(5).equals(2.0))
    assert(line(6).equals(0.0))
    assert(line(7).equals(3.0))
    assert(line(8).equals(10.0))
  }

  test("ML attributes") {
    val browser = NominalAttribute.defaultAttr.withValues("chrome", "firefox", "safari")
    val hour = NumericAttribute.defaultAttr.withMin(0.0).withMax(24.0)
    val user = new AttributeGroup("user", Array(
      NominalAttribute.defaultAttr.withName("gender").withValues("male", "female"),
      NumericAttribute.defaultAttr.withName("salary")))
    val row = (1.0, 0.5, 1, Vectors.dense(1.0, 1000.0), Vectors.sparse(2, Array(1), Array(2.0)))
    val df = Seq(row).toDF("browser", "hour", "count", "user", "ad")
      .select(
        col("browser").as("browser", browser.toMetadata()),
        col("hour").as("hour", hour.toMetadata()),
        col("count"), // "count" is an integer column without ML attribute
        col("user").as("user", user.toMetadata()),
        col("ad")) // "ad" is a vector column without ML attribute
    val assembler = new VectorAssembler()
        .setInputCols(Array("browser", "hour", "count", "user", "ad"))
        .setOutputCol("features").transform(df)
        .drop("browser", "hour", "count", "user", "ad")
    val assemSchema = assembler.schema
    val features = AttributeGroup.fromStructField(assemSchema("features"))
    assert(features.size === 7)

    // disassembler
    val disassembler = new VectorDisassembler()
      .setInputCol("features").transform(assembler)

    val schema = disassembler.schema
    assert(schema.size === 8)
    val browserOut = schema(1)
    assert(browserOut.name === "browser")
    assert(browserOut.metadata === browser.withIndex(0).withName("browser").toMetadata())
    val hourOut = schema(2)
    assert(hourOut.name === "hour")
    assert(hourOut.metadata === hour.withIndex(1).withName("hour").toMetadata())
    val countOut = schema(3)
    assert(countOut.name === "count")
    assert(countOut.metadata === NumericAttribute.defaultAttr
      .withName("count").withIndex(2).toMetadata())
    val userGenderOut = schema(4)
    assert(userGenderOut.name === "user_gender")
    assert(userGenderOut.metadata === NominalAttribute.defaultAttr
      .withName("user_gender").withValues("male", "female").withIndex(3).toMetadata())
    val userSalaryOut = schema(5)
    assert(userSalaryOut.name === "user_salary")
    assert(userSalaryOut.metadata === NumericAttribute.defaultAttr
      .withName("user_salary").withIndex(4).toMetadata())
    assert(schema(6).name === "ad_0")
    assert(schema(6).metadata === NumericAttribute.defaultAttr
      .withIndex(5).withName("ad_0").toMetadata())
    assert(schema(7).name === "ad_1")
    assert(schema(7).metadata === NumericAttribute.defaultAttr
      .withIndex(6).withName("ad_1").toMetadata())
  }

  test("read/write") {
    val t = new VectorDisassembler()
      .setInputCol("myInputCol")
    testDefaultReadWrite(t)
  }
}
