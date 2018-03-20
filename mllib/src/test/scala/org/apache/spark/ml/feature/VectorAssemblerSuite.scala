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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf}

class VectorAssemblerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new VectorAssembler)
  }

  test("assemble") {
    import org.apache.spark.ml.feature.VectorAssembler.assemble
    assert(assemble(Seq(1), true)(0.0) === Vectors.sparse(1, Array.empty, Array.empty))
    assert(assemble(Seq(1, 1), true)(0.0, 1.0) === Vectors.sparse(2, Array(1), Array(1.0)))
    val dv = Vectors.dense(2.0, 0.0)
    assert(assemble(Seq(1, 2, 1), true)(0.0, dv, 1.0) ===
      Vectors.sparse(4, Array(1, 3), Array(2.0, 1.0)))
    val sv = Vectors.sparse(2, Array(0, 1), Array(3.0, 4.0))
    assert(assemble(Seq(1, 2, 1, 2), true)(0.0, dv, 1.0, sv) ===
      Vectors.sparse(6, Array(1, 3, 4, 5), Array(2.0, 1.0, 3.0, 4.0)))
    for (v <- Seq(1, "a")) {
      intercept[SparkException](assemble(Seq(1), true)(v))
      intercept[SparkException](assemble(Seq(1, 1), true)(1.0, v))
    }
  }

  test("assemble should compress vectors") {
    import org.apache.spark.ml.feature.VectorAssembler.assemble
    val v1 = assemble(Seq(1, 1, 1, 4), true)(0.0, 0.0, 0.0, Vectors.dense(4.0))
    assert(v1.isInstanceOf[SparseVector])
    val sv = Vectors.sparse(1, Array(0), Array(4.0))
    val v2 = assemble(Seq(1, 1, 1, 1), true)(1.0, 2.0, 3.0, sv)
    assert(v2.isInstanceOf[DenseVector])
  }

  test("VectorAssembler") {
    val df = Seq(
      (0, 0.0, Vectors.dense(1.0, 2.0), "a", Vectors.sparse(2, Array(1), Array(3.0)), 10L)
    ).toDF("id", "x", "y", "name", "z", "n")
    val assembler = new VectorAssembler()
      .setInputCols(Array("x", "y", "z", "n"))
      .setOutputCol("features")
    assembler.transform(df).select("features").collect().foreach {
      case Row(v: Vector) =>
        assert(v === Vectors.sparse(6, Array(1, 2, 4, 5), Array(1.0, 2.0, 3.0, 10.0)))
    }
  }

  test("transform should throw an exception in case of unsupported type") {
    val df = Seq(("a", "b", "c")).toDF("a", "b", "c")
    val assembler = new VectorAssembler()
      .setInputCols(Array("a", "b", "c"))
      .setOutputCol("features")
    val thrown = intercept[IllegalArgumentException] {
      assembler.transform(df)
    }
    assert(thrown.getMessage contains
      "Data type StringType of column a is not supported.\n" +
      "Data type StringType of column b is not supported.\n" +
      "Data type StringType of column c is not supported.")
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
      .setOutputCol("features")
    val output = assembler.transform(df)
    val schema = output.schema
    val features = AttributeGroup.fromStructField(schema("features"))
    assert(features.size === 7)
    val browserOut = features.getAttr(0)
    assert(browserOut === browser.withIndex(0).withName("browser"))
    val hourOut = features.getAttr(1)
    assert(hourOut === hour.withIndex(1).withName("hour"))
    val countOut = features.getAttr(2)
    assert(countOut === NumericAttribute.defaultAttr.withName("count").withIndex(2))
    val userGenderOut = features.getAttr(3)
    assert(userGenderOut === user.getAttr("gender").withName("user_gender").withIndex(3))
    val userSalaryOut = features.getAttr(4)
    assert(userSalaryOut === user.getAttr("salary").withName("user_salary").withIndex(4))
    assert(features.getAttr(5) === NumericAttribute.defaultAttr.withIndex(5).withName("ad_0"))
    assert(features.getAttr(6) === NumericAttribute.defaultAttr.withIndex(6).withName("ad_1"))
  }

  test("read/write") {
    val t = new VectorAssembler()
      .setInputCols(Array("myInputCol", "myInputCol2"))
      .setOutputCol("myOutputCol")
    testDefaultReadWrite(t)
  }

  test("SPARK-22446: VectorAssembler's UDF should not apply on filtered data") {
    val df = Seq(
      (0, 0.0, Vectors.dense(1.0, 2.0), "a", Vectors.sparse(2, Array(1), Array(3.0)), 10L),
      (0, 1.0, null, "b", null, 20L)
    ).toDF("id", "x", "y", "name", "z", "n")

    val assembler = new VectorAssembler()
      .setInputCols(Array("x", "z", "n"))
      .setOutputCol("features")

    val filteredDF = df.filter($"y".isNotNull)

    val vectorUDF = udf { vector: Vector =>
      vector.numActives
    }

    assert(assembler.transform(filteredDF).select("features")
      .filter(vectorUDF($"features") > 1)
      .count() == 1)
  }

  test("assemble should keep nulls") {
    import org.apache.spark.ml.feature.VectorAssembler.assemble
    assert(assemble(Seq(1, 1), true)(1.0, null) === Vectors.dense(1.0, Double.NaN))
    assert(assemble(Seq(1, 2), true)(1.0, null) === Vectors.dense(1.0, Double.NaN, Double.NaN))
    assert(assemble(Seq(1), true)(null) === Vectors.dense(Double.NaN))
    assert(assemble(Seq(2), true)(null) === Vectors.dense(Double.NaN, Double.NaN))
  }

  test("assemble should throw errors") {
    import org.apache.spark.ml.feature.VectorAssembler.assemble
    intercept[SparkException](assemble(Seq(1, 1), false)(1.0, null) ===
      Vectors.dense(1.0, Double.NaN))
    intercept[SparkException](assemble(Seq(1, 2), false)(1.0, null) ===
      Vectors.dense(1.0, Double.NaN, Double.NaN))
    intercept[SparkException](assemble(Seq(1), false)(null) === Vectors.dense(Double.NaN))
    intercept[SparkException](assemble(Seq(2), false)(null) ===
      Vectors.dense(Double.NaN, Double.NaN))
  }

  test("get lengths function") {
    val df = Seq[(Long, Long, java.lang.Double, Vector, String, Vector, Long)](
      (1, 2, 0.0, Vectors.dense(1.0, 2.0), "a", Vectors.sparse(2, Array(1), Array(3.0)), 7L),
      (2, 1, 0.0, null, "a", Vectors.sparse(2, Array(1), Array(3.0)), 6L),
      (3, 3, null, Vectors.dense(1.0, 2.0), "a", Vectors.sparse(2, Array(1), Array(3.0)), 8L),
      (4, 4, null, null, "a", Vectors.sparse(2, Array(1), Array(3.0)), 9L)
    ).toDF("id1", "id2", "x", "y", "name", "z", "n")
    assert(VectorAssembler.getLengthsFromFirst(df, Seq("y")).exists(_ == "y" -> 2))
    intercept[NullPointerException](VectorAssembler.getLengthsFromFirst(df.sort("id2"), Seq("y")))
    intercept[NoSuchElementException](
      VectorAssembler.getLengthsFromFirst(df.filter("id1 > 4"), Seq("y")))

    assert(VectorAssembler.getLengths(
      df.sort("id2"), Seq("y"), VectorAssembler.SKIP_INVALID).exists(_ == "y" -> 2))
    intercept[NullPointerException](VectorAssembler.getLengths(
      df.sort("id2"), Seq("y"), VectorAssembler.ERROR_INVALID))
    intercept[RuntimeException](VectorAssembler.getLengths(
      df.sort("id2"), Seq("y"), VectorAssembler.KEEP_INVALID))
  }

  test("Handle Invalid should behave properly") {
    val df = Seq[(Long, Long, java.lang.Double, Vector, String, Vector, Long)](
      (1, 2, 0.0, Vectors.dense(1.0, 2.0), "a", Vectors.sparse(2, Array(1), Array(3.0)), 7L),
      (2, 1, 0.0, null, "a", Vectors.sparse(2, Array(1), Array(3.0)), 6L),
      (3, 3, null, Vectors.dense(1.0, 2.0), "a", Vectors.sparse(2, Array(1), Array(3.0)), 8L),
      (4, 4, null, null, "a", Vectors.sparse(2, Array(1), Array(3.0)), 9L)
    ).toDF("id1", "id2", "x", "y", "name", "z", "n")

    val assembler = new VectorAssembler()
      .setInputCols(Array("x", "y", "z", "n"))
      .setOutputCol("features")

    // behavior when first row has information
    assert(assembler.setHandleInvalid("skip").transform(df).count() == 1)
    intercept[RuntimeException](assembler.setHandleInvalid("keep").transform(df).collect())
    intercept[SparkException](assembler.setHandleInvalid("error").transform(df).collect())

    // numeric column is all null
    intercept[RuntimeException](
      assembler.setHandleInvalid("keep").transform(df.filter("id1==3")).count() == 1)

    // vector column is all null
    val df2 = df.filter("0 == id1 % 2")
    intercept[RuntimeException](assembler.setHandleInvalid("skip").transform(df2))
    intercept[RuntimeException](assembler.setHandleInvalid("keep").transform(df2))
    intercept[NullPointerException](assembler.setHandleInvalid("error").transform(df2).collect())
  }

}
