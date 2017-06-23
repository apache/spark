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
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

class FuncTransformerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest  {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new FuncTransformer(udf { (i: Double) => i + 1 }))
  }

  test("FuncTransformer for single input column") {
    val labelConverter = new FuncTransformer(udf { (i: Double) => if (i >= 1) 1.0 else 0.0 })
    val df = Seq(-1.0, 0.0, 1.0, 2.0).toDF("input")
    val expectDF = Seq(0.0, 0.0, 1.0, 1.0).toDF("output")
    assert(labelConverter.transform(df).select("output").collect().toSet.equals(
      expectDF.collect().toSet))
  }

  test("FuncTransformer for struct input column") {
    val df = spark.createDataFrame(Seq(
      ("a", (Array(1.2, 2), Array(2))),
      ("b", (Array(3.1, 2), Array(2))))).toDF("name", "data")
    val labelConverter = new FuncTransformer(udf { row: Row => row.getSeq[Double](0).head })
      .setInputCol("data")
    val expectDF = Seq(1.2, 3.1).toDF("output")
    assert(labelConverter.transform(df).select("output").collect().toSet.equals(
      expectDF.collect().toSet))
  }

  test("FuncTransformer for type conversion") {
    val df = Seq(-1.0, 0.0, 1.0, 2.0).toDF("input")
    val toInt = new FuncTransformer(udf { (i: Double) => i.toInt})
      .setInputCol("input").setOutputCol("ints")
    val toFloat = new FuncTransformer(udf { (i: Int) => i.toFloat})
      .setInputCol("ints").setOutputCol("floats")
    val toString = new FuncTransformer(udf { (i: Float) => i.toString})
      .setInputCol("floats").setOutputCol("strings")
    val toArray = new FuncTransformer(udf { (i: String) => Array(i)})
      .setInputCol("strings").setOutputCol("arrays")
    val toLong = new FuncTransformer(udf { (i: Seq[String]) => i.head.toLong})
      .setInputCol("arrays").setOutputCol("longs")
    val pipeline = new Pipeline()
      .setStages(Array(toInt, toFloat, toString, toArray, toLong))
    val pipelineModel = pipeline.fit(df)
    assert(pipelineModel.transform(df).schema("longs").dataType == LongType)
  }

  test("FuncTransformer throws friendly exception") {
    withClue("transforming wrong input types") {
      val df = Seq(-1.0, 0.0, 1.0, 2.0).toDF("input")
      val e: IllegalArgumentException = intercept[IllegalArgumentException] {
        val tolo = new FuncTransformer(udf { (s: String) => s.toLowerCase() })
        tolo.transform(df)
      }
      assert(e.getMessage.contains("data type mismatch"))
    }

    withClue("udf with 0 inputs") {
      val df = Seq(-1.0, 0.0, 1.0, 2.0).toDF("input")
      val e: IllegalArgumentException = intercept[IllegalArgumentException] {
        val t = new FuncTransformer(udf { () => "s" })
        t.transform(df)
      }
      assert(e.getMessage.contains("FuncTransformer only supports udf with one input"))
    }

    withClue("udf with 2 inputs") {
      val df = Seq(-1.0, 0.0, 1.0, 2.0).toDF("input")
      val e: IllegalArgumentException = intercept[IllegalArgumentException] {
        val t = new FuncTransformer(udf { (i: Int, j: Int) => "s" })
        t.transform(df)
      }
      assert(e.getMessage.contains("FuncTransformer only supports udf with one input"))
    }
  }

  test("FuncTransformer in pipeline") {
    val df = Seq(0.0, 1.0, 2.0, 4.0).toDF("input")
    val doubleToVector = new FuncTransformer( udf { (i: Double) => Vectors.dense(i) })
      .setInputCol("input").setOutputCol("vectors")
    val scaler = new MinMaxScaler()
      .setInputCol("vectors").setOutputCol("scaled")
    val vecToDouble = new  FuncTransformer(udf { (v: Vector) => v(0) })
      .setInputCol("scaled").setOutputCol("output")
    val pipeline = new Pipeline()
      .setStages(Array(doubleToVector, scaler, vecToDouble))
    val pipelineModel = pipeline.fit(df)
    val expectDF = Seq(0.0, 0.25, 0.5, 1.0).toDF("output")
    assert(pipelineModel.transform(df).select("output").collect().toSet.equals(
      expectDF.collect().toSet))
  }

  test("read/write") {
    val df = Seq(0.0, 3.1).toDF("myInputCol")
    val t = new FuncTransformer(udf { (i: Double) => i + 1 })
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    val loaded = testDefaultReadWrite(t)
    val expectDF = Seq(1.0, 4.1).toDF("myOutputCol")
    assert(loaded.transform(df).select("myOutputCol").collect().toSet.equals(
      expectDF.collect().toSet))
  }
}
