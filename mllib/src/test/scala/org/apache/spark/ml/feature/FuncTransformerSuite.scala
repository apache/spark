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

import java.sql.Date

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.types._

class FuncTransformerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest  {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new FuncTransformer((i: Double) => i + 1))
  }

  test("FuncTransformer for label conversion") {
    val labelConverter = new FuncTransformer((i: Double) => if (i >= 1) 1.0 else 0.0)
    val df = Seq(-1.0, 0.0, 1.0, 2.0).toDF("input")
    val expectDF = Seq(0.0, 0.0, 1.0, 1.0).toDF("output")
    assert(labelConverter.transform(df).select("output").collect().toSet.equals(
      expectDF.collect().toSet))
  }

  test("FuncTransformer for Array Indexing") {
    val arrayIndexer = new FuncTransformer((s: Seq[Int]) => s.head)
    val df = Seq(Array(0, 1, 2), Array(3, 4, 5)).toDF("input")
    val expectDF = Seq(0, 3).toDF("output")
    assert(arrayIndexer.transform(df).select("output").collect().toSet.equals(
      expectDF.collect().toSet))
  }

  test("FuncTransformer automatically infer output DataType") {
    assert(new FuncTransformer((i: Double) => Vectors.dense(i)).outputDataType === new VectorUDT)
    assert(new FuncTransformer((i: Double) => i + 1).outputDataType === DoubleType)
    assert(new FuncTransformer((i: Double) => i.toString).outputDataType === StringType)
    assert(new FuncTransformer((i: Double) => i.toInt).outputDataType === IntegerType)
    assert(new FuncTransformer((i: Double) => false).outputDataType === BooleanType)
    assert(new FuncTransformer((i: Double) => Array(i, i + 1.0)).outputDataType ===
      new ArrayType(DoubleType, false))
    assert(new FuncTransformer((i: Double) => Date.valueOf("2017-1-1")).outputDataType ===
     DateType)
  }

  test("FuncTransformer throws friendly exception when output data type error") {
    withClue("FuncTransformer throws friendly exception when output data type error") {
      val e: UnsupportedOperationException = intercept[UnsupportedOperationException] {
        new FuncTransformer((i: Double) => new Object)
      }
      assert(e.getMessage.contains("outputDataType cannot be automatically inferred"))
    }
  }

  test("FuncTransformer wrong input type") {
    val df = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -1.0)),
      (1, Vectors.dense(2.0, 1.1, 1.0)),
      (2, Vectors.dense(3.0, 10.1, 3.0))
    )).toDF("id", "input")
    val labelConverter = new FuncTransformer((i: Double) => i)
    intercept[IllegalArgumentException] {
      labelConverter.transformSchema(df.schema)
    }
    intercept[IllegalArgumentException] {
      labelConverter.transform(df)
    }
  }

  test("FuncTransformer in pipeline") {
    val df = Seq(0.0, 1.0, 2.0, 3.0).toDF("input")
    val labelConverter = new FuncTransformer((i: Double) => if (i >= 1) 1.0 else 0.0)
      .setInputCol("input").setOutputCol("binary")
    val doubleToVector = new FuncTransformer((i: Double) => Vectors.dense(i))
      .setInputCol("binary").setOutputCol("vectors")
    val vectorIndexer = new FuncTransformer((v: Vector) => v(0))
      .setInputCol("vectors").setOutputCol("output")
    val pipeline = new Pipeline().setStages(Array(labelConverter, doubleToVector, vectorIndexer))
    val pipelineModel = pipeline.fit(df)
    pipelineModel.transform(df).show()

    val expectDF = Seq(0.0, 1.0, 1.0, 1.0).toDF("output")
    assert(pipelineModel.transform(df).select("output").collect().toSet.equals(
      expectDF.collect().toSet))
  }

  test("read/write") {
    val t = new FuncTransformer((i: Double) => i + 1)
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    testDefaultReadWrite(t)
  }
}
