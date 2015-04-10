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

import org.scalatest.FunSuite

import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Row, SQLContext}

class VectorAssemblerSuite extends FunSuite with MLlibTestSparkContext {

  @transient var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
  }

  test("assemble") {
    import org.apache.spark.ml.feature.VectorAssembler.assemble
    assert(assemble(0.0) === Vectors.sparse(1, Array.empty, Array.empty))
    assert(assemble(0.0, 1.0) === Vectors.sparse(2, Array(1), Array(1.0)))
    val dv = Vectors.dense(2.0, 0.0)
    assert(assemble(0.0, dv, 1.0) === Vectors.sparse(4, Array(1, 3), Array(2.0, 1.0)))
    val sv = Vectors.sparse(2, Array(0, 1), Array(3.0, 4.0))
    assert(assemble(0.0, dv, 1.0, sv) ===
      Vectors.sparse(6, Array(1, 3, 4, 5), Array(2.0, 1.0, 3.0, 4.0)))
    for (v <- Seq(1, "a", null)) {
      intercept[SparkException](assemble(v))
      intercept[SparkException](assemble(1.0, v))
    }
  }

  test("VectorAssembler") {
    val df = sqlContext.createDataFrame(Seq(
      (0, 0.0, Vectors.dense(1.0, 2.0), "a", Vectors.sparse(2, Array(1), Array(3.0)), 10L)
    )).toDF("id", "x", "y", "name", "z", "n")
    val assembler = new VectorAssembler()
      .setInputCols(Array("x", "y", "z", "n"))
      .setOutputCol("features")
    assembler.transform(df).select("features").collect().foreach {
      case Row(v: Vector) =>
        assert(v === Vectors.sparse(6, Array(1, 2, 4, 5), Array(1.0, 2.0, 3.0, 10.0)))
    }
  }
}
