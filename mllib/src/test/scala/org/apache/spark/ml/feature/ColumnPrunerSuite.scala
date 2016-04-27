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

class ColumnPrunerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("params") {
    ParamsSuite.checkParams(new ColumnPruner())
  }

  test("input columns are not defined") {
    val ds = sqlContext.createDataFrame(Seq((1, 1), (2, 2))).toDF("x", "y")
    val cp = new ColumnPruner()
    val msg = "Input cols must be defined first."
    val thrownTransform = intercept[IllegalArgumentException] {
      cp.transform(ds)
    }
    assert(thrownTransform.getMessage contains msg)
    val thrownTransformSchema = intercept[IllegalArgumentException] {
      cp.transformSchema(ds.schema)
    }
    assert(thrownTransformSchema.getMessage contains msg)
  }

  test("input columns contain duplicates") {
    val ds = sqlContext.createDataFrame(Seq((1, 1), (2, 2))).toDF("x", "y")
    val cp = new ColumnPruner().setInputCols(Array("x", "x"))
    val msg = "Input cols must be distinct."
    val thrownTransform = intercept[IllegalArgumentException] {
      cp.transform(ds)
    }
    assert(thrownTransform.getMessage contains msg)
    val thrownTransformSchema = intercept[IllegalArgumentException] {
      cp.transformSchema(ds.schema)
    }
    assert(thrownTransformSchema.getMessage contains msg)
  }

  test("remove the specified columns when transforming") {
    val ds = sqlContext.createDataFrame(Seq((0, 1, 2), (3, 4, 5))).toDF("x", "y", "z")
    val cp = new ColumnPruner().setInputCols(Array("x"))
    val actual = cp.transform(ds)
    val expected = sqlContext.createDataFrame(Seq((1, 2), (4, 5))).toDF("y", "z")

    assert(expected.schema === actual.schema)
    assert(expected.collect() === actual.collect())
  }

  test("remove the specified columns from the schema when transforming the schema") {
    val ds = sqlContext.createDataFrame(Seq((0, 1, 2), (3, 4, 5))).toDF("x", "y", "z")
    val cp = new ColumnPruner().setInputCols(Array("x"))
    val actual = cp.transformSchema(ds.schema)
    val expected = sqlContext.createDataFrame(Seq((1, 2), (4, 5))).toDF("y", "z").schema

    assert(expected === actual)
  }

  test("read/write") {
    testDefaultReadWrite(new ColumnPruner().setInputCols(Array("x")))
  }
}
