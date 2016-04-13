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

import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext

class StratifiedSamplerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("params") {
    ParamsSuite.checkParams(new Binarizer)
  }

  test("StratifiedSampling on String, Int and Boolean label") {
    Logger.getRootLogger.setLevel(Level.WARN)
    val df = sqlContext.createDataFrame(Seq(
      (0, "0", false),
      (0, "0", false),
      (1, "1", true),
      (1, "1", true)
    )).toDF("int", "str", "bool")
    val strMap = Map("1" -> 0.5, "0" -> 0.1)
    assert(new StratifiedSampler(strMap).setLabel("str").transform(df).schema == df.schema)

    val intMap = Map(1 -> 0.5, 0 -> 0.1)
    assert(new StratifiedSampler(intMap).setLabel("str").transform(df).schema == df.schema)

    val boolMap = Map(true -> 0.5, false -> 0.1)
    assert(new StratifiedSampler(boolMap).setLabel("str").transform(df).schema == df.schema)
  }

  test("StratifiedSampling read/write") {
    val t = new StratifiedSampler(Map("1" -> 0.5, "2" -> 0.1))
      .setLabel("myLabel")
    val newInstance = testDefaultReadWrite(t)
    assert(t.fractions == newInstance.fractions)
  }
}
