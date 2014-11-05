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

package org.apache.spark

import org.scalatest.FunSuite

import org.apache.hadoop.io.BytesWritable

class SparkContextSuite extends FunSuite with LocalSparkContext {

  test("Only one SparkContext may be active at a time") {
    // Regression test for SPARK-4180
    val conf = new SparkConf().setAppName("test").setMaster("local")
    sc = new SparkContext(conf)
    // A SparkContext is already running, so we shouldn't be able to create a second one
    intercept[SparkException] { new SparkContext(conf) }
    // After stopping the running context, we should be able to create a new one
    resetSparkContext()
    sc = new SparkContext(conf)
  }

  test("Can still construct a new SparkContext after failing due to missing app name or master") {
    val missingMaster = new SparkConf()
    val missingAppName = missingMaster.clone.setMaster("local")
    val validConf = missingAppName.clone.setAppName("test")
    // We shouldn't be able to construct SparkContexts because these are invalid configurations
    intercept[SparkException] { new SparkContext(missingMaster) }
    intercept[SparkException] { new SparkContext(missingAppName) }
    // Even though those earlier calls failed, we should still be able to create a new SparkContext
    sc = new SparkContext(validConf)
  }

  test("Check for multiple SparkContexts can be disabled via undocumented debug option") {
    val propertyName = "spark.driver.disableMultipleSparkContextsErrorChecking"
    val originalPropertyValue = System.getProperty(propertyName)
    var secondSparkContext: SparkContext = null
    try {
      System.setProperty(propertyName, "true")
      val conf = new SparkConf().setAppName("test").setMaster("local")
      sc = new SparkContext(conf)
      secondSparkContext = new SparkContext(conf)
    } finally {
      if (secondSparkContext != null) {
        secondSparkContext.stop()
      }
      if (originalPropertyValue != null) {
        System.setProperty(propertyName, originalPropertyValue)
      } else {
        System.clearProperty(propertyName)
      }
    }
  }

  test("BytesWritable implicit conversion is correct") {
    // Regression test for SPARK-3121
    val bytesWritable = new BytesWritable()
    val inputArray = (1 to 10).map(_.toByte).toArray
    bytesWritable.set(inputArray, 0, 10)
    bytesWritable.set(inputArray, 0, 5)

    val converter = SparkContext.bytesWritableConverter()
    val byteArray = converter.convert(bytesWritable)
    assert(byteArray.length === 5)

    bytesWritable.set(inputArray, 0, 0)
    val byteArray2 = converter.convert(bytesWritable)
    assert(byteArray2.length === 0)
  }
}
