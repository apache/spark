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

import java.io.File
import java.util.concurrent.TimeUnit

import com.google.common.base.Charsets._
import com.google.common.io.Files

import org.scalatest.FunSuite

import org.apache.hadoop.io.BytesWritable

import org.apache.spark.util.Utils
import org.apache.spark.SparkContext._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class SparkContextSuite extends FunSuite with LocalSparkContext {

  test("Only one SparkContext may be active at a time") {
    // Regression test for SPARK-4180
    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set("spark.driver.allowMultipleContexts", "false")
    sc = new SparkContext(conf)
    // A SparkContext is already running, so we shouldn't be able to create a second one
    intercept[SparkException] { new SparkContext(conf) }
    // After stopping the running context, we should be able to create a new one
    resetSparkContext()
    sc = new SparkContext(conf)
  }

  test("Can still construct a new SparkContext after failing to construct a previous one") {
    val conf = new SparkConf().set("spark.driver.allowMultipleContexts", "false")
    // This is an invalid configuration (no app name or master URL)
    intercept[SparkException] {
      new SparkContext(conf)
    }
    // Even though those earlier calls failed, we should still be able to create a new context
    sc = new SparkContext(conf.setMaster("local").setAppName("test"))
  }

  test("Check for multiple SparkContexts can be disabled via undocumented debug option") {
    var secondSparkContext: SparkContext = null
    try {
      val conf = new SparkConf().setAppName("test").setMaster("local")
        .set("spark.driver.allowMultipleContexts", "true")
      sc = new SparkContext(conf)
      secondSparkContext = new SparkContext(conf)
    } finally {
      Option(secondSparkContext).foreach(_.stop())
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

  test("Cancelling job group should not cause SparkContext to shutdown (SPARK-6414)") {
    try {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      val future = sc.parallelize(Seq(0)).foreachAsync(_ => {Thread.sleep(1000L)})
      sc.cancelJobGroup("nonExistGroupId")
      Await.ready(future, Duration(2, TimeUnit.SECONDS))

      // In SPARK-6414, sc.cancelJobGroup will cause NullPointerException and cause
      // SparkContext to shutdown, so the following assertion will fail.
      assert(sc.parallelize(1 to 10).count() == 10L)
    } finally {
      sc.stop()
    }
  }
}
