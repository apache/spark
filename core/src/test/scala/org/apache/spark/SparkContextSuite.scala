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

import com.google.common.base.Charsets._
import com.google.common.io.Files

import org.scalatest.FunSuite

import org.apache.hadoop.io.BytesWritable

import org.apache.spark.util.Utils

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

    val converter = WritableConverter.bytesWritableConverter()
    val byteArray = converter.convert(bytesWritable)
    assert(byteArray.length === 5)

    bytesWritable.set(inputArray, 0, 0)
    val byteArray2 = converter.convert(bytesWritable)
    assert(byteArray2.length === 0)
  }
  
  test("addFile works") {
    val file1 = File.createTempFile("someprefix1", "somesuffix1")
    val absolutePath1 = file1.getAbsolutePath

    val pluto = Utils.createTempDir()
    val file2 = File.createTempFile("someprefix2", "somesuffix2", pluto)
    val relativePath = file2.getParent + "/../" + file2.getParentFile.getName + "/" + file2.getName
    val absolutePath2 = file2.getAbsolutePath

    try {
      Files.write("somewords1", file1, UTF_8)
      Files.write("somewords2", file2, UTF_8)
      val length1 = file1.length()
      val length2 = file2.length()

      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      sc.addFile(file1.getAbsolutePath)
      sc.addFile(relativePath)
      sc.parallelize(Array(1), 1).map(x => {
        val gotten1 = new File(SparkFiles.get(file1.getName))
        val gotten2 = new File(SparkFiles.get(file2.getName))
        if (!gotten1.exists()) {
          throw new SparkException("file doesn't exist : " + absolutePath1)
        }
        if (!gotten2.exists()) {
          throw new SparkException("file doesn't exist : " + absolutePath2)
        }

        if (length1 != gotten1.length()) {
          throw new SparkException(
            s"file has different length $length1 than added file ${gotten1.length()} : " + absolutePath1)
        }
        if (length2 != gotten2.length()) {
          throw new SparkException(
            s"file has different length $length2 than added file ${gotten2.length()} : " + absolutePath2)
        }

        if (absolutePath1 == gotten1.getAbsolutePath) {
          throw new SparkException("file should have been copied :" + absolutePath1)
        }
        if (absolutePath2 == gotten2.getAbsolutePath) {
          throw new SparkException("file should have been copied : " + absolutePath2)
        }
        x
      }).count()
    } finally {
      sc.stop()
    }
  }
  
  test("addFile recursive works") {
    val pluto = Utils.createTempDir()
    val neptune = Utils.createTempDir(pluto.getAbsolutePath)
    val saturn = Utils.createTempDir(neptune.getAbsolutePath)
    val alien1 = File.createTempFile("alien", "1", neptune)
    val alien2 = File.createTempFile("alien", "2", saturn)

    try {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      sc.addFile(neptune.getAbsolutePath, true)
      sc.parallelize(Array(1), 1).map(x => {
        val sep = File.separator
        if (!new File(SparkFiles.get(neptune.getName + sep + alien1.getName)).exists()) {
          throw new SparkException("can't access file under root added directory")
        }
        if (!new File(SparkFiles.get(neptune.getName + sep + saturn.getName + sep + alien2.getName))
            .exists()) {
          throw new SparkException("can't access file in nested directory")
        }
        if (new File(SparkFiles.get(pluto.getName + sep + neptune.getName + sep + alien1.getName))
            .exists()) {
          throw new SparkException("file exists that shouldn't")
        }
        x
      }).count()
    } finally {
      sc.stop()
    }
  }

  test("addFile recursive can't add directories by default") {
    val dir = Utils.createTempDir()

    try {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      intercept[SparkException] {
        sc.addFile(dir.getAbsolutePath)
      }
    } finally {
      sc.stop()
    }
  }
}
