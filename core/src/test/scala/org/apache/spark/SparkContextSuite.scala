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

import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}
import org.apache.spark.util.Utils

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class SparkContextSuite extends SparkFunSuite with LocalSparkContext {

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

  test("Test getOrCreate") {
    var sc2: SparkContext = null
    SparkContext.clearActiveContext()
    val conf = new SparkConf().setAppName("test").setMaster("local")

    sc = SparkContext.getOrCreate(conf)

    assert(sc.getConf.get("spark.app.name").equals("test"))
    sc2 = SparkContext.getOrCreate(new SparkConf().setAppName("test2").setMaster("local"))
    assert(sc2.getConf.get("spark.app.name").equals("test"))
    assert(sc === sc2)
    assert(sc eq sc2)

    // Try creating second context to confirm that it's still possible, if desired
    sc2 = new SparkContext(new SparkConf().setAppName("test3").setMaster("local")
        .set("spark.driver.allowMultipleContexts", "true"))

    sc2.stop()
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
    val dir = Utils.createTempDir()

    val file1 = File.createTempFile("someprefix1", "somesuffix1", dir)
    val absolutePath1 = file1.getAbsolutePath

    val file2 = File.createTempFile("someprefix2", "somesuffix2", dir)
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
            s"file has different length $length1 than added file ${gotten1.length()} : " +
              absolutePath1)
        }
        if (length2 != gotten2.length()) {
          throw new SparkException(
            s"file has different length $length2 than added file ${gotten2.length()} : " +
              absolutePath2)
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

  test("Comma separated paths for newAPIHadoopFile/wholeTextFiles/binaryFiles (SPARK-7155)") {
    // Regression test for SPARK-7155
    // dir1 and dir2 are used for wholeTextFiles and binaryFiles
    val dir1 = Utils.createTempDir()
    val dir2 = Utils.createTempDir()

    val dirpath1 = dir1.getAbsolutePath
    val dirpath2 = dir2.getAbsolutePath

    // file1 and file2 are placed inside dir1, they are also used for
    // textFile, hadoopFile, and newAPIHadoopFile
    // file3, file4 and file5 are placed inside dir2, they are used for
    // textFile, hadoopFile, and newAPIHadoopFile as well
    val file1 = new File(dir1, "part-00000")
    val file2 = new File(dir1, "part-00001")
    val file3 = new File(dir2, "part-00000")
    val file4 = new File(dir2, "part-00001")
    val file5 = new File(dir2, "part-00002")

    val filepath1 = file1.getAbsolutePath
    val filepath2 = file2.getAbsolutePath
    val filepath3 = file3.getAbsolutePath
    val filepath4 = file4.getAbsolutePath
    val filepath5 = file5.getAbsolutePath


    try {
      // Create 5 text files.
      Files.write("someline1 in file1\nsomeline2 in file1\nsomeline3 in file1", file1, UTF_8)
      Files.write("someline1 in file2\nsomeline2 in file2", file2, UTF_8)
      Files.write("someline1 in file3", file3, UTF_8)
      Files.write("someline1 in file4\nsomeline2 in file4", file4, UTF_8)
      Files.write("someline1 in file2\nsomeline2 in file5", file5, UTF_8)

      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))

      // Test textFile, hadoopFile, and newAPIHadoopFile for file1 and file2
      assert(sc.textFile(filepath1 + "," + filepath2).count() == 5L)
      assert(sc.hadoopFile(filepath1 + "," + filepath2,
        classOf[TextInputFormat], classOf[LongWritable], classOf[Text]).count() == 5L)
      assert(sc.newAPIHadoopFile(filepath1 + "," + filepath2,
        classOf[NewTextInputFormat], classOf[LongWritable], classOf[Text]).count() == 5L)

      // Test textFile, hadoopFile, and newAPIHadoopFile for file3, file4, and file5
      assert(sc.textFile(filepath3 + "," + filepath4 + "," + filepath5).count() == 5L)
      assert(sc.hadoopFile(filepath3 + "," + filepath4 + "," + filepath5,
               classOf[TextInputFormat], classOf[LongWritable], classOf[Text]).count() == 5L)
      assert(sc.newAPIHadoopFile(filepath3 + "," + filepath4 + "," + filepath5,
               classOf[NewTextInputFormat], classOf[LongWritable], classOf[Text]).count() == 5L)

      // Test wholeTextFiles, and binaryFiles for dir1 and dir2
      assert(sc.wholeTextFiles(dirpath1 + "," + dirpath2).count() == 5L)
      assert(sc.binaryFiles(dirpath1 + "," + dirpath2).count() == 5L)

    } finally {
      sc.stop()
    }
  }
}
