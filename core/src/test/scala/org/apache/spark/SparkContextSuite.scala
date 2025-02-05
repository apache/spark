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
import java.net.{MalformedURLException, URI}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, Semaphore, TimeUnit}

import scala.concurrent.duration._
import scala.io.Source

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}
import org.apache.logging.log4j.{Level, LogManager}
import org.json4s.{DefaultFormats, Extraction}
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers._

import org.apache.spark.TestUtils._
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Tests._
import org.apache.spark.internal.config.UI._
import org.apache.spark.resource.ResourceAllocation
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.resource.TestResourceIDs._
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorMetricsUpdate, SparkListenerJobStart, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

class SparkContextSuite extends SparkFunSuite with LocalSparkContext with Eventually {

  test("Only one SparkContext may be active at a time") {
    // Regression test for SPARK-4180
    val conf = new SparkConf().setAppName("test").setMaster("local")
    sc = new SparkContext(conf)
    val envBefore = SparkEnv.get
    // A SparkContext is already running, so we shouldn't be able to create a second one
    intercept[SparkException] { new SparkContext(conf) }
    val envAfter = SparkEnv.get
    // SparkEnv and other context variables should be the same
    assert(envBefore == envAfter)
    // After stopping the running context, we should be able to create a new one
    resetSparkContext()
    sc = new SparkContext(conf)
  }

  test("Can still construct a new SparkContext after failing to construct a previous one") {
    val conf = new SparkConf()
    // This is an invalid configuration (no app name or master URL)
    intercept[SparkException] {
      new SparkContext(conf)
    }
    // Even though those earlier calls failed, we should still be able to create a new context
    sc = new SparkContext(conf.setMaster("local").setAppName("test"))
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

  test("basic case for addFile and listFiles") {
    withTempDir { dir =>
      val file1 = File.createTempFile("someprefix1", "somesuffix1", dir)
      val absolutePath1 = file1.getAbsolutePath

      val file2 = File.createTempFile("someprefix2", "somesuffix2", dir)
      val relativePath = file2.getParent + "/../" + file2.getParentFile.getName +
        "/" + file2.getName
      val absolutePath2 = file2.getAbsolutePath

      try {
        Files.asCharSink(file1, StandardCharsets.UTF_8).write("somewords1")
        Files.asCharSink(file2, StandardCharsets.UTF_8).write("somewords2")
        val length1 = file1.length()
        val length2 = file2.length()

        sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
        sc.addFile(file1.getAbsolutePath)
        sc.addFile(relativePath)
        sc.parallelize(Array(1).toImmutableArraySeq, 1).map(x => {
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
        assert(sc.listFiles().count(_.contains("somesuffix1")) == 1)
      } finally {
        sc.stop()
      }
    }
  }

  test("SPARK-33530: basic case for addArchive and listArchives") {
    withTempDir { dir =>
      val file1 = File.createTempFile("someprefix1", "somesuffix1", dir)
      val file2 = File.createTempFile("someprefix2", "somesuffix2", dir)
      val file3 = File.createTempFile("someprefix3", "somesuffix3", dir)
      val file4 = File.createTempFile("someprefix4", "somesuffix4", dir)

      val jarFile = new File(dir, "test!@$jar.jar")
      val zipFile = new File(dir, "test-zip.zip")
      val relativePath1 =
        s"${zipFile.getParent}/../${zipFile.getParentFile.getName}/${zipFile.getName}"
      val relativePath2 =
        s"${jarFile.getParent}/../${jarFile.getParentFile.getName}/${jarFile.getName}#zoo"

      try {
        Files.asCharSink(file1, StandardCharsets.UTF_8).write("somewords1")
        Files.asCharSink(file2, StandardCharsets.UTF_8).write("somewords22")
        Files.asCharSink(file3, StandardCharsets.UTF_8).write("somewords333")
        Files.asCharSink(file4, StandardCharsets.UTF_8).write("somewords4444")
        val length1 = file1.length()
        val length2 = file2.length()
        val length3 = file1.length()
        val length4 = file2.length()

        createJar(Seq(file1, file2), jarFile)
        createJar(Seq(file3, file4), zipFile)

        sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
        sc.addArchive(jarFile.getAbsolutePath)
        sc.addArchive(relativePath1)
        sc.addArchive(s"${jarFile.getAbsolutePath}#foo")
        sc.addArchive(s"${zipFile.getAbsolutePath}#bar")
        sc.addArchive(relativePath2)

        sc.parallelize(Array(1).toImmutableArraySeq, 1).map { x =>
          val gotten1 = new File(SparkFiles.get(jarFile.getName))
          val gotten2 = new File(SparkFiles.get(zipFile.getName))
          val gotten3 = new File(SparkFiles.get("foo"))
          val gotten4 = new File(SparkFiles.get("bar"))
          val gotten5 = new File(SparkFiles.get("zoo"))

          Seq(gotten1, gotten2, gotten3, gotten4, gotten5).foreach { gotten =>
            if (!gotten.exists()) {
              throw new SparkException(s"The archive doesn't exist: ${gotten.getAbsolutePath}")
            }
            if (!gotten.isDirectory) {
              throw new SparkException(s"The archive was not unpacked: ${gotten.getAbsolutePath}")
            }
          }

          // Jars
          Seq(gotten1, gotten3, gotten5).foreach { gotten =>
            val actualLength1 = new File(gotten, file1.getName).length()
            val actualLength2 = new File(gotten, file2.getName).length()
            if (actualLength1 != length1 || actualLength2 != length2) {
              s"Unpacked files have different lengths $actualLength1 and $actualLength2. at " +
                s"${gotten.getAbsolutePath}. They should be $length1 and $length2."
            }
          }

          // Zip
          Seq(gotten2, gotten4).foreach { gotten =>
            val actualLength3 = new File(gotten, file1.getName).length()
            val actualLength4 = new File(gotten, file2.getName).length()
            if (actualLength3 != length3 || actualLength4 != length4) {
              s"Unpacked files have different lengths $actualLength3 and $actualLength4. at " +
                s"${gotten.getAbsolutePath}. They should be $length3 and $length4."
            }
          }
          x
        }.count()
        assert(sc.listArchives().count(_.endsWith("test!@$jar.jar")) == 1)
        assert(sc.listArchives().count(_.contains("test-zip.zip")) == 2)
      } finally {
        sc.stop()
      }
    }
  }

  test("add and list jar files") {
    val jarPath = Thread.currentThread().getContextClassLoader.getResource("TestUDTF.jar")
    try {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      sc.addJar(jarPath.toString)
      assert(sc.listJars().count(_.contains("TestUDTF.jar")) == 1)
    } finally {
      sc.stop()
    }
  }

  test("add FS jar files not exists") {
    try {
      val jarPath = "hdfs:///no/path/to/TestUDTF.jar"
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      sc.addJar(jarPath)
      assert(sc.listJars().forall(!_.contains("TestUDTF.jar")))
    } finally {
      sc.stop()
    }
  }

  test("SPARK-17650: malformed url's throw exceptions before bricking Executors") {
    try {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      Seq("http", "https", "ftp").foreach { scheme =>
        val badURL = s"$scheme://user:pwd/path"
        val e1 = intercept[MalformedURLException] {
          sc.addFile(badURL)
        }
        assert(e1.getMessage.contains(badURL))
        val e2 = intercept[MalformedURLException] {
          sc.addJar(badURL)
        }
        assert(e2.getMessage.contains(badURL))
        assert(sc.allAddedFiles.isEmpty)
        assert(sc.allAddedJars.isEmpty)
      }
    } finally {
      sc.stop()
    }
  }

  test("addFile recursive works") {
    withTempDir { pluto =>
      val neptune = Utils.createTempDir(pluto.getAbsolutePath)
      val saturn = Utils.createTempDir(neptune.getAbsolutePath)
      val alien1 = File.createTempFile("alien", "1", neptune)
      val alien2 = File.createTempFile("alien", "2", saturn)

      try {
        sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
        sc.addFile(neptune.getAbsolutePath, true)
        sc.parallelize(Array(1).toImmutableArraySeq, 1).map(x => {
          val sep = File.separator
          if (!new File(SparkFiles.get(neptune.getName + sep + alien1.getName)).exists()) {
            throw new SparkException("can't access file under root added directory")
          }
          if (!new File(SparkFiles.get(
            neptune.getName + sep + saturn.getName + sep + alien2.getName)).exists()) {
            throw new SparkException("can't access file in nested directory")
          }
          if (new File(SparkFiles.get(
            pluto.getName + sep + neptune.getName + sep + alien1.getName)).exists()) {
            throw new SparkException("file exists that shouldn't")
          }
          x
        }).count()
      } finally {
        sc.stop()
      }
    }
  }

  test("SPARK-30126: addFile when file path contains spaces with recursive works") {
    withTempDir { dir =>
      try {
        val sep = File.separator
        val tmpDir = Utils.createTempDir(dir.getAbsolutePath + sep + "test space")
        val tmpConfFile1 = File.createTempFile("test file", ".conf", tmpDir)

        sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
        sc.addFile(tmpConfFile1.getAbsolutePath, true)

        assert(sc.listFiles().size == 1)
        assert(sc.listFiles().head.contains(new Path(tmpConfFile1.getName).toUri.toString))
      } finally {
        sc.stop()
      }
    }
  }

  test("SPARK-30126: addFile when file path contains spaces without recursive works") {
    withTempDir { dir =>
      try {
          val sep = File.separator
          val tmpDir = Utils.createTempDir(dir.getAbsolutePath + sep + "test space")
          val tmpConfFile2 = File.createTempFile("test file", ".conf", tmpDir)

          sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
          sc.addFile(tmpConfFile2.getAbsolutePath)

          assert(sc.listFiles().size == 1)
          assert(sc.listFiles().head.contains(new Path(tmpConfFile2.getName).toUri.toString))
      } finally {
        sc.stop()
      }
    }
  }

  test("addFile recursive can't add directories by default") {
    withTempDir { dir =>
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

  test("cannot call addFile with different paths that have the same filename") {
    withTempDir { dir =>
      val subdir1 = new File(dir, "subdir1")
      val subdir2 = new File(dir, "subdir2")
      assert(subdir1.mkdir())
      assert(subdir2.mkdir())
      val file1 = new File(subdir1, "file")
      val file2 = new File(subdir2, "file")
      Files.asCharSink(file1, StandardCharsets.UTF_8).write("old")
      Files.asCharSink(file2, StandardCharsets.UTF_8).write("new")
      sc = new SparkContext("local-cluster[1,1,1024]", "test")
      sc.addFile(file1.getAbsolutePath)
      def getAddedFileContents(): String = {
        sc.parallelize(Seq(0)).map { _ =>
          Utils.tryWithResource(Source.fromFile(SparkFiles.get("file")))(_.mkString)
        }.first()
      }
      assert(getAddedFileContents() === "old")
      intercept[IllegalArgumentException] {
        sc.addFile(file2.getAbsolutePath)
      }
      assert(getAddedFileContents() === "old")
    }
  }

  // Regression tests for SPARK-16787
  for (
    schedulingMode <- Seq("local-mode", "non-local-mode");
    method <- Seq("addJar", "addFile")
  ) {
    val jarPath = Thread.currentThread().getContextClassLoader.getResource("TestUDTF.jar").toString
    val master = schedulingMode match {
      case "local-mode" => "local"
      case "non-local-mode" => "local-cluster[1,1,1024]"
    }
    test(s"$method can be called twice with same file in $schedulingMode (SPARK-16787)") {
      sc = new SparkContext(master, "test")
      method match {
        case "addJar" =>
          sc.addJar(jarPath)
          sc.addJar(jarPath)
        case "addFile" =>
          sc.addFile(jarPath)
          sc.addFile(jarPath)
      }
    }
  }

  test("SPARK-30126: add jar when path contains spaces") {
    withTempDir { dir =>
       try {
          val sep = File.separator
          val tmpDir = Utils.createTempDir(dir.getAbsolutePath + sep + "test space")
          val tmpJar = File.createTempFile("test", ".jar", tmpDir)

          sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
          sc.addJar(tmpJar.getAbsolutePath)

          assert(sc.listJars().size == 1)
          assert(sc.listJars().head.contains(tmpJar.getName))
       } finally {
         sc.stop()
       }
    }
  }

  test("add jar with invalid path") {
    withTempDir { tmpDir =>
      val tmpJar = File.createTempFile("test", ".jar", tmpDir)

      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      sc.addJar(tmpJar.getAbsolutePath)

      // Invalid jar path will only print the error log, will not add to file server.
      sc.addJar("dummy.jar")
      sc.addJar("")
      sc.addJar(tmpDir.getAbsolutePath)

      assert(sc.listJars().size == 1)
      assert(sc.listJars().head.contains(tmpJar.getName))
    }
  }

  test("SPARK-22585 addJar argument without scheme is interpreted literally without url decoding") {
    withTempDir { dir =>
      val tmpDir = new File(dir, "host%3A443")
      tmpDir.mkdirs()
      val tmpJar = File.createTempFile("t%2F", ".jar", tmpDir)

      sc = new SparkContext("local", "test")

      sc.addJar(tmpJar.getAbsolutePath)
      assert(sc.listJars().size === 1)
    }
  }

  test("Cancelling job group should not cause SparkContext to shutdown (SPARK-6414)") {
    try {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      val future = sc.parallelize(Seq(0)).foreachAsync(_ => {Thread.sleep(1000L)})
      sc.cancelJobGroup("nonExistGroupId")
      ThreadUtils.awaitReady(future, Duration(2, TimeUnit.SECONDS))

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
    withTempDir { dir1 =>
      withTempDir { dir2 =>
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
          Files.asCharSink(file1, StandardCharsets.UTF_8)
            .write("someline1 in file1\nsomeline2 in file1\nsomeline3 in file1")
          Files.asCharSink(file2, StandardCharsets.UTF_8)
            .write("someline1 in file2\nsomeline2 in file2")
          Files.asCharSink(file3, StandardCharsets.UTF_8).write("someline1 in file3")
          Files.asCharSink(file4, StandardCharsets.UTF_8)
            .write("someline1 in file4\nsomeline2 in file4")
          Files.asCharSink(file5, StandardCharsets.UTF_8)
            .write("someline1 in file2\nsomeline2 in file5")

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
  }

  test("Default path for file based RDDs is properly set (SPARK-12517)") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))

    // Test filetextFile, wholeTextFiles, binaryFiles, hadoopFile and
    // newAPIHadoopFile for setting the default path as the RDD name
    val mockPath = "default/path/for/"

    var targetPath = mockPath + "textFile"
    assert(sc.textFile(targetPath).name === targetPath)

    targetPath = mockPath + "wholeTextFiles"
    assert(sc.wholeTextFiles(targetPath).name === targetPath)

    targetPath = mockPath + "binaryFiles"
    assert(sc.binaryFiles(targetPath).name === targetPath)

    targetPath = mockPath + "hadoopFile"
    assert(sc.hadoopFile(targetPath).name === targetPath)

    targetPath = mockPath + "newAPIHadoopFile"
    assert(sc.newAPIHadoopFile(targetPath).name === targetPath)

    sc.stop()
  }

  test("calling multiple sc.stop() must not throw any exception") {
    noException should be thrownBy {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      val cnt = sc.parallelize(1 to 4).count()
      sc.cancelAllJobs()
      sc.stop()
      // call stop second time
      sc.stop()
    }
  }

  test("No exception when both num-executors and dynamic allocation set.") {
    noException should be thrownBy {
      sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local")
        .set(DYN_ALLOCATION_ENABLED, true).set("spark.executor.instances", "6"))
      assert(sc.executorAllocationManager.isEmpty)
      assert(sc.getConf.getInt("spark.executor.instances", 0) === 6)
    }
  }


  test("localProperties are inherited by spawned threads.") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    sc.setLocalProperty("testProperty", "testValue")
    var result = "unset";
    val thread = new Thread() {
      override def run(): Unit = {result = sc.getLocalProperty("testProperty")}
    }
    thread.start()
    thread.join()
    sc.stop()
    assert(result == "testValue")
  }

  test("localProperties do not cross-talk between threads.") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    var result = "unset";
    val thread1 = new Thread() {
      override def run(): Unit = {sc.setLocalProperty("testProperty", "testValue")}}
    // testProperty should be unset and thus return null
    val thread2 = new Thread() {
      override def run(): Unit = {result = sc.getLocalProperty("testProperty")}}
    thread1.start()
    thread1.join()
    thread2.start()
    thread2.join()
    sc.stop()
    assert(result == null)
  }

  test("log level case-insensitive and reset log level") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    val originalLevel = LogManager.getRootLogger().getLevel
    try {
      sc.setLogLevel("debug")
      assert(LogManager.getRootLogger().getLevel === Level.DEBUG)
      sc.setLogLevel("INfo")
      assert(LogManager.getRootLogger().getLevel === Level.INFO)
    } finally {
      sc.setLogLevel(originalLevel.toString)
      assert(LogManager.getRootLogger().getLevel === originalLevel)
      sc.stop()
    }
  }

  test("SPARK-43782: conf to override log level") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local")
      .set(SPARK_LOG_LEVEL, "ERROR"))
    assert(LogManager.getRootLogger().getLevel === Level.ERROR)
    sc.stop()

    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local")
      .set(SPARK_LOG_LEVEL, "TRACE"))
    assert(LogManager.getRootLogger().getLevel === Level.TRACE)
    sc.stop()
  }

  test("register and deregister Spark listener from SparkContext") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    val sparkListener1 = new SparkListener { }
    val sparkListener2 = new SparkListener { }
    sc.addSparkListener(sparkListener1)
    sc.addSparkListener(sparkListener2)
    assert(sc.listenerBus.listeners.contains(sparkListener1))
    assert(sc.listenerBus.listeners.contains(sparkListener2))
    sc.removeSparkListener(sparkListener1)
    assert(!sc.listenerBus.listeners.contains(sparkListener1))
    assert(sc.listenerBus.listeners.contains(sparkListener2))
  }

  test("Cancelling stages/jobs with custom reasons.") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    sc.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "true")
    val REASON = "You shall not pass"

    for (cancelWhat <- Seq("stage", "job")) {
      // This countdown latch used to make sure stage or job canceled in listener
      val latch = new CountDownLatch(1)

      val listener = cancelWhat match {
        case "stage" =>
          new SparkListener {
            override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
              sc.cancelStage(taskStart.stageId, REASON)
              latch.countDown()
            }
          }
        case "job" =>
          new SparkListener {
            override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
              sc.cancelJob(jobStart.jobId, REASON)
              latch.countDown()
            }
          }
      }
      sc.addSparkListener(listener)

      val ex = intercept[SparkException] {
        sc.range(0, 10000L, numSlices = 10).mapPartitions { x =>
          x.synchronized {
            x.wait()
          }
          x
        }.count()
      }

      ex.getCause() match {
        case null =>
          assert(ex.getMessage().contains(REASON))
        case cause: SparkException =>
          assert(cause.getMessage().contains(REASON))
        case cause: Throwable =>
          fail("Expected the cause to be SparkException, got " + cause.toString() + " instead.")
      }

      latch.await(20, TimeUnit.SECONDS)
      eventually(timeout(20.seconds)) {
        assert(sc.statusTracker.getExecutorInfos.map(_.numRunningTasks()).sum == 0)
      }
      sc.removeSparkListener(listener)
    }
  }

  test("client mode with a k8s master url") {
    intercept[SparkException] {
      sc = new SparkContext("k8s://https://host:port", "test", new SparkConf())
    }
  }

  testCancellingTasks("that raise interrupted exception on cancel") {
    Thread.sleep(9999999)
  }

  // SPARK-20217 should not fail stage if task throws non-interrupted exception
  testCancellingTasks("that raise runtime exception on cancel") {
    try {
      Thread.sleep(9999999)
    } catch {
      case t: Throwable =>
        throw new RuntimeException("killed")
    }
  }

  // Launches one task that will block forever. Once the SparkListener detects the task has
  // started, kill and re-schedule it. The second run of the task will complete immediately.
  // If this test times out, then the first version of the task wasn't killed successfully.
  def testCancellingTasks(desc: String)(blockFn: => Unit): Unit = test(s"Killing tasks $desc") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))

    SparkContextSuite.isTaskStarted = false
    SparkContextSuite.taskKilled = false
    SparkContextSuite.taskSucceeded = false

    val listener = new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        eventually(timeout(10.seconds)) {
          assert(SparkContextSuite.isTaskStarted)
        }
        if (!SparkContextSuite.taskKilled) {
          SparkContextSuite.taskKilled = true
          sc.killTaskAttempt(taskStart.taskInfo.taskId, true, "first attempt will hang")
        }
      }
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        if (taskEnd.taskInfo.attemptNumber == 1 && taskEnd.reason == Success) {
          SparkContextSuite.taskSucceeded = true
        }
      }
    }
    sc.addSparkListener(listener)
    eventually(timeout(20.seconds)) {
      sc.parallelize(1 to 1).foreach { x =>
        // first attempt will hang
        if (!SparkContextSuite.isTaskStarted) {
          SparkContextSuite.isTaskStarted = true
          blockFn
        }
        // second attempt succeeds immediately
      }
    }
    eventually(timeout(10.seconds)) {
      assert(SparkContextSuite.taskSucceeded)
    }
  }

  test("SPARK-19446: DebugFilesystem.assertNoOpenStreams should report " +
    "open streams to help debugging") {
    val fs = new DebugFilesystem()
    fs.initialize(new URI("file:///"), new Configuration())
    val file = File.createTempFile("SPARK19446", "temp")
    file.deleteOnExit()
    Files.write(Array.ofDim[Byte](1000), file)
    val path = new Path("file:///" + file.getCanonicalPath)
    val stream = fs.open(path)
    val exc = intercept[RuntimeException] {
      DebugFilesystem.assertNoOpenStreams()
    }
    assert(exc != null)
    assert(exc.getCause() != null)
    stream.close()
  }

  test("support barrier execution mode under local mode") {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Seq(1, 2, 3, 4), 2)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      // If we don't get the expected taskInfos, the job shall abort due to stage failure.
      if (context.getTaskInfos().length != 2) {
        throw new SparkException("Expected taksInfos length is 2, actual length is " +
          s"${context.getTaskInfos().length}.")
      }
      context.barrier()
      it
    }
    rdd2.collect()

    eventually(timeout(10.seconds)) {
      assert(sc.statusTracker.getExecutorInfos.map(_.numRunningTasks()).sum == 0)
    }
  }

  test("support barrier execution mode under local-cluster mode") {
    val conf = new SparkConf()
      .setMaster("local-cluster[3, 1, 1024]")
      .setAppName("test-cluster")
    sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Seq(1, 2, 3, 4), 2)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      // If we don't get the expected taskInfos, the job shall abort due to stage failure.
      if (context.getTaskInfos().length != 2) {
        throw new SparkException("Expected taksInfos length is 2, actual length is " +
          s"${context.getTaskInfos().length}.")
      }
      context.barrier()
      it
    }
    rdd2.collect()

    eventually(timeout(10.seconds)) {
      assert(sc.statusTracker.getExecutorInfos.map(_.numRunningTasks()).sum == 0)
    }
  }

  test("cancel zombie tasks in a result stage when the job finishes") {
    val conf = new SparkConf()
      .setMaster("local-cluster[1,2,1024]")
      .setAppName("test-cluster")
      .set(UI_ENABLED.key, "false")
      // Disable this so that if a task is running, we can make sure the executor will always send
      // task metrics via heartbeat to driver.
      .set(EXECUTOR_HEARTBEAT_DROP_ZERO_ACCUMULATOR_UPDATES.key, "false")
      // Set a short heartbeat interval to send SparkListenerExecutorMetricsUpdate fast
      .set(EXECUTOR_HEARTBEAT_INTERVAL.key, "1s")
    sc = new SparkContext(conf)
    sc.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "true")
    @volatile var runningTaskIds: Seq[Long] = null
    val listener = new SparkListener {
      override def onExecutorMetricsUpdate(
          executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
        if (executorMetricsUpdate.execId != SparkContext.DRIVER_IDENTIFIER) {
          runningTaskIds = executorMetricsUpdate.accumUpdates.map(_._1)
        }
      }
    }
    sc.addSparkListener(listener)
    sc.range(0, 2).groupBy((x: Long) => x % 2, 2).map { case (x, _) =>
      val context = org.apache.spark.TaskContext.get()
      if (context.stageAttemptNumber() == 0) {
        if (context.partitionId() == 0) {
          // Make the first task in the first stage attempt fail.
          throw new FetchFailedException(SparkEnv.get.blockManager.blockManagerId, 0, 0L, 0, 0,
            new java.io.IOException("fake"))
        } else {
          // Make the second task in the first stage attempt sleep to generate a zombie task
          Thread.sleep(60000)
        }
      } else {
        // Make the second stage attempt successful.
      }
      x
    }.collect()
    sc.listenerBus.waitUntilEmpty()
    // As executors will send the metrics of running tasks via heartbeat, we can use this to check
    // whether there is any running task.
    eventually(timeout(10.seconds)) {
      // Make sure runningTaskIds has been set
      assert(runningTaskIds != null)
      // Verify there is no running task.
      assert(runningTaskIds.isEmpty)
    }
  }

  test(s"Avoid setting ${CPUS_PER_TASK.key} unreasonably (SPARK-27192)") {
    val FAIL_REASON = " has to be >= the number of cpus per task"
    Seq(
      ("local", 2, None),
      ("local[2]", 3, None),
      ("local[2, 1]", 3, None),
      ("spark://test-spark-cluster", 2, Option(1)),
      ("local-cluster[1, 1, 1000]", 2, Option(1)),
      ("yarn", 2, Option(1))
    ).foreach { case (master, cpusPerTask, executorCores) =>
      val conf = new SparkConf()
      conf.set(CPUS_PER_TASK, cpusPerTask)
      executorCores.map(executorCores => conf.set(EXECUTOR_CORES, executorCores))
      val ex = intercept[SparkException] {
        sc = new SparkContext(master, "test", conf)
      }
      assert(ex.getMessage.contains(FAIL_REASON))
      resetSparkContext()
    }
  }

  test("test driver discovery under local-cluster mode") {
    withTempDir { dir =>
      val scriptPath = createTempScriptWithExpectedOutput(dir, "gpuDiscoveryScript",
        """{"name": "gpu","addresses":["5", "6"]}""")

      val conf = new SparkConf()
        .setMaster("local-cluster[1, 1, 1024]")
        .setAppName("test-cluster")
      conf.set(DRIVER_GPU_ID.amountConf, "2")
      conf.set(DRIVER_GPU_ID.discoveryScriptConf, scriptPath)
      sc = new SparkContext(conf)

      // Ensure all executors has started
      TestUtils.waitUntilExecutorsUp(sc, 1, 60000)
      assert(sc.resources.size === 1)
      assert(sc.resources.get(GPU).get.addresses === Array("5", "6"))
      assert(sc.resources.get(GPU).get.name === "gpu")
    }
  }

  test("test gpu driver resource files and discovery under local-cluster mode") {
    withTempDir { dir =>
      val scriptPath = createTempScriptWithExpectedOutput(dir, "gpuDiscoveryScript",
        """{"name": "gpu","addresses":["5", "6"]}""")

      implicit val formats = DefaultFormats
      val gpusAllocated =
        ResourceAllocation(DRIVER_GPU_ID, Seq("0", "1", "8"))
      val ja = Extraction.decompose(Seq(gpusAllocated))
      val resourcesFile = createTempJsonFile(dir, "resources", ja)

      val conf = new SparkConf()
        .set(DRIVER_RESOURCES_FILE, resourcesFile)
        .setMaster("local-cluster[1, 1, 1024]")
        .setAppName("test-cluster")
        .set(DRIVER_GPU_ID.amountConf, "3")
        .set(DRIVER_GPU_ID.discoveryScriptConf, scriptPath)

      sc = new SparkContext(conf)

      // Ensure all executors has started
      TestUtils.waitUntilExecutorsUp(sc, 1, 60000)
      // driver gpu resources file should take precedence over the script
      assert(sc.resources.size === 1)
      assert(sc.resources.get(GPU).get.addresses === Array("0", "1", "8"))
      assert(sc.resources.get(GPU).get.name === "gpu")
    }
  }

  test("Test parsing resources task configs with missing executor config") {
    val conf = new SparkConf()
      .setMaster("local-cluster[1, 1, 1024]")
      .setAppName("test-cluster")
    conf.set(TASK_GPU_ID.amountConf, "1")

    val error = intercept[SparkException] {
      sc = new SparkContext(conf)
    }.getMessage()

    assert(error.contains("No executor resource configs were specified for the following " +
      "task configs: gpu"))
  }

  test("Test parsing resources executor config < task requirements") {
    val conf = new SparkConf()
      .setMaster("local-cluster[1, 1, 1024]")
      .setAppName("test-cluster")
    conf.set(TASK_GPU_ID.amountConf, "2")
    conf.set(EXECUTOR_GPU_ID.amountConf, "1")

    val error = intercept[SparkException] {
      sc = new SparkContext(conf)
    }.getMessage()

    assert(error.contains("The executor resource: gpu, amount: 1 needs to be >= the task " +
      "resource request amount of 2.0"))
  }

  test("Parse resources executor config not the same multiple numbers of the task requirements") {
    val conf = new SparkConf()
      .setMaster("local-cluster[1, 1, 1024]")
      .setAppName("test-cluster")
    conf.set(RESOURCES_WARNING_TESTING, true)
    conf.set(TASK_GPU_ID.amountConf, "2")
    conf.set(EXECUTOR_GPU_ID.amountConf, "4")

    val error = intercept[SparkException] {
      sc = new SparkContext(conf)
    }.getMessage()

    assert(error.contains(
      "The configuration of resource: gpu (exec = 4, task = 2.0/1, runnable tasks = 2) will " +
        "result in wasted resources due to resource cpus limiting the number of runnable " +
        "tasks per executor to: 1. Please adjust your configuration."))
  }

  test("test resource scheduling under local-cluster mode") {
    import org.apache.spark.TestUtils._

    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val discoveryScript = createTempScriptWithExpectedOutput(dir, "resourceDiscoveryScript",
        """{"name": "gpu","addresses":["0", "1", "2"]}""")

      val conf = new SparkConf()
        .setMaster("local-cluster[3, 2, 1024]")
        .setAppName("test-cluster")
        .set(CPUS_PER_TASK, 2)
        .set(WORKER_GPU_ID.amountConf, "3")
        .set(WORKER_GPU_ID.discoveryScriptConf, discoveryScript)
        .set(TASK_GPU_ID.amountConf, "3")
        .set(EXECUTOR_GPU_ID.amountConf, "3")

      sc = new SparkContext(conf)

      // Ensure all executors has started
      TestUtils.waitUntilExecutorsUp(sc, 3, 60000)

      val rdd1 = sc.makeRDD(1 to 10, 3).mapPartitions { it =>
        val context = TaskContext.get()
        Iterator(context.cpus())
      }
      val cpus = rdd1.collect()
      assert(cpus === Array(2, 2, 2))

      val rdd2 = sc.makeRDD(1 to 10, 3).mapPartitions { it =>
        val context = TaskContext.get()
        context.resources().get(GPU).get.addresses.iterator
      }
      val gpus = rdd2.collect()
      assert(gpus.sorted === Seq("0", "0", "0", "1", "1", "1", "2", "2", "2"))

      eventually(timeout(10.seconds)) {
        assert(sc.statusTracker.getExecutorInfos.map(_.numRunningTasks()).sum == 0)
      }
    }
  }

  test("SPARK-32160: Disallow to create SparkContext in executors") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))

    val error = intercept[SparkException] {
      sc.range(0, 1).foreach { _ =>
        new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
      }
    }.getMessage()

    assert(error.contains("SparkContext should only be created and accessed on the driver."))
  }

  test("SPARK-32160: Allow to create SparkContext in executors if the config is set") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))

    sc.range(0, 1).foreach { _ =>
      new SparkContext(new SparkConf().setAppName("test").setMaster("local")
        .set(EXECUTOR_ALLOW_SPARK_CONTEXT, true)).stop()
    }
  }

  test("SPARK-33084: Add jar support Ivy URI -- default transitive = true") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))
    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0")
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
    assert(sc.listJars().exists(_.contains("commons-lang_commons-lang-2.6.jar")))
  }

  test("SPARK-33084: Add jar support Ivy URI -- invalid transitive use default false") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))
    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0?transitive=foo")
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
    assert(!sc.listJars().exists(_.contains("org.slf4j_slf4j-api-1.7.10.jar")))
    assert(!sc.listJars().exists(_.contains("commons-lang_commons-lang-2.6.jar")))
  }

  test("SPARK-33084: Add jar support Ivy URI -- transitive=true will download dependency jars") {
    val logAppender = new LogAppender("transitive=true will download dependency jars")
    withLogAppender(logAppender) {
      sc = new SparkContext(
        new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))
      sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0?transitive=true")
      val dependencyJars = Array(
        "org.apache.hive_hive-storage-api-2.7.0.jar",
        "org.slf4j_slf4j-api-1.7.10.jar",
        "commons-lang_commons-lang-2.6.jar")

      dependencyJars.foreach(jar => assert(sc.listJars().exists(_.contains(jar))))

      eventually(timeout(10.seconds), interval(1.second)) {
        assert(logAppender.loggingEvents.count(_.getMessage.getFormattedMessage.contains(
          "Added dependency jars of Ivy URI " +
            "ivy://org.apache.hive:hive-storage-api:2.7.0?transitive=true")) == 1)
      }

      // test dependency jars exist
      sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0?transitive=true")
      eventually(timeout(10.seconds), interval(1.second)) {
        assert(logAppender.loggingEvents.count(_.getMessage.getFormattedMessage.contains(
          "The dependency jars of Ivy URI " +
            "ivy://org.apache.hive:hive-storage-api:2.7.0?transitive=true")) == 1)
        val existMsg = logAppender.loggingEvents.filter(_.getMessage.getFormattedMessage.contains(
          "The dependency jars of Ivy URI " +
            "ivy://org.apache.hive:hive-storage-api:2.7.0?transitive=true"))
          .head.getMessage.getFormattedMessage
        dependencyJars.foreach(jar => assert(existMsg.contains(jar)))
      }
    }
  }

  test("SPARK-34506: Add jar support Ivy URI -- transitive=false will not download " +
    "dependency jars") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))
    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0?transitive=false")
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
    assert(!sc.listJars().exists(_.contains("commons-lang_commons-lang-2.6.jar")))
  }

  test("SPARK-34506: Add jar support Ivy URI -- test exclude param when transitive unspecified") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))
    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0?exclude=commons-lang:commons-lang")
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
    assert(sc.listJars().exists(_.contains("org.slf4j_slf4j-api-1.7.10.jar")))
    assert(!sc.listJars().exists(_.contains("commons-lang_commons-lang-2.6.jar")))
  }

  test("SPARK-33084: Add jar support Ivy URI -- test exclude param when transitive=true") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))
    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0" +
      "?exclude=commons-lang:commons-lang&transitive=true")
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
    assert(sc.listJars().exists(_.contains("org.slf4j_slf4j-api-1.7.10.jar")))
    assert(!sc.listJars().exists(_.contains("commons-lang_commons-lang-2.6.jar")))
  }

  test("SPARK-33084: Add jar support Ivy URI -- test different version") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))
    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0")
    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.6.0")
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.6.0.jar")))
  }

  test("SPARK-33084: Add jar support Ivy URI -- test invalid param") {
    val logAppender = new LogAppender("test log when have invalid parameter")
    withLogAppender(logAppender) {
      sc = new SparkContext(
        new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))
      sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0?" +
        "invalidParam1=foo&invalidParam2=boo")
      assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
      eventually(timeout(10.seconds), interval(1.second)) {
        assert(logAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(
          "Invalid parameters `invalidParam1,invalidParam2` found in Ivy URI query " +
            "`invalidParam1=foo&invalidParam2=boo`.")))
      }
    }
  }

  test("SPARK-33084: Add jar support Ivy URI -- test multiple transitive params") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))
    // transitive=invalidValue will win and treated as false
    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0?" +
      "transitive=true&transitive=invalidValue")
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
    assert(!sc.listJars().exists(_.contains("commons-lang_commons-lang-2.6.jar")))

    // transitive=true will win
    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0?" +
      "transitive=false&transitive=invalidValue&transitive=true")
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
    assert(sc.listJars().exists(_.contains("commons-lang_commons-lang-2.6.jar")))
  }

  test("SPARK-33084: Add jar support Ivy URI -- test param key case sensitive") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))
    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0?transitive=false")
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
    assert(!sc.listJars().exists(_.contains("commons-lang_commons-lang-2.6.jar")))

    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0?TRANSITIVE=false")
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
    assert(sc.listJars().exists(_.contains("commons-lang_commons-lang-2.6.jar")))
  }

  test("SPARK-33084: Add jar support Ivy URI -- test transitive value case insensitive") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))
    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0?transitive=FALSE")
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
    assert(!sc.listJars().exists(_.contains("commons-lang_commons-lang-2.6.jar")))

    sc.addJar("ivy://org.apache.hive:hive-storage-api:2.7.0?transitive=false")
    assert(sc.listJars().exists(_.contains("org.apache.hive_hive-storage-api-2.7.0.jar")))
    assert(!sc.listJars().exists(_.contains("commons-lang_commons-lang-2.6.jar")))
  }

  test("SPARK-34346: hadoop configuration priority for spark/hive/hadoop configs") {
    val testKey = "hadoop.tmp.dir"
    val bufferKey = "io.file.buffer.size"
    val hadoopConf0 = new Configuration()
    hadoopConf0.set(testKey, "/tmp/hive_zero")

    val hiveConfFile = Utils.getContextOrSparkClassLoader.getResource("hive-site.xml")
    assert(hiveConfFile != null)
    hadoopConf0.addResource(hiveConfFile)
    assert(hadoopConf0.get(testKey) === "/tmp/hive_zero")
    assert(hadoopConf0.get(bufferKey) === "201811")

    val sparkConf = new SparkConf()
      .setAppName("test")
      .setMaster("local")
      .set(BUFFER_SIZE, 65536)
    sc = new SparkContext(sparkConf)
    assert(sc.hadoopConfiguration.get(testKey) === "/tmp/hive_one",
      "hive configs have higher priority than hadoop ones ")
    assert(sc.hadoopConfiguration.get(bufferKey).toInt === 65536,
      "spark configs have higher priority than hive ones")

    resetSparkContext()

    sparkConf
      .set("spark.hadoop.hadoop.tmp.dir", "/tmp/hive_two")
      .set(s"spark.hadoop.$bufferKey", "20181117")
    sc = new SparkContext(sparkConf)
    assert(sc.hadoopConfiguration.get(testKey) === "/tmp/hive_two",
      "spark.hadoop configs have higher priority than hive/hadoop ones")
    assert(sc.hadoopConfiguration.get(bufferKey).toInt === 65536,
      "spark configs have higher priority than spark.hadoop configs")
  }

  test("SPARK-34225: addFile/addJar shouldn't further encode URI if a URI form string is passed") {
    withTempDir { dir =>
      val jar1 = File.createTempFile("testprefix", "test jar.jar", dir)
      val jarUrl1 = jar1.toURI.toString
      val file1 = File.createTempFile("testprefix", "test file.txt", dir)
      val fileUrl1 = file1.toURI.toString
      val jar2 = File.createTempFile("testprefix", "test %20jar.jar", dir)
      val file2 = File.createTempFile("testprefix", "test %20file.txt", dir)

      try {
        sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
        sc.addJar(jarUrl1)
        sc.addFile(fileUrl1)
        sc.addJar(jar2.toString)
        sc.addFile(file2.toString)
        sc.parallelize(Array(1).toImmutableArraySeq, 1).map { x =>
          val gottenJar1 = new File(SparkFiles.get(jar1.getName))
          if (!gottenJar1.exists()) {
            throw new SparkException("file doesn't exist : " + jar1)
          }
          val gottenFile1 = new File(SparkFiles.get(file1.getName))
          if (!gottenFile1.exists()) {
            throw new SparkException("file doesn't exist : " + file1)
          }
          val gottenJar2 = new File(SparkFiles.get(jar2.getName))
          if (!gottenJar2.exists()) {
            throw new SparkException("file doesn't exist : " + jar2)
          }
          val gottenFile2 = new File(SparkFiles.get(file2.getName))
          if (!gottenFile2.exists()) {
            throw new SparkException("file doesn't exist : " + file2)
          }
          x
        }.collect()
      } finally {
        sc.stop()
      }
    }
  }

  test("SPARK-35383: Fill missing S3A magic committer configs if needed") {
    val c1 = new SparkConf().setAppName("s3a-test").setMaster("local")
    sc = new SparkContext(c1)
    assert(!sc.getConf.contains("spark.hadoop.fs.s3a.committer.name"))

    resetSparkContext()
    val c2 = c1.clone.set("spark.hadoop.fs.s3a.bucket.mybucket.committer.magic.enabled", "false")
    sc = new SparkContext(c2)
    assert(!sc.getConf.contains("spark.hadoop.fs.s3a.committer.name"))

    resetSparkContext()
    val c3 = c1.clone.set("spark.hadoop.fs.s3a.bucket.mybucket.committer.magic.enabled", "true")
    sc = new SparkContext(c3)
    Seq(
      "spark.hadoop.fs.s3a.committer.magic.enabled" -> "true",
      "spark.hadoop.fs.s3a.committer.name" -> "magic",
      "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a" ->
        "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
      "spark.sql.parquet.output.committer.class" ->
        "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
      "spark.sql.sources.commitProtocolClass" ->
        "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"
    ).foreach { case (k, v) =>
      assert(v == sc.getConf.get(k))
    }

    // Respect a user configuration
    resetSparkContext()
    val c4 = c1.clone
      .set("spark.hadoop.fs.s3a.committer.magic.enabled", "false")
      .set("spark.hadoop.fs.s3a.bucket.mybucket.committer.magic.enabled", "true")
    sc = new SparkContext(c4)
    Seq(
      "spark.hadoop.fs.s3a.committer.magic.enabled" -> "false",
      "spark.hadoop.fs.s3a.committer.name" -> null,
      "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a" -> null,
      "spark.sql.parquet.output.committer.class" -> null,
      "spark.sql.sources.commitProtocolClass" -> null
    ).foreach { case (k, v) =>
      if (v == null) {
        assert(!sc.getConf.contains(k))
      } else {
        assert(v == sc.getConf.get(k))
      }
    }
  }

  test("SPARK-35691: addFile/addJar/addDirectory should put CanonicalFile") {
    withTempDir { dir =>
      try {
        sc = new SparkContext(
          new SparkConf().setAppName("test").setMaster("local-cluster[3, 1, 1024]"))

        val sep = File.separator
        val tmpCanonicalDir = Utils.createTempDir(dir.getAbsolutePath + sep + "test space")
        val tmpAbsoluteDir = new File(tmpCanonicalDir.getAbsolutePath + sep + '.' + sep)
        val tmpJar = File.createTempFile("test", ".jar", tmpAbsoluteDir)
        val tmpFile = File.createTempFile("test", ".txt", tmpAbsoluteDir)

        // Check those files and directory are not canonical
        assert(tmpAbsoluteDir.getAbsolutePath !== tmpAbsoluteDir.getCanonicalPath)
        assert(tmpJar.getAbsolutePath !== tmpJar.getCanonicalPath)
        assert(tmpFile.getAbsolutePath !== tmpFile.getCanonicalPath)

        sc.addJar(tmpJar.getAbsolutePath)
        sc.addFile(tmpFile.getAbsolutePath)

        assert(sc.listJars().size === 1)
        assert(sc.listFiles().size === 1)
        assert(sc.listJars().head.contains(tmpJar.getName))
        assert(sc.listFiles().head.contains(tmpFile.getName))
        assert(!sc.listJars().head.contains("." + sep))
        assert(!sc.listFiles().head.contains("." + sep))
      } finally {
        sc.stop()
      }
    }
  }

  test("SPARK-36772: Store application attemptId in BlockStoreClient for push based shuffle") {
    val conf = new SparkConf().setAppName("testAppAttemptId")
      .setMaster("pushbasedshuffleclustermanager")
    conf.set(PUSH_BASED_SHUFFLE_ENABLED.key, "true")
    conf.set(IS_TESTING.key, "true")
    conf.set(SHUFFLE_SERVICE_ENABLED.key, "true")
    sc = new SparkContext(conf)
    val env = SparkEnv.get
    assert(env.blockManager.blockStoreClient.getAppAttemptId.equals("1"))
  }

  test("SPARK-34659: check invalid UI_REVERSE_PROXY_URL") {
    val reverseProxyUrl = "http://proxyhost:8080/path/proxy/spark"
    val conf = new SparkConf().setAppName("testAppAttemptId")
      .setMaster("pushbasedshuffleclustermanager")
    conf.set(UI_REVERSE_PROXY, true)
    conf.set(UI_REVERSE_PROXY_URL, reverseProxyUrl)
    val msg = intercept[java.lang.IllegalArgumentException] {
      new SparkContext(conf)
    }.getMessage
    assert(msg.contains("Cannot use the keyword 'proxy' or 'history' in reverse proxy URL"))
  }

  test("SPARK-39957: ExitCode HEARTBEAT_FAILURE should be counted as network failure") {
    // This test is used to prove that driver will receive executorExitCode before onDisconnected
    // removes the executor. If the executor is removed by onDisconnected, the executor loss will be
    // considered as a task failure. Spark will throw a SparkException because TASK_MAX_FAILURES is
    // 1. On the other hand, driver removes executor with exitCode HEARTBEAT_FAILURE, the loss
    // should be counted as network failure, and thus the job should not throw SparkException.

    val conf = new SparkConf().set(TASK_MAX_FAILURES, 1)
    val sc = new SparkContext("local-cluster[1, 1, 1024]", "test-exit-code-heartbeat", conf)
    val result = sc.parallelize(1 to 10, 1).map { x =>
      val context = org.apache.spark.TaskContext.get()
      if (context.taskAttemptId() == 0) {
        System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
      } else {
        x
      }
    }.count()
    assert(result == 10L)
    sc.stop()
  }

  test("SPARK-39957: ExitCode HEARTBEAT_FAILURE will be counted as task failure when" +
    "EXECUTOR_REMOVE_DELAY is disabled") {
    // If the executor is removed by onDisconnected, the executor loss will be considered as a task
    // failure. Spark will throw a SparkException because TASK_MAX_FAILURES is 1.

    val conf = new SparkConf().set(TASK_MAX_FAILURES, 1).set(EXECUTOR_REMOVE_DELAY.key, "0s")
    val sc = new SparkContext("local-cluster[1, 1, 1024]", "test-exit-code-heartbeat", conf)
    eventually(timeout(30.seconds), interval(1.seconds)) {
      val e = intercept[SparkException] {
        sc.parallelize(1 to 10, 1).map { x =>
          val context = org.apache.spark.TaskContext.get()
          if (context.taskAttemptId() == 0) {
            System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
          } else {
            x
          }
        }.count()
      }
      assert(e.getMessage.contains("Remote RPC client disassociated"))
    }
    sc.stop()
  }

  test("SPARK-42689: ShuffleDataIO initialized after application id has been configured") {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    // TestShuffleDataIO will validate if application id has been configured in its constructor
    conf.set(SHUFFLE_IO_PLUGIN_CLASS.key, classOf[TestShuffleDataIOWithMockedComponents].getName)
    sc = new SparkContext(conf)
    sc.stop()
  }

  test("SPARK-50247: BLOCK_MANAGER_REREGISTRATION_FAILED should be counted as network failure") {
    // This test case follows the test structure of HEARTBEAT_FAILURE error code (SPARK-39957)
    val conf = new SparkConf().set(TASK_MAX_FAILURES, 1)
    val sc = new SparkContext("local-cluster[1, 1, 1024]", "test-exit-code", conf)
    val result = sc.parallelize(1 to 10, 1).map { x =>
      val context = org.apache.spark.TaskContext.get()
      if (context.taskAttemptId() == 0) {
        System.exit(ExecutorExitCode.BLOCK_MANAGER_REREGISTRATION_FAILED)
      } else {
        x
      }
    }.count()
    assert(result == 10L)
    sc.stop()
  }

  test("SPARK-50247: BLOCK_MANAGER_REREGISTRATION_FAILED will be counted as task failure when " +
    "EXECUTOR_REMOVE_DELAY is disabled") {
    // This test case follows the test structure of HEARTBEAT_FAILURE error code (SPARK-39957)
    val conf = new SparkConf().set(TASK_MAX_FAILURES, 1).set(EXECUTOR_REMOVE_DELAY.key, "0s")
    val sc = new SparkContext("local-cluster[1, 1, 1024]", "test-exit-code", conf)
    eventually(timeout(30.seconds), interval(1.seconds)) {
      val e = intercept[SparkException] {
        sc.parallelize(1 to 10, 1).map { x =>
          val context = org.apache.spark.TaskContext.get()
          if (context.taskAttemptId() == 0) {
            System.exit(ExecutorExitCode.BLOCK_MANAGER_REREGISTRATION_FAILED)
          } else {
            x
          }
        }.count()
      }
      assert(e.getMessage.contains("Remote RPC client disassociated"))
    }
    sc.stop()
  }
}

object SparkContextSuite {
  @volatile var isTaskStarted = false
  @volatile var taskKilled = false
  @volatile var taskSucceeded = false
  val semaphore = new Semaphore(0)
}
