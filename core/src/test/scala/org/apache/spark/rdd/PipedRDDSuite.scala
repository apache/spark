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

package org.apache.spark.rdd

import java.io.File

import scala.collection.Map
import scala.io.Codec

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, JobConf, TextInputFormat}

import org.apache.spark._
import org.apache.spark.util.Utils

class PipedRDDSuite extends SparkFunSuite with SharedSparkContext {
  val envCommand = if (Utils.isWindows) {
    "cmd.exe /C set"
  } else {
    "printenv"
  }

  test("basic pipe") {
    assume(TestUtils.testCommandAvailable("cat"))
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)

    val piped = nums.pipe(Seq("cat"))

    val c = piped.collect()
    assert(c.size === 4)
    assert(c(0) === "1")
    assert(c(1) === "2")
    assert(c(2) === "3")
    assert(c(3) === "4")
  }

  test("basic pipe with tokenization") {
    assume(TestUtils.testCommandAvailable("wc"))
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)

    // verify that both RDD.pipe(command: String) and RDD.pipe(command: String, env) work good
    for (piped <- Seq(nums.pipe("wc -l"), nums.pipe("wc -l", Map[String, String]()))) {
      val c = piped.collect()
      assert(c.size === 2)
      assert(c(0).trim === "2")
      assert(c(1).trim === "2")
    }
  }

  test("failure in iterating over pipe input") {
    assume(TestUtils.testCommandAvailable("cat"))
    val nums =
      sc.makeRDD(Array(1, 2, 3, 4), 2)
        .mapPartitionsWithIndex((index, iterator) => {
        new Iterator[Int] {
          def hasNext = true
          def next() = {
            throw new SparkException("Exception to simulate bad scenario")
          }
        }
      })

    val piped = nums.pipe(Seq("cat"))

    intercept[SparkException] {
      piped.collect()
    }
  }

  test("advanced pipe") {
    assume(TestUtils.testCommandAvailable("cat"))
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val bl = sc.broadcast(List("0"))

    val piped = nums.pipe(Seq("cat"),
      Map[String, String](),
      (f: String => Unit) => {
        bl.value.foreach(f); f("\u0001")
      },
      (i: Int, f: String => Unit) => f(i + "_"))

    val c = piped.collect()

    assert(c.size === 8)
    assert(c(0) === "0")
    assert(c(1) === "\u0001")
    assert(c(2) === "1_")
    assert(c(3) === "2_")
    assert(c(4) === "0")
    assert(c(5) === "\u0001")
    assert(c(6) === "3_")
    assert(c(7) === "4_")

    val nums1 = sc.makeRDD(Array("a\t1", "b\t2", "a\t3", "b\t4"), 2)
    val d = nums1.groupBy(str => str.split("\t")(0)).
      pipe(Seq("cat"),
        Map[String, String](),
        (f: String => Unit) => {
          bl.value.foreach(f); f("\u0001")
        },
        (i: Tuple2[String, Iterable[String]], f: String => Unit) => {
          for (e <- i._2) {
            f(e + "_")
          }
        }).collect()
    assert(d.size === 8)
    assert(d(0) === "0")
    assert(d(1) === "\u0001")
    assert(d(2) === "b\t2_")
    assert(d(3) === "b\t4_")
    assert(d(4) === "0")
    assert(d(5) === "\u0001")
    assert(d(6) === "a\t1_")
    assert(d(7) === "a\t3_")
  }

  test("pipe with empty partition") {
    val data = sc.parallelize(Seq("foo", "bing"), 8)
    val piped = data.pipe("wc -c")
    assert(piped.count == 8)
    val charCounts = piped.map(_.trim.toInt).collect().toSet
    val expected = if (Utils.isWindows) {
      // Note that newline character on Windows is \r\n which are two.
      Set(0, 5, 6)
    } else {
      Set(0, 4, 5)
    }
    assert(expected == charCounts)
  }

  test("pipe with env variable") {
    assume(TestUtils.testCommandAvailable(envCommand))
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val piped = nums.pipe(s"$envCommand MY_TEST_ENV", Map("MY_TEST_ENV" -> "LALALA"))
    val c = piped.collect()
    assert(c.length === 2)
    // On Windows, `cmd.exe /C set` is used which prints out it as `varname=value` format
    // whereas `printenv` usually prints out `value`. So, `varname=` is stripped here for both.
    assert(c(0).stripPrefix("MY_TEST_ENV=") === "LALALA")
    assert(c(1).stripPrefix("MY_TEST_ENV=") === "LALALA")
  }

  test("pipe with process which cannot be launched due to bad command") {
    assume(!TestUtils.testCommandAvailable("some_nonexistent_command"))
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val command = Seq("some_nonexistent_command")
    val piped = nums.pipe(command)
    val exception = intercept[SparkException] {
      piped.collect()
    }
    assert(exception.getMessage.contains(command.mkString(" ")))
  }

  test("pipe with process which is launched but fails with non-zero exit status") {
    assume(TestUtils.testCommandAvailable("cat"))
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val command = Seq("cat", "nonexistent_file")
    val piped = nums.pipe(command)
    val exception = intercept[SparkException] {
      piped.collect()
    }
    assert(exception.getMessage.contains(command.mkString(" ")))
  }

  test("basic pipe with separate working directory") {
    assume(TestUtils.testCommandAvailable("cat"))
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val piped = nums.pipe(Seq("cat"), separateWorkingDir = true)
    val c = piped.collect()
    assert(c.size === 4)
    assert(c(0) === "1")
    assert(c(1) === "2")
    assert(c(2) === "3")
    assert(c(3) === "4")
    val pipedPwd = nums.pipe(Seq("pwd"), separateWorkingDir = true)
    val collectPwd = pipedPwd.collect()
    assert(collectPwd(0).contains("tasks/"))
    val pipedLs = nums.pipe(Seq("ls"), separateWorkingDir = true, bufferSize = 16384).collect()
    // make sure symlinks were created
    assert(pipedLs.length > 0)
    // clean up top level tasks directory
    Utils.deleteRecursively(new File("tasks"))
  }

  test("test pipe exports map_input_file") {
    testExportInputFile("map_input_file")
  }

  test("test pipe exports mapreduce_map_input_file") {
    testExportInputFile("mapreduce_map_input_file")
  }

  def testExportInputFile(varName: String) {
    assume(TestUtils.testCommandAvailable(envCommand))
    val nums = new HadoopRDD(sc, new JobConf(), classOf[TextInputFormat], classOf[LongWritable],
      classOf[Text], 2) {
      override def getPartitions: Array[Partition] = Array(generateFakeHadoopPartition())

      override val getDependencies = List[Dependency[_]]()

      override def compute(theSplit: Partition, context: TaskContext) = {
        new InterruptibleIterator[(LongWritable, Text)](context, Iterator((new LongWritable(1),
          new Text("b"))))
      }
    }
    val hadoopPart1 = generateFakeHadoopPartition()
    val pipedRdd =
      new PipedRDD(
        nums,
        PipedRDD.tokenize(s"$envCommand $varName"),
        Map(),
        null,
        null,
        false,
        4092,
        Codec.defaultCharsetCodec.name)
    val tContext = TaskContext.empty()
    val rddIter = pipedRdd.compute(hadoopPart1, tContext)
    val arr = rddIter.toArray
    // On Windows, `cmd.exe /C set` is used which prints out it as `varname=value` format
    // whereas `printenv` usually prints out `value`. So, `varname=` is stripped here for both.
    assert(arr(0).stripPrefix(s"$varName=") === "/some/path")
  }

  def generateFakeHadoopPartition(): HadoopPartition = {
    val split = new FileSplit(new Path("/some/path"), 0, 1,
      Array[String]("loc1", "loc2", "loc3", "loc4", "loc5"))
    new HadoopPartition(sc.newRddId(), 1, split)
  }

}
