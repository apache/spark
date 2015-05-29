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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, JobConf, TextInputFormat}
import org.scalatest.FunSuite

import scala.collection.Map
import scala.language.postfixOps
import scala.sys.process._
import scala.util.Try

import org.apache.spark._
import org.apache.spark.util.Utils

class PipedRDDSuite extends FunSuite with SharedSparkContext {

  test("basic pipe") {
    if (testCommandAvailable("cat")) {
      val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)

      val piped = nums.pipe(Seq("cat"))

      val c = piped.collect()
      assert(c.size === 4)
      assert(c(0) === "1")
      assert(c(1) === "2")
      assert(c(2) === "3")
      assert(c(3) === "4")
    } else {
      assert(true)
    }
  }

  test("advanced pipe") {
    if (testCommandAvailable("cat")) {
      val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
      val bl = sc.broadcast(List("0"))

      val piped = nums.pipe(Seq("cat"),
        Map[String, String](),
        (f: String => Unit) => {
          bl.value.map(f(_)); f("\u0001")
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
            bl.value.map(f(_)); f("\u0001")
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
    } else {
      assert(true)
    }
  }

  test("pipe with env variable") {
    if (testCommandAvailable("printenv")) {
      val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
      val piped = nums.pipe(Seq("printenv", "MY_TEST_ENV"), Map("MY_TEST_ENV" -> "LALALA"))
      val c = piped.collect()
      assert(c.size === 2)
      assert(c(0) === "LALALA")
      assert(c(1) === "LALALA")
    } else {
      assert(true)
    }
  }

  test("pipe with non-zero exit status") {
    if (testCommandAvailable("cat")) {
      val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
      val piped = nums.pipe(Seq("cat nonexistent_file", "2>", "/dev/null"))
      intercept[SparkException] {
        piped.collect()
      }
    } else {
      assert(true)
    }
  }

  test("basic pipe with separate working directory") {
    if (testCommandAvailable("cat")) {
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
      val pipedLs = nums.pipe(Seq("ls"), separateWorkingDir = true).collect()
      // make sure symlinks were created
      assert(pipedLs.length > 0)
      // clean up top level tasks directory
      Utils.deleteRecursively(new File("tasks"))
    } else {
      assert(true)
    }
  }

  test("test pipe exports map_input_file") {
    testExportInputFile("map_input_file")
  }

  test("test pipe exports mapreduce_map_input_file") {
    testExportInputFile("mapreduce_map_input_file")
  }

  def testCommandAvailable(command: String): Boolean = {
    Try(Process(command) !!).isSuccess
  }

  def testExportInputFile(varName: String) {
    if (testCommandAvailable("printenv")) {
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
      val pipedRdd = new PipedRDD(nums, "printenv " + varName)
      val tContext = new TaskContextImpl(0, 0, 0, 0, null)
      val rddIter = pipedRdd.compute(hadoopPart1, tContext)
      val arr = rddIter.toArray
      assert(arr(0) == "/some/path")
    } else {
      // printenv isn't available so just pass the test
    }
  }

  def generateFakeHadoopPartition(): HadoopPartition = {
    val split = new FileSplit(new Path("/some/path"), 0, 1,
      Array[String]("loc1", "loc2", "loc3", "loc4", "loc5"))
    new HadoopPartition(sc.newRddId(), 1, split)
  }

}
