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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.util.Utils

class InputFileBlockHolderSuite extends SparkFunSuite {
  test("InputFileBlockHolder should work with both HadoopRDD and NewHadoopRDD when in new thread") {
    val sc = new SparkContext("local", "test")
    val tempDir: File = Utils.createTempDir()
    val outDir = new File(tempDir, "output").getAbsolutePath
    sc.makeRDD(1 to 4, 1).saveAsTextFile(outDir)

    // Test HadoopRDD
    val filename1 = sc.textFile(outDir).mapPartitions { iter =>
      // Consume the iterator so InputFileBlockHolder will be set.
      iter.next()
      val output = new ArrayBuffer[String]()
      val thread = new TestThread(output)
      thread.start()
      thread.join()
      output.toIterator
    }.collect().head

    assert(filename1.endsWith("part-00000"))

    // Test NewHadoopRDD
    val filename2 =
      sc.newAPIHadoopFile[LongWritable, Text, NewTextInputFormat](outDir).mapPartitions { iter =>
        // Consume the iterator so InputFileBlockHolder will be set.
        iter.next()
        val output = new ArrayBuffer[String]()
        val thread = new TestThread(output)
        thread.start()
        thread.join()
        output.toIterator
      }.collect().head

    assert(filename2.endsWith("part-00000"))

    Utils.deleteRecursively(tempDir)
    sc.stop()
  }
}

private class TestThread(output: ArrayBuffer[String])
    extends Thread("test thread for InputFileBlockHolder") {
  override def run(): Unit = {
    output += InputFileBlockHolder.getInputFilePath.toString
  }
}
