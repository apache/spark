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

package org.apache.spark.api.python

import java.io.{BufferedReader, ByteArrayOutputStream, DataOutputStream, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SharedSparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDDMultipleTextOutputFormat
import org.scalatest.FunSuite

import scala.collection.mutable.HashSet

class PythonRDDSuite extends FunSuite with SharedSparkContext{

  test("Writing large strings to the worker") {
    val input: List[String] = List("a"*100000)
    val buffer = new DataOutputStream(new ByteArrayOutputStream)
    PythonRDD.writeIteratorToStream(input.iterator, buffer)
  }

  test("Handle nulls gracefully") {
    val buffer = new DataOutputStream(new ByteArrayOutputStream)
    // Should not have NPE when write an Iterator with null in it
    // The correctness will be tested in Python
    PythonRDD.writeIteratorToStream(Iterator("a", null), buffer)
    PythonRDD.writeIteratorToStream(Iterator(null, "a"), buffer)
    PythonRDD.writeIteratorToStream(Iterator("a".getBytes, null), buffer)
    PythonRDD.writeIteratorToStream(Iterator(null, "a".getBytes), buffer)
    PythonRDD.writeIteratorToStream(Iterator((null, null), ("a", null), (null, "b")), buffer)
    PythonRDD.writeIteratorToStream(
      Iterator((null, null), ("a".getBytes, null), (null, "b".getBytes)), buffer)
  }

  test("saveAsHadoopFileByKey should generate a text file per key") {
    val testPairs : JavaRDD[Array[Byte]] = sc.parallelize(
      Seq(
        Array(1.toByte,1.toByte),
        Array(2.toByte,4.toByte),
        Array(3.toByte,9.toByte),
        Array(4.toByte,16.toByte),
        Array(5.toByte,25.toByte))
    ).toJavaRDD()

    val fs = FileSystem.get(new Configuration())
    val basePath = sc.conf.get("spark.local.dir", "/tmp")
    val fullPath = basePath + "/testPath"
    fs.delete(new Path(fullPath), true)

    PythonRDD.saveAsHadoopFileByKey(
      testPairs,
      false,
      fullPath,
      classOf[RDDMultipleTextOutputFormat[Int, Int]].toString,
      classOf[Int].toString,
      classOf[Int].toString,
      null,
      null,
      new java.util.HashMap(), "")

    // Test that a file was created for each key
    (1 to 5).foreach(key => {
      val testPath = new Path(fullPath + "/" + key)
      assert(fs.exists(testPath))

      // Read the file and test that the contents are the values matching that key split by line
      val input = fs.open(testPath)
      val reader = new BufferedReader(new InputStreamReader(input))
      val values = new HashSet[Int]
      val lines = Stream.continually(reader.readLine()).takeWhile(_ != null)
      lines.foreach(s => values += s.toInt)

      assert(values.contains(key*key))
    })

    fs.delete(new Path(fullPath), true)
  }
}
