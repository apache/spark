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

package org.apache.spark.serializer

import java.io.File

import com.google.common.base.Charsets._
import com.google.common.io.Files

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.util.Utils

class KryoClosureSerializerSuite extends SparkFunSuite with SharedSparkContext {
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.closure.serializer", "org.apache.spark.serializer.KryoSerializer")

  test("accumulator kryo serialization") {
    val accum = sc.accumulator(0)
    val rdd = sc.parallelize(0 until 3)
    rdd.foreach(elem => accum += elem)
    assert(accum.value == 3)
  }

  test("Hadoop RDD kryo serialization") {
    // Regression test for Spark-7708. This test used to fail
    // with StreamCorruptedException due to incorrect handling of
    // JavaSerializable (FileSplit)
    val dir = Utils.createTempDir()
    val file = new File(dir, "textFile")
    Files.write("Someline1 in file\nSomeline2 in file\nSomeline3 in file", file, UTF_8)
    val rdd = sc.textFile(file.getAbsolutePath)
    assert(rdd.count == 3)
  }
}
