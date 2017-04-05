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

package org.apache.spark.util.collection

import org.apache.spark.{SparkContext, SparkConf, LocalSparkContext}
import org.apache.spark.io.CompressionCodec
import org.scalatest.FunSuite

class ChunkBufferSuite extends FunSuite with LocalSparkContext {
  private val allCompressionCodecs = CompressionCodec.ALL_COMPRESSION_CODECS

  private def createSparkConf(loadDefaults: Boolean, codec: Option[String] = None): SparkConf = {
    val conf = new SparkConf(loadDefaults)

    conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    conf.set("spark.shuffle.spill.compress", codec.isDefined.toString)
    conf.set("spark.shuffle.compress", codec.isDefined.toString)
    codec.foreach { c => conf.set("spark.io.compression.codec", c) }
    // Ensure that we actually have multiple batches per spill file
    conf.set("spark.join.spill.batchSize", "10")
    conf
  }

  test("DiskChunkBuffer: append and read") {
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)
    val parameters = new ChunkParameters
    val buffer = new DiskChunkBuffer[Int](parameters)

    (0 until 1000).foreach(buffer += _)

    assert(100 === buffer.size)
    assert((0 until 1000) === buffer.iterator.flatMap(chunk => chunk).toList)

    sc.stop()
  }

  test("DiskChunkBuffer: append to a frozen DiskChunkBuffer") {
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)

    val parameters = new ChunkParameters
    val buffer = new DiskChunkBuffer[Int](parameters)

    (0 until 1000).foreach(buffer += _)

    val iter = buffer.iterator

    val e = intercept[IllegalStateException] {
      buffer += 1
    }
    assert("DiskChunkBuffer has been frozen" === e.getMessage)

    sc.stop()
  }

  test("ChunkBuffer: in-memory append and read") {
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)

    val parameters = new ChunkParameters
    val buffer = new ChunkBuffer[Int](parameters)

    (0 until 1000).foreach(buffer += _)

    assert(1 === buffer.size)
    assert((0 until 1000) === buffer.iterator.flatMap(chunk => chunk).toList)

    sc.stop()
  }

  test("ChunkBuffer: disk append and read") {
    val conf = createSparkConf(loadDefaults = false)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    sc = new SparkContext("local-cluster[1,1,512]", "test", conf)

    val parameters = new ChunkParameters
    val buffer = new ChunkBuffer[Long](parameters)

    (0L until 1000000L).foreach(buffer += _) // make sure it will use at least 5MB

    assert(100000 === buffer.size)
    assert((0L until 1000000L) === buffer.iterator.flatMap(chunk => chunk).toList)

    sc.stop()
  }

  test("ChunkBuffer: empty") {
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)

    val parameters = new ChunkParameters
    val buffer = new ChunkBuffer[Int](parameters)

    assert(0 === buffer.size)
    assert(Nil === buffer.iterator.flatMap(chunk => chunk).toList)

    sc.stop()
  }
}
