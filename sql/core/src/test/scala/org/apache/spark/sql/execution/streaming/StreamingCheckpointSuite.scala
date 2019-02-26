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

package org.apache.spark.sql.execution.streaming

import java.net.URI

import scala.collection.mutable

import org.apache.hadoop.fs.{LocalFileSystem, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{Encoder, LocalSparkSession, SparkSession, SQLContext}

class StreamingCheckpointSuite extends SparkFunSuite with LocalSparkSession {

  test("SPARK-26825: set temp checkpoint dir to local even default filesystem is not local") {
    val conf = new SparkConf()
      .set("spark.hadoop.fs.defaultFS", "mockfs:///")
      .set("spark.hadoop.fs.file.impl", classOf[LocalFileSystem].getName)
      .set("spark.hadoop.fs.mockfs.impl", classOf[MkdirRecordingFileSystem].getName)

    spark = SparkSession.builder().master("local").appName("test").config(conf).getOrCreate()

    implicit val intEncoder: Encoder[Int] = spark.implicits.newIntEncoder
    implicit val sqlContext: SQLContext = spark.sqlContext

    MkdirRecordingFileSystem.reset()

    val inputData = MemoryStream[Int]
    val df = inputData.toDF()

    val query = df.writeStream.format("console").start()
    try {
      assert(MkdirRecordingFileSystem.requests.isEmpty,
        "Expected mkdir happens in mocked filesystem")
    } finally {
      query.stop()
    }
  }
}

class MkdirRecordingFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "mockfs"

  override def getUri: URI = URI.create(s"$getScheme:///")

  override def mkdirs(f: Path): Boolean = {
    mkdirs(f, null)
  }

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    MkdirRecordingFileSystem.add(f)
    super.mkdirs(f, permission)
  }
}

/**
 * Hacked approach to retrive requests for mkdir.
 * All tests relying on this should call reset() first.
 */
object MkdirRecordingFileSystem {
  val requests = new mutable.ArrayBuffer[Path]()

  def reset(): Unit = requests.clear()

  def add(p: Path): mutable.ArrayBuffer[Path] = requests += p
}
