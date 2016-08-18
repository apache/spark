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

package org.apache.spark.ml.source.libsvm

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Files

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.util.Utils


class LibSVMRelationSuite extends SparkFunSuite with MLlibTestSparkContext {
  // Path for dataset
  var path: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val lines =
      """
        |1 1:1.0 3:2.0 5:3.0
        |0
        |0 2:4.0 4:5.0 6:6.0
      """.stripMargin
    val dir = Utils.createDirectory(tempDir.getCanonicalPath, "data")
    val file = new File(dir, "part-00000")
    Files.write(lines, file, StandardCharsets.UTF_8)
    path = dir.toURI.toString
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(new File(path))
    } finally {
      super.afterAll()
    }
  }

  test("select as sparse vector") {
    val df = spark.read.format("libsvm").load(path)
    assert(df.columns(0) == "label")
    assert(df.columns(1) == "features")
    val row1 = df.first()
    assert(row1.getDouble(0) == 1.0)
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(6, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
  }

  test("select as dense vector") {
    val df = spark.read.format("libsvm").options(Map("vectorType" -> "dense"))
      .load(path)
    assert(df.columns(0) == "label")
    assert(df.columns(1) == "features")
    assert(df.count() == 3)
    val row1 = df.first()
    assert(row1.getDouble(0) == 1.0)
    val v = row1.getAs[DenseVector](1)
    assert(v == Vectors.dense(1.0, 0.0, 2.0, 0.0, 3.0, 0.0))
  }

  test("select a vector with specifying the longer dimension") {
    val df = spark.read.option("numFeatures", "100").format("libsvm")
      .load(path)
    val row1 = df.first()
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(100, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
  }

  test("write libsvm data and read it again") {
    val df = spark.read.format("libsvm").load(path)
    val tempDir2 = new File(tempDir, "read_write_test")
    val writepath = tempDir2.toURI.toString
    // TODO: Remove requirement to coalesce by supporting multiple reads.
    df.coalesce(1).write.format("libsvm").mode(SaveMode.Overwrite).save(writepath)

    val df2 = spark.read.format("libsvm").load(writepath)
    val row1 = df2.first()
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(6, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
  }

  test("write libsvm data failed due to invalid schema") {
    val df = spark.read.format("text").load(path)
    intercept[SparkException] {
      df.write.format("libsvm").save(path + "_2")
    }
  }

  test("select features from libsvm relation") {
    val df = spark.read.format("libsvm").load(path)
    df.select("features").rdd.map { case Row(d: Vector) => d }.first
    df.select("features").collect
  }
}
