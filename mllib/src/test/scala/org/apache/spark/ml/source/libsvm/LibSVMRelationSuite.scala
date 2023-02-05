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

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets

import com.google.common.io.Files

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.execution.datasources.CommonFileDataSourceSuite
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.util.Utils

class LibSVMRelationSuite
  extends SparkFunSuite
  with MLlibTestSparkContext
  with CommonFileDataSourceSuite {

  override protected def dataSourceFormat = "libsvm"
  override protected def inputDataset = {
    val rawData = new java.util.ArrayList[Row]()
    rawData.add(Row(1.0, Vectors.sparse(1, Seq((0, 1.0)))))
    val struct = new StructType()
      .add("labelFoo", DoubleType, false)
      .add("featuresBar", VectorType, false)
    spark.createDataFrame(rawData, struct)
  }

  // Path for dataset
  var path: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val lines0 =
      """
        |1 1:1.0 3:2.0 5:3.0
        |0
      """.stripMargin
    val lines1 =
      """
        |0 2:4.0 4:5.0 6:6.0
      """.stripMargin
    val dir = Utils.createTempDir()
    val succ = new File(dir, "_SUCCESS")
    val file0 = new File(dir, "part-00000")
    val file1 = new File(dir, "part-00001")
    Files.write("", succ, StandardCharsets.UTF_8)
    Files.write(lines0, file0, StandardCharsets.UTF_8)
    Files.write(lines1, file1, StandardCharsets.UTF_8)
    path = dir.getPath
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
    assert(AttributeGroup.fromStructField(df.schema("features")).size === v.size)
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
    assert(AttributeGroup.fromStructField(df.schema("features")).size === v.size)
  }

  test("illegal vector types") {
    val e = intercept[IllegalArgumentException] {
      spark.read.format("libsvm").options(Map("VectorType" -> "sparser")).load(path)
    }.getMessage
    assert(e.contains("Invalid value `sparser` for parameter `vectorType`. Expected " +
      "types are `sparse` and `dense`."))
  }

  test("select a vector with specifying the longer dimension") {
    val df = spark.read.option("numFeatures", "100").format("libsvm")
      .load(path)
    val row1 = df.first()
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(100, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
    assert(AttributeGroup.fromStructField(df.schema("features")).size === v.size)
  }

  test("case insensitive option") {
    val df = spark.read.option("NuMfEaTuReS", "100").format("libsvm").load(path)
    assert(df.first().getAs[SparseVector](1) ==
      Vectors.sparse(100, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
    assert(AttributeGroup.fromStructField(df.schema("features")).size === 100)
  }

  test("write libsvm data and read it again") {
    val df = spark.read.format("libsvm").load(path)
    val writePath = Utils.createTempDir().getPath

    // TODO: Remove requirement to coalesce by supporting multiple reads.
    df.coalesce(1).write.format("libsvm").mode(SaveMode.Overwrite).save(writePath)

    val df2 = spark.read.format("libsvm").load(writePath)
    val row1 = df2.first()
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(6, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
    assert(AttributeGroup.fromStructField(df.schema("features")).size === v.size)
    assert(AttributeGroup.fromStructField(df2.schema("features")).size === v.size)
  }

  test("write libsvm data failed due to invalid schema") {
    val df = spark.read.format("text").load(path)
    intercept[IOException] {
      df.write.format("libsvm").save(path + "_2")
    }
  }

  test("write libsvm data from scratch and read it again") {
    val rawData = new java.util.ArrayList[Row]()
    rawData.add(Row(1.0, Vectors.sparse(3, Seq((0, 2.0), (1, 3.0)))))
    rawData.add(Row(4.0, Vectors.sparse(3, Seq((0, 5.0), (2, 6.0)))))

    val struct = StructType(Array(
      StructField("labelFoo", DoubleType, false),
      StructField("featuresBar", VectorType, false))
    )
    val df = spark.sqlContext.createDataFrame(rawData, struct)

    val writePath = Utils.createTempDir().getPath

    df.coalesce(1).write.format("libsvm").mode(SaveMode.Overwrite).save(writePath)

    val df2 = spark.read.format("libsvm").load(writePath)
    val row1 = df2.first()
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(3, Seq((0, 2.0), (1, 3.0))))
    assert(AttributeGroup.fromStructField(df2.schema("features")).size === v.size)
  }

  test("select features from libsvm relation") {
    val df = spark.read.format("libsvm").load(path)
    df.select("features").rdd.map { case Row(d: Vector) => d }.first
    df.select("features").collect
  }

  test("create libsvmTable table without schema") {
    try {
      spark.sql(
        s"""
           |CREATE TABLE libsvmTable
           |USING libsvm
           |OPTIONS (
           |  path '$path'
           |)
         """.stripMargin)
      val df = spark.table("libsvmTable")
      assert(df.columns(0) == "label")
      assert(df.columns(1) == "features")
    } finally {
      spark.sql("DROP TABLE IF EXISTS libsvmTable")
    }
  }

  test("create libsvmTable table without schema and path") {
    try {
      val e = intercept[IllegalArgumentException] {
        spark.sql("CREATE TABLE libsvmTable USING libsvm")
      }
      assert(e.getMessage.contains("No input path specified for libsvm data"))
    } finally {
      spark.sql("DROP TABLE IF EXISTS libsvmTable")
    }
  }

  test("SPARK-32815: Test LibSVM data source on file paths with glob metacharacters") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      // test libsvm writer / reader without specifying schema
      val svmFileName = "[abc]"
      val escapedSvmFileName = "\\[abc\\]"
      val rawData = new java.util.ArrayList[Row]()
      rawData.add(Row(1.0, Vectors.sparse(2, Seq((0, 2.0), (1, 3.0)))))
      val struct = new StructType()
        .add("labelFoo", DoubleType, false)
        .add("featuresBar", VectorType, false)
      val df = spark.createDataFrame(rawData, struct)
      df.write.format("libsvm").save(s"$basePath/$svmFileName")
      val df2 = spark.read.format("libsvm").load(s"$basePath/$escapedSvmFileName")
      val row1 = df2.first()
      val v = row1.getAs[SparseVector](1)
      assert(v == Vectors.sparse(2, Seq((0, 2.0), (1, 3.0))))
    }
  }
}
