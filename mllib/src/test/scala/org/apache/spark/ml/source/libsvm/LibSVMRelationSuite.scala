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
import java.util.List

import com.google.common.io.Files

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.util.Utils


class LibSVMRelationSuite extends SparkFunSuite with MLlibTestSparkContext {
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
  }

  test("case insensitive option") {
    val df = spark.read.option("NuMfEaTuReS", "100").format("libsvm").load(path)
    assert(df.first().getAs[SparseVector](1) ==
      Vectors.sparse(100, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
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

    val struct = StructType(
      StructField("labelFoo", DoubleType, false) ::
      StructField("featuresBar", VectorType, false) :: Nil
    )
    val df = spark.sqlContext.createDataFrame(rawData, struct)

    val writePath = Utils.createTempDir().getPath

    df.coalesce(1).write.format("libsvm").mode(SaveMode.Overwrite).save(writePath)

    val df2 = spark.read.format("libsvm").load(writePath)
    val row1 = df2.first()
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(3, Seq((0, 2.0), (1, 3.0))))
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

  def testLineSeparator(lineSep: String): Unit = {
    test(s"SPARK-21289: Support line separator - lineSep: '$lineSep'") {
      val data = Seq(
        "1.0 1:1.0 3:2.0 5:3.0", "0.0", "0.0", "0.0 2:4.0 4:5.0 6:6.0").mkString(lineSep)
      val dataWithTrailingLineSep = s"$data$lineSep"

      Seq(data, dataWithTrailingLineSep).foreach { lines =>
        val path0 = new File(tempDir.getCanonicalPath, "write0")
        val path1 = new File(tempDir.getCanonicalPath, "write1")
        try {
          // Read
          java.nio.file.Files.write(path0.toPath, lines.getBytes(StandardCharsets.UTF_8))
          val df = spark.read
            .option("lineSep", lineSep)
            .format("libsvm")
            .load(path0.getAbsolutePath)

          assert(df.columns(0) == "label")
          assert(df.columns(1) == "features")

          val results = df.collect()

          assert(results.map(_.getDouble(0)).toSet == Seq(1.0, 0.0, 0.0, 0.0).toSet)

          val actual = results.map(_.getAs[SparseVector](1))
          val expected = Seq(
            Vectors.sparse(6, Seq((0, 1.0), (2, 2.0), (4, 3.0))),
            Vectors.sparse(6, Nil),
            Vectors.sparse(6, Nil),
            Vectors.sparse(6, Seq((1, 4.0), (3, 5.0), (5, 6.0))))
          assert(actual.toSet == expected.toSet)

          // Write
          df.coalesce(1)
            .write.option("lineSep", lineSep).format("libsvm").save(path1.getAbsolutePath)
          val partFile = Utils.recursiveList(path1).filter(f => f.getName.startsWith("part-")).head
          val readBack = new String(
            java.nio.file.Files.readAllBytes(partFile.toPath), StandardCharsets.UTF_8)
          assert(readBack == dataWithTrailingLineSep)

          // Roundtrip
          val readBackDF = spark.read
            .option("lineSep", lineSep)
            .format("libsvm")
            .load(path1.getAbsolutePath)
          assert(df.collect().toSet == readBackDF.collect().toSet)
        } finally {
          Utils.deleteRecursively(path0)
          Utils.deleteRecursively(path1)
        }
      }
    }
  }

  Seq("123!!@", "^", "&@").foreach { lineSep =>
    testLineSeparator(lineSep)
  }
}
