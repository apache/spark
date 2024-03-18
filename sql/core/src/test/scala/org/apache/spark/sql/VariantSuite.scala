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

package org.apache.spark.sql

import java.io.File

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.VariantVal
import org.apache.spark.util.ArrayImplicits._

class VariantSuite extends QueryTest with SharedSparkSession {
  test("basic tests") {
    def verifyResult(df: DataFrame): Unit = {
      val result = df.collect()
        .map(_.get(0).asInstanceOf[VariantVal].toString)
        .sorted
        .toSeq
      val expected = (1 until 10).map(id => "1" * id)
      assert(result == expected)
    }

    val query = spark.sql("select parse_json(repeat('1', id)) as v from range(1, 10)")
    verifyResult(query)

    // Write into and read from Parquet.
    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath
      query.write.parquet(tempDir)
      verifyResult(spark.read.parquet(tempDir))
    }
  }

  test("round trip tests") {
    val rand = new Random(42)
    val input = Seq.fill(50) {
      if (rand.nextInt(10) == 0) {
        null
      } else {
        val value = new Array[Byte](rand.nextInt(50))
        rand.nextBytes(value)
        val metadata = new Array[Byte](rand.nextInt(50))
        rand.nextBytes(metadata)
        new VariantVal(value, metadata)
      }
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(input.map(Row(_))),
      StructType.fromDDL("v variant")
    )
    val result = df.collect().map(_.get(0).asInstanceOf[VariantVal])

    def prepareAnswer(values: Seq[VariantVal]): Seq[String] = {
      values.map(v => if (v == null) "null" else v.debugString()).sorted
    }
    assert(prepareAnswer(input) == prepareAnswer(result.toImmutableArraySeq))

    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath
      df.write.parquet(tempDir)
      val readResult = spark.read.parquet(tempDir).collect().map(_.get(0).asInstanceOf[VariantVal])
      assert(prepareAnswer(input) == prepareAnswer(readResult.toImmutableArraySeq))
    }
  }

  test("array of variant") {
    val rand = new Random(42)
    val input = Seq.fill(3) {
      if (rand.nextInt(10) == 0) {
        null
      } else {
        val value = new Array[Byte](rand.nextInt(50))
        rand.nextBytes(value)
        val metadata = new Array[Byte](rand.nextInt(50))
        rand.nextBytes(metadata)
        val numElements = 3 // rand.nextInt(10)
        Seq.fill(numElements)(new VariantVal(value, metadata))
      }
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(input.map { v =>
        Row.fromSeq(Seq(v))
      }),
      StructType.fromDDL("v array<variant>")
    )

    def prepareAnswer(values: Seq[Row]): Seq[String] = {
      values.map(_.get(0)).map { v =>
        if (v == null) {
          "null"
        } else {
          v.asInstanceOf[mutable.ArraySeq[Any]]
           .map(_.asInstanceOf[VariantVal].debugString()).mkString(",")
        }
      }.sorted
    }

    // Test conversion to UnsafeRow in both codegen and interpreted code paths.
    val codegenModes = Seq(CodegenObjectFactoryMode.NO_CODEGEN.toString,
                           CodegenObjectFactoryMode.FALLBACK.toString)
    codegenModes.foreach { codegen =>
      withTempDir { dir =>
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegen) {
          val tempDir = new File(dir, "files").getCanonicalPath
          df.write.parquet(tempDir)
          Seq(false, true).foreach { vectorizedReader =>
            withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key ->
                vectorizedReader.toString) {
              val readResult = spark.read.parquet(tempDir).collect().toSeq
              assert(prepareAnswer(df.collect().toSeq) == prepareAnswer(readResult))
            }
          }
        }
      }
    }
  }

  test("write partitioned file") {
    def verifyResult(df: DataFrame): Unit = {
      val result = df.selectExpr("v").collect()
        .map(_.get(0).asInstanceOf[VariantVal].toString)
        .sorted
        .toSeq
      val expected = (1 until 10).map(id => "1" * id)
      assert(result == expected)
    }

    // At this point, JSON parsing logic is not really implemented. We just construct some number
    // inputs that are also valid JSON. This exercises passing VariantVal throughout the system.
    val queryString = "select id, parse_json(repeat('1', id)) as v from range(1, 10)"
    val query = spark.sql(queryString)
    verifyResult(query)

    // Partition by another column should work.
    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath
      query.write.partitionBy("id").parquet(tempDir)
      verifyResult(spark.read.parquet(tempDir))
    }

    // Partitioning by Variant column is not allowed.
    withTempDir { dir =>
      val tempDir = new File(dir, "files").getCanonicalPath
      intercept[AnalysisException] {
        query.write.partitionBy("v").parquet(tempDir)
      }
    }

    // Same as above, using saveAsTable
    withTable("t") {
      query.write.partitionBy("id").saveAsTable("t")
      verifyResult(spark.sql("select * from t"))
    }

    withTable("t") {
      intercept[AnalysisException] {
        query.write.partitionBy("v").saveAsTable("t")
      }
    }

    // Same as above, using SQL CTAS
    withTable("t") {
      spark.sql(s"CREATE TABLE t USING PARQUET PARTITIONED BY (id) AS $queryString")
      verifyResult(spark.sql("select * from t"))
    }

    withTable("t") {
      intercept[AnalysisException] {
        spark.sql(s"CREATE TABLE t USING PARQUET PARTITIONED BY (v) AS $queryString")
      }
    }
  }
}
