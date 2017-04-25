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

package org.apache.spark.sql.execution.datasources.orc

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

/**
 * This test suite is a port of org.apache.spark.sql.hive.orc.OrcHadoopFsRelationSuite.
 */
class OrcHadoopFsRelationSuite extends QueryTest with SharedSQLContext with OrcTest {
  import testImplicits._

  val dataSourceName: String = classOf[OrcFileFormat].getCanonicalName

  val dataSchema =
    StructType(
      Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("b", StringType, nullable = false)))

  def checkQueries(df: DataFrame): Unit = {
    // Selects everything
    checkAnswer(
      df,
      for (i <- 1 to 3; p1 <- 1 to 2; p2 <- Seq("foo", "bar")) yield Row(i, s"val_$i", p1, p2))

    // Simple filtering and partition pruning
    checkAnswer(
      df.filter('a > 1 && 'p1 === 2),
      for (i <- 2 to 3; p2 <- Seq("foo", "bar")) yield Row(i, s"val_$i", 2, p2))

    // Simple projection and filtering
    checkAnswer(
      df.filter('a > 1).select('b, 'a + 1),
      for (i <- 2 to 3; _ <- 1 to 2; _ <- Seq("foo", "bar")) yield Row(s"val_$i", i + 1))

    // Simple projection and partition pruning
    checkAnswer(
      df.filter('a > 1 && 'p1 < 2).select('b, 'p1),
      for (i <- 2 to 3; _ <- Seq("foo", "bar")) yield Row(s"val_$i", 1))

    // Project many copies of columns with different types (reproduction for SPARK-7858)
    checkAnswer(
      df.filter('a > 1 && 'p1 < 2).select('b, 'b, 'b, 'b, 'p1, 'p1, 'p1, 'p1),
      for (i <- 2 to 3; _ <- Seq("foo", "bar"))
        yield Row(s"val_$i", s"val_$i", s"val_$i", s"val_$i", 1, 1, 1, 1))

    // Self-join
    df.createOrReplaceTempView("t")
    withTempView("t") {
      checkAnswer(
        sql(
          """SELECT l.a, r.b, l.p1, r.p2
            |FROM t l JOIN t r
            |ON l.a = r.a AND l.p1 = r.p1 AND l.p2 = r.p2
          """.stripMargin),
        for (i <- 1 to 3; p1 <- 1 to 2; p2 <- Seq("foo", "bar")) yield Row(i, s"val_$i", p1, p2))
    }
  }

  test("save()/load() - partitioned table - simple queries - partition columns in data") {
    Seq("false", "true").foreach { value =>
      withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> value) {
        withTempDir { file =>
          for (p1 <- 1 to 2; p2 <- Seq("foo", "bar")) {
            val partitionDir = new Path(
              CatalogUtils.URIToString(makeQualifiedPath(file.getCanonicalPath)), s"p1=$p1/p2=$p2")
            sparkContext
              .parallelize(for (i <- 1 to 3) yield (i, s"val_$i", p1))
              .toDF("a", "b", "p1")
              .write
              .format(ORC_FILE_FORMAT)
              .save(partitionDir.toString)
          }

          val dataSchemaWithPartition =
            StructType(dataSchema.fields :+ StructField("p1", IntegerType, nullable = true))

          checkQueries(
            spark.read.options(Map(
              "path" -> file.getCanonicalPath,
              "dataSchema" -> dataSchemaWithPartition.json)).format(dataSourceName).load())
        }
      }
    }
  }

  test("SPARK-12218: 'Not' is included in ORC filter pushdown") {
    import testImplicits._

    Seq("false", "true").foreach { value =>
      withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> value) {
        withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
          withTempPath { dir =>
            val path = s"${dir.getCanonicalPath}/table1"
            (1 to 5).map(i => (i, (i % 2).toString)).toDF("a", "b")
              .write.format(ORC_FILE_FORMAT).save(path)

            checkAnswer(
              spark.read.format(ORC_FILE_FORMAT).load(path).where("not (a = 2) or not(b in ('1'))"),
              (1 to 5).map(i => Row(i, (i % 2).toString)))

            checkAnswer(
              spark.read.format(ORC_FILE_FORMAT).load(path).where("not (a = 2 and b in ('1'))"),
              (1 to 5).map(i => Row(i, (i % 2).toString)))
          }
        }
      }
    }
  }

  test("SPARK-13543: Support for specifying compression codec for ORC via option()") {
    Seq("false", "true").foreach { value =>
      withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> value) {
        withTempPath { dir =>
          val path = s"${dir.getCanonicalPath}/table1"
          val df = (1 to 5).map(i => (i, (i % 2).toString)).toDF("a", "b")
          df.write
            .option("compression", "ZlIb")
            .format(ORC_FILE_FORMAT)
            .save(path)

          val maybeOrcFile = new File(path).listFiles().find(_.getName.endsWith(".zlib.orc"))
          assert(maybeOrcFile.isDefined)

          val orcFilePath = new Path(maybeOrcFile.get.getAbsolutePath)
          val conf = OrcFile.readerOptions(new Configuration())
          assert("ZLIB" === OrcFile.createReader(orcFilePath, conf).getCompressionKind.name)

          val copyDf = spark
            .read
            .format(ORC_FILE_FORMAT)
            .load(path)
          checkAnswer(df, copyDf)
        }
      }
    }
  }

  test("Default compression codec is snappy for ORC compression") {
    Seq("false", "true").foreach { value =>
      withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> value) {
        withTempPath { path =>
          spark.range(0, 10).write.format(ORC_FILE_FORMAT).save(path.getAbsolutePath)

          assert(path.listFiles().exists(f => f.getAbsolutePath.endsWith(".snappy.orc")))

          val conf = OrcFile.readerOptions(new Configuration())
          assert(path.listFiles().forall { f =>
            val filePath = new Path(f.getAbsolutePath)
            !f.getAbsolutePath.endsWith(".snappy.orc") ||
              "SNAPPY" === OrcFile.createReader(filePath, conf).getCompressionKind.name
          })
        }
      }
    }
  }
}
