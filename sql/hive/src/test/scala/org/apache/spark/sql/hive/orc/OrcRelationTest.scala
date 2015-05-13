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

package org.apache.spark.sql.hive.orc

import org.apache.hadoop.fs.Path

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.parquet.ParquetTest
import org.apache.spark.sql.sources.{FSBasedRelation, LogicalRelation}
import org.apache.spark.sql.types._

// TODO Don't extend ParquetTest
// This test suite extends ParquetTest for some convenient utility methods. These methods should be
// moved to some more general places, maybe QueryTest.
class OrcRelationTest extends QueryTest with ParquetTest {
  override val sqlContext: SQLContext = TestHive

  import sqlContext._
  import sqlContext.implicits._

  val dataSourceName = classOf[DefaultSource].getCanonicalName

  val dataSchema =
    StructType(
      Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("b", StringType, nullable = false)))

  val testDF = (1 to 3).map(i => (i, s"val_$i")).toDF("a", "b")

  val partitionedTestDF1 = (for {
    i <- 1 to 3
    p2 <- Seq("foo", "bar")
  } yield (i, s"val_$i", 1, p2)).toDF("a", "b", "p1", "p2")

  val partitionedTestDF2 = (for {
    i <- 1 to 3
    p2 <- Seq("foo", "bar")
  } yield (i, s"val_$i", 2, p2)).toDF("a", "b", "p1", "p2")

  val partitionedTestDF = partitionedTestDF1.unionAll(partitionedTestDF2)

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

    // Self-join
    df.registerTempTable("t")
    withTempTable("t") {
      checkAnswer(
        sql(
          """SELECT l.a, r.b, l.p1, r.p2
            |FROM t l JOIN t r
            |ON l.a = r.a AND l.p1 = r.p1 AND l.p2 = r.p2
          """.stripMargin),
        for (i <- 1 to 3; p1 <- 1 to 2; p2 <- Seq("foo", "bar")) yield Row(i, s"val_$i", p1, p2))
    }
  }

  test("save()/load() - non-partitioned table - Overwrite") {
    withTempPath { file =>
      testDF.save(
        path = file.getCanonicalPath,
        source = dataSourceName,
        mode = SaveMode.Overwrite)

      testDF.save(
        path = file.getCanonicalPath,
        source = dataSourceName,
        mode = SaveMode.Overwrite)

      checkAnswer(
        load(
          source = dataSourceName,
          options = Map(
            "path" -> file.getCanonicalPath,
            "dataSchema" -> dataSchema.json)),
        testDF.collect())
    }
  }

  test("save()/load() - non-partitioned table - Append") {
    withTempPath { file =>
      testDF.save(
        path = file.getCanonicalPath,
        source = dataSourceName,
        mode = SaveMode.Overwrite)

      testDF.save(
        path = file.getCanonicalPath,
        source = dataSourceName,
        mode = SaveMode.Append)

      checkAnswer(
        load(
          source = dataSourceName,
          options = Map(
            "path" -> file.getCanonicalPath,
            "dataSchema" -> dataSchema.json)).orderBy("a"),
        testDF.unionAll(testDF).orderBy("a").collect())
    }
  }

  test("save()/load() - non-partitioned table - ErrorIfExists") {
    withTempDir { file =>
      intercept[RuntimeException] {
        testDF.save(
          path = file.getCanonicalPath,
          source = dataSourceName,
          mode = SaveMode.ErrorIfExists)
      }
    }
  }

  test("save()/load() - non-partitioned table - Ignore") {
    withTempDir { file =>
      testDF.save(
        path = file.getCanonicalPath,
        source = dataSourceName,
        mode = SaveMode.Ignore)

      val path = new Path(file.getCanonicalPath)
      val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
      assert(fs.listStatus(path).isEmpty)
    }
  }

  test("save()/load() - partitioned table - simple queries") {
    withTempPath { file =>
      partitionedTestDF.save(
        source = dataSourceName,
        mode = SaveMode.ErrorIfExists,
        options = Map("path" -> file.getCanonicalPath),
        partitionColumns = Seq("p1", "p2"))

      checkQueries(
        load(
          source = dataSourceName,
          options = Map(
            "path" -> file.getCanonicalPath,
            "dataSchema" -> dataSchema.json)))
    }
  }

  test("save()/load() - partitioned table - Overwrite") {
    withTempPath { file =>
      partitionedTestDF.save(
        source = dataSourceName,
        mode = SaveMode.Overwrite,
        options = Map("path" -> file.getCanonicalPath),
        partitionColumns = Seq("p1", "p2"))

      partitionedTestDF.save(
        source = dataSourceName,
        mode = SaveMode.Overwrite,
        options = Map("path" -> file.getCanonicalPath),
        partitionColumns = Seq("p1", "p2"))

      checkAnswer(
        load(
          source = dataSourceName,
          options = Map(
            "path" -> file.getCanonicalPath,
            "dataSchema" -> dataSchema.json)),
        partitionedTestDF.collect())
    }
  }

  test("save()/load() - partitioned table - Append") {
    withTempPath { file =>
      partitionedTestDF.save(
        source = dataSourceName,
        mode = SaveMode.Overwrite,
        options = Map("path" -> file.getCanonicalPath),
        partitionColumns = Seq("p1", "p2"))

      partitionedTestDF.save(
        source = dataSourceName,
        mode = SaveMode.Append,
        options = Map("path" -> file.getCanonicalPath),
        partitionColumns = Seq("p1", "p2"))

      checkAnswer(
        load(
          source = dataSourceName,
          options = Map(
            "path" -> file.getCanonicalPath,
            "dataSchema" -> dataSchema.json)),
        partitionedTestDF.unionAll(partitionedTestDF).collect())
    }
  }

  test("save()/load() - partitioned table - Append - new partition values") {
    withTempPath { file =>
      partitionedTestDF1.save(
        source = dataSourceName,
        mode = SaveMode.Overwrite,
        options = Map("path" -> file.getCanonicalPath),
        partitionColumns = Seq("p1", "p2"))

      partitionedTestDF2.save(
        source = dataSourceName,
        mode = SaveMode.Append,
        options = Map("path" -> file.getCanonicalPath),
        partitionColumns = Seq("p1", "p2"))

      checkAnswer(
        load(
          source = dataSourceName,
          options = Map(
            "path" -> file.getCanonicalPath,
            "dataSchema" -> dataSchema.json)),
        partitionedTestDF.collect())
    }
  }

  test("save()/load() - partitioned table - ErrorIfExists") {
    withTempDir { file =>
      intercept[RuntimeException] {
        partitionedTestDF.save(
          source = dataSourceName,
          mode = SaveMode.ErrorIfExists,
          options = Map("path" -> file.getCanonicalPath),
          partitionColumns = Seq("p1", "p2"))
      }
    }
  }

  test("save()/load() - partitioned table - Ignore") {
    withTempDir { file =>
      partitionedTestDF.save(
        path = file.getCanonicalPath,
        source = dataSourceName,
        mode = SaveMode.Ignore)

      val path = new Path(file.getCanonicalPath)
      val fs = path.getFileSystem(SparkHadoopUtil.get.conf)
      assert(fs.listStatus(path).isEmpty)
    }
  }

  def withTable(tableName: String)(f: => Unit): Unit = {
    try f finally sql(s"DROP TABLE $tableName")
  }

  test("saveAsTable()/load() - non-partitioned table - Overwrite") {
    testDF.saveAsTable(
      tableName = "t",
      source = dataSourceName,
      mode = SaveMode.Overwrite,
      Map("dataSchema" -> dataSchema.json))

    withTable("t") {
      checkAnswer(table("t"), testDF.collect())
    }
  }

  test("saveAsTable()/load() - non-partitioned table - Append") {
    testDF.saveAsTable(
      tableName = "t",
      source = dataSourceName,
      mode = SaveMode.Overwrite)

    testDF.saveAsTable(
      tableName = "t",
      source = dataSourceName,
      mode = SaveMode.Append)

    withTable("t") {
      checkAnswer(table("t"), testDF.unionAll(testDF).orderBy("a").collect())
    }
  }

  test("saveAsTable()/load() - non-partitioned table - ErrorIfExists") {
    Seq.empty[(Int, String)].toDF().registerTempTable("t")

    withTempTable("t") {
      intercept[AnalysisException] {
        testDF.saveAsTable(
          tableName = "t",
          source = dataSourceName,
          mode = SaveMode.ErrorIfExists)
      }
    }
  }

  test("saveAsTable()/load() - non-partitioned table - Ignore") {
    Seq.empty[(Int, String)].toDF().registerTempTable("t")

    withTempTable("t") {
      testDF.saveAsTable(
        tableName = "t",
        source = dataSourceName,
        mode = SaveMode.Ignore)

      assert(table("t").collect().isEmpty)
    }
  }

  test("saveAsTable()/load() - partitioned table - simple queries") {
    partitionedTestDF.saveAsTable(
      tableName = "t",
      source = dataSourceName,
      mode = SaveMode.Overwrite,
      Map("dataSchema" -> dataSchema.json))

    withTable("t") {
      checkQueries(table("t"))
    }
  }

  test("saveAsTable()/load() - partitioned table - Overwrite") {
    partitionedTestDF.saveAsTable(
      tableName = "t",
      source = dataSourceName,
      mode = SaveMode.Overwrite,
      options = Map("dataSchema" -> dataSchema.json),
      partitionColumns = Seq("p1", "p2"))

    partitionedTestDF.saveAsTable(
      tableName = "t",
      source = dataSourceName,
      mode = SaveMode.Overwrite,
      options = Map("dataSchema" -> dataSchema.json),
      partitionColumns = Seq("p1", "p2"))

    withTable("t") {
      checkAnswer(table("t"), partitionedTestDF.collect())
    }
  }

  test("saveAsTable()/load() - partitioned table - Append") {
    partitionedTestDF.saveAsTable(
      tableName = "t",
      source = dataSourceName,
      mode = SaveMode.Overwrite,
      options = Map("dataSchema" -> dataSchema.json),
      partitionColumns = Seq("p1", "p2"))

    partitionedTestDF.saveAsTable(
      tableName = "t",
      source = dataSourceName,
      mode = SaveMode.Append,
      options = Map("dataSchema" -> dataSchema.json),
      partitionColumns = Seq("p1", "p2"))

    withTable("t") {
      checkAnswer(table("t"), partitionedTestDF.unionAll(partitionedTestDF).collect())
    }
  }

  test("saveAsTable()/load() - partitioned table - Append - new partition values") {
    partitionedTestDF1.saveAsTable(
      tableName = "t",
      source = dataSourceName,
      mode = SaveMode.Overwrite,
      options = Map("dataSchema" -> dataSchema.json),
      partitionColumns = Seq("p1", "p2"))

    partitionedTestDF2.saveAsTable(
      tableName = "t",
      source = dataSourceName,
      mode = SaveMode.Append,
      options = Map("dataSchema" -> dataSchema.json),
      partitionColumns = Seq("p1", "p2"))

    withTable("t") {
      checkAnswer(table("t"), partitionedTestDF.collect())
    }
  }

  test("saveAsTable()/load() - partitioned table - Append - mismatched partition columns") {
    partitionedTestDF1.saveAsTable(
      tableName = "t",
      source = dataSourceName,
      mode = SaveMode.Overwrite,
      options = Map("dataSchema" -> dataSchema.json),
      partitionColumns = Seq("p1", "p2"))

    // Using only a subset of all partition columns
    intercept[Throwable] {
      partitionedTestDF2.saveAsTable(
        tableName = "t",
        source = dataSourceName,
        mode = SaveMode.Append,
        options = Map("dataSchema" -> dataSchema.json),
        partitionColumns = Seq("p1"))
    }

    // Using different order of partition columns
    intercept[Throwable] {
      partitionedTestDF2.saveAsTable(
        tableName = "t",
        source = dataSourceName,
        mode = SaveMode.Append,
        options = Map("dataSchema" -> dataSchema.json),
        partitionColumns = Seq("p2", "p1"))
    }
  }

  test("saveAsTable()/load() - partitioned table - ErrorIfExists") {
    Seq.empty[(Int, String)].toDF().registerTempTable("t")

    withTempTable("t") {
      intercept[AnalysisException] {
        partitionedTestDF.saveAsTable(
          tableName = "t",
          source = dataSourceName,
          mode = SaveMode.ErrorIfExists,
          options = Map("dataSchema" -> dataSchema.json),
          partitionColumns = Seq("p1", "p2"))
      }
    }
  }

  test("saveAsTable()/load() - partitioned table - Ignore") {
    Seq.empty[(Int, String)].toDF().registerTempTable("t")

    withTempTable("t") {
      partitionedTestDF.saveAsTable(
        tableName = "t",
        source = dataSourceName,
        mode = SaveMode.Ignore,
        options = Map("dataSchema" -> dataSchema.json),
        partitionColumns = Seq("p1", "p2"))

      assert(table("t").collect().isEmpty)
    }
  }

  test("Hadoop style globbing") {
    withTempPath { file =>
      partitionedTestDF.save(
        source = dataSourceName,
        mode = SaveMode.Overwrite,
        options = Map("path" -> file.getCanonicalPath),
        partitionColumns = Seq("p1", "p2"))

      val df = load(
        source = dataSourceName,
        options = Map(
          "path" -> s"${file.getCanonicalPath}/p1=*/p2=???",
          "dataSchema" -> dataSchema.json))

      val expectedPaths = Set(
        s"${file.getCanonicalFile}/p1=1/p2=foo",
        s"${file.getCanonicalFile}/p1=2/p2=foo",
        s"${file.getCanonicalFile}/p1=1/p2=bar",
        s"${file.getCanonicalFile}/p1=2/p2=bar"
      ).map { p =>
        val path = new Path(p)
        val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        path.makeQualified(fs.getUri, fs.getWorkingDirectory).toString
      }
      val actualPaths = df.queryExecution.analyzed.collectFirst {
        case LogicalRelation(relation: FSBasedRelation) =>
          relation.paths.toSet
      }.getOrElse {
        fail("Expect an FSBasedRelation, but none could be found")
      }

      assert(actualPaths === expectedPaths)
      checkAnswer(df, partitionedTestDF.collect())
    }
  }
}

class FSBasedOrcRelationSuite extends OrcRelationTest {
  override val dataSourceName: String = classOf[DefaultSource].getCanonicalName

  import sqlContext._
  import sqlContext.implicits._

  test("save()/load() - partitioned table - simple queries - partition columns in data") {
    withTempDir { file =>
      val basePath = new Path(file.getCanonicalPath)
      val fs = basePath.getFileSystem(SparkHadoopUtil.get.conf)
      val qualifiedBasePath = fs.makeQualified(basePath)

      for (p1 <- 1 to 2; p2 <- Seq("foo", "bar")) {
        val partitionDir = new Path(qualifiedBasePath, s"p1=$p1/p2=$p2")
        sparkContext
          .parallelize(for (i <- 1 to 3) yield (i, s"val_$i", p1))
          .toDF("a", "b", "p1")
          .saveAsOrcFile(partitionDir.toString)
      }

      val dataSchemaWithPartition =
        StructType(dataSchema.fields :+ StructField("p1", IntegerType, nullable = true))

      checkQueries(
        load(
          source = dataSourceName,
          options = Map(
            "path" -> file.getCanonicalPath,
            "dataSchema" -> dataSchemaWithPartition.json)))
    }
  }
}