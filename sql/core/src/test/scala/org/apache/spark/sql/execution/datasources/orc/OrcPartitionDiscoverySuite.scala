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

import org.apache.hadoop.fs.{Path, PathFilter}

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

// The data where the partitioning key exists only in the directory structure.
case class OrcParData(intField: Int, stringField: String)

// The data that also includes the partitioning key
case class OrcParDataWithKey(intField: Int, pi: Int, stringField: String, ps: String)

class TestFileFilter extends PathFilter {
  override def accept(path: Path): Boolean = path.getParent.getName != "p=2"
}

abstract class OrcPartitionDiscoveryTest extends OrcTest {
  val defaultPartitionName = "__HIVE_DEFAULT_PARTITION__"

  protected def withTempTable(tableName: String)(f: => Unit): Unit = {
    try f finally spark.catalog.dropTempView(tableName)
  }

  protected def makePartitionDir(
      basePath: File,
      defaultPartitionName: String,
      partitionCols: (String, Any)*): File = {
    val partNames = partitionCols.map { case (k, v) =>
      val valueString = if (v == null || v == "") defaultPartitionName else v.toString
      s"$k=$valueString"
    }

    val partDir = partNames.foldLeft(basePath) { (parent, child) =>
      new File(parent, child)
    }

    assert(partDir.mkdirs(), s"Couldn't create directory $partDir")
    partDir
  }

  test("read partitioned table - normal case") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", "bar")
      } {
        makeOrcFile(
          (1 to 10).map(i => OrcParData(i, i.toString)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      spark.read.orc(base.getCanonicalPath).createOrReplaceTempView("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", "bar")
          } yield Row(i, i.toString, pi, ps))

        checkAnswer(
          sql("SELECT intField, pi FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            _ <- Seq("foo", "bar")
          } yield Row(i, pi))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi = 1"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", "bar")
          } yield Row(i, i.toString, 1, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps = 'foo'"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, i.toString, pi, "foo"))
      }
    }
  }

  test("read partitioned table - with nulls") {
    withTempDir { base =>
      for {
      // Must be `Integer` rather than `Int` here. `null.asInstanceOf[Int]` results in a zero...
        pi <- Seq(1, null.asInstanceOf[Integer])
        ps <- Seq("foo", null.asInstanceOf[String])
      } {
        makeOrcFile(
          (1 to 10).map(i => OrcParData(i, i.toString)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      spark.read
        .option("hive.exec.default.partition.name", defaultPartitionName)
        .orc(base.getCanonicalPath)
        .createOrReplaceTempView("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, null.asInstanceOf[Integer])
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, i.toString, pi, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi IS NULL"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, i.toString, null, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps IS NULL"),
          for {
            i <- 1 to 10
            pi <- Seq(1, null.asInstanceOf[Integer])
          } yield Row(i, i.toString, pi, null))
      }
    }
  }

  test("SPARK-27162: handle pathfilter configuration correctly") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df = spark.range(2)
      df.write.orc(path + "/p=1")
      df.write.orc(path + "/p=2")
      assert(spark.read.orc(path).count() === 4)

      val extraOptions = Map(
        "mapred.input.pathFilter.class" -> classOf[TestFileFilter].getName,
        "mapreduce.input.pathFilter.class" -> classOf[TestFileFilter].getName
      )
      assert(spark.read.options(extraOptions).orc(path).count() === 2)
    }
  }
}

class OrcPartitionDiscoverySuite extends OrcPartitionDiscoveryTest with SharedSparkSession {
  override protected def sparkConf: SparkConf = super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("read partitioned table - partition key included in orc file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", "bar")
      } {
        makeOrcFile(
          (1 to 10).map(i => OrcParDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      spark.read.orc(base.getCanonicalPath).createOrReplaceTempView("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", "bar")
          } yield Row(i, i.toString, pi, ps))

        checkAnswer(
          sql("SELECT intField, pi FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            _ <- Seq("foo", "bar")
          } yield Row(i, pi))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi = 1"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", "bar")
          } yield Row(i, i.toString, 1, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps = 'foo'"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, i.toString, pi, "foo"))
      }
    }
  }

  test("read partitioned table - with nulls and partition keys are included in Orc file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", null.asInstanceOf[String])
      } {
        makeOrcFile(
          (1 to 10).map(i => OrcParDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      spark.read
        .option("hive.exec.default.partition.name", defaultPartitionName)
        .orc(base.getCanonicalPath)
        .createOrReplaceTempView("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, i.toString, pi, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps IS NULL"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, i.toString, pi, null))
      }
    }
  }
}

class OrcV1PartitionDiscoverySuite extends OrcPartitionDiscoveryTest with SharedSparkSession {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "orc")

  test("read partitioned table - partition key included in orc file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", "bar")
      } {
        makeOrcFile(
          (1 to 10).map(i => OrcParDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      spark.read.orc(base.getCanonicalPath).createOrReplaceTempView("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", "bar")
          } yield Row(i, pi, i.toString, ps))

        checkAnswer(
          sql("SELECT intField, pi FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            _ <- Seq("foo", "bar")
          } yield Row(i, pi))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi = 1"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", "bar")
          } yield Row(i, 1, i.toString, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps = 'foo'"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, pi, i.toString, "foo"))
      }
    }
  }

  test("read partitioned table - with nulls and partition keys are included in Orc file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", null.asInstanceOf[String])
      } {
        makeOrcFile(
          (1 to 10).map(i => OrcParDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      spark.read
        .option("hive.exec.default.partition.name", defaultPartitionName)
        .orc(base.getCanonicalPath)
        .createOrReplaceTempView("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, pi, i.toString, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps IS NULL"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, pi, i.toString, null))
      }
    }
  }
}
