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

package org.apache.spark.sql.execution.datasources.text

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}

abstract class WholeTextFileSuite extends QueryTest with SharedSparkSession {

  // Hadoop's FileSystem caching does not use the Configuration as part of its cache key, which
  // can cause Filesystem.get(Configuration) to return a cached instance created with a different
  // configuration than the one passed to get() (see HADOOP-8490 for more details). This caused
  // hard-to-reproduce test failures, since any suites that were run after this one would inherit
  // the new value of "fs.local.block.size" (see SPARK-5227 and SPARK-5679). To work around this,
  // we disable FileSystem caching in this suite.
  protected override def sparkConf =
    super.sparkConf.set("spark.hadoop.fs.file.impl.disable.cache", "true")

  test("reading text file with option wholetext=true") {
    val df = spark.read.option("wholetext", "true")
      .format("text")
      .load(testFile("test-data/text-suite.txt"))
    // schema
    assert(df.schema == new StructType().add("value", StringType))

    // verify content
    val data = df.collect()
    assert(data(0) ==
      Row(
        // scalastyle:off nonascii
        """This is a test file for the text data source
          |1+1
          |数据砖头
          |"doh"
          |""".stripMargin))
    // scalastyle:on nonascii
    assert(data.length == 1)
  }

  test("correctness of wholetext option") {
    import org.apache.spark.sql.catalyst.util._
    withTempDir { dir =>
      val file1 = new File(dir, "text1.txt")
      stringToFile(file1,
        """text file 1 contents.
          |From: None to: ??
        """.stripMargin)
      val file2 = new File(dir, "text2.txt")
      stringToFile(file2, "text file 2 contents.")
      val file3 = new File(dir, "text3.txt")
      stringToFile(file3, "text file 3 contents.")
      val df = spark.read.option("wholetext", "true").text(dir.getAbsolutePath)
      // Since wholetext option reads each file into a single row, df.length should be no. of files.
      val data = df.sort("value").collect()
      assert(data.length == 3)
      // Each files should represent a single Row/element in Dataframe/Dataset
      assert(data(0) == Row(
        """text file 1 contents.
          |From: None to: ??
        """.stripMargin))
      assert(data(1) == Row(
        """text file 2 contents.""".stripMargin))
      assert(data(2) == Row(
        """text file 3 contents.""".stripMargin))
    }
  }


  test("Correctness of wholetext option with gzip compression mode.") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val df1 = spark.range(0, 1000).selectExpr("CAST(id AS STRING) AS s").repartition(1)
      df1.write.option("compression", "gzip").mode("overwrite").text(path)
      // On reading through wholetext mode, one file will be read as a single row, i.e. not
      // delimited by "next line" character.
      val expected = Row(df1.collect().map(_.getString(0)).mkString("", "\n", "\n"))
      Seq(10, 100, 1000).foreach { bytes =>
        withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> bytes.toString) {
          val df2 = spark.read.option("wholetext", "true").format("text").load(path)
          val result = df2.collect().head
          assert(result === expected)
        }
      }
    }
  }
}

class WholeTextFileV1Suite extends WholeTextFileSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "text")
}

class WholeTextFileV2Suite extends WholeTextFileSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
