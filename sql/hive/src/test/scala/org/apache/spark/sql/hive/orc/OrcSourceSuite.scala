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

import org.apache.spark.sql.QueryTest
import org.scalatest.BeforeAndAfterAll
import java.io.File
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.orc.OrcData
import org.apache.spark.sql.hive.orc.OrcDataWithKey

// The data where the partitioning key exists only in the directory structure.
case class OrcData(intField: Int, stringField: String)
// The data that also includes the partitioning key
case class OrcDataWithKey(p: Int, intField: Int, stringField: String)

/**
 * A collection of tests for parquet data with various forms of partitioning.
 */
abstract class OrcTest extends QueryTest with BeforeAndAfterAll {
  var partitionedTableDir: File = null
  var partitionedTableDirWithKey: File = null

  override def beforeAll(): Unit = {
    partitionedTableDir = File.createTempFile("orctests", "sparksql")
    partitionedTableDir.delete()
    partitionedTableDir.mkdir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDir, s"p=$p")
      sparkContext.makeRDD(1 to 10)
        .map(i => OrcData(i, s"part-$p"))
        .saveAsOrcFile(partDir.getCanonicalPath)
    }

    partitionedTableDirWithKey = File.createTempFile("parquettests", "sparksql")
    partitionedTableDirWithKey.delete()
    partitionedTableDirWithKey.mkdir()

    (1 to 10).foreach { p =>
      val partDir = new File(partitionedTableDirWithKey, s"p=$p")
      sparkContext.makeRDD(1 to 10)
        .map(i => OrcDataWithKey(p, i, s"part-$p"))
        .saveAsOrcFile(partDir.getCanonicalPath)
    }
  }

  Seq("partitioned_parquet", "partitioned_parquet_with_key").foreach { table =>
    test(s"project the partitioning column $table") {
      checkAnswer(
        sql(s"SELECT p, count(*) FROM $table group by p"),
        (1, 10) ::
          (2, 10) ::
          (3, 10) ::
          (4, 10) ::
          (5, 10) ::
          (6, 10) ::
          (7, 10) ::
          (8, 10) ::
          (9, 10) ::
          (10, 10) :: Nil
      )
    }

    test(s"project partitioning and non-partitioning columns $table") {
      checkAnswer(
        sql(s"SELECT stringField, p, count(intField) FROM $table GROUP BY p, stringField"),
        ("part-1", 1, 10) ::
          ("part-2", 2, 10) ::
          ("part-3", 3, 10) ::
          ("part-4", 4, 10) ::
          ("part-5", 5, 10) ::
          ("part-6", 6, 10) ::
          ("part-7", 7, 10) ::
          ("part-8", 8, 10) ::
          ("part-9", 9, 10) ::
          ("part-10", 10, 10) :: Nil
      )
    }

    test(s"simple count $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table"),
        100)
    }

    test(s"pruned count $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE p = 1"),
        10)
    }

    test(s"multi-partition pruned count $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE p IN (1,2,3)"),
        30)
    }

    test(s"non-partition predicates $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE intField IN (1,2,3)"),
        30)
    }

    test(s"sum $table") {
      checkAnswer(
        sql(s"SELECT SUM(intField) FROM $table WHERE intField IN (1,2,3) AND p = 1"),
        1 + 2 + 3)
    }

    test(s"hive udfs $table") {
      checkAnswer(
        sql(s"SELECT concat(stringField, stringField) FROM $table"),
        sql(s"SELECT stringField FROM $table").map {
          case Row(s: String) => Row(s + s)
        }.collect().toSeq)
    }
  }

  test("non-part select(*)") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM normal_parquet"),
      10)
  }
}

class OrcSourceSuite {

}
