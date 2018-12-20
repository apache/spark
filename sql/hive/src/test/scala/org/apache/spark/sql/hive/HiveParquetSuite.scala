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

package org.apache.spark.sql.hive

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf

case class Cases(lower: String, UPPER: String)

class HiveParquetSuite extends QueryTest with ParquetTest with TestHiveSingleton {

  test("Case insensitive attribute names") {
    withParquetTable((1 to 4).map(i => Cases(i.toString, i.toString)), "cases") {
      val expected = (1 to 4).map(i => Row(i.toString))
      checkAnswer(sql("SELECT upper FROM cases"), expected)
      checkAnswer(sql("SELECT LOWER FROM cases"), expected)
    }
  }

  test("SELECT on Parquet table") {
    val data = (1 to 4).map(i => (i, s"val_$i"))
    withParquetTable(data, "t") {
      checkAnswer(sql("SELECT * FROM t"), data.map(Row.fromTuple))
    }
  }

  test("Simple column projection + filter on Parquet table") {
    withParquetTable((1 to 4).map(i => (i % 2 == 0, i, s"val_$i")), "t") {
      checkAnswer(
        sql("SELECT `_1`, `_3` FROM t WHERE `_1` = true"),
        Seq(Row(true, "val_2"), Row(true, "val_4")))
    }
  }

  test("Converting Hive to Parquet Table via saveAsParquetFile") {
    withTempPath { dir =>
      sql("SELECT * FROM src").write.parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("p")
      withTempView("p") {
        checkAnswer(
          sql("SELECT * FROM src ORDER BY key"),
          sql("SELECT * from p ORDER BY key").collect().toSeq)
      }
    }
  }

  test("INSERT OVERWRITE TABLE Parquet table") {
    // Don't run with vectorized: currently relies on UnsafeRow.
    withParquetTable((1 to 10).map(i => (i, s"val_$i")), "t", false) {
      withTempPath { file =>
        sql("SELECT * FROM t LIMIT 1").write.parquet(file.getCanonicalPath)
        spark.read.parquet(file.getCanonicalPath).createOrReplaceTempView("p")
        withTempView("p") {
          // let's do three overwrites for good measure
          sql("INSERT OVERWRITE TABLE p SELECT * FROM t")
          sql("INSERT OVERWRITE TABLE p SELECT * FROM t")
          sql("INSERT OVERWRITE TABLE p SELECT * FROM t")
          checkAnswer(sql("SELECT * FROM p"), sql("SELECT * FROM t").collect().toSeq)
        }
      }
    }
  }

  test("SPARK-25206: wrong records are returned by filter pushdown " +
    "when Hive metastore schema and parquet schema are in different letter cases") {
    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> true.toString) {
      withTempPath { path =>
        val data = spark.range(1, 10).toDF("id")
        data.write.parquet(path.getCanonicalPath)
        withTable("SPARK_25206") {
          sql("CREATE TABLE SPARK_25206 (ID LONG) USING parquet LOCATION " +
            s"'${path.getCanonicalPath}'")
          checkAnswer(sql("select id from SPARK_25206 where id > 0"), data)
        }
      }
    }
  }

  test("SPARK-25271: write empty map into hive parquet table") {
    import testImplicits._

    Seq(Map(1 -> "a"), Map.empty[Int, String]).toDF("m").createOrReplaceTempView("p")
    withTempView("p") {
      val targetTable = "targetTable"
      withTable(targetTable) {
        sql(s"CREATE TABLE $targetTable STORED AS PARQUET AS SELECT m FROM p")
        checkAnswer(sql(s"SELECT m FROM $targetTable"),
          Row(Map(1 -> "a")) :: Row(Map.empty[Int, String]) :: Nil)
      }
    }
  }
}
