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

import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.{AnalysisException, QueryTest, SaveMode}

class MultiDatabaseSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  private lazy val df = sqlContext.range(10).coalesce(1)

  private def checkTablePath(dbName: String, tableName: String): Unit = {
    val metastoreTable = hiveContext.catalog.client.getTable(dbName, tableName)
    val expectedPath = hiveContext.catalog.client.getDatabase(dbName).location + "/" + tableName

    assert(metastoreTable.serdeProperties("path") === expectedPath)
  }

  test(s"saveAsTable() to non-default database - with USE - Overwrite") {
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.mode(SaveMode.Overwrite).saveAsTable("t")
        assert(sqlContext.tableNames().contains("t"))
        checkAnswer(sqlContext.table("t"), df)
      }

      assert(sqlContext.tableNames(db).contains("t"))
      checkAnswer(sqlContext.table(s"$db.t"), df)

      checkTablePath(db, "t")
    }
  }

  test(s"saveAsTable() to non-default database - without USE - Overwrite") {
    withTempDatabase { db =>
      df.write.mode(SaveMode.Overwrite).saveAsTable(s"$db.t")
      assert(sqlContext.tableNames(db).contains("t"))
      checkAnswer(sqlContext.table(s"$db.t"), df)

      checkTablePath(db, "t")
    }
  }

  test(s"createExternalTable() to non-default database - with USE") {
    withTempDatabase { db =>
      activateDatabase(db) {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          df.write.format("parquet").mode(SaveMode.Overwrite).save(path)

          sqlContext.createExternalTable("t", path, "parquet")
          assert(sqlContext.tableNames(db).contains("t"))
          checkAnswer(sqlContext.table("t"), df)

          sql(
            s"""
              |CREATE TABLE t1
              |USING parquet
              |OPTIONS (
              |  path '$path'
              |)
            """.stripMargin)
          assert(sqlContext.tableNames(db).contains("t1"))
          checkAnswer(sqlContext.table("t1"), df)
        }
      }
    }
  }

  test(s"createExternalTable() to non-default database - without USE") {
    withTempDatabase { db =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        df.write.format("parquet").mode(SaveMode.Overwrite).save(path)
        sqlContext.createExternalTable(s"$db.t", path, "parquet")

        assert(sqlContext.tableNames(db).contains("t"))
        checkAnswer(sqlContext.table(s"$db.t"), df)

        sql(
          s"""
              |CREATE TABLE $db.t1
              |USING parquet
              |OPTIONS (
              |  path '$path'
              |)
            """.stripMargin)
        assert(sqlContext.tableNames(db).contains("t1"))
        checkAnswer(sqlContext.table(s"$db.t1"), df)
      }
    }
  }

  test(s"saveAsTable() to non-default database - with USE - Append") {
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.mode(SaveMode.Overwrite).saveAsTable("t")
        df.write.mode(SaveMode.Append).saveAsTable("t")
        assert(sqlContext.tableNames().contains("t"))
        checkAnswer(sqlContext.table("t"), df.unionAll(df))
      }

      assert(sqlContext.tableNames(db).contains("t"))
      checkAnswer(sqlContext.table(s"$db.t"), df.unionAll(df))

      checkTablePath(db, "t")
    }
  }

  test(s"saveAsTable() to non-default database - without USE - Append") {
    withTempDatabase { db =>
      df.write.mode(SaveMode.Overwrite).saveAsTable(s"$db.t")
      df.write.mode(SaveMode.Append).saveAsTable(s"$db.t")
      assert(sqlContext.tableNames(db).contains("t"))
      checkAnswer(sqlContext.table(s"$db.t"), df.unionAll(df))

      checkTablePath(db, "t")
    }
  }

  test(s"insertInto() non-default database - with USE") {
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.mode(SaveMode.Overwrite).saveAsTable("t")
        assert(sqlContext.tableNames().contains("t"))

        df.write.insertInto(s"$db.t")
        checkAnswer(sqlContext.table(s"$db.t"), df.unionAll(df))
      }
    }
  }

  test(s"insertInto() non-default database - without USE") {
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.mode(SaveMode.Overwrite).saveAsTable("t")
        assert(sqlContext.tableNames().contains("t"))
      }

      assert(sqlContext.tableNames(db).contains("t"))

      df.write.insertInto(s"$db.t")
      checkAnswer(sqlContext.table(s"$db.t"), df.unionAll(df))
    }
  }

  test("Looks up tables in non-default database") {
    withTempDatabase { db =>
      activateDatabase(db) {
        sql("CREATE TABLE t (key INT)")
        checkAnswer(sqlContext.table("t"), sqlContext.emptyDataFrame)
      }

      checkAnswer(sqlContext.table(s"$db.t"), sqlContext.emptyDataFrame)
    }
  }

  test("Drops a table in a non-default database") {
    withTempDatabase { db =>
      activateDatabase(db) {
        sql(s"CREATE TABLE t (key INT)")
        assert(sqlContext.tableNames().contains("t"))
        assert(!sqlContext.tableNames("default").contains("t"))
      }

      assert(!sqlContext.tableNames().contains("t"))
      assert(sqlContext.tableNames(db).contains("t"))

      activateDatabase(db) {
        sql(s"DROP TABLE t")
        assert(!sqlContext.tableNames().contains("t"))
        assert(!sqlContext.tableNames("default").contains("t"))
      }

      assert(!sqlContext.tableNames().contains("t"))
      assert(!sqlContext.tableNames(db).contains("t"))
    }
  }

  test("Refreshes a table in a non-default database - with USE") {
    import org.apache.spark.sql.functions.lit

    withTempDatabase { db =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        activateDatabase(db) {
          sql(
            s"""CREATE EXTERNAL TABLE t (id BIGINT)
               |PARTITIONED BY (p INT)
               |STORED AS PARQUET
               |LOCATION '$path'
             """.stripMargin)

          checkAnswer(sqlContext.table("t"), sqlContext.emptyDataFrame)

          df.write.parquet(s"$path/p=1")
          sql("ALTER TABLE t ADD PARTITION (p=1)")
          sql("REFRESH TABLE t")
          checkAnswer(sqlContext.table("t"), df.withColumn("p", lit(1)))

          df.write.parquet(s"$path/p=2")
          sql("ALTER TABLE t ADD PARTITION (p=2)")
          hiveContext.refreshTable("t")
          checkAnswer(
            sqlContext.table("t"),
            df.withColumn("p", lit(1)).unionAll(df.withColumn("p", lit(2))))
        }
      }
    }
  }

  test("Refreshes a table in a non-default database - without USE") {
    import org.apache.spark.sql.functions.lit

    withTempDatabase { db =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        sql(
          s"""CREATE EXTERNAL TABLE $db.t (id BIGINT)
               |PARTITIONED BY (p INT)
               |STORED AS PARQUET
               |LOCATION '$path'
             """.stripMargin)

        checkAnswer(sqlContext.table(s"$db.t"), sqlContext.emptyDataFrame)

        df.write.parquet(s"$path/p=1")
        sql(s"ALTER TABLE $db.t ADD PARTITION (p=1)")
        sql(s"REFRESH TABLE $db.t")
        checkAnswer(sqlContext.table(s"$db.t"), df.withColumn("p", lit(1)))

        df.write.parquet(s"$path/p=2")
        sql(s"ALTER TABLE $db.t ADD PARTITION (p=2)")
        hiveContext.refreshTable(s"$db.t")
        checkAnswer(
          sqlContext.table(s"$db.t"),
          df.withColumn("p", lit(1)).unionAll(df.withColumn("p", lit(2))))
      }
    }
  }

  test("invalid database name and table names") {
    {
      val message = intercept[AnalysisException] {
        df.write.format("parquet").saveAsTable("`d:b`.`t:a`")
      }.getMessage
      assert(message.contains("is not a valid name for metastore"))
    }

    {
      val message = intercept[AnalysisException] {
        df.write.format("parquet").saveAsTable("`d:b`.`table`")
      }.getMessage
      assert(message.contains("is not a valid name for metastore"))
    }

    withTempPath { dir =>
      val path = dir.getCanonicalPath

      {
        val message = intercept[AnalysisException] {
          sql(
            s"""
            |CREATE TABLE `d:b`.`t:a` (a int)
            |USING parquet
            |OPTIONS (
            |  path '$path'
            |)
            """.stripMargin)
        }.getMessage
        assert(message.contains("is not a valid name for metastore"))
      }

      {
        val message = intercept[AnalysisException] {
          sql(
            s"""
              |CREATE TABLE `d:b`.`table` (a int)
              |USING parquet
              |OPTIONS (
              |  path '$path'
              |)
              """.stripMargin)
        }.getMessage
        assert(message.contains("is not a valid name for metastore"))
      }
    }
  }
}
