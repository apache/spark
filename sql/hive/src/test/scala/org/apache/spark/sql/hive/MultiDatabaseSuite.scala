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

import org.apache.spark.sql.hive.test.HiveTestUtils
import org.apache.spark.sql.{QueryTest, SaveMode}

class MultiDatabaseSuite extends QueryTest with HiveTestUtils {
  private lazy val df = ctx.range(10).coalesce(1)

  test(s"saveAsTable() to non-default database - with USE - Overwrite") {
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.mode(SaveMode.Overwrite).saveAsTable("t")
        assert(ctx.tableNames().contains("t"))
        checkAnswer(ctx.table("t"), df)
      }

      assert(ctx.tableNames(db).contains("t"))
      checkAnswer(ctx.table(s"$db.t"), df)
    }
  }

  test(s"saveAsTable() to non-default database - without USE - Overwrite") {
    withTempDatabase { db =>
      df.write.mode(SaveMode.Overwrite).saveAsTable(s"$db.t")
      assert(ctx.tableNames(db).contains("t"))
      checkAnswer(ctx.table(s"$db.t"), df)
    }
  }

  test(s"saveAsTable() to non-default database - with USE - Append") {
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.mode(SaveMode.Overwrite).saveAsTable("t")
        df.write.mode(SaveMode.Append).saveAsTable("t")
        assert(ctx.tableNames().contains("t"))
        checkAnswer(ctx.table("t"), df.unionAll(df))
      }

      assert(ctx.tableNames(db).contains("t"))
      checkAnswer(ctx.table(s"$db.t"), df.unionAll(df))
    }
  }

  test(s"saveAsTable() to non-default database - without USE - Append") {
    withTempDatabase { db =>
      df.write.mode(SaveMode.Overwrite).saveAsTable(s"$db.t")
      df.write.mode(SaveMode.Append).saveAsTable(s"$db.t")
      assert(ctx.tableNames(db).contains("t"))
      checkAnswer(ctx.table(s"$db.t"), df.unionAll(df))
    }
  }

  test(s"insertInto() non-default database - with USE") {
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.mode(SaveMode.Overwrite).saveAsTable("t")
        assert(ctx.tableNames().contains("t"))

        df.write.insertInto(s"$db.t")
        checkAnswer(ctx.table(s"$db.t"), df.unionAll(df))
      }
    }
  }

  test(s"insertInto() non-default database - without USE") {
    withTempDatabase { db =>
      activateDatabase(db) {
        df.write.mode(SaveMode.Overwrite).saveAsTable("t")
        assert(ctx.tableNames().contains("t"))
      }

      assert(ctx.tableNames(db).contains("t"))

      df.write.insertInto(s"$db.t")
      checkAnswer(ctx.table(s"$db.t"), df.unionAll(df))
    }
  }

  test("Looks up tables in non-default database") {
    withTempDatabase { db =>
      activateDatabase(db) {
        ctx.sql("CREATE TABLE t (key INT)")
        checkAnswer(ctx.table("t"), ctx.emptyDataFrame)
      }

      checkAnswer(ctx.table(s"$db.t"), ctx.emptyDataFrame)
    }
  }

  test("Drops a table in a non-default database") {
    withTempDatabase { db =>
      activateDatabase(db) {
        ctx.sql(s"CREATE TABLE t (key INT)")
        assert(ctx.tableNames().contains("t"))
        assert(!ctx.tableNames("default").contains("t"))
      }

      assert(!ctx.tableNames().contains("t"))
      assert(ctx.tableNames(db).contains("t"))

      activateDatabase(db) {
        ctx.sql(s"DROP TABLE t")
        assert(!ctx.tableNames().contains("t"))
        assert(!ctx.tableNames("default").contains("t"))
      }

      assert(!ctx.tableNames().contains("t"))
      assert(!ctx.tableNames(db).contains("t"))
    }
  }

  test("Refreshes a table in a non-default database") {
    import org.apache.spark.sql.functions.lit

    withTempDatabase { db =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        activateDatabase(db) {
          ctx.sql(
            s"""CREATE EXTERNAL TABLE t (id BIGINT)
               |PARTITIONED BY (p INT)
               |STORED AS PARQUET
               |LOCATION '$path'
             """.stripMargin)

          checkAnswer(ctx.table("t"), ctx.emptyDataFrame)

          df.write.parquet(s"$path/p=1")
          ctx.sql("ALTER TABLE t ADD PARTITION (p=1)")
          ctx.sql("REFRESH TABLE t")
          checkAnswer(ctx.table("t"), df.withColumn("p", lit(1)))
        }
      }
    }
  }
}
