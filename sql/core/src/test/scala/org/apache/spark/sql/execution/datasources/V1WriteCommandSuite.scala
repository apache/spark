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

package org.apache.spark.sql.execution.datasources

import org.apache.logging.log4j.Level

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

abstract class V1WriteCommandSuiteBase extends QueryTest with SQLTestUtils {

  import testImplicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    (0 to 20).map(i => (i, i % 5, (i % 10).toString))
      .toDF("i", "j", "k")
      .write
      .saveAsTable("t0")
  }

  protected override def afterAll(): Unit = {
    sql("drop table if exists t0")
    super.afterAll()
  }

  protected def checkOrdering(logAppender: LogAppender, matched: Boolean): Unit = {
    assert(logAppender.loggingEvents.exists { event =>
      event.getLevel.equals(Level.INFO) &&
      event.getMessage.getFormattedMessage.contains("Output ordering is matched")
    } == matched)
  }
}

class V1WriteCommandSuite extends V1WriteCommandSuiteBase with SharedSparkSession {
  test("FileFormatWriter should log when ordering is matched") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.PLANNED_WRITE_ENABLED.key -> enabled.toString) {
        withTable("t1", "t2") {
          val logAppender1 = new LogAppender()
          withLogAppender(logAppender1) {
            sql("CREATE TABLE t1 USING PARQUET AS SELECT * FROM t0")
            checkOrdering(logAppender1, matched = true)
          }
          val logAppender2 = new LogAppender()
          // Case 2: query is already sorted.
          withLogAppender(logAppender2) {
            sql("CREATE TABLE t2 USING PARQUET PARTITIONED BY (k) " +
              "AS SELECT * FROM t0 ORDER BY k")
            // TODO: in this case the executed plan of the command's query will be an AQE plan,
            // and FileFormatWriter is not able to detect the actual ordering of the AQE plan.
            checkOrdering(logAppender2, matched = false)
          }
        }
      }
    }
  }

  test("create table with partition columns") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.PLANNED_WRITE_ENABLED.key -> enabled.toString) {
        withTable("t") {
          val logAppender = new LogAppender("create table")
          withLogAppender(logAppender) {
            sql("CREATE TABLE t USING PARQUET PARTITIONED BY (k) AS SELECT * FROM t0")
            checkOrdering(logAppender, matched = enabled)
          }
        }
      }
    }
  }

  test("insert into table with partition, bucketed and sort columns") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.PLANNED_WRITE_ENABLED.key -> enabled.toString) {
        withTable("t") {
          sql(
            """
              |CREATE TABLE t(i INT, j INT) USING PARQUET
              |PARTITIONED BY (k STRING)
              |CLUSTERED BY (i, j) SORTED BY (j) INTO 2 BUCKETS
              |""".stripMargin)
          val logAppender = new LogAppender("insert into")
          withLogAppender(logAppender) {
            sql("INSERT INTO t SELECT * FROM t0")
            checkOrdering(logAppender, matched = enabled)
          }
        }
      }
    }
  }
}
