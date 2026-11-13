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

package org.apache.spark.sql.jdbc.v2

import java.util.Locale

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper}
import org.apache.spark.sql.connector.DataSourcePushdownTestUtils
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{H2Dialect, JdbcDialect, JdbcDialects}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class JDBCV2JoinPushdownSuite
  extends SharedSparkSession
  with ExplainSuiteHelper
  with DataSourcePushdownTestUtils
  with JDBCV2JoinPushdownIntegrationSuiteBase {
  val tempDir = Utils.createTempDir()
  override val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"

  override val jdbcDialect: JdbcDialect = H2Dialect()

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalogName.driver", "org.h2.Driver")

  override def caseConvert(identifier: String): String = identifier.toUpperCase(Locale.ROOT)

  override def beforeAll(): Unit = {
    Utils.classForName("org.h2.Driver")
    super.beforeAll()
    dataPreparation()
    // Registering the dialect because of CI running multiple tests. For example, in
    // QueryExecutionErrorsSuite H2 dialect is being registered, and somewhere it is
    // not registered back. The suite should be fixed, but to be safe for now, we are
    // always registering H2 dialect before test execution.
    JdbcDialects.registerDialect(H2Dialect())
  }

  test("SPARK-59345: a pushed join with a pushed LIMIT or top-N is not repeatable") {
    val join = s"$catalogAndNamespace.$casedJoinTableName1 a " +
      s"JOIN $catalogAndNamespace.$casedJoinTableName1 b ON b.id = a.id + 1"
    withSQLConf(SQLConf.DATA_SOURCE_V2_JOIN_PUSHDOWN.key -> "true") {
      def repeatableScans(df: DataFrame): Seq[Boolean] = {
        checkJoinPushed(df)
        df.queryExecution.optimizedPlan.collect {
          case s: DataSourceV2ScanRelation => s.repeatableScan
        }
      }
      assert(repeatableScans(sql(s"SELECT a.id FROM $join")) == Seq(true))
      // The database returns whichever rows come first, possibly different ones on each scan.
      Seq(
        s"SELECT a.id FROM $join LIMIT 1",
        s"SELECT a.id FROM $join ORDER BY a.address LIMIT 1"
      ).foreach { query =>
        val df = sql(query)
        checkLimitRemoved(df)
        assert(repeatableScans(df) == Seq(false))
      }
    }
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }
}
