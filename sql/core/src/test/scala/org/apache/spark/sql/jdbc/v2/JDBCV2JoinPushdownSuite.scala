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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ExplainSuiteHelper, QueryTest}
import org.apache.spark.sql.connector.DataSourcePushdownTestUtils
import org.apache.spark.sql.jdbc.{H2Dialect, JdbcDialect}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class JDBCV2JoinPushdownSuite
  extends QueryTest
  with SharedSparkSession
  with ExplainSuiteHelper
  with DataSourcePushdownTestUtils
  with JDBCV2JoinPushdownIntegrationSuiteBase {
  val tempDir = Utils.createTempDir()
  override val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"

  override val jdbcDialect: JdbcDialect = H2Dialect()

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.h2.driver", "org.h2.Driver")

  override def qualifyTableName(tableName: String): String = s""""$namespace"."$tableName""""

  override def qualifySchemaName(schemaName: String): String = s""""$namespace""""

  override def beforeAll(): Unit = {
    Utils.classForName("org.h2.Driver")
    super.beforeAll()
    dataPreparation()
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }
}
