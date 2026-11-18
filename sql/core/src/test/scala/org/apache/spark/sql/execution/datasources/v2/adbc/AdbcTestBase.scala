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

package org.apache.spark.sql.execution.datasources.v2.adbc

import java.sql.{Connection, DriverManager}

import scala.util.Using

import org.apache.arrow.adbc.driver.jdbc.JdbcDriverFactory

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

/** An H2 database for the tests, accessed with the ADBC JDBC bridge driver. */
trait AdbcTestBase extends SharedSparkSession {

  protected val tempDir = Utils.createTempDir()
  protected val url = s"jdbc:h2:${tempDir.getCanonicalPath}/adbc;user=testUser;password=testPass"

  protected val jdbcBridge = classOf[JdbcDriverFactory].getName

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterAll()
    }
  }

  // The Arrow memory allocated before a test, e.g. by other suites in the same JVM.
  private var allocatedBefore = 0L

  override def beforeEach(): Unit = {
    super.beforeEach()
    allocatedBefore = ArrowUtils.rootAllocator.getAllocatedMemory
  }

  override def afterEach(): Unit = {
    try {
      // Readers, writers and planning connections must release all the Arrow memory they use.
      assertArrowMemoryReleased()
    } finally {
      super.afterEach()
    }
  }

  protected def assertArrowMemoryReleased(): Unit =
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == allocatedBefore)

  /** Runs `f` with a JDBC connection to the database. */
  protected def withJdbc[T](f: Connection => T): T =
    Using.resource(DriverManager.getConnection(url))(f)

  /** Executes SQL statements with JDBC. */
  protected def execJdbc(sqls: String*): Unit = withJdbc { conn =>
    sqls.foreach(sql => Using.resource(conn.createStatement())(_.execute(sql)))
  }
}
