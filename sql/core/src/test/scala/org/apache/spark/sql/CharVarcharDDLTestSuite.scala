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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.connector.InMemoryPartitionTableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

trait CharVarcharDDLTestSuite extends QueryTest with SQLTestUtils {

  def format: String

  test("change column from char to string type") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(4)) USING $format")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE STRING")
    }
  }

  test("change column from string to char type") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c STRING) USING $format")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE CHAR(5)")
    }
  }

  test("change column from char to char type") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(4)) USING $format")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE CHAR(5)")
    }
  }

  test("change column from int to char type") {
    withTable("t") {
      sql(s"CREATE TABLE t(i int, c CHAR(4)) USING $format")
      sql("ALTER TABLE t CHANGE COLUMN i TYPE CHAR(5)")
    }
  }
}

class FileSourceCharVarcharDDLTestSuite extends CharVarcharDDLTestSuite with SharedSparkSession {
  override def format: String = "parquet"
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "parquet")
  }
}

class DSV2CharVarcharDDLTestSuite extends CharVarcharDDLTestSuite
  with SharedSparkSession {
  override def format: String = "foo"
  protected override def sparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.testcat", classOf[InMemoryPartitionTableCatalog].getName)
      .set(SQLConf.DEFAULT_CATALOG.key, "testcat")
  }
}
