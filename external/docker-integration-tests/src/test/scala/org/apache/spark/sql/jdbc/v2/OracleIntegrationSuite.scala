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

import java.sql.Connection

import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.{DatabaseOnDocker, DockerJDBCIntegrationSuite}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * The following would be the steps to test this
 * 1. Build Oracle database in Docker, please refer below link about how to.
 *    https://github.com/oracle/docker-images/blob/master/OracleDatabase/SingleInstance/README.md
 * 2. export ORACLE_DOCKER_IMAGE_NAME=$ORACLE_DOCKER_IMAGE_NAME
 *    Pull oracle $ORACLE_DOCKER_IMAGE_NAME image - docker pull $ORACLE_DOCKER_IMAGE_NAME
 * 3. Start docker - sudo service docker start
 * 4. Run spark test - ./build/sbt -Pdocker-integration-tests
 *    "test-only org.apache.spark.sql.jdbc.v2.OracleIntegrationSuite"
 *
 * An actual sequence of commands to run the test is as follows
 *
 *  $ git clone https://github.com/oracle/docker-images.git
 *  // Head SHA: 3e352a22618070595f823977a0fd1a3a8071a83c
 *  $ cd docker-images/OracleDatabase/SingleInstance/dockerfiles
 *  $ ./buildDockerImage.sh -v 18.4.0 -x
 *  $ export ORACLE_DOCKER_IMAGE_NAME=oracle/database:18.4.0-xe
 *  $ cd $SPARK_HOME
 *  $ ./build/sbt -Pdocker-integration-tests
 *    "test-only org.apache.spark.sql.jdbc.v2.OracleIntegrationSuite"
 *
 * It has been validated with 18.4.0 Express Edition.
 */
@DockerTest
class OracleIntegrationSuite extends DockerJDBCIntegrationSuite with SharedSparkSession {
  override val db = new DatabaseOnDocker {
    override val imageName = sys.env("ORACLE_DOCKER_IMAGE_NAME")
    override val env = Map(
      "ORACLE_PWD" -> "oracle"
    )
    override val usesIpc = false
    override val jdbcPort: Int = 1521
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:oracle:thin:system/oracle@//$ip:$port/xe"
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.oracle", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.oracle.url", db.getJdbcUrl(dockerIp, externalPort))

  override val connectionTimeout = timeout(7.minutes)
  override def dataPreparation(conn: Connection): Unit = {}

  test("SPARK-33034: alter table ... add column") {
    withTable("oracle.alt_table") {
      sql("CREATE TABLE oracle.alt_table (ID STRING) USING _")
      sql("ALTER TABLE oracle.alt_table ADD COLUMNS (C1 STRING, C2 STRING)")
      var t = spark.table("oracle.alt_table")
      var expectedSchema = new StructType()
        .add("ID", StringType)
        .add("C1", StringType)
        .add("C2", StringType)
      assert(t.schema === expectedSchema)
      sql("ALTER TABLE oracle.alt_table ADD COLUMNS (C3 STRING)")
      t = spark.table("oracle.alt_table")
      expectedSchema = expectedSchema.add("C3", StringType)
      assert(t.schema === expectedSchema)
      // Add already existing column
      intercept[AnalysisException] {
        sql(s"ALTER TABLE oracle.alt_table ADD COLUMNS (C3 DOUBLE)")
      }
    }
    // Add a column to not existing table
    intercept[AnalysisException] {
      sql(s"ALTER TABLE oracle.not_existing_table ADD COLUMNS (C4 STRING)")
    }
  }

  test("SPARK-33034: alter table ... update column type") {
    withTable("oracle.alt_table") {
      sql("CREATE TABLE oracle.alt_table (ID INTEGER) USING _")
      sql("ALTER TABLE oracle.alt_table ALTER COLUMN id TYPE STRING")
      val t = spark.table("oracle.alt_table")
      val expectedSchema = new StructType().add("ID", StringType)
      assert(t.schema === expectedSchema)
      // Update not existing column
      intercept[AnalysisException] {
        sql("ALTER TABLE oracle.alt_table ALTER COLUMN bad_column TYPE DOUBLE")
      }
      // Update column to wrong type
      intercept[AnalysisException] {
        sql("ALTER TABLE oracle.alt_table ALTER COLUMN id TYPE bad_type")
      }
    }
    // Update column type in not existing table
    intercept[AnalysisException] {
      sql(s"ALTER TABLE oracle.not_existing_table ALTER COLUMN id TYPE DOUBLE")
    }
  }

  test("SPARK-33034: alter table ... update column nullability") {
    withTable("oracle.alt_table") {
      sql("CREATE TABLE oracle.alt_table (ID STRING NOT NULL) USING _")
      sql("ALTER TABLE oracle.alt_table ALTER COLUMN ID DROP NOT NULL")
      val t = spark.table("oracle.alt_table")
      val expectedSchema = new StructType().add("ID", StringType, nullable = true)
      assert(t.schema === expectedSchema)
      // Update nullability of not existing column
      intercept[AnalysisException] {
        sql("ALTER TABLE oracle.alt_table ALTER COLUMN bad_column DROP NOT NULL")
      }
    }
    // Update column nullability in not existing table
    intercept[AnalysisException] {
      sql(s"ALTER TABLE oracle.not_existing_table ALTER COLUMN ID DROP NOT NULL")
    }
  }
}
