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

package org.apache.spark.sql.jdbc

import java.sql.Connection
import java.util.Properties
import javax.security.auth.login.Configuration

import com.spotify.docker.client.messages.{ContainerConfig, HostConfig}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.connection.PostgresConnectionProvider
import org.apache.spark.sql.types.StringType
import org.apache.spark.tags.DockerTest

@DockerTest
class PostgresKrbIntegrationSuite extends DockerKrbJDBCIntegrationSuite {
  override protected val userName = s"postgres/$dockerIp"
  override protected val keytabFileName = "postgres.keytab"

  override val db = new DatabaseOnDocker {
    override val imageName = "postgres:12.0"
    override val env = Map(
      "POSTGRES_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort = 5432

    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:postgresql://$ip:$port/postgres?user=$principal&gsslib=gssapi"

    override def getStartupProcessName: Option[String] = None

    override def beforeContainerStart(
        hostConfigBuilder: HostConfig.Builder,
        containerConfigBuilder: ContainerConfig.Builder): Unit = {
      def replaceIp(s: String): String = s.replace("__IP_ADDRESS_REPLACE_ME__", dockerIp)
      copyExecutableResource("postgres_krb_setup.sh", workDir, replaceIp)

      hostConfigBuilder.appendBinds(
        HostConfig.Bind.from(workDir.getAbsolutePath)
          .to("/docker-entrypoint-initdb.d").readOnly(true).build()
      )
    }
  }

  override protected def setAuthentication(keytabFile: String, principal: String): Unit = {
    val config = new PostgresConnectionProvider.PGJDBCConfiguration(
      Configuration.getConfiguration, "pgjdbc", keytabFile, principal)
    Configuration.setConfiguration(config)
  }

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE DATABASE foo").executeUpdate()
    conn.setCatalog("foo")
    conn.prepareStatement("CREATE TABLE bar (c0 text)").executeUpdate()
    conn.prepareStatement("INSERT INTO bar VALUES ('hello')").executeUpdate()
  }

  test("Basic read test in query option") {
    // This makes sure Spark must do authentication
    Configuration.setConfiguration(null)

    val expectedResult = Set("hello").map(Row(_))

    val query = "SELECT c0 FROM bar"
    // query option to pass on the query string.
    val df = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("keytab", keytabFullPath)
      .option("principal", principal)
      .option("query", query)
      .load()
    assert(df.collect().toSet === expectedResult)
  }

  test("Basic read test in create table path") {
    // This makes sure Spark must do authentication
    Configuration.setConfiguration(null)

    val expectedResult = Set("hello").map(Row(_))

    val query = "SELECT c0 FROM bar"
    // query option in the create table path.
    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW queryOption
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$jdbcUrl', query '$query', keytab '$keytabFullPath', principal '$principal')
       """.stripMargin.replaceAll("\n", " "))
    assert(sql("select c0 from queryOption").collect().toSet === expectedResult)
  }

  test("Basic write test") {
    // This makes sure Spark must do authentication
    Configuration.setConfiguration(null)

    val props = new Properties
    props.setProperty("keytab", keytabFullPath)
    props.setProperty("principal", principal)

    val tableName = "write_test"
    sqlContext.createDataFrame(Seq(("foo", "bar")))
      .write.jdbc(jdbcUrl, tableName, props)
    val df = sqlContext.read.jdbc(jdbcUrl, tableName, props)

    val schema = df.schema
    assert(schema.map(_.dataType).toSeq === Seq(StringType, StringType))
    val rows = df.collect()
    assert(rows.length === 1)
    assert(rows(0).getString(0) === "foo")
    assert(rows(0).getString(1) === "bar")
  }
}
