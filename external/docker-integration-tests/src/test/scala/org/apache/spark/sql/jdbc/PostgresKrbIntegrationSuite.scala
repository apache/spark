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

import com.spotify.docker.client.messages.{ContainerConfig, HostConfig}
import java.io.{File, FileInputStream, FileOutputStream}
import java.sql.Connection
import java.util.Properties
import javax.security.auth.login.Configuration
import org.apache.hadoop.minikdc.MiniKdc
import scala.io.Source

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.connection.PostgresConnectionProvider
import org.apache.spark.sql.types.StringType
import org.apache.spark.tags.DockerTest
import org.apache.spark.util.{SecurityUtils, Utils}

@DockerTest
class PostgresKrbIntegrationSuite extends DockerJDBCIntegrationSuite {
  private var kdc: MiniKdc = _
  private var workDir: File = _
  private val postgresUser = s"postgres/$dockerIp"
  private var postgresPrincipal: String = _
  private var keytabFile: File = _

  override val db = new DatabaseOnDocker {
    override val imageName = "postgres:12.0"
    override val env = Map(
      "POSTGRES_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort = 5432

    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:postgresql://$ip:$port/postgres?user=$postgresPrincipal&gsslib=gssapi"

    override def getStartupProcessName: Option[String] = None

    override def beforeContainerStart(hostConfigBuilder: HostConfig.Builder,
        containerConfigBuilder: ContainerConfig.Builder): Unit = {
      def replaceIp(s: String): String = s.replace("__IP_ADDRESS_REPLACE_ME__", dockerIp)
      copyExecutableResource("postgres_krb_setup.sh", workDir, replaceIp)

      hostConfigBuilder.appendBinds(
        HostConfig.Bind.from(workDir.getAbsolutePath)
          .to("/docker-entrypoint-initdb.d").readOnly(true).build()
      )
    }
  }

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE DATABASE foo").executeUpdate()
    conn.setCatalog("foo")
    conn.prepareStatement("CREATE TABLE bar (c0 text)").executeUpdate()
    conn.prepareStatement("INSERT INTO bar VALUES ('hello')").executeUpdate()
  }

  private def copyExecutableResource(fileName: String, dir: File, processLine: String => String) = {
    val newEntry = new File(dir.getAbsolutePath, fileName)
    newEntry.createNewFile()
    val inputStream = new FileInputStream(getClass.getClassLoader.getResource(fileName).getFile)
    val outputStream = new FileOutputStream(newEntry)
    for (line <- Source.fromInputStream(inputStream).getLines()) {
      val processedLine = processLine(line) + System.lineSeparator()
      outputStream.write(processedLine.getBytes)
    }
    newEntry.setExecutable(true, false)
    logInfo(s"Created executable resource file: ${newEntry.getAbsolutePath}")
    newEntry
  }

  override def beforeAll(): Unit = {
    SecurityUtils.setGlobalKrbDebug(true)

    val kdcDir = Utils.createTempDir()
    val kdcConf = MiniKdc.createConf()
    kdcConf.setProperty(MiniKdc.DEBUG, "true")
    kdc = new MiniKdc(kdcConf, kdcDir)
    kdc.start()

    postgresPrincipal = s"$postgresUser@${kdc.getRealm}"

    workDir = Utils.createTempDir()
    keytabFile = new File(workDir, "postgres.keytab")
    kdc.createPrincipal(keytabFile, postgresUser)
    logInfo(s"Created keytab file: ${keytabFile.getAbsolutePath}")

    val config = new PostgresConnectionProvider.PGJDBCConfiguration(
      Configuration.getConfiguration, keytabFile.getAbsolutePath, postgresPrincipal)
    Configuration.setConfiguration(config)

    // This must be executed intentionally later
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      if (kdc != null) {
        kdc.stop()
      }
      Configuration.setConfiguration(null)
      SecurityUtils.setGlobalKrbDebug(false)
    } finally {
      super.afterAll()
    }
  }

  test("Basic read test") {
    // This makes sure Spark must do authentication
    Configuration.setConfiguration(null)

    val expectedResult = Set("hello").map(Row(_))

    val query = "SELECT c0 FROM bar"
    // query option to pass on the query string.
    val df = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("keytab", keytabFile.getAbsolutePath)
      .option("principal", postgresPrincipal)
      .option("query", query)
      .load()
    assert(df.collect().toSet === expectedResult)

    // query option in the create table path.
    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW queryOption
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$jdbcUrl', query '$query')
       """.stripMargin.replaceAll("\n", " "))
    assert(sql("select c0 from queryOption").collect().toSet === expectedResult)
  }

  test("Basic write test") {
    // This makes sure Spark must do authentication
    Configuration.setConfiguration(null)

    val props = new Properties
    props.setProperty("keytab", keytabFile.getAbsolutePath)
    props.setProperty("principal", postgresPrincipal)

    val tableName = "write_test"
    sqlContext.createDataFrame(Seq(("foo", "bar")))
      .write.jdbc(jdbcUrl, tableName, props)
    val df = sqlContext.read.jdbc(jdbcUrl, tableName, props)

    val schema = df.schema
    assert(schema.head.dataType == StringType)
    assert(schema(1).dataType == StringType)
    val rows = df.collect()
    assert(rows.length === 1)
    assert(rows(0).getString(0) === "foo")
    assert(rows(0).getString(1) === "bar")
  }
}
