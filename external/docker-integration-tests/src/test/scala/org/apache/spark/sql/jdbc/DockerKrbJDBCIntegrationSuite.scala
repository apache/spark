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

import java.io.{File, FileInputStream, FileOutputStream}
import java.sql.Connection
import java.util.Properties
import javax.security.auth.login.Configuration

import scala.io.Source

import org.apache.hadoop.minikdc.MiniKdc

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.{SecurityUtils, Utils}

abstract class DockerKrbJDBCIntegrationSuite extends DockerJDBCIntegrationSuite {
  private var kdc: MiniKdc = _
  private val KRB5_CONF_PROP = "java.security.krb5.conf"
  protected var entryPointDir: File = _
  protected var initDbDir: File = _
  protected val userName: String
  protected var principal: String = _
  protected val keytabFileName: String
  protected var keytabFullPath: String = _
  protected def setAuthentication(keytabFile: String, principal: String): Unit

  override def beforeAll(): Unit = {
    SecurityUtils.setGlobalKrbDebug(true)

    val kdcDir = Utils.createTempDir()
    val kdcConf = MiniKdc.createConf()
    kdcConf.setProperty(MiniKdc.DEBUG, "true")
    kdc = new MiniKdc(kdcConf, kdcDir)
    kdc.start()

    principal = s"$userName@${kdc.getRealm}"

    entryPointDir = Utils.createTempDir()
    initDbDir = Utils.createTempDir()
    val keytabFile = new File(initDbDir, keytabFileName)
    keytabFullPath = keytabFile.getAbsolutePath
    kdc.createPrincipal(keytabFile, userName)
    logInfo(s"Created keytab file: $keytabFullPath")

    setAuthentication(keytabFullPath, principal)

    // This must be executed intentionally later
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      if (kdc != null) {
        kdc.stop()
        kdc = null
      }
      Configuration.setConfiguration(null)
      SecurityUtils.setGlobalKrbDebug(false)
    } finally {
      super.afterAll()
    }
  }

  protected def replaceIp(s: String): String = s.replace("__IP_ADDRESS_REPLACE_ME__", dockerIp)

  protected def copyExecutableResource(
      fileName: String, dir: File, processLine: String => String = identity) = {
    val newEntry = new File(dir.getAbsolutePath, fileName)
    newEntry.createNewFile()
    Utils.tryWithResource(
      new FileInputStream(getClass.getClassLoader.getResource(fileName).getFile)
    ) { inputStream =>
      val outputStream = new FileOutputStream(newEntry)
      try {
        for (line <- Source.fromInputStream(inputStream).getLines()) {
          val processedLine = processLine(line) + System.lineSeparator()
          outputStream.write(processedLine.getBytes)
        }
      } finally {
        outputStream.close()
      }
    }
    newEntry.setExecutable(true, false)
    logInfo(s"Created executable resource file: ${newEntry.getAbsolutePath}")
    newEntry
  }

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE TABLE bar (c0 VARCHAR(8))").executeUpdate()
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

  test("SPARK-35226: JDBCOption should accept refreshKrb5Config parameter") {
    // This makes sure Spark must do authentication
    Configuration.setConfiguration(null)
    withTempDir { dir =>
      val dummyKrb5Conf = File.createTempFile("dummy", "krb5.conf", dir)
      val origKrb5Conf = sys.props(KRB5_CONF_PROP)
      try {
        // Set dummy krb5.conf and refresh config so this assertion is expected to fail.
        // The thrown exception is dependent on the actual JDBC driver class.
        intercept[Exception] {
          sys.props(KRB5_CONF_PROP) = dummyKrb5Conf.getAbsolutePath
          spark.read.format("jdbc")
            .option("url", jdbcUrl)
            .option("keytab", keytabFullPath)
            .option("principal", principal)
            .option("refreshKrb5Config", "true")
            .option("dbtable", "bar")
            .load()
        }

        sys.props(KRB5_CONF_PROP) = origKrb5Conf
        val df = spark.read.format("jdbc")
          .option("url", jdbcUrl)
          .option("keytab", keytabFullPath)
          .option("principal", principal)
          .option("refreshKrb5Config", "true")
          .option("dbtable", "bar")
          .load()
        val result = df.collect().map(_.getString(0))
        assert(result.length === 1)
        assert(result(0) === "hello")
      } finally {
        sys.props(KRB5_CONF_PROP) = origKrb5Conf
      }
    }
  }
}
