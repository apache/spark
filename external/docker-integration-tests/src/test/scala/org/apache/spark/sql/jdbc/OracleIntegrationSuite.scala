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

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.tags.DockerTest

/**
 * This patch was tested using the Oracle docker. Created this integration suite for the same.
 * The ojdbc6-11.2.0.2.0.jar was to be downloaded from the maven repository. Since there was
 * no jdbc jar available in the maven repository, the jar was downloaded from oracle site
 * manually and installed in the local; thus tested. So, for SparkQA test case run, the
 * ojdbc jar might be manually placed in the local maven repository(com/oracle/ojdbc6/11.2.0.2.0)
 * while Spark QA test run.
 *
 * The following would be the steps to test this
 * 1. Pull oracle 11g image - docker pull wnameless/oracle-xe-11g
 * 2. Start docker - sudo service docker start
 * 3. Download oracle 11g driver jar and put it in maven local repo:
 *    (com/oracle/ojdbc6/11.2.0.2.0/ojdbc6-11.2.0.2.0.jar)
 * 4. The timeout and interval parameter to be increased from 60,1 to a high value for oracle test
 *    in DockerJDBCIntegrationSuite.scala (Locally tested with 200,200 and executed successfully).
 * 5. Run spark test - ./build/sbt "test-only org.apache.spark.sql.jdbc.OracleIntegrationSuite"
 *
 * All tests in this suite are ignored because of the dependency with the oracle jar from maven
 * repository.
 */
@DockerTest
class OracleIntegrationSuite extends DockerJDBCIntegrationSuite with SharedSQLContext {
  import testImplicits._

  override val db = new DatabaseOnDocker {
    override val imageName = "wnameless/oracle-xe-11g:14.04.4"
    override val env = Map(
      "ORACLE_ROOT_PASSWORD" -> "oracle"
    )
    override val usesIpc = false
    override val jdbcPort: Int = 1521
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:oracle:thin:system/oracle@//$ip:$port/xe"
    override def getStartupProcessName: Option[String] = None
  }

  override def dataPreparation(conn: Connection): Unit = {
  }

  test("SPARK-12941: String datatypes to be mapped to Varchar in Oracle") {
    // create a sample dataframe with string type
    val df1 = sparkContext.parallelize(Seq(("foo"))).toDF("x")
    // write the dataframe to the oracle table tbl
    df1.write.jdbc(jdbcUrl, "tbl2", new Properties)
    // read the table from the oracle
    val dfRead = sqlContext.read.jdbc(jdbcUrl, "tbl2", new Properties)
    // get the rows
    val rows = dfRead.collect()
    // verify the data type is inserted
    val types = rows(0).toSeq.map(x => x.getClass.toString)
    assert(types(0).equals("class java.lang.String"))
    // verify the value is the inserted correct or not
    assert(rows(0).getString(0).equals("foo"))
  }
}
