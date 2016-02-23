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

import java.math.BigDecimal
import java.sql.{Connection, Date, Timestamp}
import java.util.Properties

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

@DockerTest
class OracleIntegrationSuite extends DockerJDBCIntegrationSuite with SharedSQLContext {
  import testImplicits._

  override val db = new DatabaseOnDocker {
    override val imageName = "wnameless/oracle-xe-11g:latest"
    override val env = Map(
      "ORACLE_ROOT_PASSWORD" -> "oracle"
    )
    override val jdbcPort: Int = 1521
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:oracle:thin:system/oracle@//$ip:$port/xe"
  }

  ignore("SPARK-12941: String datatypes to be mapped to Varchar in Oracle") {
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
