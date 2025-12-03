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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition, JDBCRDD}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class PostgresDialectSuite extends SparkFunSuite with MockitoSugar with SharedSparkSession {

  gridTest("PostgresDialect sets autoCommit correctly with fetchSize option")(
    Seq(
      ("fetchsize", Some("100"), true),
      ("fetchSize", Some("100"), true),
      ("FETCHSIZE", Some("100"), true),
      ("fetchsize", Some("0"), false),
      ("fetchsize", None, false)
    )
  ) { case (optionKey: String, optionValue: Option[String], shouldSetAutoCommit: Boolean) =>
    val conn = mock[Connection]
    when(conn.prepareStatement(any[String], any[Int], any[Int]))
      .thenReturn(mock[java.sql.PreparedStatement])

    val optionsMap = Map(
      "url" -> "jdbc:postgresql://localhost/test",
      "dbtable" -> "test_table"
    ) ++ optionValue.map(v => Map(optionKey -> v)).getOrElse(Map.empty)

    val options = new JDBCOptions(CaseInsensitiveMap(optionsMap), None)

    val schema = StructType(Seq(StructField("id", IntegerType)))
    val partition = new JDBCPartition(null, 0)
    val rdd = new JDBCRDD(
      spark.sparkContext,
      _ => conn,
      schema,
      Array.empty,
      Array.empty,
      Array(partition),
      "jdbc:postgresql://localhost/test",
      options,
      Some(PostgresDialect()),
      None,
      None,
      0,
      Array.empty,
      0,
      Map.empty
    )

    try {
      rdd.compute(partition, org.apache.spark.TaskContext.empty())
    } catch {
      case _: Exception => // Expected to fail, we just want beforeFetch to be called
    }

    if (shouldSetAutoCommit) {
      verify(conn).setAutoCommit(false)
    } else {
      verify(conn, never()).setAutoCommit(false)
    }
  }
}
