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

package org.apache.spark.sql.execution.datasources.jdbc.connection

import java.sql.{Connection, Driver}
import java.util.Properties

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

class BasicConnectionProviderSuite extends ConnectionProviderSuiteBase with MockitoSugar {
  test("Check properties of BasicConnectionProvider") {
    val opts = options("jdbc:postgresql://localhost/postgres")
    val provider = new BasicConnectionProvider()
    assert(provider.name == "basic")
    assert(provider.getAdditionalProperties(opts).isEmpty())
  }

  test("Check that JDBC options don't contain data source configs") {
    val provider = new BasicConnectionProvider()
    val driver = mock[Driver]
    when(driver.connect(any(), any())).thenAnswer((invocation: InvocationOnMock) => {
      val props = invocation.getArguments().apply(1).asInstanceOf[Properties]
      val conn = mock[Connection]
      when(conn.getClientInfo()).thenReturn(props)
      conn
    })

    val opts = Map(
      JDBCOptions.JDBC_URL -> "jdbc:postgresql://localhost/postgres",
      JDBCOptions.JDBC_TABLE_NAME -> "table",
      JDBCOptions.JDBC_CONNECTION_PROVIDER -> "basic")
    val conn = provider.getConnection(driver, opts)
    assert(!conn.getClientInfo().containsKey(JDBCOptions.JDBC_URL))
    assert(!conn.getClientInfo().containsKey(JDBCOptions.JDBC_TABLE_NAME))
    assert(!conn.getClientInfo().containsKey(JDBCOptions.JDBC_CONNECTION_PROVIDER))
  }
}
