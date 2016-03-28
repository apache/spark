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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.test.SharedSQLContext

class DDLSuite extends QueryTest with SharedSQLContext {

  test("Create/Drop/Alter/Describe Database") {
    val catalog = sqlContext.sessionState.catalog

    val databaseName = "db1"
    sql(s"CREATE DATABASE $databaseName")
    val db1 = catalog.getDatabase(databaseName)
    assert(db1 == CatalogDatabase(databaseName, "", "db1.db", Map.empty))

    sql(s"DESCRIBE DATABASE EXTENDED $databaseName").show(false)
    sql(s"ALTER DATABASE $databaseName SET DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')")
    sql(s"DESCRIBE DATABASE EXTENDED $databaseName").show(false)
    sql(s"ALTER DATABASE $databaseName SET DBPROPERTIES ('d'='d')")
    sql(s"DESCRIBE DATABASE EXTENDED $databaseName").show(false)
    sql(s"DROP DATABASE $databaseName CASCADE")
    assert(!catalog.databaseExists(databaseName))
  }
}