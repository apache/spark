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
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.test.SharedSQLContext

class DDLSuite extends QueryTest with SharedSQLContext {

  test("SPARK-14123: Create / drop function DDL") {
    val sql1 =
      """
        |CREATE TEMPORARY FUNCTION helloworld1 AS
        |'spark.example.SimpleUDFExample1' USING JAR '/path/to/jar1',
        |JAR '/path/to/jar2'
      """.stripMargin
    val sql2 =
      """
        |CREATE FUNCTION helloworld2 AS
        |'spark.example.SimpleUDFExample2' USING ARCHIVE '/path/to/archive',
        |FILE '/path/to/file'
      """.stripMargin
    sql(sql1)
    sql(sql2)

    val catalog = sqlContext.sessionState.catalog
    val id1 = FunctionIdentifier("helloworld1", Some(catalog.getCurrentDatabase))
    val id2 = FunctionIdentifier("helloworld2", Some(catalog.getCurrentDatabase))

    val f1 = catalog.getFunction(id1)
    val f2 = catalog.getFunction(id2)
    assert(f1 == CatalogFunction(id1, "spark.example.SimpleUDFExample1"))
    assert(f2 == CatalogFunction(id2, "spark.example.SimpleUDFExample2"))

    assert(catalog.listFunctions(catalog.getCurrentDatabase, "helloworld*").size == 2)
    catalog.dropFunction(id1)
    catalog.dropFunction(id2)
    assert(catalog.listFunctions(catalog.getCurrentDatabase, "helloworld*").isEmpty)
  }
}
