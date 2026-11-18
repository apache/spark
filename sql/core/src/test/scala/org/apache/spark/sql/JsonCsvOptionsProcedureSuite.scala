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

package org.apache.spark.sql

import java.util.Collections

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.catalog.procedures.{BoundProcedure, ProcedureParameter, UnboundProcedure}
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class JsonCsvOptionsProcedureSuite extends QueryTest with SharedSparkSession {
  for (capture <- Seq(true, false); qualified <- Seq(true, false)) {
    test(s"nested function creation uses the current procedure path: $capture, $qualified") {
      withSQLConf(SQLConf.JSON_CSV_EXPRESSION_USE_CAPTURED_OPTION_DEFAULTS.key -> capture.toString,
          SQLConf.PATH_ENABLED.key -> "true",
          "spark.sql.catalog.procedurecat" -> classOf[InMemoryCatalog].getName) {
        val catalog = spark.sessionState.catalogManager.catalog("procedurecat")
          .asInstanceOf[InMemoryCatalog]
        val procId = Identifier.of(Array("ns"), "nested")
        val proc = new UnboundProcedure {
          override def name: String = "nested"
          override def description: String = "Create a SQL function after changing PATH"
          override def bind(inputType: StructType): BoundProcedure = new BoundProcedure {
            override def name: String = "nested"
            override def description: String = "Create a SQL function after changing PATH"
            override def isDeterministic: Boolean = false
            override def parameters: Array[ProcedureParameter] = Array.empty
            override def call(input: InternalRow): java.util.Iterator[Scan] = {
              sql("SET PATH = spark_catalog.procedure_b, system.builtin")
              sql("CREATE OR REPLACE FUNCTION procedure_a.target() " +
                "RETURNS INT RETURN helper()")
              Collections.emptyIterator[Scan]()
            }
          }
        }
        catalog.createProcedure(procId, proc)
        try {
          withDatabase("procedure_a", "procedure_b") {
            sql("CREATE DATABASE procedure_a")
            sql("CREATE DATABASE procedure_b")
            withUserDefinedFunction("procedure_a.target" -> false,
                "procedure_a.helper" -> false, "procedure_b.helper" -> false) {
              sql("CREATE FUNCTION procedure_a.target() RETURNS INT RETURN 1")
              sql("CREATE FUNCTION procedure_a.helper() RETURNS INT RETURN procedure_a.target()")
              sql("CREATE FUNCTION procedure_b.helper() RETURNS INT RETURN 2")
              try {
                sql("USE procedure_b")
                sql("SET PATH = spark_catalog.procedure_a, procedurecat.ns, system.builtin")
                sql(if (qualified) "CALL procedurecat.ns.nested()" else "CALL nested()")
                checkAnswer(sql("SELECT procedure_a.target()"), Row(2))
              } finally {
                sql("USE default")
                sql("SET PATH = DEFAULT_PATH")
              }
            }
          }
        } finally {
          spark.sessionState.catalogManager.reset()
        }
      }
    }
  }
}
