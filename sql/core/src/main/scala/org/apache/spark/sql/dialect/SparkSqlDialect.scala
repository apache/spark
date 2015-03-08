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

package org.apache.spark.sql.dialect

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.DDLParser
import org.apache.spark.sql.{SparkSQLParser, catalyst}

object SparkSqlDialect extends Dialect {

  protected[sql] val ddlParser = new DDLParser(sqlParser.apply(_))

  protected[sql] val sqlParser = {
   val fallback = new catalyst.SqlParser
   new SparkSQLParser(fallback(_))
  }

  override def parse(sql: String): LogicalPlan = {
   ddlParser(sql, false).getOrElse(sqlParser(sql))
  }

  val name = "sql"

  override def description = "spark sql native parser"
}

