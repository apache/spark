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

package org.apache.spark.sql.catalyst

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Root class of SQL Parser Dialect, and we don't guarantee the binary
 * compatibility for the future release, let's keep it as the internal
 * interface for advanced user.
 *
 */
@DeveloperApi
abstract class ParserDialect {
  // this is the main function that will be implemented by sql parser.
  def parse(sqlText: String): LogicalPlan
}

/**
 * Currently we support the default dialect named "sql", associated with the class
 * [[DefaultParserDialect]]
 *
 * And we can also provide custom SQL Dialect, for example in Spark SQL CLI:
 * {{{
 *-- switch to "hiveql" dialect
 *   spark-sql>SET spark.sql.dialect=hiveql;
 *   spark-sql>SELECT * FROM src LIMIT 1;
 *
 *-- switch to "sql" dialect
 *   spark-sql>SET spark.sql.dialect=sql;
 *   spark-sql>SELECT * FROM src LIMIT 1;
 *
 *-- register the new SQL dialect
 *   spark-sql> SET spark.sql.dialect=com.xxx.xxx.SQL99Dialect;
 *   spark-sql> SELECT * FROM src LIMIT 1;
 *
 *-- register the non-exist SQL dialect
 *   spark-sql> SET spark.sql.dialect=NotExistedClass;
 *   spark-sql> SELECT * FROM src LIMIT 1;
 *
 *-- Exception will be thrown and switch to dialect
 *-- "sql" (for SQLContext) or
 *-- "hiveql" (for HiveContext)
 * }}}
 */
private[spark] class DefaultParserDialect extends ParserDialect {
  @transient
  protected val sqlParser = SqlParser

  override def parse(sqlText: String): LogicalPlan = {
    sqlParser.parse(sqlText)
  }
}
