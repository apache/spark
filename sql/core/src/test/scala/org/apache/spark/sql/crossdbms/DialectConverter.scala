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

package org.apache.spark.sql.crossdbms

trait DialectConverter {

  final val NOOP = ""
  final val SET_COMMAND_PREFIX = "set"
  final val COMMAND_PREFIXES_TO_IGNORE = Seq(SET_COMMAND_PREFIX)

  final val SPARK_SET_COMMAND_PATTERN = """set (\w+) = (true|false);""".r

  protected def checkIgnore(query: String): Boolean =
    COMMAND_PREFIXES_TO_IGNORE.count(prefix => query.startsWith(prefix)) > 0

  /**
   * Transform the query string, which is compatible with Spark, such that it is compatible with
   * dialect. This transforms a query string itself. Note that this is a best effort conversion and
   * may not always produce a compatible query string.
   */
  def preprocessQuery(query: String): String

  /**
   * Transform the query SET, which is compatible with Spark, such that it is compatible with
   * dialect.
   * For example:
   * {{{
   *   1. create temporary view t1 as values (..);
   *   2. set spark.sql... = true;
   *   3. select * from ...
   *   4. set spark.sql... = false;
   * }}}
   * We can remove everything between lines 2 and 4 because if setting configs is not applicable to
   * other DBMS systems, and the tests between such config setting is not relevant.
   */
  def preprocessQuerySet(queries: Seq[String]): Seq[String]
}

/**
 * Wrapper around utility functions that help convert from Spark SQL to PostgreSQL.
 */
object PostgresDialectConverter extends DialectConverter {

  def preprocessQuery(query: String): String = {
    query
  }

  override def preprocessQuerySet(queries: Seq[String]): Seq[String] = {
    queries
  }
}
