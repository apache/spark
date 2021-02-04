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

import java.sql.SQLException
import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}

private object H2Dialect extends JdbcDialect {
  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:h2")

  override def classifyException(message: String, e: Throwable): AnalysisException = {
    if (e.isInstanceOf[SQLException]) {
      // Error codes are from https://www.h2database.com/javadoc/org/h2/api/ErrorCode.html
      e.asInstanceOf[SQLException].getErrorCode match {
        // TABLE_OR_VIEW_ALREADY_EXISTS_1
        case 42101 =>
          throw new TableAlreadyExistsException(message, cause = Some(e))
        // TABLE_OR_VIEW_NOT_FOUND_1
        case 42102 =>
          throw new NoSuchTableException(message, cause = Some(e))
        // SCHEMA_NOT_FOUND_1
        case 90079 =>
          throw new NoSuchNamespaceException(message, cause = Some(e))
        case _ =>
      }
    }
    super.classifyException(message, e)
  }
}
