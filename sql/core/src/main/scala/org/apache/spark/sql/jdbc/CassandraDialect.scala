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

import java.sql.Types

import org.apache.spark.sql.types._


private case object CassandraDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean =
    url.startsWith("jdbc:datadirect:cassandra") || 
    url.startsWith("jdbc:weblogic:cassandra")

  override def getInsertStatement(table: String, rddSchema: StructType): String = {
    val sql = new StringBuilder(s"INSERT INTO $table ( ")
    var fieldsLeft = rddSchema.fields.length
    var i = 0
    // Build list of column names
    while (fieldsLeft > 0) {
      sql.append(rddSchema.fields(i).name)
      if (fieldsLeft > 1) sql.append(", ")
      fieldsLeft = fieldsLeft - 1
      i = i + 1
    }
    sql.append(" ) VALUES ( ")
    // Build values clause
    fieldsLeft = rddSchema.fields.length
    while (fieldsLeft > 0) {
      sql.append("?")
      if (fieldsLeft > 1) sql.append(", ")
      fieldsLeft = fieldsLeft - 1
    }
    sql.append(" ) ")
    return sql.toString()
  }
}
