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

package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class JdbcUtilsSuite extends SparkFunSuite {

  val tableSchema = StructType(Seq(
    StructField("C1", StringType, false), StructField("C2", IntegerType, false)))
  val caseSensitive = org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
  val caseInsensitive = org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution

  test("Parse user specified column types") {
    assert(JdbcUtils.getCustomSchema(tableSchema, null, caseInsensitive) === tableSchema)
    assert(JdbcUtils.getCustomSchema(tableSchema, "", caseInsensitive) === tableSchema)

    assert(JdbcUtils.getCustomSchema(tableSchema, "c1 DATE", caseInsensitive) ===
      StructType(Seq(StructField("C1", DateType, false), StructField("C2", IntegerType, false))))
    assert(JdbcUtils.getCustomSchema(tableSchema, "c1 DATE", caseSensitive) ===
      StructType(Seq(StructField("C1", StringType, false), StructField("C2", IntegerType, false))))

    assert(
      JdbcUtils.getCustomSchema(tableSchema, "c1 DATE, C2 STRING", caseInsensitive) ===
      StructType(Seq(StructField("C1", DateType, false), StructField("C2", StringType, false))))
    assert(JdbcUtils.getCustomSchema(tableSchema, "c1 DATE, C2 STRING", caseSensitive) ===
      StructType(Seq(StructField("C1", StringType, false), StructField("C2", StringType, false))))

    // Throw AnalysisException
    val duplicate = intercept[AnalysisException]{
      JdbcUtils.getCustomSchema(tableSchema, "c1 DATE, c1 STRING", caseInsensitive) ===
        StructType(Seq(StructField("c1", DateType, false), StructField("c1", StringType, false)))
    }
    checkError(
      exception = duplicate,
      condition = "COLUMN_ALREADY_EXISTS",
      parameters = Map("columnName" -> "`c1`"))

    // Throw ParseException
    checkError(
      exception = intercept[ParseException]{
        JdbcUtils.getCustomSchema(tableSchema, "c3 DATEE, C2 STRING", caseInsensitive)
      },
      condition = "UNSUPPORTED_DATATYPE",
      parameters = Map("typeName" -> "\"DATEE\""))

    checkError(
      exception = intercept[ParseException]{
        JdbcUtils.getCustomSchema(tableSchema, "c3 DATE. C2 STRING", caseInsensitive)
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'.'", "hint" -> ""))
  }

  test("redactUrl redacts credentials embedded in a JDBC URL") {
    val redaction = Utils.REDACTION_REPLACEMENT_TEXT

    // URLs with no userinfo and no query/property portion are returned unchanged.
    Seq(
      "jdbc:mysql://localhost/db",
      "jdbc:postgresql://localhost:5432/postgres",
      "jdbc:h2:mem:testdb").foreach { url =>
      assert(JDBCOptions.redactUrl(url, None) === url)
    }

    // The authority's userinfo is redacted wholesale (the scheme, host, port and database are
    // kept), including a bare username and a password that itself contains an '@'.
    assert(JDBCOptions.redactUrl("jdbc:mysql://user:secret@host:3306/db", None) ===
      s"jdbc:mysql://$redaction@host:3306/db")
    assert(JDBCOptions.redactUrl("jdbc:mysql://user@host:3306/db", None) ===
      s"jdbc:mysql://$redaction@host:3306/db")
    assert(JDBCOptions.redactUrl("jdbc:mysql://user:p@ss@host:3306/db", None) ===
      s"jdbc:mysql://$redaction@host:3306/db")

    // The entire query / connection-property portion (from the first '?' or ';') is redacted,
    // since the credential-bearing property names are driver-specific and open-ended.
    assert(JDBCOptions.redactUrl(
      "jdbc:postgresql://host/db?user=alice&password=secret&ssl=true", None) ===
      s"jdbc:postgresql://host/db?$redaction")
    assert(JDBCOptions.redactUrl(
      "jdbc:redshift://host:5439/db;PWD=secret;LogLevel=1", None) ===
      s"jdbc:redshift://host:5439/db;$redaction")
    assert(JDBCOptions.redactUrl(
      "jdbc:sqlserver://localhost:1433;databaseName=testdb", None) ===
      s"jdbc:sqlserver://localhost:1433;$redaction")
    assert(JDBCOptions.redactUrl("jdbc:snowflake://host/?token=abc123", None) ===
      s"jdbc:snowflake://host/?$redaction")

    // Both userinfo and the property portion in the same URL are redacted.
    assert(JDBCOptions.redactUrl(
      "jdbc:mysql://user:secret@host:3306/db?password=other", None) ===
      s"jdbc:mysql://$redaction@host:3306/db?$redaction")

    // Null and empty inputs are passed through.
    assert(JDBCOptions.redactUrl(null, None) === null)
    assert(JDBCOptions.redactUrl("", None) === "")
  }
}
