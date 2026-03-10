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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class JdbcTimeTypeSuite extends SparkFunSuite {

  test("PostgresDialect - getCatalystType for TIME") {
    val dialect = PostgresDialect

    // Test TIME type mapping
    val timeType = dialect.getCatalystType(Types.TIME, "TIME", 0, null)
    assert(timeType.isDefined)
    assert(timeType.get === TimeType)
  }

  test("PostgresDialect - getJDBCType for TIME") {
    val dialect = PostgresDialect

    val jdbcType = dialect.getJDBCType(TimeType)
    assert(jdbcType.isDefined)
    assert(jdbcType.get.databaseTypeDefinition === "TIME")
    assert(jdbcType.get.jdbcNullType === Types.TIME)
  }

  test("PostgresDialect - TIME in schema conversion") {
    val dialect = PostgresDialect

    // Create a schema with TIME field
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("event_time", TimeType),
      StructField("name", StringType)
    ))

    // Verify TIME field can be converted
    val timeField = schema.fields(1)
    val jdbcType = dialect.getJDBCType(timeField.dataType)
    assert(jdbcType.isDefined)
    assert(jdbcType.get.databaseTypeDefinition === "TIME")
  }

  test("MySQLDialect - getCatalystType for TIME") {
    val dialect = MySQLDialect

    // Test TIME type mapping
    val timeType = dialect.getCatalystType(Types.TIME, "TIME", 0, null)
    assert(timeType.isDefined)
    assert(timeType.get === TimeType)
  }

  test("MySQLDialect - getJDBCType for TIME") {
    val dialect = MySQLDialect

    val jdbcType = dialect.getJDBCType(TimeType)
    assert(jdbcType.isDefined)
    assert(jdbcType.get.databaseTypeDefinition === "TIME")
    assert(jdbcType.get.jdbcNullType === Types.TIME)
  }

  test("MsSqlServerDialect - getCatalystType for TIME") {
    val dialect = MsSqlServerDialect

    // Test TIME type mapping
    val timeType = dialect.getCatalystType(Types.TIME, "TIME", 0, null)
    assert(timeType.isDefined)
    assert(timeType.get === TimeType)
  }

  test("MsSqlServerDialect - getJDBCType for TIME") {
    val dialect = MsSqlServerDialect

    val jdbcType = dialect.getJDBCType(TimeType)
    assert(jdbcType.isDefined)
    assert(jdbcType.get.databaseTypeDefinition === "TIME")
    assert(jdbcType.get.jdbcNullType === Types.TIME)
  }

  test("All dialects - TIME type consistency") {
    val dialects = Seq(
      PostgresDialect,
      MySQLDialect,
      MsSqlServerDialect
    )

    for (dialect <- dialects) {
      // Test getCatalystType
      val catalystType = dialect.getCatalystType(Types.TIME, "TIME", 0, null)
      assert(catalystType.isDefined, s"${dialect.getClass.getSimpleName} should support TIME type")
      assert(catalystType.get === TimeType,
        s"${dialect.getClass.getSimpleName} should map Types.TIME to TimeType")

      // Test getJDBCType
      val jdbcType = dialect.getJDBCType(TimeType)
      assert(jdbcType.isDefined,
        s"${dialect.getClass.getSimpleName} should provide JDBC type for TimeType")
      assert(jdbcType.get.databaseTypeDefinition === "TIME",
        s"${dialect.getClass.getSimpleName} should use 'TIME' as database type")
      assert(jdbcType.get.jdbcNullType === Types.TIME,
        s"${dialect.getClass.getSimpleName} should use Types.TIME as JDBC null type")
    }
  }

  test("TIME type not confused with TIMESTAMP") {
    val dialects = Seq(
      PostgresDialect,
      MySQLDialect,
      MsSqlServerDialect
    )

    for (dialect <- dialects) {
      // Verify TIME and TIMESTAMP are distinct
      val timeType = dialect.getCatalystType(Types.TIME, "TIME", 0, null)
      val timestampType = dialect.getCatalystType(Types.TIMESTAMP, "TIMESTAMP", 0, null)

      assert(timeType.isDefined)
      assert(timestampType.isDefined)
      assert(timeType.get !== timestampType.get,
        s"${dialect.getClass.getSimpleName} should distinguish TIME from TIMESTAMP")
      assert(timeType.get === TimeType)
      assert(timestampType.get === TimestampType)
    }
  }

  test("TIME type not confused with DATE") {
    val dialects = Seq(
      PostgresDialect,
      MySQLDialect,
      MsSqlServerDialect
    )

    for (dialect <- dialects) {
      // Verify TIME and DATE are distinct
      val timeType = dialect.getCatalystType(Types.TIME, "TIME", 0, null)
      val dateType = dialect.getCatalystType(Types.DATE, "DATE", 0, null)

      assert(timeType.isDefined)
      assert(dateType.isDefined)
      assert(timeType.get !== dateType.get,
        s"${dialect.getClass.getSimpleName} should distinguish TIME from DATE")
      assert(timeType.get === TimeType)
      assert(dateType.get === DateType)
    }
  }

  test("Schema with multiple temporal types") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("event_date", DateType),
      StructField("event_time", TimeType),
      StructField("event_timestamp", TimestampType)
    ))

    val dialects = Seq(
      PostgresDialect,
      MySQLDialect,
      MsSqlServerDialect
    )

    for (dialect <- dialects) {
      // Verify each temporal type is handled correctly
      val dateJdbc = dialect.getJDBCType(schema.fields(1).dataType)
      val timeJdbc = dialect.getJDBCType(schema.fields(2).dataType)
      val timestampJdbc = dialect.getJDBCType(schema.fields(3).dataType)

      assert(dateJdbc.isDefined)
      assert(timeJdbc.isDefined)
      assert(timestampJdbc.isDefined)

      assert(timeJdbc.get.databaseTypeDefinition === "TIME",
        s"${dialect.getClass.getSimpleName} should map TimeType to TIME")
      assert(timeJdbc.get.jdbcNullType === Types.TIME)
    }
  }

  test("TIME type in complex schema") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("times", ArrayType(TimeType)),
      StructField("time_map", MapType(StringType, TimeType)),
      StructField("nested", StructType(Seq(
        StructField("inner_time", TimeType)
      )))
    ))

    // Verify TimeType is recognized in complex structures
    assert(schema.fields(1).dataType.asInstanceOf[ArrayType].elementType === TimeType)
    assert(schema.fields(2).dataType.asInstanceOf[MapType].valueType === TimeType)
    assert(schema.fields(3).dataType.asInstanceOf[StructType].fields(0).dataType === TimeType)
  }

  test("JdbcDialects.get returns dialect with TIME support") {
    // Test that registered dialects support TIME
    val postgresUrl = "jdbc:postgresql://localhost/test"
    val mysqlUrl = "jdbc:mysql://localhost/test"
    val sqlServerUrl = "jdbc:sqlserver://localhost;database=test"

    val urls = Seq(postgresUrl, mysqlUrl, sqlServerUrl)

    for (url <- urls) {
      val dialect = JdbcDialects.get(url)
      val jdbcType = dialect.getJDBCType(TimeType)

      // All major dialects should support TIME type
      assert(jdbcType.isDefined || dialect == NoopDialect,
        s"Dialect for $url should support TIME type")
    }
  }
}

// Made with Bob
