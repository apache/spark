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

package org.apache.spark.sql.connector

import java.sql.Connection

import org.apache.spark.SparkFunSuite

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, EvalMode}
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.{Cast => ConnectorCast}
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types._

trait ConnectorCastSuiteBase extends SparkFunSuite {
  def tableCatalog: TableCatalog
  protected def dropTable(table: Identifier): Unit
  protected def checkCast(cast: ConnectorCast, tableIdentifier: Identifier): Unit
}

trait ConnectorNumericCastSuite extends ConnectorCastSuiteBase {

  var numericTypesTable: Identifier = _

  private val integerTypes = Seq(ByteType, ShortType, IntegerType, LongType)
  private val realTypes = Seq(FloatType, DoubleType, DecimalType(9, 2))

  private val numberSparkCasts: Map[String, Seq[DataType]] = Map(
    "ByteType" -> (integerTypes ++ realTypes :+ StringType :+ TimestampType :+ BooleanType),
    "ShortType" -> (integerTypes ++ realTypes :+ StringType :+ TimestampType :+ BooleanType),
    "IntegerType" -> (integerTypes ++ realTypes :+ StringType :+ TimestampType :+ BooleanType),
    "LongType" -> (integerTypes ++ realTypes :+ StringType :+ TimestampType :+ BooleanType),
    "FloatType" -> (integerTypes ++ realTypes :+ StringType :+ TimestampType :+ BooleanType),
    "DecimalType" -> (integerTypes ++ realTypes :+ StringType :+ TimestampType :+ BooleanType),
    "DoubleType" -> (integerTypes ++ realTypes :+ StringType :+ TimestampType :+ BooleanType)
  )

  def createNumericTypesTable: Identifier

  test("Cast number types") {
    val table = tableCatalog.loadTable(numericTypesTable)
    val schema = table.schema()
    schema.fields.foreach(field => {
      val targetTypes = numberSparkCasts.get(field.dataType.getClass.getSimpleName.stripSuffix("$"))
      assert(targetTypes.isDefined, s"Mapping not found for type ${field.dataType.getClass}")
      targetTypes.get.foreach(targetType => {
        val catalystCast = new Cast(
          AttributeReference(field.name, field.dataType, field.nullable)(),
          targetType,
          timeZoneId = None,
          evalMode = EvalMode.ANSI
        )
        val connectorCast = new V2ExpressionBuilder(catalystCast, isPredicate = false).build()
        if (connectorCast.isDefined) {
          checkCast(connectorCast.get.asInstanceOf[ConnectorCast], numericTypesTable)
        }
      })
    })
  }
}

trait ConnectorStringCastSuite extends ConnectorCastSuiteBase {

  var stringTypeTable: Identifier = _

  def createStringTypeTable: Identifier

  test("Cast string types") {
    val table = tableCatalog.loadTable(stringTypeTable)
    val fields = table.schema().fields
    val stringToIntField = fields.find(_.name.equalsIgnoreCase("col_int"))
    val stringToLongField = fields.find(_.name.equalsIgnoreCase("col_long"))
    val stringToDateField = fields.find(_.name.equalsIgnoreCase("col_date"))
    val stringToTimestampField = fields.find(_.name.equalsIgnoreCase("col_timestamp"))
    val stringToTimestampNtzField = fields.find(_.name.equalsIgnoreCase("col_timestamp_ntz"))

    val castsToTest = Seq(
      (stringToIntField, IntegerType),
      (stringToLongField, LongType),
      (stringToDateField, DateType),
      (stringToTimestampField, TimestampType),
      (stringToTimestampNtzField, TimestampNTZType)
    )
    castsToTest.foreach({
      case (field, targetType) =>
        // TODO: Add ability to skip some columns if inheritor does not have that type
        // e.g. some databases have no timestamp_ntz type
        assert(field.isDefined)
        val catalystCast = new Cast(
          AttributeReference(field.get.name, field.get.dataType, field.get.nullable)(),
          targetType,
          timeZoneId = None,
          evalMode = EvalMode.ANSI
        )
        val connectorCast = new V2ExpressionBuilder(catalystCast, isPredicate = false).build()
        if (connectorCast.isDefined) {
          checkCast(connectorCast.get.asInstanceOf[ConnectorCast], stringTypeTable)
        }
    })
  }
}

trait JDBCConnectorCastSuiteBase extends ConnectorNumericCastSuite
    with ConnectorStringCastSuite {
  private var connection: Connection = _

  def dialect: JdbcDialect

  protected def createConnection: Connection

  protected val schemaName: String = "CAST_SCHEMA"

  override def beforeAll(): Unit = {
    super.beforeAll()
    connection = createConnection
    createSchema(schemaName)
    numericTypesTable = createNumericTypesTable
    stringTypeTable = createStringTypeTable
  }

  override def afterAll(): Unit = {
    dropTable(numericTypesTable)
    dropTable(stringTypeTable)
    dropSchema(schemaName)
    connection.close()
    super.afterAll()
  }

  protected def createSchema(schemaName: String): Unit = {
    connection.prepareStatement(s"CREATE SCHEMA $schemaName").executeUpdate()
  }

  protected def dropSchema(schemaName: String): Unit = {
    connection.prepareStatement(s"DROP SCHEMA $schemaName CASCADE").executeUpdate()
  }

  override def createNumericTypesTable: Identifier = {
    val identifier = Identifier.of(Array(schemaName), "CAST_NUMERIC_TABLE")
    execUpdate(
      s"""CREATE TABLE IF NOT EXISTS $schemaName.CAST_NUMERIC_TABLE
         |(COL_INT INT, COL_FLOAT FLOAT)
         |""".stripMargin)
    execUpdate(s"""INSERT INTO $schemaName.CAST_NUMERIC_TABLE VALUES (1, 1.2)""")
    execUpdate(s"""INSERT INTO $schemaName.CAST_NUMERIC_TABLE VALUES (0, 1.0)""")
    execUpdate(s"""INSERT INTO $schemaName.CAST_NUMERIC_TABLE VALUES (-1, -1.0)""")
    identifier
  }

  override def createStringTypeTable: Identifier = {
    val identifier = Identifier.of(Array(schemaName), "CAST_STRING_TABLE")
    execUpdate(
      s"""CREATE TABLE IF NOT EXISTS $schemaName.CAST_STRING_TABLE
         |(COL_INT VARCHAR(50), COL_LONG VARCHAR(50),
         | COL_DATE VARCHAR(50), COL_TIMESTAMP VARCHAR(50), COL_TIMESTAMP_NTZ VARCHAR(50),
         | COL_BOOLEAN VARCHAR(50))
         |""".stripMargin)
    execUpdate(
      s"""INSERT INTO $schemaName.CAST_STRING_TABLE VALUES
         |('123', '3000000000',
         | '2024-01-01', '2024-01-01 00:00:00', '2024-01-01 00:00:00',
         | 'true')""".stripMargin)
    execUpdate(
      s"""INSERT INTO $schemaName.CAST_STRING_TABLE VALUES
         |('-5', '-3000000000',
         | '2024-01-01', '2024-01-01 00:00:00 +0100', '2024-01-01 21:00:00',
         | 'false')""".stripMargin)
    execUpdate(
      s"""INSERT INTO $schemaName.CAST_STRING_TABLE VALUES
         |('20000000', '0',
         | '1960-01-01', '2024-01-01 21:00:00 -0100', '2024-05-05 21:00:00',
         | '0')""".stripMargin)
    execUpdate(
      s"""INSERT INTO $schemaName.CAST_STRING_TABLE VALUES
         |('0', '0',
         | '1960-01-01', '2024-01-01 21:00:00 -0100', '2024-05-05 21:00:00',
         | '1')""".stripMargin)
    execUpdate(
      s"""INSERT INTO $schemaName.CAST_STRING_TABLE VALUES
         |('0', '0',
         | '1960-01-01', '2024-01-01 21:00:00 -0100', '2024-05-05 21:00:00',
         | 'y')""".stripMargin)
    execUpdate(
      s"""INSERT INTO $schemaName.CAST_STRING_TABLE VALUES
         |('0', '0',
         | '1960-01-01', '2024-01-01 21:00:00 -0100', '2024-05-05 21:00:00',
         | 'n')""".stripMargin)
    identifier
  }

  override protected def checkCast(cast: ConnectorCast, tableIdentifier: Identifier): Unit = {
    val compiledCast: Option[String] = dialect.compileExpression(cast)
    if (compiledCast.isDefined) {
      // TODO: Switch to smart approach to craft name
      val tableName = tableIdentifier.toString
      val rs = connection
          .prepareStatement(s"SELECT ${compiledCast.get} FROM $tableName")
          .executeQuery()
      var atLeastOneRow = false
      while (rs.next()) {
        atLeastOneRow = true
        val firstCol = rs.getObject(1)
        assert(firstCol != null)
      }
      assert(atLeastOneRow, "There should be at least one result to verify cast")
    }
    else {
      log.info(s"${cast.toString()} is not supported by dialect")
    }
  }

  protected def execUpdate(sqlQuery: String): Unit =
    this.connection.prepareStatement(sqlQuery).executeUpdate()
}
