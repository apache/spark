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

import java.util.Collections

import org.scalatest.BeforeAndAfter

import org.apache.spark.{SPARK_DOC_ROOT, SparkException, SparkNumberFormatException}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, DefaultValue, Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.catalog.procedures.{BoundProcedure, ProcedureParameter, UnboundProcedure}
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter.Mode
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter.Mode.{IN, INOUT, OUT}
import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.connector.read.{LocalScan, Scan}
import org.apache.spark.sql.errors.DataTypeErrors.{toSQLType, toSQLValue}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, DataTypes, IntegerType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class ProcedureSuite extends QueryTest with SharedSparkSession with BeforeAndAfter {

  before {
    spark.conf.set(s"spark.sql.catalog.cat", classOf[InMemoryCatalog].getName)
    spark.conf.set(s"spark.sql.catalog.cat2", classOf[InMemoryCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf(s"spark.sql.catalog.cat")
    spark.sessionState.conf.unsetConf(s"spark.sql.catalog.cat2")
  }

  private def catalog: InMemoryCatalog = catalog("cat")

  private def catalog(name: String): InMemoryCatalog = {
    val catalog = spark.sessionState.catalogManager.catalog(name)
    catalog.asInstanceOf[InMemoryCatalog]
  }

  test("position arguments") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    checkAnswer(sql("CALL cat.ns.sum(5, 5)"), Row(10) :: Nil)
  }

  test("named arguments") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    checkAnswer(sql("CALL cat.ns.sum(in2 => 3, in1 => 5)"), Row(8) :: Nil)
  }

  test("position and named arguments") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    checkAnswer(sql("CALL cat.ns.sum(3, in2 => 1)"), Row(4) :: Nil)
  }

  test("foldable expressions") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    checkAnswer(sql("CALL cat.ns.sum(1 + 1, in2 => 2)"), Row(4) :: Nil)
    checkAnswer(sql("CALL cat.ns.sum(in2 => 1, in1 => 2 + 1)"), Row(4) :: Nil)
    checkAnswer(sql("CALL cat.ns.sum((1 + 1) * 2, in2 => (2 + 1) / 3)"), Row(5) :: Nil)
  }

  test("type coercion") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundLongSum)
    checkAnswer(sql("CALL cat.ns.sum(1, 2)"), Row(3) :: Nil)
    checkAnswer(sql("CALL cat.ns.sum(1L, 2)"), Row(3) :: Nil)
    checkAnswer(sql("CALL cat.ns.sum(1, 2L)"), Row(3) :: Nil)
  }

  test("multiple output rows") {
    catalog.createProcedure(Identifier.of(Array("ns"), "complex"), UnboundComplexProcedure)
    checkAnswer(
      sql("CALL cat.ns.complex('X', 'Y', 3)"),
      Row(1, "X1", "Y1") :: Row(2, "X2", "Y2") :: Row(3, "X3", "Y3") :: Nil)
  }

  test("parameters with default values") {
    catalog.createProcedure(Identifier.of(Array("ns"), "complex"), UnboundComplexProcedure)
    checkAnswer(sql("CALL cat.ns.complex()"), Row(1, "A1", "B1") :: Nil)
    checkAnswer(sql("CALL cat.ns.complex('X', 'Y')"), Row(1, "X1", "Y1") :: Nil)
  }

  test("parameters with invalid default values") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundInvalidDefaultProcedure)
    checkError(
      exception = intercept[AnalysisException](
        sql("CALL cat.ns.sum()")
      ),
      condition = "INVALID_DEFAULT_VALUE.DATA_TYPE",
      parameters = Map(
        "statement" -> "CALL",
        "colName" -> toSQLId("in2"),
        "defaultValue" -> toSQLValue("B"),
        "expectedType" -> toSQLType("INT"),
        "actualType" -> toSQLType("STRING")))
  }

  test("IDENTIFIER") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    checkAnswer(
      spark.sql("CALL IDENTIFIER(:p1)(1, 2)", Map("p1" -> "cat.ns.sum")),
      Row(3) :: Nil)
  }

  test("parameterized statements") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    checkAnswer(
      spark.sql("CALL cat.ns.sum(?, ?)", Array(2, 3)),
      Row(5) :: Nil)
  }

  test("undefined procedure") {
    checkError(
      exception = intercept[AnalysisException](
        sql("CALL cat.non_exist(1, 2)")
      ),
      sqlState = Some("38000"),
      condition = "FAILED_TO_LOAD_ROUTINE",
      parameters = Map("routineName" -> "`cat`.`non_exist`")
    )
  }

  test("non-procedure catalog") {
    withSQLConf("spark.sql.catalog.testcat" -> classOf[BasicInMemoryTableCatalog].getName) {
      checkError(
        exception = intercept[AnalysisException](
          sql("CALL testcat.procedure(1, 2)")
        ),
        condition = "_LEGACY_ERROR_TEMP_1184",
        parameters = Map("plugin" -> "testcat", "ability" -> "procedures")
      )

      checkError(
        exception = intercept[AnalysisException](
          sql("SHOW PROCEDURES IN testcat")
        ),
        condition = "_LEGACY_ERROR_TEMP_1184",
        parameters = Map("plugin" -> "testcat", "ability" -> "procedures")
      )
    }
  }

  test("too many arguments") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    checkError(
      exception = intercept[AnalysisException](
        sql("CALL cat.ns.sum(1, 2, 3)")
      ),
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> toSQLId("sum"),
        "expectedNum" -> "2",
        "actualNum" -> "3",
        "docroot" -> SPARK_DOC_ROOT))
  }

  test("custom default catalog") {
    withSQLConf(SQLConf.DEFAULT_CATALOG.key -> "cat") {
      catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
      val df = sql("CALL ns.sum(1, 2)")
      checkAnswer(df, Row(3) :: Nil)
    }
  }

  test("custom default catalog and namespace") {
    withSQLConf(SQLConf.DEFAULT_CATALOG.key -> "cat") {
      catalog.createNamespace(Array("ns"), Collections.emptyMap)
      catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
      sql("USE ns")
      val df = sql("CALL sum(1, 2)")
      checkAnswer(df, Row(3) :: Nil)
    }
  }

  test("required parameter not found") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    checkError(
      exception = intercept[AnalysisException] {
        sql("CALL cat.ns.sum()")
      },
      condition = "REQUIRED_PARAMETER_NOT_FOUND",
      parameters = Map(
        "routineName" -> toSQLId("sum"),
        "parameterName" -> toSQLId("in1"),
        "index" -> "0"))
  }

  test("conflicting position and named parameter assignments") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    checkError(
      exception = intercept[AnalysisException] {
        sql("CALL cat.ns.sum(1, in1 => 2)")
      },
      condition = "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.BOTH_POSITIONAL_AND_NAMED",
      parameters = Map(
        "routineName" -> toSQLId("sum"),
        "parameterName" -> toSQLId("in1")))
  }

  test("duplicate named parameter assignments") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    checkError(
      exception = intercept[AnalysisException] {
        sql("CALL cat.ns.sum(in1 => 1, in1 => 2)")
      },
      condition = "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
      parameters = Map(
        "routineName" -> toSQLId("sum"),
        "parameterName" -> toSQLId("in1")))
  }

  test("unknown parameter name") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    checkError(
      exception = intercept[AnalysisException] {
        sql("CALL cat.ns.sum(in1 => 1, in5 => 2)")
      },
      condition = "UNRECOGNIZED_PARAMETER_NAME",
      parameters = Map(
        "routineName" -> toSQLId("sum"),
        "argumentName" -> toSQLId("in5"),
        "proposal" -> (toSQLId("in1") + " " + toSQLId("in2"))))
  }

  test("position parameter after named parameter") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    checkError(
      exception = intercept[AnalysisException] {
        sql("CALL cat.ns.sum(in1 => 1, 2)")
      },
      condition = "UNEXPECTED_POSITIONAL_ARGUMENT",
      parameters = Map(
        "routineName" -> toSQLId("sum"),
        "parameterName" -> toSQLId("in1")))
  }

  test("invalid argument type") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
    val call = "CALL cat.ns.sum(1, TIMESTAMP '2016-11-15 20:54:00.000')"
    checkError(
      exception = intercept[AnalysisException] {
        sql(call)
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "CALL",
        "paramIndex" -> "second",
        "inputSql" -> "\"TIMESTAMP '2016-11-15 20:54:00'\"",
        "inputType" -> toSQLType("TIMESTAMP"),
        "requiredType" -> toSQLType("INT")),
      context = ExpectedContext(fragment = call, start = 0, stop = call.length - 1))
  }

  test("malformed input to implicit cast") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> true.toString) {
      catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
      val call = "CALL cat.ns.sum('A', 2)"
      checkError(
        exception = intercept[SparkNumberFormatException](
          sql(call)
        ),
        condition = "CAST_INVALID_INPUT",
        parameters = Map(
          "expression" -> toSQLValue("A"),
          "sourceType" -> toSQLType("STRING"),
          "targetType" -> toSQLType("INT"),
          "ansiConfig" -> f"\"${SQLConf.ANSI_ENABLED.key}\""),
        context = ExpectedContext(fragment = call, start = 0, stop = call.length - 1))
    }
  }

  test("required parameters after optional") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundInvalidSum)
    val e = intercept[SparkException] {
      sql("CALL cat.ns.sum(in2 => 1)")
    }
    assert(e.getMessage.contains("required arguments should come before optional arguments"))
  }

  test("INOUT parameters are not supported") {
    catalog.createProcedure(Identifier.of(Array("ns"), "procedure"), UnboundInoutProcedure)
    val e = intercept[SparkException] {
      sql("CALL cat.ns.procedure(1)")
    }
    assert(e.getMessage.contains(" Unsupported parameter mode: INOUT"))
  }

  test("OUT parameters are not supported") {
    catalog.createProcedure(Identifier.of(Array("ns"), "procedure"), UnboundOutProcedure)
    val e = intercept[SparkException] {
      sql("CALL cat.ns.procedure(1)")
    }
    assert(e.getMessage.contains("Unsupported parameter mode: OUT"))
  }

  test("EXPLAIN") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundNonExecutableSum)
    val explain1 = sql("EXPLAIN CALL cat.ns.sum(5, 5)").head().get(0)
    assert(explain1.toString.contains("cat.ns.sum(5, 5)"))
    val explain2 = sql("EXPLAIN EXTENDED CALL cat.ns.sum(10, 10)").head().get(0)
    assert(explain2.toString.contains("cat.ns.sum(10, 10)"))
  }

  test("void procedure") {
    catalog.createProcedure(Identifier.of(Array("ns"), "proc"), UnboundVoidProcedure)
    checkAnswer(sql("CALL cat.ns.proc('A', 'B')"), Nil)
  }

  test("multi-result procedure") {
    catalog.createProcedure(Identifier.of(Array("ns"), "proc"), UnboundMultiResultProcedure)
    val e = intercept[SparkException] {
      sql("CALL cat.ns.proc()")
    }
    assert(e.getMessage.contains("Multi-result procedures are temporarily not supported"))
  }

  test("invalid input to struct procedure") {
    catalog.createProcedure(Identifier.of(Array("ns"), "proc"), UnboundStructProcedure)
    val actualType =
      StructType(Seq(
        StructField("X", DataTypes.DateType, nullable = false),
        StructField("Y", DataTypes.IntegerType, nullable = false)))
    val expectedType = StructProcedure.parameters.head.dataType
    val call = "CALL cat.ns.proc(named_struct('X', DATE '2011-11-11', 'Y', 2), 'VALUE')"
    checkError(
      exception = intercept[AnalysisException](sql(call)),
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "CALL",
        "paramIndex" -> "first",
        "inputSql" -> "\"named_struct(X, DATE '2011-11-11', Y, 2)\"",
        "inputType" -> toSQLType(actualType),
        "requiredType" -> toSQLType(expectedType)),
      context = ExpectedContext(fragment = call, start = 0, stop = call.length - 1))
  }

  test("save execution summary") {
    withTable("summary") {
      catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSum)
      val result = sql("CALL cat.ns.sum(1, 2)")
      result.write.saveAsTable("summary")
      checkAnswer(spark.table("summary"), Row(3) :: Nil)
    }
  }

  test("SPARK-51350: Implement SHOW procedures") {
    catalog.createProcedure(Identifier.of(Array("ns"), "foo"), UnboundSum)
    catalog.createProcedure(Identifier.of(Array("ns"), "abc"), UnboundLongSum)
    catalog.createProcedure(Identifier.of(Array(""), "xyz"), UnboundComplexProcedure)
    catalog.createProcedure(Identifier.of(Array(), "xxx"), UnboundStructProcedure)

    sql("USE cat")
    withNamespace("cat2.db_1") {
      sql("CREATE NAMESPACE cat2.db_1")

      catalog("cat2").createProcedure(Identifier.of(Array("ns_1", "db_1"), "foo"),
        UnboundVoidProcedure)
      catalog("cat2").createProcedure(Identifier.of(Array("ns_1", "db_1"), "bar"),
        UnboundMultiResultProcedure)
      catalog("cat2").createProcedure(Identifier.of(Array(""), "foo"),
        UnboundVoidProcedure)
      catalog("cat2").createProcedure(Identifier.of(Array(), "bar"),
        UnboundMultiResultProcedure)

      checkAnswer(
        // uses default catalog and ns
        sql("SHOW PROCEDURES"),
        Row("cat", Array(), null, "xxx") :: Nil)

      checkAnswer(
        sql("SHOW PROCEDURES IN ns"),
        Row("cat", Array("ns"), "ns", "abc") ::
          Row("cat", Array("ns"), "ns", "foo") :: Nil)

      checkAnswer(
        sql("SHOW PROCEDURES IN cat.ns"),
        Row("cat", Array("ns"), "ns", "abc") ::
          Row("cat", Array("ns"), "ns", "foo") :: Nil)

      checkAnswer(
        sql("SHOW PROCEDURES FROM cat.ns"),
        Row("cat", Array("ns"), "ns", "abc") ::
          Row("cat", Array("ns"), "ns", "foo") :: Nil)

      checkAnswer(
        sql("SHOW PROCEDURES FROM ``"),
        Row("cat", Array(""), "", "xyz") :: Nil)

      checkAnswer(
        sql("SHOW PROCEDURES FROM cat.``"),
        Row("cat", Array(""), "", "xyz") :: Nil)

      checkAnswer(
        sql("SHOW PROCEDURES FROM cat2.ns_1.db_1"),
        Row("cat2", Array("ns_1", "db_1"), "db_1", "foo") ::
        Row("cat2", Array("ns_1", "db_1"), "db_1", "bar") :: Nil)

      // Switch catalog.
      sql("USE cat2")

      checkAnswer(
        sql("SHOW PROCEDURES IN cat.ns"),
        Row("cat", Array("ns"), "ns", "abc") ::
          Row("cat", Array("ns"), "ns", "foo") :: Nil)

      checkAnswer(
        // uses default catalog and ns
        sql("SHOW PROCEDURES"),
        Row("cat2", Array(), null, "bar") :: Nil)

      checkAnswer(
        sql("SHOW PROCEDURES FROM ns_1.db_1"),
        Row("cat2", Array("ns_1", "db_1"), "db_1", "foo") ::
          Row("cat2", Array("ns_1", "db_1"), "db_1", "bar") :: Nil)

      checkAnswer(
        sql("SHOW PROCEDURES FROM cat2.ns_1.db_1"),
        Row("cat2", Array("ns_1", "db_1"), "db_1", "foo") ::
          Row("cat2", Array("ns_1", "db_1"), "db_1", "bar") :: Nil)

      checkAnswer(
        sql("SHOW PROCEDURES FROM ``"),
        Row("cat2", Array(""), "", "foo") :: Nil)

      checkAnswer(
        sql("SHOW PROCEDURES FROM cat2.``"),
        Row("cat2", Array(""), "", "foo") :: Nil)

      // Switch catalog back to 'cat' before clean up.
      sql("USE cat")
    }
  }

  test("default values with expressions") {
    catalog.createProcedure(Identifier.of(Array("ns"), "sum"), UnboundSumWithDefaultExpr)
    checkAnswer(sql("CALL cat.ns.sum(5)"), Row(9) :: Nil)
  }

  test("SPARK-51780: Implement DESC PROCEDURE") {
    catalog.createProcedure(Identifier.of(Array("ns"), "foo"), UnboundSum)
    catalog.createProcedure(Identifier.of(Array("ns", "db"), "abc"), UnboundLongSum)
    catalog.createProcedure(Identifier.of(Array(""), "xyz"), UnboundComplexProcedure)
    catalog.createProcedure(Identifier.of(Array(), "xxx"), UnboundStructProcedure)

    sql("USE cat")
    withNamespace("cat2.db_1") {
      sql("CREATE NAMESPACE cat2.db_1")

      catalog("cat2").createProcedure(Identifier.of(Array("ns_1", "db_1"), "foo"),
        UnboundVoidProcedure)

      checkError(
        // check non-existing procedure
        exception = intercept[AnalysisException](
          sql("DESC PROCEDURE cat.ns.non_exist")
        ),
        sqlState = Some("38000"),
        condition = "FAILED_TO_LOAD_ROUTINE",
        parameters = Map("routineName" -> "`cat`.`ns`.`non_exist`")
      )

      checkAnswer(
        sql("DESC PROCEDURE cat.ns.foo"),
        Row("Procedure:   sum") ::
          Row("Description: sum integers") :: Nil)

      checkAnswer(
        // use DESCRIBE instead of DESC
        sql("DESCRIBE PROCEDURE cat.ns.foo"),
        Row("Procedure:   sum") ::
          Row("Description: sum integers") :: Nil)

      checkAnswer(
        // use default catalog
        sql("DESC PROCEDURE ns.foo"),
        Row("Procedure:   sum") ::
          Row("Description: sum integers") :: Nil)

      checkAnswer(
        // use multi-part namespace
        sql("DESCRIBE PROCEDURE cat.ns.db.abc"),
        Row("Procedure:   long_sum") ::
          Row("Description: sum longs") :: Nil)

      checkAnswer(
        // use multi-part namespace with default catalog
        sql("DESCRIBE PROCEDURE ns.db.abc"),
        Row("Procedure:   long_sum") ::
          Row("Description: sum longs") :: Nil)

      checkAnswer(
        sql("DESC PROCEDURE cat.``.xyz"),
        Row("Procedure:   complex") ::
          Row("Description: complex procedure") :: Nil)

      checkAnswer(
        sql("DESC PROCEDURE cat.xxx"),
        Row("Procedure:   struct_input") ::
          Row("Description: struct procedure") :: Nil)

      checkAnswer(
        // check across catalogs
        sql("DESC PROCEDURE cat2.ns_1.db_1.foo"),
        Row("Procedure:   void") ::
          Row("Description: void procedure") :: Nil)
    }
  }

  object UnboundVoidProcedure extends UnboundProcedure {
    override def name: String = "void"
    override def description: String = "void procedure"
    override def bind(inputType: StructType): BoundProcedure = VoidProcedure
  }

  object VoidProcedure extends BoundProcedure {
    override def name: String = "void"

    override def description: String = "void procedure"

    override def isDeterministic: Boolean = true

    override def parameters: Array[ProcedureParameter] = Array(
      ProcedureParameter.in("in1", DataTypes.StringType).build(),
      ProcedureParameter.in("in2", DataTypes.StringType).build()
    )

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      Collections.emptyIterator
    }
  }

  object UnboundMultiResultProcedure extends UnboundProcedure {
    override def name: String = "multi"
    override def description: String = "multi-result procedure"
    override def bind(inputType: StructType): BoundProcedure = MultiResultProcedure
  }

  object MultiResultProcedure extends BoundProcedure {
    override def name: String = "multi"

    override def description: String = "multi-result procedure"

    override def isDeterministic: Boolean = true

    override def parameters: Array[ProcedureParameter] = Array()

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      val scans = java.util.Arrays.asList[Scan](
        Result(
          new StructType().add("out", DataTypes.IntegerType),
          Array(InternalRow(1))),
        Result(
          new StructType().add("out", DataTypes.StringType),
          Array(InternalRow(UTF8String.fromString("last"))))
      )
      scans.iterator()
    }
  }

  object UnboundNonExecutableSum extends UnboundProcedure {
    override def name: String = "sum"
    override def description: String = "sum integers"
    override def bind(inputType: StructType): BoundProcedure = Sum
  }

  object NonExecutableSum extends BoundProcedure {
    override def name: String = "sum"

    override def description: String = "sum integers"

    override def isDeterministic: Boolean = true

    override def parameters: Array[ProcedureParameter] = Array(
      ProcedureParameter.in("in1", DataTypes.IntegerType).build(),
      ProcedureParameter.in("in2", DataTypes.IntegerType).build()
    )

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      throw new UnsupportedOperationException()
    }
  }

  object UnboundSum extends UnboundProcedure {
    override def name: String = "sum"
    override def description: String = "sum integers"
    override def bind(inputType: StructType): BoundProcedure = Sum
  }

  object Sum extends BoundProcedure {
    override def name: String = "sum"

    override def description: String = "sum integers"

    override def isDeterministic: Boolean = true

    override def parameters: Array[ProcedureParameter] = Array(
      ProcedureParameter.in("in1", DataTypes.IntegerType).build(),
      ProcedureParameter.in("in2", DataTypes.IntegerType).build()
    )

    def outputType: StructType = new StructType().add("out", DataTypes.IntegerType)

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      val in1 = input.getInt(0)
      val in2 = input.getInt(1)
      val result = Result(outputType, Array(InternalRow(in1 + in2)))
      Collections.singleton[Scan](result).iterator()
    }
  }

  object UnboundLongSum extends UnboundProcedure {
    override def name: String = "long_sum"
    override def description: String = "sum longs"
    override def bind(inputType: StructType): BoundProcedure = LongSum
  }

  object LongSum extends BoundProcedure {
    override def name: String = "long_sum"

    override def description: String = "sum longs"

    override def isDeterministic: Boolean = true

    override def parameters: Array[ProcedureParameter] = Array(
      ProcedureParameter.in("in1", DataTypes.LongType).build(),
      ProcedureParameter.in("in2", DataTypes.LongType).build()
    )

    def outputType: StructType = new StructType().add("out", DataTypes.LongType)

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      val in1 = input.getLong(0)
      val in2 = input.getLong(1)
      val result = Result(outputType, Array(InternalRow(in1 + in2)))
      Collections.singleton[Scan](result).iterator()
    }
  }

  object UnboundInvalidSum extends UnboundProcedure {
    override def name: String = "invalid"
    override def description: String = "sum integers"
    override def bind(inputType: StructType): BoundProcedure = InvalidSum
  }

  object InvalidSum extends BoundProcedure {
    override def name: String = "invalid"

    override def description: String = "sum integers"

    override def isDeterministic: Boolean = false

    override def parameters: Array[ProcedureParameter] = Array(
      ProcedureParameter.in("in1", DataTypes.IntegerType).defaultValue("1").build(),
      ProcedureParameter.in("in2", DataTypes.IntegerType).build()
    )

    def outputType: StructType = new StructType().add("out", DataTypes.IntegerType)

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      throw new UnsupportedOperationException()
    }
  }

  object UnboundInvalidDefaultProcedure extends UnboundProcedure {
    override def name: String = "sum"
    override def description: String = "invalid default value procedure"
    override def bind(inputType: StructType): BoundProcedure = InvalidDefaultProcedure
  }

  object InvalidDefaultProcedure extends BoundProcedure {
    override def name: String = "sum"

    override def description: String = "invalid default value procedure"

    override def isDeterministic: Boolean = true

    override def parameters: Array[ProcedureParameter] = Array(
      ProcedureParameter.in("in1", DataTypes.IntegerType).defaultValue("10").build(),
      ProcedureParameter.in("in2", DataTypes.IntegerType).defaultValue("'B'").build()
    )

    def outputType: StructType = new StructType().add("out", DataTypes.IntegerType)

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      throw new UnsupportedOperationException()
    }
  }

  object UnboundComplexProcedure extends UnboundProcedure {
    override def name: String = "complex"
    override def description: String = "complex procedure"
    override def bind(inputType: StructType): BoundProcedure = ComplexProcedure
  }

  object ComplexProcedure extends BoundProcedure {
    override def name: String = "complex"

    override def description: String = "complex procedure"

    override def isDeterministic: Boolean = true

    override def parameters: Array[ProcedureParameter] = Array(
      ProcedureParameter.in("in1", DataTypes.StringType).defaultValue("'A'").build(),
      ProcedureParameter.in("in2", DataTypes.StringType).defaultValue("'B'").build(),
      ProcedureParameter.in("in3", DataTypes.IntegerType).defaultValue("1 + 1 - 1").build()
    )

    def outputType: StructType = new StructType()
      .add("out1", DataTypes.IntegerType)
      .add("out2", DataTypes.StringType)
      .add("out3", DataTypes.StringType)


    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      val in1 = input.getString(0)
      val in2 = input.getString(1)
      val in3 = input.getInt(2)

      val rows = (1 to in3).map { index =>
        val v1 = UTF8String.fromString(s"$in1$index")
        val v2 = UTF8String.fromString(s"$in2$index")
        InternalRow(index, v1, v2)
      }.toArray

      val result = Result(outputType, rows)
      Collections.singleton[Scan](result).iterator()
    }
  }

  object UnboundStructProcedure extends UnboundProcedure {
    override def name: String = "struct_input"
    override def description: String = "struct procedure"
    override def bind(inputType: StructType): BoundProcedure = StructProcedure
  }

  object StructProcedure extends BoundProcedure {
    override def name: String = "struct_input"

    override def description: String = "struct procedure"

    override def isDeterministic: Boolean = true

    override def parameters: Array[ProcedureParameter] = Array(
      ProcedureParameter
        .in(
          "in1",
          StructType(Seq(
            StructField("nested1", DataTypes.IntegerType),
            StructField("nested2", DataTypes.StringType))))
        .build(),
      ProcedureParameter.in("in2", DataTypes.StringType).build()
    )

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      Collections.emptyIterator
    }
  }

  object UnboundInoutProcedure extends UnboundProcedure {
    override def name: String = "procedure"
    override def description: String = "inout procedure"
    override def bind(inputType: StructType): BoundProcedure = InoutProcedure
  }

  object InoutProcedure extends BoundProcedure {
    override def name: String = "procedure"

    override def description: String = "inout procedure"

    override def isDeterministic: Boolean = true

    override def parameters: Array[ProcedureParameter] = Array(
      CustomParameterImpl(INOUT, "in1", DataTypes.IntegerType)
    )

    def outputType: StructType = new StructType().add("out", DataTypes.IntegerType)

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      throw new UnsupportedOperationException()
    }
  }

  object UnboundOutProcedure extends UnboundProcedure {
    override def name: String = "procedure"
    override def description: String = "out procedure"
    override def bind(inputType: StructType): BoundProcedure = OutProcedure
  }

  object OutProcedure extends BoundProcedure {
    override def name: String = "procedure"

    override def description: String = "out procedure"

    override def isDeterministic: Boolean = true

    override def parameters: Array[ProcedureParameter] = Array(
      CustomParameterImpl(IN, "in1", DataTypes.IntegerType),
      CustomParameterImpl(OUT, "out1", DataTypes.IntegerType)
    )

    def outputType: StructType = new StructType().add("out", DataTypes.IntegerType)

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      throw new UnsupportedOperationException()
    }
  }

  object UnboundSumWithDefaultExpr extends UnboundProcedure {
    override def name: String = "sum"
    override def description: String = "sum longs"
    override def bind(inputType: StructType): BoundProcedure = SumWithDefaultExpr
  }

  object SumWithDefaultExpr extends BoundProcedure {
    override def name: String = "sum"

    override def description: String = "sum longs"

    override def isDeterministic: Boolean = true

    override def parameters: Array[ProcedureParameter] = Array(
      ProcedureParameter.in("in1", DataTypes.LongType).build(),
      ProcedureParameter.in("in2", DataTypes.LongType)
        .defaultValue(
          new GeneralScalarExpression(
            "+",
            Array[Expression](LiteralValue(1, IntegerType), LiteralValue(3, IntegerType))))
        .build()
    )

    def outputType: StructType = new StructType().add("out", DataTypes.LongType)

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      val in1 = input.getLong(0)
      val in2 = input.getLong(1)
      val result = Result(outputType, Array(InternalRow(in1 + in2)))
      Collections.singleton[Scan](result).iterator()
    }
  }

  case class Result(readSchema: StructType, rows: Array[InternalRow]) extends LocalScan

  case class CustomParameterImpl(
      mode: Mode,
      name: String,
      dataType: DataType) extends ProcedureParameter {
    override def defaultValue: DefaultValue = null
    override def comment: String = null
  }
}
