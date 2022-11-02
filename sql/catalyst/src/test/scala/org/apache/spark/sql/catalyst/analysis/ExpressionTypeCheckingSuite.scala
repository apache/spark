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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class ExpressionTypeCheckingSuite extends SparkFunSuite with SQLHelper with QueryErrorsBase {

  val testRelation = LocalRelation(
    $"intField".int,
    $"stringField".string,
    $"booleanField".boolean,
    $"decimalField".decimal(8, 0),
    $"arrayField".array(StringType),
    Symbol("mapField").map(StringType, LongType))

  def assertError(expr: Expression, errorMessage: String): Unit = {
    val e = intercept[AnalysisException] {
      assertSuccess(expr)
    }
    assert(e.getMessage.contains(
      s"cannot resolve '${expr.sql}' due to data type mismatch:"))
    assert(e.getMessage.contains(errorMessage))
  }

  def assertSuccess(expr: Expression): Unit = {
    val analyzed = testRelation.select(expr.as("c")).analyze
    SimpleAnalyzer.checkAnalysis(analyzed)
  }

  def assertErrorForBinaryDifferingTypes(
      expr: Expression, messageParameters: Map[String, String]): Unit = {
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(expr)
      },
      errorClass = "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES",
      parameters = messageParameters)
  }

  def assertErrorForOrderingTypes(
      expr: Expression, messageParameters: Map[String, String]): Unit = {
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(expr)
      },
      errorClass = "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE",
      parameters = messageParameters)
  }

  def assertErrorForDataDifferingTypes(
      expr: Expression, messageParameters: Map[String, String]): Unit = {
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(expr)
      },
      errorClass = "DATATYPE_MISMATCH.DATA_DIFF_TYPES",
      parameters = messageParameters)
  }

  def assertErrorForWrongNumParameters(
      expr: Expression, messageParameters: Map[String, String]): Unit = {
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(expr)
      },
      errorClass = "DATATYPE_MISMATCH.WRONG_NUM_ARGS",
      parameters = messageParameters)
  }

  def assertForWrongType(expr: Expression, messageParameters: Map[String, String]): Unit = {
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(expr)
      },
      errorClass = "DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE",
      parameters = messageParameters)
  }

  test("check types for unary arithmetic") {
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(BitwiseNot($"stringField"))
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"~stringField\"",
        "paramIndex" -> "1",
        "inputSql" -> "\"stringField\"",
        "inputType" -> "\"STRING\"",
        "requiredType" -> "\"INTEGRAL\""))
  }

  test("check types for binary arithmetic") {
    // We will cast String to Double for binary arithmetic
    assertSuccess(Add($"intField", $"stringField"))
    assertSuccess(Subtract($"intField", $"stringField"))
    assertSuccess(Multiply($"intField", $"stringField"))
    assertSuccess(Divide($"intField", $"stringField"))
    assertSuccess(Remainder($"intField", $"stringField"))
    // checkAnalysis(BitwiseAnd($"intField", $"stringField"))

    assertErrorForBinaryDifferingTypes(
      expr = Add($"intField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField + booleanField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"BOOLEAN\""))
    assertErrorForBinaryDifferingTypes(
      expr = Subtract($"intField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField - booleanField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"BOOLEAN\""))
    assertErrorForBinaryDifferingTypes(
      expr = Multiply($"intField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField * booleanField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"BOOLEAN\""))
    assertErrorForBinaryDifferingTypes(
      expr = Divide($"intField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField / booleanField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"BOOLEAN\""))
    assertErrorForBinaryDifferingTypes(
      expr = Remainder($"intField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField % booleanField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"BOOLEAN\""))
    assertErrorForBinaryDifferingTypes(
      expr = BitwiseAnd($"intField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField & booleanField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"BOOLEAN\""))
    assertErrorForBinaryDifferingTypes(
      expr = BitwiseOr($"intField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField | booleanField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"BOOLEAN\""))
    assertErrorForBinaryDifferingTypes(
      expr = BitwiseXor($"intField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField ^ booleanField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"BOOLEAN\""))

    // scalastyle:off line.size.limit
    assertForWrongType(
      expr = Add($"booleanField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(booleanField + booleanField)\"",
        "inputType" -> "(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" or \"INTERVAL YEAR TO MONTH\" or \"INTERVAL\")",
        "actualDataType" -> "\"BOOLEAN\""))
    assertForWrongType(
      expr = Subtract($"booleanField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(booleanField - booleanField)\"",
        "inputType" -> "(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" or \"INTERVAL YEAR TO MONTH\" or \"INTERVAL\")",
        "actualDataType" -> "\"BOOLEAN\""))
    assertForWrongType(
      expr = Multiply($"booleanField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(booleanField * booleanField)\"",
        "inputType" -> "\"NUMERIC\"",
        "actualDataType" -> "\"BOOLEAN\""))
    assertForWrongType(
      expr = Divide($"booleanField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(booleanField / booleanField)\"",
        "inputType" -> "(\"DOUBLE\" or \"DECIMAL\")",
        "actualDataType" -> "\"BOOLEAN\""))
    assertForWrongType(
      expr = Remainder($"booleanField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(booleanField % booleanField)\"",
        "inputType" -> "\"NUMERIC\"",
        "actualDataType" -> "\"BOOLEAN\""))

    assertForWrongType(
      expr = BitwiseAnd($"booleanField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(booleanField & booleanField)\"",
        "inputType" -> "\"INTEGRAL\"",
        "actualDataType" -> "\"BOOLEAN\""))
    assertForWrongType(
      expr = BitwiseOr($"booleanField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(booleanField | booleanField)\"",
        "inputType" -> "\"INTEGRAL\"",
        "actualDataType" -> "\"BOOLEAN\""))
    assertForWrongType(
      expr = BitwiseXor($"booleanField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(booleanField ^ booleanField)\"",
        "inputType" -> "\"INTEGRAL\"",
        "actualDataType" -> "\"BOOLEAN\""))
    // scalastyle:on line.size.limit
  }

  test("check types for predicates") {
    // We will cast String to Double for binary comparison
    assertSuccess(EqualTo($"intField", $"stringField"))
    assertSuccess(EqualNullSafe($"intField", $"stringField"))
    assertSuccess(LessThan($"intField", $"stringField"))
    assertSuccess(LessThanOrEqual($"intField", $"stringField"))
    assertSuccess(GreaterThan($"intField", $"stringField"))
    assertSuccess(GreaterThanOrEqual($"intField", $"stringField"))

    // We will transform EqualTo with numeric and boolean types to CaseKeyWhen
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      assertSuccess(EqualTo($"intField", $"booleanField"))
      assertSuccess(EqualNullSafe($"intField", $"booleanField"))
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      assertErrorForBinaryDifferingTypes(
        expr = EqualTo($"intField", $"booleanField"),
        messageParameters = Map(
          "sqlExpr" -> "\"(intField = booleanField)\"",
          "left" -> "\"INT\"",
          "right" -> "\"BOOLEAN\""))
      assertErrorForBinaryDifferingTypes(
        expr = EqualNullSafe($"intField", $"booleanField"),
        messageParameters = Map(
          "sqlExpr" -> "\"(intField <=> booleanField)\"",
          "left" -> "\"INT\"",
          "right" -> "\"BOOLEAN\""))
    }

    assertErrorForBinaryDifferingTypes(
      expr = EqualTo($"intField", $"mapField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField = mapField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"MAP<STRING, BIGINT>\""))
    assertErrorForBinaryDifferingTypes(
      expr = EqualNullSafe($"intField", $"mapField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField <=> mapField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"MAP<STRING, BIGINT>\""))
    assertErrorForBinaryDifferingTypes(
      expr = LessThan($"intField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField < booleanField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"BOOLEAN\""))
    assertErrorForBinaryDifferingTypes(
      expr = LessThanOrEqual($"intField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField <= booleanField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"BOOLEAN\""))
    assertErrorForBinaryDifferingTypes(
      expr = GreaterThan($"intField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField > booleanField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"BOOLEAN\""))
    assertErrorForBinaryDifferingTypes(
      expr = GreaterThanOrEqual($"intField", $"booleanField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(intField >= booleanField)\"",
        "left" -> "\"INT\"",
        "right" -> "\"BOOLEAN\""))

    assertErrorForOrderingTypes(
      expr = EqualTo($"mapField", $"mapField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(mapField = mapField)\"",
        "functionName" -> "`=`",
        "dataType" -> "\"MAP<STRING, BIGINT>\""
      )
    )
    assertErrorForOrderingTypes(
      expr = EqualTo($"mapField", $"mapField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(mapField = mapField)\"",
        "functionName" -> "`=`",
        "dataType" -> "\"MAP<STRING, BIGINT>\""
      )
    )
    assertErrorForOrderingTypes(
      expr = EqualNullSafe($"mapField", $"mapField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(mapField <=> mapField)\"",
        "functionName" -> "`<=>`",
        "dataType" -> "\"MAP<STRING, BIGINT>\""
      )
    )
    assertErrorForOrderingTypes(
      expr = LessThan($"mapField", $"mapField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(mapField < mapField)\"",
        "functionName" -> "`<`",
        "dataType" -> "\"MAP<STRING, BIGINT>\""
      )
    )
    assertErrorForOrderingTypes(
      expr = LessThanOrEqual($"mapField", $"mapField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(mapField <= mapField)\"",
        "functionName" -> "`<=`",
        "dataType" -> "\"MAP<STRING, BIGINT>\""
      )
    )
    assertErrorForOrderingTypes(
      expr = GreaterThan($"mapField", $"mapField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(mapField > mapField)\"",
        "functionName" -> "`>`",
        "dataType" -> "\"MAP<STRING, BIGINT>\""
      )
    )
    assertErrorForOrderingTypes(
      expr = GreaterThanOrEqual($"mapField", $"mapField"),
      messageParameters = Map(
        "sqlExpr" -> "\"(mapField >= mapField)\"",
        "functionName" -> "`>=`",
        "dataType" -> "\"MAP<STRING, BIGINT>\""
      )
    )

    assert(If(Literal(1), Literal("a"), Literal("b")).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> "1",
          "requiredType" -> toSQLType(BooleanType),
          "inputSql" -> "\"1\"",
          "inputType" -> "\"INT\""
        )
      )
    )

    assert(If(Literal(true), Literal(1), Literal(false)).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "DATA_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> "`if`",
          "dataType" -> "[\"INT\", \"BOOLEAN\"]"
        )
      )
    )

    assert(CaseWhen(Seq((Literal(true), Literal(1)),
      (Literal(true), Literal("a")))).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "DATA_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> "`casewhen`",
          "dataType" -> "[\"INT\", \"STRING\"]"
        )
      )
    )

    assert(CaseKeyWhen(Literal(1), Seq(Literal(1), Literal("a"),
      Literal(2), Literal(3))).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "DATA_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> "`casewhen`",
          "dataType" -> "[\"STRING\", \"INT\"]"
        )
      )
    )

    assert(CaseWhen(Seq((Literal(true), Literal(1)),
      (Literal(2), Literal(3)))).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> "2",
          "requiredType" -> "\"BOOLEAN\"",
          "inputSql" -> "\"2\"",
          "inputType" -> "\"INT\""
        )
      )
    )
  }

  test("check types for aggregates") {
    // We use AggregateFunction directly at here because the error will be thrown from it
    // instead of from AggregateExpression, which is the wrapper of an AggregateFunction.

    // We will cast String to Double for sum and average
    assertSuccess(Sum($"stringField"))
    assertSuccess(Average($"stringField"))
    assertSuccess(Min($"arrayField"))
    assertSuccess(new BoolAnd($"booleanField"))
    assertSuccess(new BoolOr($"booleanField"))

    assertErrorForOrderingTypes(
      expr = Min($"mapField"),
      messageParameters = Map(
        "sqlExpr" -> "\"min(mapField)\"",
        "functionName" -> "`min`",
        "dataType" -> "\"MAP<STRING, BIGINT>\""
      )
    )
    assertErrorForOrderingTypes(
      expr = Max($"mapField"),
      messageParameters = Map(
        "sqlExpr" -> "\"max(mapField)\"",
        "functionName" -> "`max`",
        "dataType" -> "\"MAP<STRING, BIGINT>\""
      )
    )

    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(Sum($"booleanField"))
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"sum(booleanField)\"",
        "paramIndex" -> "1",
        "inputSql" -> "\"booleanField\"",
        "inputType" -> "\"BOOLEAN\"",
        "requiredType" -> "\"NUMERIC\" or \"ANSI INTERVAL\""))
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(Average($"booleanField"))
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"avg(booleanField)\"",
        "paramIndex" -> "1",
        "inputSql" -> "\"booleanField\"",
        "inputType" -> "\"BOOLEAN\"",
        "requiredType" -> "\"NUMERIC\" or \"ANSI INTERVAL\""))
  }

  test("check types for others") {
    assertErrorForDataDifferingTypes(
      expr = CreateArray(Seq($"intField", $"booleanField")),
      messageParameters = Map(
        "sqlExpr" -> "\"array(intField, booleanField)\"",
        "functionName" -> "`array`",
        "dataType" -> "(\"INT\" or \"BOOLEAN\")"
      )
    )
    assertErrorForDataDifferingTypes(
      expr = Coalesce(Seq($"intField", $"booleanField")),
      messageParameters = Map(
        "sqlExpr" -> "\"coalesce(intField, booleanField)\"",
        "functionName" -> "`coalesce`",
        "dataType" -> "(\"INT\" or \"BOOLEAN\")"
      )
    )

    val coalesce = Coalesce(Nil)
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(coalesce)
      },
      errorClass = "DATATYPE_MISMATCH.WRONG_NUM_ARGS",
      parameters = Map(
        "sqlExpr" -> "\"coalesce()\"",
        "functionName" -> toSQLId(coalesce.prettyName),
        "expectedNum" -> "> 0",
        "actualNum" -> "0"))

    val murmur3Hash = new Murmur3Hash(Nil)
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(murmur3Hash)
      },
      errorClass = "DATATYPE_MISMATCH.WRONG_NUM_ARGS",
      parameters = Map(
        "sqlExpr" -> "\"hash()\"",
        "functionName" -> toSQLId(murmur3Hash.prettyName),
        "expectedNum" -> "> 0",
        "actualNum" -> "0"))

    val xxHash64 = new XxHash64(Nil)
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(xxHash64)
      },
      errorClass = "DATATYPE_MISMATCH.WRONG_NUM_ARGS",
      parameters = Map(
        "sqlExpr" -> "\"xxhash64()\"",
        "functionName" -> toSQLId(xxHash64.prettyName),
        "expectedNum" -> "> 0",
        "actualNum" -> "0"))

    assertError(Explode($"intField"),
      "input to function explode should be array or map type")
    assertError(PosExplode($"intField"),
      "input to function explode should be array or map type")
  }

  test("check types for CreateNamedStruct") {
    assertError(
      CreateNamedStruct(Seq("a", "b", 2.0)), "even number of arguments")
    assertError(
      CreateNamedStruct(Seq(1, "a", "b", 2.0)),
      "Only foldable string expressions are allowed to appear at odd position")
    assertError(
      CreateNamedStruct(Seq($"a".string.at(0), "a", "b", 2.0)),
      "Only foldable string expressions are allowed to appear at odd position")
    assertError(
      CreateNamedStruct(Seq(Literal.create(null, StringType), "a")),
      "Field name should not be null")
  }

  test("check types for CreateMap") {
    assertError(CreateMap(Seq("a", "b", 2.0)), "even number of arguments")
    assertError(
      CreateMap(Seq($"intField", $"stringField",
        $"booleanField", $"stringField")),
      "keys of function map should all be the same type")
    assertError(
      CreateMap(Seq($"stringField", $"intField",
        $"stringField", $"booleanField")),
      "values of function map should all be the same type")
  }

  test("check types for ROUND/BROUND") {
    assertSuccess(Round(Literal(null), Literal(null)))
    assertSuccess(Round($"intField", Literal(1)))

    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(Round($"intField", $"intField"))
      },
      errorClass = "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT",
      parameters = Map(
        "sqlExpr" -> "\"round(intField, intField)\"",
        "inputName" -> "scala",
        "inputType" -> "\"INT\"",
        "inputExpr" -> "\"intField\""))

    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(Round($"intField", $"booleanField"))
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"round(intField, booleanField)\"",
        "paramIndex" -> "2",
        "inputSql" -> "\"booleanField\"",
        "inputType" -> "\"BOOLEAN\"",
        "requiredType" -> "\"INT\""))
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(Round($"intField", $"mapField"))
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"round(intField, mapField)\"",
        "paramIndex" -> "2",
        "inputSql" -> "\"mapField\"",
        "inputType" -> "\"MAP<STRING, BIGINT>\"",
        "requiredType" -> "\"INT\""))
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(Round($"booleanField", $"intField"))
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"round(booleanField, intField)\"",
        "paramIndex" -> "1",
        "inputSql" -> "\"booleanField\"",
        "inputType" -> "\"BOOLEAN\"",
        "requiredType" -> "\"NUMERIC\""))

    assertSuccess(BRound(Literal(null), Literal(null)))
    assertSuccess(BRound($"intField", Literal(1)))
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(BRound($"intField", $"intField"))
      },
      errorClass = "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT",
      parameters = Map(
        "sqlExpr" -> "\"bround(intField, intField)\"",
        "inputName" -> "scala",
        "inputType" -> "\"INT\"",
        "inputExpr" -> "\"intField\""))
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(BRound($"intField", $"booleanField"))
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"bround(intField, booleanField)\"",
        "paramIndex" -> "2",
        "inputSql" -> "\"booleanField\"",
        "inputType" -> "\"BOOLEAN\"",
        "requiredType" -> "\"INT\""))
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(BRound($"intField", $"mapField"))
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"bround(intField, mapField)\"",
        "paramIndex" -> "2",
        "inputSql" -> "\"mapField\"",
        "inputType" -> "\"MAP<STRING, BIGINT>\"",
        "requiredType" -> "\"INT\""))
    checkError(
      exception = intercept[AnalysisException] {
        assertSuccess(BRound($"booleanField", $"intField"))
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"bround(booleanField, intField)\"",
        "paramIndex" -> "1",
        "inputSql" -> "\"booleanField\"",
        "inputType" -> "\"BOOLEAN\"",
        "requiredType" -> "\"NUMERIC\""))
  }

  test("check types for Greatest/Least") {
    for (operator <- Seq[(Seq[Expression] => Expression)](Greatest, Least)) {
      val expr1 = operator(Seq($"booleanField"))
      assertErrorForWrongNumParameters(
        expr = expr1,
        messageParameters = Map(
          "sqlExpr" -> toSQLExpr(expr1),
          "functionName" -> toSQLId(expr1.prettyName),
          "expectedNum" -> "> 1",
          "actualNum" -> "1")
      )

      val expr2 = operator(Seq($"intField", $"stringField"))
      assertErrorForDataDifferingTypes(
        expr = expr2,
        messageParameters = Map(
          "sqlExpr" -> toSQLExpr(expr2),
          "functionName" -> toSQLId(expr2.prettyName),
          "dataType" -> "[\"INT\", \"STRING\"]"
        )
      )

      val expr3 = operator(Seq($"mapField", $"mapField"))
      assertErrorForOrderingTypes(
        expr = expr3,
        messageParameters = Map(
          "sqlExpr" -> toSQLExpr(expr3),
          "functionName" -> s"`${expr3.prettyName}`",
          "dataType" -> "\"MAP<STRING, BIGINT>\""
        )
      )
    }
  }

  test("check types for SQL string generation") {
    assert(Literal.create(Array(1, 2, 3), ArrayType(IntegerType)).sql ==
      "ARRAY(1, 2, 3)")
    assert(Literal.create(Array(1, 2, null), ArrayType(IntegerType)).sql ==
      "ARRAY(1, 2, CAST(NULL AS INT))")
    assert(Literal.default(StructType(Seq(StructField("col", StringType)))).sql ==
      "NAMED_STRUCT('col', '')")
    assert(Literal.default(StructType(Seq(StructField("col", NullType)))).sql ==
      "NAMED_STRUCT('col', NULL)")
    assert(Literal.create(Map(42L -> true), MapType(LongType, BooleanType)).sql ==
      "MAP(42L, true)")
    assert(Literal.create(Map(42L -> null), MapType(LongType, NullType)).sql ==
      "MAP(42L, NULL)")
  }

  test("hash expressions are prohibited on MapType elements") {
    val argument = Literal.create(Map(42L -> true), MapType(LongType, BooleanType))
    val murmur3Hash = new Murmur3Hash(Seq(argument))
    assert(murmur3Hash.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "HASH_MAP_TYPE",
        messageParameters = Map("functionName" -> toSQLId(murmur3Hash.prettyName))
      )
    )
  }
}
