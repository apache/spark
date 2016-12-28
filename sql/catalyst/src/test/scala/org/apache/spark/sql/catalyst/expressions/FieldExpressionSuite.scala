package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import java.sql.Timestamp
import java.sql.Date

class FieldExpressionSuite extends SparkFunSuite with ExpressionEvalHelper{

  test("field") {
    checkEvaluation(Field(Seq(Literal("花花世界"), Literal("a"), Literal("b"), Literal("花花世界"))), 3)
    checkEvaluation(Field(Seq(Literal("aaa"), Literal("aaa"), Literal("aaa"), Literal("花花世界"))), 1)
    checkEvaluation(Field(Seq(Literal(""), Literal(""), Literal(""), Literal("花花世界"))), 1)
    checkEvaluation(Field(Seq(Literal(true), Literal(false), Literal(true), Literal(true))), 2)
    checkEvaluation(Field(Seq(Literal(1), Literal(2), Literal(3), Literal(1))), 3)
    checkEvaluation(Field(Seq(Literal(1.222), Literal(1.224), Literal(1.221), Literal(1.222))), 3)
    checkEvaluation(Field(Seq(Literal(new Timestamp(2016, 12, 27, 14, 22, 1, 1)), Literal(new Timestamp(1988, 6, 3, 1, 1, 1, 1)), Literal(new Timestamp(1990, 6, 5, 1, 1, 1, 1)), Literal(new Timestamp(2016, 12, 27, 14, 22, 1, 1)))), 3)
    checkEvaluation(Field(Seq(Literal(new Date(1949, 1, 1)), Literal(new Date(1949, 1, 1)), Literal(new Date(1979, 1, 1)), Literal(new Date(1989, 1, 1)))), 1)
    checkEvaluation(Field(Seq(Literal(999), Literal(1.224), Literal("999"), Literal(true), Literal(new Date(2016, 1, 1)), Literal(999))), 5)
    checkEvaluation(Field(Seq(Literal("Cannot find me"), Literal("abc"), Literal("bcd"), Literal("花花世界"))), 0)
  }

}
