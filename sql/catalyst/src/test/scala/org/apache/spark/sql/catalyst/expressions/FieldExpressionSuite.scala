package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.Field
/**
  * Created by gcz on 16-12-20.
  */
class FieldExpressionSuite extends SparkFunSuite with ExpressionEvalHelper{
  test("field") {
    checkEvaluation(Field(Seq(Literal("花花世界"), Literal("shit"), Literal("花花世界"))), 2)
  }
}
