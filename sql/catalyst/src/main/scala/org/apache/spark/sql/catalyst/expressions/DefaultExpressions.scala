package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.ExpressionBuilders._

object DefaultExpressions {
  val expressions: Map[String,ExpressionBuilder] = Map(
    expression[Sum],
    expression[Count],
    expression[First],
    expression[Last],
    expression[Average],
    expression[Min],
    expression[Max],
    expression[Upper],
    expression[Lower],
    expression[If],
    expression[Substring], expression[Substring]("SUBSTR"),
    expression[Coalesce],
    expression[Sqrt],
    expression[Abs]
  )

}
