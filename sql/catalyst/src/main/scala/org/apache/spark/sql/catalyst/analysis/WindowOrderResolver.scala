package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  RankLike,
  WindowExpression,
  WindowFunction
}
import org.apache.spark.sql.errors.QueryCompilationErrors

object WindowOrderResolver {
  def resolve(expression: Expression): Expression = expression match {
    case WindowExpression(wf: WindowFunction, spec) if spec.orderSpec.isEmpty =>
      throw QueryCompilationErrors.windowFunctionWithWindowFrameNotOrderedError(wf)

    case WindowExpression(rank: RankLike, spec) if spec.resolved =>
      val order = spec.orderSpec.map(_.child)
      WindowExpression(rank.withOrder(order), spec)

    case e => e
  }
}
