package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{
  CurrentRow,
  Expression,
  FrameLessOffsetWindowFunction,
  RangeFrame,
  RowFrame,
  SpecifiedWindowFrame,
  UnboundedFollowing,
  UnboundedPreceding,
  UnspecifiedFrame,
  WindowExpression,
  WindowFunction,
  WindowSpecDefinition
}
import org.apache.spark.sql.errors.QueryCompilationErrors

object WindowFrameResolver {
  def resolve(expression: Expression): Expression = expression match {
    case WindowExpression(
    wf: FrameLessOffsetWindowFunction,
    WindowSpecDefinition(_, _, f: SpecifiedWindowFrame)
    ) if wf.frame != f =>
      throw QueryCompilationErrors.cannotSpecifyWindowFrameError(wf.prettyName)

    case WindowExpression(wf: WindowFunction, WindowSpecDefinition(_, _, f: SpecifiedWindowFrame))
      if wf.frame != UnspecifiedFrame && wf.frame != f =>
      throw QueryCompilationErrors.windowFrameNotMatchRequiredFrameError(f, wf.frame)

    case WindowExpression(wf: WindowFunction, s @ WindowSpecDefinition(_, _, UnspecifiedFrame))
      if wf.frame != UnspecifiedFrame =>
      WindowExpression(wf, s.copy(frameSpecification = wf.frame))

    case we @ WindowExpression(e, s @ WindowSpecDefinition(_, o, UnspecifiedFrame)) if e.resolved =>
      val frame = if (o.nonEmpty) {
        SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
      } else {
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)
      }
      we.copy(windowSpec = s.copy(frameSpecification = frame))

    case e => e
  }
}

