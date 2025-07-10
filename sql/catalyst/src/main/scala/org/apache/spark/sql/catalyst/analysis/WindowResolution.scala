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

import org.apache.spark.sql.catalyst.expressions.{
  CurrentRow,
  Expression,
  FrameLessOffsetWindowFunction,
  RangeFrame,
  RankLike,
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

/**
 * Utility object for resolving [[WindowExpression]].
 *
 * It ensures that window frame defintions and order specs are consistent between the
 * [[WindowFunction]] and [[WindowSpecDefinition]], throwing errors if configurations are
 * incompatible or missing.
 */
object WindowResolution {

  /**
   * Validates the window frame of a [[WindowExpression]].
   *
   * It enforces that the frame in [[WindowExpression.windowFunction]] matches the frame
   * in [[WindowExpression.windowSpec]], alterantively it provides a default frame when it
   * is unspecified.
   */
  def resolveFrame(expression: Expression): Expression = expression match {
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

  /**
   * Ensures that [[WindowExpression.windowSpec.orderSpec]] is not missing.
   *
   * In case of [[RankLike]] window functions, it attaches the resolved order to the
   * function to finalize it.
   */
  def resolveOrder(expression: Expression): Expression = expression match {
    case WindowExpression(wf: WindowFunction, spec) if spec.orderSpec.isEmpty =>
      throw QueryCompilationErrors.windowFunctionWithWindowFrameNotOrderedError(wf)

    case WindowExpression(rank: RankLike, spec) if spec.resolved =>
      val order = spec.orderSpec.map(_.child)
      WindowExpression(rank.withOrder(order), spec)

    case e => e
  }
}
