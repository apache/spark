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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ExpressionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.TreePattern.{DELEGATE_EXPRESSION, INPUT_TYPE_MARKER, TreePattern}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, DataType}

/**
 * A transparent, named delegate over a `definition` expression -- a LOGICAL-phase construct.
 *
 * `DelegateExpression` lets a high-level function (e.g. `right(a, b)`) stay readable in the
 * analyzed and optimized logical plan, and lets optimizer rules introduce such nodes (e.g.
 * `multi_get_json_object`), without hand-written `eval`/`doGenCode`. Every behavior delegates to
 * `definition`, a real child fully visible to the analyzer and optimizer.
 *
 * `name`/`inputs` are purely informational (EXPLAIN/SQL): nothing enforces that `definition`
 * matches what they claim, so the wrapper is never exposed to physical planning or external
 * systems.
 * `LowerDelegateExpression` strips it to `definition` in `QueryExecution.createSparkPlan` -- the
 * single entry point to the planner, used by both the main query and AQE re-planning -- so the
 * planner and every physical consumer (join-key extraction, data source pushdown, columnar rules,
 * codegen) sees the real executed expression. (Data source V2 pushdown runs earlier, in the logical
 * optimizer, so it unfolds the wrapper directly in `V2ExpressionBuilder`.) The wrapper survives the
 * logical optimizer, so the optimized plan stays readable and optimizer rules can introduce these
 * nodes; `eval`/`doGenCode` still delegate, as a safety net if a delegate ever reaches execution.
 *
 * Note: because the strip runs before planning, a `DelegateExpression` created by a *physical* rule
 * (after `createSparkPlan`) is not stripped and may reach an external system un-lowered. That is
 * acceptable -- like any other expression the system does not recognize, it simply falls back, and
 * `eval`/`doGenCode` keep it correct within Spark. Analysis- and optimizer-inserted nodes (the
 * common case) are always stripped, so physical-rule insertion is the only uncovered path.
 */
case class DelegateExpression(
    name: String,
    inputs: Seq[Expression],
    definition: Expression)
  extends Expression with UnaryLike[Expression] {

  override def child: Expression = definition
  override def dataType: DataType = definition.dataType
  override def nullable: Boolean = definition.nullable
  override def foldable: Boolean = definition.foldable
  // Delegate `nullIntolerant` too (it is not derived from children, unlike `throwable`), so that
  // null-intolerance optimizations -- `IsNotNull`-constraint inference in
  // `QueryPlanConstraints.scanNullIntolerantAttribute` and `NullPropagation`'s `IsNull`/`IsNotNull`
  // simplifications -- still fire while the wrapper is in the logical plan (e.g. for the
  // `multi_get_json_object` delegate, whose `Invoke` definition is null-intolerant).
  override def nullIntolerant: Boolean = definition.nullIntolerant
  override lazy val deterministic: Boolean = definition.deterministic
  override lazy val canonicalized: Expression = definition.canonicalized

  override def eval(input: InternalRow): Any = definition.eval(input)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    definition.genCode(ctx)

  final override val nodePatterns: Seq[TreePattern] = Seq(DELEGATE_EXPRESSION)
  override protected def withNewChildInternal(newChild: Expression): DelegateExpression =
    copy(definition = newChild)

  override def prettyName: String = name
  override def sql: String = s"$name(${inputs.map(_.sql).mkString(", ")})"
  override def toString: String = s"$name(${inputs.mkString(", ")})"
}

/**
 * Analysis-only marker that requests an implicit cast of `child` to `expectedType`: it declares the
 * expected type so the standard `TypeCoercion` rule casts the child, then is removed at the end of
 * analysis by [[org.apache.spark.sql.catalyst.analysis.RemoveInputTypeMarkers]]. It never reaches
 * execution, hence [[Unevaluable]]. Modeled on
 * [[org.apache.spark.sql.catalyst.analysis.TempResolvedColumn]].
 */
case class ImplicitCastInput(child: Expression, expectedType: AbstractDataType)
  extends UnaryExpression with Unevaluable with ImplicitCastInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(expectedType)
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override lazy val canonicalized: Expression = child.canonicalized
  final override val nodePatterns: Seq[TreePattern] = Seq(INPUT_TYPE_MARKER)
  override protected def withNewChildInternal(newChild: Expression): ImplicitCastInput =
    copy(child = newChild)
}

/**
 * Analysis-only marker that requires `child` to already match `expectedType` (no cast is inserted),
 * failing analysis otherwise. Removed at the end of analysis like [[ImplicitCastInput]].
 */
case class TypeCheckInput(child: Expression, expectedType: AbstractDataType)
  extends UnaryExpression with Unevaluable with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(expectedType)
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override lazy val canonicalized: Expression = child.canonicalized
  final override val nodePatterns: Seq[TreePattern] = Seq(INPUT_TYPE_MARKER)
  override protected def withNewChildInternal(newChild: Expression): TypeCheckInput =
    copy(child = newChild)
}

/**
 * The per-function object each built-in function defines (e.g. `object Right extends
 * DelegateFunction`). It is just an [[ExpressionBuilder]] -- registered with the ordinary
 * `expressionBuilder(...)`, with its `@ExpressionDescription` annotation read off the object as
 * usual -- specialized for the delegate pattern: replace the `InheritAnalysisRules` ceremony with
 * one `lower` method plus a couple of flags. `apply` is the direct-construction entry point.
 *
 * Input-type contract, covering all three cases (applied per argument):
 *   - `inputTypes` empty (or `AnyDataType` for a position): accept any type (no check, no cast).
 *   - `inputTypes` set, `implicitCast = true`  (default): implicit-cast each arg to its type.
 *   - `inputTypes` set, `implicitCast = false`           : type-check each arg, no cast.
 */
trait DelegateFunction extends ExpressionBuilder {
  def name: String
  def inputTypes: Seq[AbstractDataType] = Nil
  def implicitCast: Boolean = true

  /** Lower the function into the expression it delegates to. */
  def lower(args: Seq[Expression]): Expression

  /**
   * ExpressionBuilder contract: invoked by the registry during function resolution. ONLY this
   * (analysis-time) path inserts the input-type markers, because the analyzer's `TypeCoercion`
   * casts them and `RemoveInputTypeMarkers` strips them afterwards.
   */
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // `inputTypes` carries one entry per argument position (`AnyDataType` for an accept-any-type
    // position), so when it is set its length is the function's arity. Validate it here so a
    // wrong-arity call fails with the structured WRONG_NUM_ARGS error rather than an
    // IndexOutOfBounds from `lower` (too few args) or a silently-ignored extra argument (too many).
    // An empty `inputTypes` marks a variadic function whose `lower` accepts any argument count.
    if (inputTypes.nonEmpty && expressions.length != inputTypes.length) {
      throw QueryCompilationErrors.wrongNumArgsError(
        funcName, Seq(inputTypes.length), expressions.length)
    }
    val args = expressions.zipWithIndex.map { case (e, i) =>
      val expected = if (i < inputTypes.length) inputTypes(i) else AnyDataType
      expected match {
        case AnyDataType => e
        case t if implicitCast => ImplicitCastInput(e, t)
        case t => TypeCheckInput(e, t)
      }
    }
    DelegateExpression(name, expressions, lower(args))
  }

  /**
   * Direct construction for use anywhere, including optimizer rules. Unlike [[build]] this inserts
   * NO input-type markers -- there is no analyzer pass left to coerce or strip them -- so callers
   * must pass arguments that are already resolved and of the expected types, exactly as when
   * constructing any other expression (`Add`, `Substring`, ...) after analysis. The resolved
   * precondition is asserted so misuse fails loudly here rather than later.
   */
  final def apply(inputs: Expression*): DelegateExpression = {
    require(inputs.forall(_.resolved),
      s"$name: arguments to DelegateFunction.apply must be resolved; use it after analysis " +
        "(e.g. in optimizer rules) with already-typed arguments, or register the function and " +
        "let the analyzer build it")
    DelegateExpression(name, inputs, lower(inputs))
  }

  def unapply(e: Expression): Option[Seq[Expression]] = e match {
    case d: DelegateExpression if d.name == name => Some(d.inputs)
    case _ => None
  }
}
