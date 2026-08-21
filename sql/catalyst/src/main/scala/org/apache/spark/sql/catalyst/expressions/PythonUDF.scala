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

import org.apache.spark.SparkException.internalError
import org.apache.spark.api.python.{PythonEvalType, PythonFunction}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.TreePattern.{PYTHON_UDF, TRANSPILED_PYTHON_UDF,
  TRANSPILED_UDF_PARAMETER, TreePattern}
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.types._

/**
 * Helper functions for [[PythonUDF]]
 */
object PythonUDF {
  private[this] val SCALAR_TYPES = Set(
    PythonEvalType.SQL_BATCHED_UDF,
    PythonEvalType.SQL_ARROW_BATCHED_UDF,
    // Element-wise UDFs are row-shaped from the plan's point of view: one array column in, one
    // array column out per row. They are extracted by `ExtractPythonUDFs` like any other scalar
    // UDF; only the Python worker treats them element-wise. One eval type per lifted flavor keeps
    // the worker's pandas- vs. Arrow-shaped batching and the iterator contract distinct.
    PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF,
    PythonEvalType.SQL_SCALAR_PANDAS_ELEMENTWISE_UDF,
    PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF,
    PythonEvalType.SQL_SCALAR_ARROW_ELEMENTWISE_UDF,
    PythonEvalType.SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF,
    PythonEvalType.SQL_SCALAR_PANDAS_UDF,
    PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
    PythonEvalType.SQL_SCALAR_ARROW_UDF,
    PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF
  )

  def isScalarPythonUDF(e: Expression): Boolean = {
    e.isInstanceOf[PythonUDF] && SCALAR_TYPES.contains(e.asInstanceOf[PythonUDF].evalType)
  }

  /**
   * Whether `e` is a Python UDF that can be lifted out of a higher-order function's lambda by
   * `ExtractPythonUDFFromLambda`, which applies it to the whole array outside the lambda.
   *
   * Both the row-at-a-time eval types (plain and Arrow batched) and the vectorized scalar eval
   * types (scalar pandas / Arrow and their iterator variants) qualify: the rule lifts the UDF
   * structurally over `array<T>` arguments, and the Python worker flattens each array, invokes the
   * function on the flat element column with its own batching contract, and re-nests. See
   * [[liftedElementwiseEvalType]] for the mapping to the eval type the lifted UDF runs under.
   *
   * Otherwise-eligible shapes are excluded because the rewrite cannot preserve them:
   *   - a zero-argument call, `f()`: the lift turns each argument into an aligned array, so with no
   *     argument there is no array to carry the iterated shape, and the element-wise UDF would
   *     reach the worker with no input column and crash there instead of failing analysis;
   *   - a call with named arguments on an *iterator* UDF (scalar pandas / Arrow iterator): iterator
   *     UDFs do not take keyword arguments (the worker ignores their kwargs offsets), so such a
   *     call is invalid regardless of the lift. Named arguments on the non-iterator flavors are
   *     supported: the lift keeps each `NamedArgumentExpression` as a direct child of the lifted
   *     UDF (only its value becomes an aligned array), so the runner still derives the kwargs map;
   *   - a UDF whose argument or return type involves a UDT: the lift forces an Arrow element-wise
   *     eval type, which has no UDT fallback (unlike `correctEvalType`'s Arrow -> pickle path), so
   *     it would fail at runtime instead of at analysis.
   * All keep the previous behavior (an analysis error) rather than being rewritten.
   *
   * This is shared with `CheckAnalysis` so that the shapes analysis accepts are exactly those the
   * optimizer rule can rewrite.
   */
  def isElementwiseRewritableUDF(e: Expression): Boolean = e match {
    case udf: PythonUDF =>
      isElementwiseRewritableEvalType(udf.evalType) &&
        udf.children.nonEmpty &&
        (supportsNamedArgumentsWhenLifted(udf.evalType) ||
          !udf.children.exists(_.isInstanceOf[NamedArgumentExpression])) &&
        !containsUDT(udf.dataType) &&
        !udf.children.exists(c => containsUDT(c.dataType))
    case _ => false
  }

  /**
   * Whether a lifted UDF of this eval type can carry keyword arguments. Iterator UDFs (scalar
   * pandas / Arrow iterator, and their lifted element-wise forms) cannot - the worker binds only
   * positional arguments for them - so a named-argument call on an iterator UDF is not rewritable.
   */
  private def supportsNamedArgumentsWhenLifted(evalType: Int): Boolean = evalType match {
    case PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF |
         PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF |
         PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF |
         PythonEvalType.SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF => false
    case _ => true
  }

  private def isElementwiseRewritableEvalType(evalType: Int): Boolean = evalType match {
    case PythonEvalType.SQL_BATCHED_UDF |
         PythonEvalType.SQL_ARROW_BATCHED_UDF |
         PythonEvalType.SQL_SCALAR_PANDAS_UDF |
         PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF |
         PythonEvalType.SQL_SCALAR_ARROW_UDF |
         PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF => true
    // The already-lifted element-wise types are rewritable again: a UDF inside a nested lambda is
    // lifted once onto the inner lambda's variable (producing an element-wise UDF over that
    // variable) and then re-lifted onto the enclosing array, incrementing its nesting depth. Users
    // cannot create these eval types directly, so they only appear mid-rewrite - `CheckAnalysis`,
    // which runs before this rule, never sees them.
    case _ => PythonEvalType.isElementwiseUDF(evalType)
  }

  /**
   * The eval type a rewritable UDF runs under once lifted out of the lambda. Each maps to the
   * element-wise flavor that preserves its worker contract: the row-at-a-time types share the one
   * pickle-based element-wise path, while each vectorized scalar type keeps its own pandas- vs.
   * Arrow-shaped batching and iterator behavior. An already-lifted element-wise type maps to itself
   * (re-lifting for a nested lambda keeps the flavor and only bumps the nesting depth). `evalType`
   * must satisfy [[isElementwiseRewritableEvalType]].
   */
  def liftedElementwiseEvalType(evalType: Int): Int = evalType match {
    case PythonEvalType.SQL_BATCHED_UDF | PythonEvalType.SQL_ARROW_BATCHED_UDF =>
      PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF
    case PythonEvalType.SQL_SCALAR_PANDAS_UDF =>
      PythonEvalType.SQL_SCALAR_PANDAS_ELEMENTWISE_UDF
    case PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF =>
      PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF
    case PythonEvalType.SQL_SCALAR_ARROW_UDF =>
      PythonEvalType.SQL_SCALAR_ARROW_ELEMENTWISE_UDF
    case PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF =>
      PythonEvalType.SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF
    case elementwise if PythonEvalType.isElementwiseUDF(elementwise) => elementwise
    case other =>
      throw internalError(s"Not a rewritable elementwise UDF eval type: $other")
  }

  /**
   * Whether every Python UDF in `hof`'s lambdas can be lifted out by `ExtractPythonUDFFromLambda`.
   * Used by `CheckAnalysis` to decide whether to reject the plan; `hof` must be a *nest root* - one
   * that iterates real columns, not a free lambda variable - because `CheckAnalysis` fires only at
   * nest roots (see its guard) and this predicate validates the whole nest below the root.
   *
   * Both the row-at-a-time and the vectorized scalar eval types are liftable, in a single lambda or
   * nested lambdas: an inner lambda's UDF is lifted onto its (enclosing-variable) argument and then
   * re-lifted outward one array level at a time, so `transform(arr, i -> transform(i, x -> f(x)))`
   * works (`f` lifts to a depth-2 element-wise UDF over `arr`). A UDF in a nested *argument*,
   * `transform(arr, x -> transform(udf(x), y -> y))`, lifts onto `arr` the same way.
   *
   * These shapes still cannot be rewritten and are rejected:
   *   - a UDF in `aggregate` / `reduce`: the fold is sequential, so the UDF sees earlier steps'
   *     outputs, not array elements, and cannot be applied once to the whole array (see
   *     [[isRewritableShape]]).
   *   - a *nondeterministic iterated argument*, `filter(shuffle(arr), x -> f(x))` (at the root or
   *     any nested level): the rewrite references that argument several times (the carrier's `c0`,
   *     each lifted UDF's argument, the `map_keys`/`map_values` desugar, the pairwise `array_sort`
   *     path), and nondeterministic expressions are not subexpression-eliminated, so the copies
   *     would evaluate independently and disagree - keeping the results misaligned. (This is
   *     distinct from a nondeterministic UDF *call*, which `ExtractPythonUDFFromLambda.liftKey`
   *     keeps distinct but well-defined.)
   *   - a HOF (at any level) whose shape the rewrite does not model (see [[isRewritableShape]]).
   */
  def canRewritePythonUDFInLambda(hof: HigherOrderFunction): Boolean = {
    // Every Python UDF anywhere in the lambdas must be a rewritable flavor. `collect` is recursive,
    // so this also covers UDFs in nested lambdas.
    val allUDFsRewritable = hof.functions.forall { f =>
      f.collect { case udf: PythonUDF => udf }.forall(isElementwiseRewritableUDF)
    }
    // Reading a free lambda variable means `hof` is itself nested in an enclosing lambda, so the
    // array it iterates is not a real column. Such an inner HOF is validated as part of its
    // enclosing root's nest by `everyHofInNestRewritable`, never on its own.
    val iteratesRealColumns = !hasFreeLambdaVariable(hof)
    iteratesRealColumns && allUDFsRewritable && everyHofInNestRewritable(hof)
  }

  /**
   * Whether `hof` and every higher-order function nested within its lambda bodies is a rewritable
   * shape (see [[isRewritableShape]]) with deterministic arguments. This is the recursive core that
   * supports UDFs in *nested* lambdas: every HOF on the path from the root down to a UDF must be
   * rewritable, because the rule lifts the UDF out one lambda level at a time. A nested HOF's
   * iterated argument is legitimately an enclosing lambda variable, which is why the free-variable
   * check in [[canRewritePythonUDFInLambda]] applies only at the root, not here.
   */
  private def everyHofInNestRewritable(hof: HigherOrderFunction): Boolean = {
    val nestedHofs = hof.functions.flatMap(_.collect { case h: HigherOrderFunction => h })
    (hof +: nestedHofs).forall { h =>
      isRewritableShape(h) && h.arguments.forall(_.deterministic)
    }
  }

  /**
   * The structural assumption the rewrite makes: one lambda with plain-variable parameters, over at
   * least one array- or map-valued argument (`transform`, `filter`, the map family, ...).
   * `aggregate` / `reduce` fail this - they have two lambdas (`merge`, `finish`) - so a UDF in a
   * fold is rejected: the fold is sequential, so the UDF sees earlier steps' outputs, not array
   * elements. Checking the shape rather than listing classes means a new function of a familiar
   * shape needs no change here.
   *
   * The function must also carry one of the result-type marker traits the rewrite dispatches on
   * ([[ResultTypeFromArgument]] or [[ResultTypeFromFunction]]). Every built-in single-lambda HOF is
   * marked today, but requiring it here keeps "analysis accepts exactly what the rule rewrites"
   * structural: a future HOF missing both traits is rejected at analysis rather than slipping
   * through and leaving the UDF inside the lambda at runtime.
   */
  private def isRewritableShape(hof: HigherOrderFunction): Boolean =
    hof.functions.length == 1 &&
      hof.functions.head.isInstanceOf[LambdaFunction] &&
      hof.functions.head.asInstanceOf[LambdaFunction].arguments
        .forall(_.isInstanceOf[NamedLambdaVariable]) &&
      (hof.isInstanceOf[ResultTypeFromArgument] || hof.isInstanceOf[ResultTypeFromFunction]) &&
      hof.arguments.exists { a =>
        a.dataType.isInstanceOf[ArrayType] || a.dataType.isInstanceOf[MapType]
      }

  /**
   * Whether `e` references a [[NamedLambdaVariable]] that it does not itself bind, i.e. one bound
   * by an enclosing lambda. Such an expression cannot be evaluated outside that lambda.
   *
   * Shared with `ExtractPythonUDFFromLambda` so the rule can re-check the nested-lambda guard on
   * its own, rather than relying only on `CheckAnalysis` having already rejected such plans.
   */
  def hasFreeLambdaVariable(e: Expression): Boolean = {
    def check(expr: Expression, bound: Set[ExprId]): Boolean = expr match {
      case LambdaFunction(function, arguments, _) =>
        check(function, bound ++ arguments.map(_.exprId))
      case v: NamedLambdaVariable => !bound.contains(v.exprId)
      case other => other.children.exists(check(_, bound))
    }
    check(e, Set.empty)
  }

  def isWindowPandasUDF(e: PythonFuncExpression): Boolean = {
    // `PythonUDAF` (SQL_GROUPED_AGG_PANDAS_UDF or SQL_GROUPED_AGG_ARROW_UDF) and the incremental
    // `PythonAggregate` are the Python aggregate functions that run over a window through the
    // Python window operator, rather than the JVM SQL window path. We might support new types in
    // the future, e.g. N -> N transform.
    e.isInstanceOf[PythonUDAF] || e.isInstanceOf[PythonAggregate]
  }

  def correctEvalType(udf: PythonUDF, pythonUDFArrowFallbackOnUDT: Boolean): Int = {
    if (udf.evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF) {
      if (pythonUDFArrowFallbackOnUDT &&
        (containsUDT(udf.dataType) || udf.children.exists(expr => containsUDT(expr.dataType)))) {
        PythonEvalType.SQL_BATCHED_UDF
      } else {
        PythonEvalType.SQL_ARROW_BATCHED_UDF
      }
    } else {
      udf.evalType
    }
  }

  private def containsUDT(dataType: DataType): Boolean = dataType match {
    case _: UserDefinedType[_] => true
    case ArrayType(elementType, _) => containsUDT(elementType)
    case StructType(fields) => fields.exists(field => containsUDT(field.dataType))
    case MapType(keyType, valueType, _) => containsUDT(keyType) || containsUDT(valueType)
    case _ => false
  }
}


trait PythonFuncExpression extends NonSQLExpression with UserDefinedExpression { self: Expression =>
  def name: String
  def func: PythonFunction
  def evalType: Int
  def udfDeterministic: Boolean
  def resultId: ExprId

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  override def toString: String = s"$name(${children.mkString(", ")})#${resultId.id}$typeSuffix"

  override def nullable: Boolean = true
}


/**
 * Marks a subtree of a transpiled option as the argument that got dropped in for the UDF's
 * `index`th parameter, so `PreEvaluateTranspiledUDFInputs` can give it one column and compute it
 * once per row.
 *
 * `UserDefinedPythonFunction`'s builder fills in the `_udf_param_N` placeholders when the call is
 * built, and the marker is the only thing that survives that -- once they are filled in, the copies
 * are ordinary argument expressions, no different from the rest of the option's body.
 *
 * `id` is what ties one parameter's copies together: every copy the builder drops in for it carries
 * the same id. We need that for a nondeterministic argument, where matching on shape is not enough
 * -- an argument whose seed was still unresolved (`expr("rand()")`, or SQL text) gets a fresh seed
 * per copy from `ResolveRandomSeed`, because the placeholders are filled in before analysis runs.
 *
 * The id is minted once per builder call, which is once per `Column` rather than once per place
 * that `Column` lands in a plan. So reusing one transpiled `Column` in two spots leaves both spots
 * carrying the same ids, and `PreEvaluateTranspiledUDFInputs` gives them one shared column -- one
 * draw where the Python path would make two. Fixing that needs the ids re-minted per plan
 * occurrence during analysis, which is not done yet.
 *
 * A [[TaggingExpression]], so it evaluates as its child. It is not fully see-through, though:
 * `canonicalized` keeps `index`, so a marker is not `semanticEquals` the bare argument, and one
 * that survived into an [[Aggregate]]'s expressions would break grouping-expression matching
 * rather than just cost an extra evaluation. `ConvertToCatalyst` is non-excludable and strips every
 * marker in the same pass that puts them to use, so nothing today can leave one behind.
 */
case class TranspiledUDFParameter(child: Expression, index: Int, id: ExprId)
  extends TaggingExpression {

  final override val nodePatterns: Seq[TreePattern] = Seq(TRANSPILED_UDF_PARAMETER)

  // `id` is bookkeeping for the rewrite, not part of the value, so canonicalize it away the way
  // PythonUDF does with resultId. `index` stays: it is what lets you tie a marker in an `explain`
  // back to a `_udf_param_N` in the transpiled body.
  override lazy val canonicalized: Expression = copy(id = ExprId(-1), child = child.canonicalized)

  // Markers live in the plan from call construction to the first optimizer batch, so they show up
  // in analyzed-plan strings and analysis errors. Print the id's number, not the full ExprId,
  // whose per-JVM UUID would differ on every run.
  override def stringArgs: Iterator[Any] = Iterator(child, index, id.id)

  override protected def withNewChildInternal(newChild: Expression): TranspiledUDFParameter =
    copy(child = newChild)
}


case class TranspiledPythonUDF(
  name: String,
  pythonUDFExpr: Expression,
  transpiledOptions: List[Expression],
  // Per-option input-type categories ("numeric"/"string" per public param),
  // parallel to `transpiledOptions`. ResolveTranspiledPythonUDFOptions prunes the
  // options to those whose categories match the resolved input types (before
  // CheckAnalysis can reject a type-incompatible option) and clears this field;
  // ConvertToCatalyst then picks the first survivor or falls back to the Python
  // UDF. Empty means "no restriction" (kept as-is).
  optionInputCategories: List[List[String]] = Nil) extends Expression with Unevaluable {
  require(
    optionInputCategories.isEmpty || optionInputCategories.length == transpiledOptions.length,
    s"optionInputCategories (${optionInputCategories.length}) must be parallel to " +
    s"transpiledOptions (${transpiledOptions.length}) or empty"
  )
  override def children: Seq[Expression] = pythonUDFExpr +: transpiledOptions
  override def dataType: DataType = pythonUDFExpr.dataType
  override def nullable: Boolean = pythonUDFExpr.nullable
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]):
      TranspiledPythonUDF =
    copy(pythonUDFExpr = newChildren.head, transpiledOptions = newChildren.tail.toList)
  final override val nodePatterns: Seq[TreePattern] = Seq(TRANSPILED_PYTHON_UDF)

  // True when every direct input to pythonUDFExpr is a plain PythonUDF (not a
  // TranspiledPythonUDF). Used to decide whether to preserve the UDF batch pipeline
  // rather than inserting a Catalyst node in the middle of a Python UDF chain.
  def hasOnlyPythonUDFInputs: Boolean =
    pythonUDFExpr.children.nonEmpty &&
    pythonUDFExpr.children.forall {
      _.isInstanceOf[PythonUDF]
    }
}

/**
 * A serialized version of a Python lambda function. This is a special expression, which needs a
 * dedicated physical operator to execute it, and thus can't be pushed down to data sources.
 */
case class PythonUDF(
    name: String,
    func: PythonFunction,
    dataType: DataType,
    children: Seq[Expression],
    evalType: Int,
    udfDeterministic: Boolean,
    resultId: ExprId = NamedExpression.newExprId,
    // For an element-wise UDF lifted out of a higher-order function's lambda (see
    // `ExtractPythonUDFFromLambda`), the number of `array` levels the Python worker flattens off
    // each argument before invoking the function, and re-nests onto the result: 1 for a UDF in a
    // single lambda, and one more for each enclosing lambda when the UDF is lifted out of a nested
    // lambda (e.g. `transform(arr, i -> transform(i, x -> f(x)))` lifts `f` to depth 2). Ignored
    // for every non-element-wise eval type, where it stays at its default of 1.
    elementwiseNestingDepth: Int = 1)
  extends Expression with PythonFuncExpression with Unevaluable {

  lazy val resultAttribute: Attribute = AttributeReference(toPrettySQL(this), dataType, nullable)(
    exprId = resultId)

  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    // `resultId` can be seen as cosmetic variation in PythonUDF, as it doesn't affect the result.
    this.copy(resultId = ExprId(-1)).withNewChildren(canonicalizedChildren)
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(PYTHON_UDF)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): PythonUDF =
    copy(children = newChildren)
}

abstract class UnevaluableAggregateFunc extends AggregateFunction {
  override def aggBufferSchema: StructType = throw internalError(
    "UnevaluableAggregateFunc.aggBufferSchema should not be called.")
  override def aggBufferAttributes: Seq[AttributeReference] = throw internalError(
    "UnevaluableAggregateFunc.aggBufferAttributes should not be called.")
  override def inputAggBufferAttributes: Seq[AttributeReference] = throw internalError(
    "UnevaluableAggregateFunc.inputAggBufferAttributes should not be called.")
  final override def eval(input: InternalRow = null): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)
  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

/**
 * A serialized version of a Python lambda function for aggregation. This is a special expression,
 * which needs a dedicated physical operator to execute it, instead of the normal Aggregate
 * operator.
 */
case class PythonUDAF(
    name: String,
    func: PythonFunction,
    dataType: DataType,
    children: Seq[Expression],
    udfDeterministic: Boolean,
    evalType: Int = PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
    resultId: ExprId = NamedExpression.newExprId)
  extends UnevaluableAggregateFunc with PythonFuncExpression {

  override def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$name($distinct${children.mkString(", ")})"
  }

  override def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    name + children.mkString(start, ", ", ")") + s"#${resultId.id}$typeSuffix"
  }

  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    // `resultId` can be seen as cosmetic variation in PythonUDAF, as it doesn't affect the result.
    this.copy(resultId = ExprId(-1)).withNewChildren(canonicalizedChildren)
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(PYTHON_UDF)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): PythonUDAF =
    copy(children = newChildren)
}

/**
 * A serialized Python aggregator that supports true incremental (partial) aggregation, the
 * analog of the Scala typed `org.apache.spark.sql.expressions.Aggregator[IN, BUF, OUT]`. Unlike
 * [[PythonUDAF]] (which materializes the whole group and calls Python once), this is planned as a
 * two-stage aggregation by
 * [[org.apache.spark.sql.execution.python.PythonIncrementalAggregateExec]]: a map-side PARTIAL
 * stage folds input rows into a per-group buffer via the aggregator's `reduce`, and a post-shuffle
 * FINAL stage
 * merges the partial buffers via `merge` and produces the output via `finish`.
 *
 * `bufferSchema` is the schema of the intermediate buffer that crosses the shuffle between the two
 * stages (the analog of the Scala aggregator's `bufferEncoder`). It is exposed here rather than via
 * [[aggBufferAttributes]] because, like [[PythonUDAF]], this expression is unevaluable in the JVM;
 * the physical operator derives the buffer attributes from `bufferSchema` directly.
 */
case class PythonAggregate(
    name: String,
    func: PythonFunction,
    dataType: DataType,
    children: Seq[Expression],
    udfDeterministic: Boolean,
    bufferSchema: StructType,
    evalType: Int = PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_FINAL_UDF,
    resultId: ExprId = NamedExpression.newExprId)
  extends UnevaluableAggregateFunc with PythonFuncExpression {

  override def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$name($distinct${children.mkString(", ")})"
  }

  override def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    name + children.mkString(start, ", ", ")") + s"#${resultId.id}$typeSuffix"
  }

  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    // `resultId` can be seen as cosmetic variation, as it doesn't affect the result.
    this.copy(resultId = ExprId(-1)).withNewChildren(canonicalizedChildren)
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(PYTHON_UDF)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): PythonAggregate =
    copy(children = newChildren)
}

abstract class UnevaluableGenerator extends Generator {
  final override def eval(input: InternalRow): IterableOnce[InternalRow] =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

/**
 * A serialized version of a Python table-valued function call. This is a special expression,
 * which needs a dedicated physical operator to execute it.
 * @param name name of the Python UDTF being called
 * @param func string contents of the Python code in the UDTF, along with other environment state
 * @param elementSchema result schema of the function call
 * @param pickledAnalyzeResult if the UDTF defined an 'analyze' method, this contains the pickled
 *                             'AnalyzeResult' instance from that method, which contains all
 *                             metadata returned including the result schema of the function call as
 *                             well as optional other information
 * @param children input arguments to the UDTF call; for scalar arguments these are the expressions
 *                 themeselves, and for TABLE arguments, these are instances of
 *                 [[FunctionTableSubqueryArgumentExpression]]
 * @param evalType identifies whether this is a scalar or aggregate or table function, using an
 *                 instance of the [[PythonEvalType]] enumeration
 * @param udfDeterministic true if this function is deterministic wherein it returns the same result
 *                         rows for every call with the same input arguments
 * @param resultId unique expression ID for this function invocation
 * @param pythonUDTFPartitionColumnIndexes holds the zero-based indexes of the projected results of
 *                                         all PARTITION BY expressions within the TABLE argument of
 *                                         the Python UDTF call, if applicable
 * @param tableArguments holds whether an input argument is a table argument
 */
case class PythonUDTF(
    name: String,
    func: PythonFunction,
    elementSchema: StructType,
    pickledAnalyzeResult: Option[Array[Byte]],
    children: Seq[Expression],
    evalType: Int,
    udfDeterministic: Boolean,
    resultId: ExprId = NamedExpression.newExprId,
    pythonUDTFPartitionColumnIndexes: Option[PythonUDTFPartitionColumnIndexes] = None,
    tableArguments: Option[Seq[Boolean]] = None)
  extends UnevaluableGenerator with PythonFuncExpression {

  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    // `resultId` can be seen as cosmetic variation in PythonUDTF, as it doesn't affect the result.
    this.copy(resultId = ExprId(-1)).withNewChildren(canonicalizedChildren)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): PythonUDTF =
    copy(children = newChildren)
}

/**
 * Holds the indexes of the TABLE argument to a Python UDTF call, if applicable.
 * @param partitionChildIndexes The indexes of the partitioning columns in each TABLE argument.
 */
case class PythonUDTFPartitionColumnIndexes(partitionChildIndexes: Seq[Int])

/**
 * A placeholder of a polymorphic Python table-valued function.
 */
case class UnresolvedPolymorphicPythonUDTF(
    name: String,
    func: PythonFunction,
    children: Seq[Expression],
    evalType: Int,
    udfDeterministic: Boolean,
    resolveElementMetadata: (PythonFunction, Seq[Expression]) => PythonUDTFAnalyzeResult,
    resultId: ExprId = NamedExpression.newExprId,
    tableArguments: Option[Seq[Boolean]] = None)
  extends UnevaluableGenerator with PythonFuncExpression {

  override lazy val resolved = false

  override def elementSchema: StructType = throw new UnresolvedException("elementSchema")

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): UnresolvedPolymorphicPythonUDTF =
    copy(children = newChildren)
}

/**
 * Represents the result of invoking the polymorphic 'analyze' method on a Python user-defined table
 * function. This returns the table function's output schema in addition to other optional metadata.
 *
 * @param schema result schema of this particular function call in response to the particular
 *               arguments provided, including the types of any provided scalar arguments (and
 *               their values, in the case of literals) as well as the names and types of columns of
 *               the provided TABLE argument (if any)
 * @param withSinglePartition true if the 'analyze' method explicitly indicated that the UDTF call
 *                            should consume all rows of the input TABLE argument in a single
 *                            instance of the UDTF class, in which case Catalyst will invoke a
 *                            repartitioning to a separate stage with a single worker for this
 *                            purpose
 * @param partitionByExpressions if non-empty, this contains the list of column names that the
 *                               'analyze' method explicitly indicated that the UDTF call should
 *                               partition the input table by, wherein all rows corresponding to
 *                               each unique combination of values of the partitioning columns are
 *                               consumed by exactly one unique instance of the UDTF class
 * @param orderByExpressions if non-empty, this contains the list of ordering items that the
 *                           'analyze' method explicitly indicated that the UDTF call should consume
 *                           the input table rows by
 * @param selectedInputExpressions If non-empty, this is a list of expressions that the UDTF is
 *                                 specifying for Catalyst to evaluate against the columns in the
 *                                 input TABLE argument. In this case, Catalyst will insert a
 *                                 projection to evaluate these expressions and return the result to
 *                                 the UDTF. The UDTF then receives one input column for each
 *                                 expression in the list, in the order they are listed.
 * @param pickledAnalyzeResult this is the pickled 'AnalyzeResult' instance from the UDTF, which
 *                             contains all metadata returned by the Python UDTF 'analyze' method
 *                             including the result schema of the function call as well as optional
 *                             other information
 */
case class PythonUDTFAnalyzeResult(
    schema: StructType,
    withSinglePartition: Boolean,
    partitionByExpressions: Seq[Expression],
    orderByExpressions: Seq[SortOrder],
    selectedInputExpressions: Seq[PythonUDTFSelectedExpression],
    pickledAnalyzeResult: Array[Byte]) {
  /**
   * Applies the requested properties from this analysis result to the target TABLE argument
   * expression of a UDTF call, throwing an error if any properties of the UDTF call are
   * incompatible.
   */
  def applyToTableArgument(
      pythonUDTFName: String,
      t: FunctionTableSubqueryArgumentExpression): FunctionTableSubqueryArgumentExpression = {
    if (withSinglePartition && partitionByExpressions.nonEmpty) {
      throw QueryCompilationErrors.tableValuedFunctionRequiredMetadataInvalid(
        functionName = pythonUDTFName,
        reason = "the 'with_single_partition' field cannot be assigned to true " +
          "if the 'partition_by' list is non-empty")
    }
    if (orderByExpressions.nonEmpty && !withSinglePartition && partitionByExpressions.isEmpty) {
      throw QueryCompilationErrors.tableValuedFunctionRequiredMetadataInvalid(
        functionName = pythonUDTFName,
        reason = "the 'order_by' field cannot be non-empty unless the " +
          "'with_single_partition' field is set to true or the 'partition_by' list " +
          "is non-empty")
    }
    if ((withSinglePartition || partitionByExpressions.nonEmpty) && t.hasRepartitioning) {
      throw QueryCompilationErrors
        .tableValuedFunctionRequiredMetadataIncompatibleWithCall(
          functionName = pythonUDTFName,
          requestedMetadata =
            "specified its own required partitioning of the input table",
          invalidFunctionCallProperty =
            "specified the WITH SINGLE PARTITION or PARTITION BY clause; " +
              "please remove these clauses and retry the query again.")
    }
    var newWithSinglePartition = t.withSinglePartition
    var newPartitionByExpressions = t.partitionByExpressions
    var newOrderByExpressions = t.orderByExpressions
    var newSelectedInputExpressions = t.selectedInputExpressions
    if (withSinglePartition) {
      newWithSinglePartition = true
    }
    if (partitionByExpressions.nonEmpty) {
      newPartitionByExpressions = partitionByExpressions
    }
    if (orderByExpressions.nonEmpty) {
      newOrderByExpressions = orderByExpressions
    }
    if (selectedInputExpressions.nonEmpty) {
      newSelectedInputExpressions = selectedInputExpressions
    }
    t.copy(
      withSinglePartition = newWithSinglePartition,
      partitionByExpressions = newPartitionByExpressions,
      orderByExpressions = newOrderByExpressions,
      selectedInputExpressions = newSelectedInputExpressions)
  }
}

/**
 * Represents an expression that the UDTF is specifying for Catalyst to evaluate against the
 * columns in the input TABLE argument. The UDTF then receives one input column for each expression
 * in the list, in the order they are listed.
 *
 * @param expression the expression that the UDTF is specifying for Catalyst to evaluate against the
 *                   columns in the input TABLE argument
 * @param alias If present, this is the alias for the column or expression as visible from the
 *              UDTF's 'eval' method. This is required if the expression is not a simple column
 *              reference.
 */
case class PythonUDTFSelectedExpression(expression: Expression, alias: Option[String])

/**
 * A place holder used when printing expressions without debugging information such as the
 * result id.
 */
case class PrettyPythonUDF(
    name: String,
    dataType: DataType,
    children: Seq[Expression])
  extends UnevaluableAggregateFunc with NonSQLExpression {

  override def toString: String = s"$name(${children.mkString(", ")})"

  override def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$name($distinct${children.mkString(", ")})"
  }

  override def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    name + children.mkString(start, ", ", ")")
  }

  override def nullable: Boolean = true

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): PrettyPythonUDF = copy(children = newChildren)
}
