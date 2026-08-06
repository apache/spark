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
  TreePattern}
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
    // UDF; only the Python worker treats them element-wise.
    PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF,
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
   * Only the row-at-a-time eval types qualify. A pandas UDF receives a `Series` rather than one
   * value per call, so the element-wise rewrite would change its meaning.
   *
   * This is shared with `CheckAnalysis` so that the shapes analysis accepts are exactly those the
   * optimizer rule can rewrite.
   */
  def isElementwiseRewritableUDF(e: Expression): Boolean = e match {
    case udf: PythonUDF =>
      udf.evalType == PythonEvalType.SQL_BATCHED_UDF ||
        udf.evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF
    case _ => false
  }

  /**
   * Whether every Python UDF inside `hof`'s lambdas can be lifted out by
   * `ExtractPythonUDFFromLambda`. Used by `CheckAnalysis` to decide whether to reject the plan.
   *
   * A shape is rewritable when the UDF's inputs are values the higher-order function iterates, so
   * that the UDF can be applied to all of them at once outside the lambda. All twelve lambda-taking
   * functions qualify, including a pairwise `array_sort` comparator (precomputed over the cross
   * product of pairs). Only two placements do not:
   *   - a UDF reading `aggregate`'s accumulator, whose inputs are outputs of earlier fold steps
   *     rather than elements of anything;
   *   - a pandas UDF, which receives a `Series` rather than one value per call.
   *
   * Note this is deliberately not concerned with whether the iterated collection is itself a
   * lambda variable (a nested higher-order function). The optimizer rule rewrites innermost-first
   * and repeats to a fixed point, so the inner rewrite makes the outer one rewritable in a later
   * pass; rejecting it here would forbid a shape that does in fact work.
   */
  def canRewritePythonUDFInLambda(hof: HigherOrderFunction): Boolean = {
    // Every Python UDF in the lambdas must itself be of a rewritable eval type. A pandas UDF,
    // for instance, is never rewritable regardless of the enclosing function.
    val allUDFsRewritable = hof.functions.forall { f =>
      f.collect { case udf: PythonUDF => udf }.forall(isElementwiseRewritableUDF)
    }
    // Every higher-order function currently in Spark is handled by the rewrite, so rather than
    // listing them, this states the two UDF *placements* that no array can precompute. A future
    // higher-order function is accepted automatically; `ExtractPythonUDFFromLambda` rewrites any
    // function generically from the `HigherOrderFunction` API, and `isRewritableShape` guards the
    // one structural assumption that rewrite makes.
    allUDFsRewritable && isRewritableShape(hof) && (hof match {
      case ArrayAggregate(_, _, merge, _) =>
        // A UDF on the element, or in `finish`, is rewritable. One reading the *accumulator* is not
        // rewritable *as an expression*, which is the single remaining exclusion.
        //
        // The fold is sequential: `acc_k = merge(udf(acc_{k-1}), x_k)`, so the values the UDF sees
        // are outputs of earlier steps rather than elements of any collection. Folding [1,2,3] with
        // `acc*2 + x` calls the UDF on 0, 1, 4. No precomputation reaches them - not even the cross
        // product that makes a pairwise `array_sort` comparator work - because computing `acc_n`
        // means alternating Python (`udf`) and JVM (`merge`) n times, and a single UDF call cannot
        // interleave JVM work.
        //
        // This is a limit of expression rewriting, not of Spark. The fold can be restated as an
        // iteration over the whole *column*: carry `(row, step, acc)` and advance every row one
        // step per iteration, so each iteration is one UDF call over all rows and the number of
        // Python round trips is the longest array, not the row count. `UnionLoop` (recursive CTE)
        // already provides the dynamic looping that needs. It is not done here because it is an
        // operator-level restructuring rather than an expression rewrite: the aggregate has to be
        // lifted out of whatever expression contains it, rows need identities to rejoin the loop
        // result, and one long array stalls every other row. Worth its own change if users ask for
        // it; see SPARK-27052.
        !mergeReadsAccumulator(merge)

      case _ => true
    })
  }

  /**
   * The structural assumption the generic rewrite makes: the function has exactly one lambda, whose
   * parameters are plain lambda variables, and it iterates at least one array- or map-valued
   * argument that the precomputed UDF results can be zipped alongside.
   *
   * `aggregate` is the exception: it has two lambdas and folds rather than iterates, so it has its
   * own rewrite. Checking this rather than listing classes means a new higher-order function of a
   * familiar shape needs no change here.
   */
  private def isRewritableShape(hof: HigherOrderFunction): Boolean = hof match {
    case _: ArrayAggregate => true
    case _ =>
      hof.functions.length == 1 &&
        hof.functions.head.isInstanceOf[LambdaFunction] &&
        hof.functions.head.asInstanceOf[LambdaFunction].arguments
          .forall(_.isInstanceOf[NamedLambdaVariable]) &&
        hof.arguments.exists { a =>
          a.dataType.isInstanceOf[ArrayType] || a.dataType.isInstanceOf[MapType]
        }
  }

  /**
   * Whether any Python UDF in an `array_sort` comparator receives both of the comparator's
   * parameters in a single call, e.g. `(a, b) -> my_compare(a, b)`. Such a call cannot be reduced
   * to a per-element key.
   */
  def comparatorTakesBothElements(function: Expression): Boolean = function match {
    case LambdaFunction(body, Seq(left: NamedLambdaVariable, right: NamedLambdaVariable), _) =>
      body.exists {
        case udf: PythonUDF if isElementwiseRewritableUDF(udf) =>
          def reads(id: ExprId) = udf.exists {
            case v: NamedLambdaVariable => v.exprId == id
            case _ => false
          }
          reads(left.exprId) && reads(right.exprId)
        case _ => false
      }
    case _ => false
  }

  /**
   * Whether `e` references a [[NamedLambdaVariable]] that it does not itself bind, i.e. one bound
   * by an enclosing lambda. Such an expression cannot be evaluated outside that lambda.
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

  private def mergeReadsAccumulator(merge: Expression): Boolean = merge match {
    case LambdaFunction(body, Seq(accVar: NamedLambdaVariable, _), _) =>
      body.exists {
        case udf: PythonUDF if isElementwiseRewritableUDF(udf) =>
          udf.exists {
            case v: NamedLambdaVariable => v.exprId == accVar.exprId
            case _ => false
          }
        case _ => false
      }
    case _ => true
  }

  def isWindowPandasUDF(e: PythonFuncExpression): Boolean = {
    // This is currently only `PythonUDAF` (which means SQL_GROUPED_AGG_PANDAS_UDF or
    // SQL_GROUPED_AGG_ARROW_UDF), but we might
    // support new types in the future, e.g, N -> N transform.
    e.isInstanceOf[PythonUDAF]
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
    // For SQL_ARROW_ELEMENTWISE_UDF only: how many array levels each child must be flattened by
    // before its values reach the user's (scalar) function. One entry per child; 0 marks a
    // per-row argument that is broadcast across the row's leaf elements instead. Depth is greater
    // than 1 when the UDF came from a nested higher-order function. Empty for every other eval
    // type. See `ExtractPythonUDFFromLambda`.
    elementwiseDepths: Seq[Int] = Nil)
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
