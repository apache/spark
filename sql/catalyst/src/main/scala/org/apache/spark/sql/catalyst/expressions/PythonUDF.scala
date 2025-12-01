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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException.internalError
import org.apache.spark.api.python.{PythonEvalType, PythonFunction}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.TreePattern.{PYTHON_UDF, TreePattern}
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
    PythonEvalType.SQL_SCALAR_PANDAS_UDF,
    PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
    PythonEvalType.SQL_SCALAR_ARROW_UDF,
    PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF
  )

  def isScalarPythonUDF(e: Expression): Boolean = {
    e.isInstanceOf[PythonUDF] && SCALAR_TYPES.contains(e.asInstanceOf[PythonUDF].evalType)
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
    safe_src: Option[String],
    safe_ast: Option[Any],
    resultId: ExprId = NamedExpression.newExprId)
  extends Expression with PythonFuncExpression with Unevaluable with Logging {

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

  /**
   * Try and convert the provided AST to a native Catalyst expression if
   * possible, otherwise return None.
   */
  def toCatalyst(): Option[Expression] = {
    safe_ast match {
      case None => None
      case Some(ast) =>
        if (ast.isInstanceOf[java.util.List[_]]) {
          val happy_ast = _recursive_to_scala_for_java_lists(ast.asInstanceOf[java.util.List[Any]])
          convert_ast(happy_ast)
        } else {
          None
        }
    }
  }

  def convert_ast(ast: List[Any]): Option[Expression] = {
    val lambda_ast_opt = _get_lambda_ast(ast)
    lambda_ast_opt match {
      case None => None
      case Some(lambda_ast) =>
        val params = _get_paramater_list(lambda_ast)
        val body_ast = _get_lambda_body(lambda_ast)
        logWarning(s"Got {params} {body_ast} from {ast}!")
        None
    }
  }

  def _recursive_to_scala_for_java_lists(ast: java.util.List[Any]): List[Any] = {
    ast.asScala.map {
      case inner_list: java.util.List[_] =>
        _recursive_to_scala_for_java_lists(inner_list.asInstanceOf[java.util.List[Any]])
      case other => other
    }.toList
  }

  private def _get_child_ast_from_node_name(node_name: String, ast: List[_]): Option[List[_]] = {
    logWarning(f"Looking for {node_name} in {ast}")
    val direct_child_with_name = ast.find(elem =>
      elem.isInstanceOf[List[_]] &&
        elem.asInstanceOf[List[_]].headOption == Some(node_name))
    direct_child_with_name.map(_.asInstanceOf[List[_]].tail.asInstanceOf[List[_]])
  }

  private def _get_child_after_match_in_order(
    node_names: List[String],
    ast: List[_]): Option[List[_]] = {
    node_names match {
      case Nil => Some(ast)
      case head :: tail =>
        _get_child_ast_from_node_name(head, ast) match {
          case Some(child_ast) =>
            _get_child_after_match_in_order(tail, child_ast)
          case None =>
            None
        }
    }
  }

  /**
   * Get the top level list of parameter names from the AST.
   * There could be multiple of these nested, but we only care about the top level one.
   */
  private def _get_paramater_list(lambda_ast: List[_]): List[String] = {
    val arguments_ast = _get_child_after_match_in_order(
      List("args", "arguments", "args"),
      lambda_ast)
    arguments_ast.map { arg_nested_tuple =>
      // todo: named params check.
      // ['arg', [['arg', "'x'"]]]
      arg_nested_tuple(1).asInstanceOf[List[_]](0).asInstanceOf[List[_]](1)
        .asInstanceOf[String]
    }.toList
  }

  private def _get_lambda_ast(ast: List[_]): Option[List[_]] = {
    // The general Tree that is "ok" for us to start with is
    // "Module" -> "Body" -> "Assign", we can ignore what we are being assigned to for now
    // then grab the value side of the assignment and it could be either lambda OR
    // Call -> Func -> Args -> Lambda
    val assigned = _get_child_after_match_in_order(
      List("Module", "Body", "Assign"),
      ast).getOrElse(throw new Exception("No Assignment?"))
    val body_direct = _get_child_after_match_in_order(
      List("Lambda"),
      assigned)
    body_direct match {
      case Some(body) => body_direct
      case None => _get_child_after_match_in_order(
        List("Call", "Func", "Args", "Lambda"),
        assigned)
    }
  }

  private def _get_lambda_body(lambda_ast: List[_]): Option[List[_]] = {
    _get_child_ast_from_node_name("Body", lambda_ast)
  }
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
    safe_src: Option[String] = None,
    safe_ast: Option[Any] = None,
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
