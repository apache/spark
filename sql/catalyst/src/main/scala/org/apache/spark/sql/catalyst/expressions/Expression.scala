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

import java.util.Locale

import org.apache.spark.{QueryContext, SparkException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast.{toSQLExpr, toSQLType}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, CurrentOrigin, LeafLike, QuaternaryLike, TernaryLike, TreeNode, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern.{LAZY_ANALYSIS_EXPRESSION, RUNTIME_REPLACEABLE, TreePattern}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.errors.{QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.MULTI_COMMUTATIVE_OP_OPT_THRESHOLD
import org.apache.spark.sql.types._

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the basic expression abstract classes in Catalyst.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * An expression in Catalyst.
 *
 * If an expression wants to be exposed in the function registry (so users can call it with
 * "name(arguments...)", the concrete implementation must be a case class whose constructor
 * arguments are all Expressions types. See [[Substring]] for an example.
 *
 * There are a few important traits or abstract classes:
 *
 * - [[Nondeterministic]]: an expression that is not deterministic.
 * - [[Unevaluable]]: an expression that is not supposed to be evaluated.
 * - [[CodegenFallback]]: an expression that does not have code gen implemented and falls back to
 *                        interpreted mode.
 * - [[NonSQLExpression]]: a common base trait for the expressions that do not have SQL
 *                         expressions like representation. For example, `ScalaUDF`, `ScalaUDAF`,
 *                         and object `MapObjects` and `Invoke`.
 * - [[UserDefinedExpression]]: a common base trait for user-defined functions, including
 *                              UDF/UDAF/UDTF.
 * - [[HigherOrderFunction]]: a common base trait for higher order functions that take one or more
 *                            (lambda) functions and applies these to some objects. The function
 *                            produces a number of variables which can be consumed by some lambda
 *                            functions.
 * - [[NamedExpression]]: An [[Expression]] that is named.
 * - [[TimeZoneAwareExpression]]: A common base trait for time zone aware expressions.
 * - [[SubqueryExpression]]: A base interface for expressions that contain a
 *                           [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]].
 *
 * - [[LeafExpression]]: an expression that has no child.
 * - [[UnaryExpression]]: an expression that has one child.
 * - [[BinaryExpression]]: an expression that has two children.
 * - [[TernaryExpression]]: an expression that has three children.
 * - [[QuaternaryExpression]]: an expression that has four children.
 * - [[BinaryOperator]]: a special case of [[BinaryExpression]] that requires two children to have
 *                       the same output data type.
 *
 * A few important traits used for type coercion rules:
 * - [[ExpectsInputTypes]]: an expression that has the expected input types. This trait is typically
 *                          used by operator expressions (e.g. [[Add]], [[Subtract]]) to define
 *                          expected input types without any implicit casting.
 * - [[ImplicitCastInputTypes]]: an expression that has the expected input types, which can be
 *                               implicitly castable using [[TypeCoercion.ImplicitTypeCasts]].
 * - [[ComplexTypeMergingExpression]]: to resolve output types of the complex expressions
 *                                     (e.g., [[CaseWhen]]).
 */
abstract class Expression extends TreeNode[Expression] {

  /**
   * Returns true when an expression is a candidate for static evaluation before the query is
   * executed. A typical use case: [[org.apache.spark.sql.catalyst.optimizer.ConstantFolding]]
   *
   * The following conditions are used to determine suitability for constant folding:
   *  - A [[Coalesce]] is foldable if all of its children are foldable
   *  - A [[BinaryExpression]] is foldable if its both left and right child are foldable
   *  - A [[Not]], [[IsNull]], or [[IsNotNull]] is foldable if its child is foldable
   *  - A [[Literal]] is foldable
   *  - A [[Cast]] or [[UnaryMinus]] is foldable if its child is foldable
   */
  def foldable: Boolean = false

  /**
   * Returns true when the current expression always return the same result for fixed inputs from
   * children. The non-deterministic expressions should not change in number and order. They should
   * not be evaluated during the query planning.
   *
   * Note that this means that an expression should be considered as non-deterministic if:
   * - it relies on some mutable internal state, or
   * - it relies on some implicit input that is not part of the children expression list.
   * - it has non-deterministic child or children.
   * - it assumes the input satisfies some certain condition via the child operator.
   *
   * An example would be `SparkPartitionID` that relies on the partition id returned by TaskContext.
   * By default leaf expressions are deterministic as Nil.forall(_.deterministic) returns true.
   */
  lazy val deterministic: Boolean = children.forall(_.deterministic)

  def nullable: Boolean

  /**
   * Workaround scala compiler so that we can call super on lazy vals
   */
  @transient
  private lazy val _references: AttributeSet =
    AttributeSet.fromAttributeSets(children.map(_.references))

  def references: AttributeSet = _references

  /**
   * Returns true if the expression contains mutable state.
   *
   * A stateful expression should never be evaluated multiple times for a single row. This should
   * only be a problem for interpreted execution. This can be prevented by creating fresh copies
   * of the stateful expression before execution. A common example to trigger this issue:
   * {{{
   *   val rand = functions.rand()
   *   df.select(rand, rand) // These 2 rand should not share a state.
   * }}}
   */
  def stateful: Boolean = false


  /**
   * When an expression inherits this, meaning the expression is null intolerant (i.e. any null
   * input will result in null output). We will use this information during constructing IsNotNull
   * constraints.
   */
  def nullIntolerant: Boolean = false

  /**
   * Returns true if the expression could potentially throw an exception when evaluated.
   */
  lazy val throwable: Boolean = children.exists(_.throwable)

  /**
   * Returns a copy of this expression where all stateful expressions are replaced with fresh
   * uninitialized copies. If the expression contains no stateful expressions then the original
   * expression is returned.
   */
  def freshCopyIfContainsStatefulExpression(): Expression = {
    val childrenIndexedSeq: IndexedSeq[Expression] = children match {
      case types: IndexedSeq[Expression] => types
      case other => other.toIndexedSeq
    }
    val newChildren = childrenIndexedSeq.map(_.freshCopyIfContainsStatefulExpression())
    // A more efficient version of `children.zip(newChildren).exists(_ ne _)`
    val anyChildChanged = {
      val size = newChildren.length
      var i = 0
      var res: Boolean = false
      while (!res && i < size) {
        res |= (childrenIndexedSeq(i) ne newChildren(i))
        i += 1
      }
      res
    }
    // If the children contain stateful expressions and get copied, or this expression is stateful,
    // copy this expression with the new children.
    if (anyChildChanged || stateful) {
      CurrentOrigin.withOrigin(origin) {
        val res = withNewChildrenInternal(newChildren)
        res.copyTagsFrom(this)
        res
      }
    } else {
      this
    }
  }

  /** Returns the result of evaluating this expression on a given input Row */
  def eval(input: InternalRow = null): Any

  /**
   * Returns an [[ExprCode]], that contains the Java source code to generate the result of
   * evaluating the expression on an input row.
   *
   * @param ctx a [[CodegenContext]]
   * @return [[ExprCode]]
   */
  def genCode(ctx: CodegenContext): ExprCode = {
    ctx.subExprEliminationExprs.get(ExpressionEquals(this)).map { subExprState =>
      // This expression is repeated which means that the code to evaluate it has already been added
      // as a function before. In that case, we just re-use it.
      ExprCode(
        ctx.registerComment(this.toString),
        subExprState.eval.isNull,
        subExprState.eval.value)
    }.getOrElse {
      val isNull = ctx.freshName("isNull")
      val value = ctx.freshName("value")
      val eval = doGenCode(ctx, ExprCode(
        JavaCode.isNullVariable(isNull),
        JavaCode.variable(value, dataType)))
      reduceCodeSize(ctx, eval)
      if (eval.code.toString.nonEmpty) {
        // Add `this` in the comment.
        eval.copy(code = ctx.registerComment(this.toString) + eval.code)
      } else {
        eval
      }
    }
  }

  private def reduceCodeSize(ctx: CodegenContext, eval: ExprCode): Unit = {
    // TODO: support whole stage codegen too
    val splitThreshold = SQLConf.get.methodSplitThreshold
    if (eval.code.length > splitThreshold && ctx.INPUT_ROW != null && ctx.currentVars == null) {
      val setIsNull = if (!eval.isNull.isInstanceOf[LiteralValue]) {
        val globalIsNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "globalIsNull")
        val localIsNull = eval.isNull
        eval.isNull = JavaCode.isNullGlobal(globalIsNull)
        s"$globalIsNull = $localIsNull;"
      } else {
        ""
      }

      val javaType = CodeGenerator.javaType(dataType)
      val newValue = ctx.freshName("value")

      val funcName = ctx.freshName(nodeName)
      val funcFullName = ctx.addNewFunction(funcName,
        s"""
           |private $javaType $funcName(InternalRow ${ctx.INPUT_ROW}) {
           |  ${eval.code}
           |  $setIsNull
           |  return ${eval.value};
           |}
           """.stripMargin)

      eval.value = JavaCode.variable(newValue, dataType)
      eval.code = code"$javaType $newValue = $funcFullName(${ctx.INPUT_ROW});"
    }
  }

  /**
   * Returns Java source code that can be compiled to evaluate this expression.
   * The default behavior is to call the eval method of the expression. Concrete expression
   * implementations should override this to do actual code generation.
   *
   * @param ctx a [[CodegenContext]]
   * @param ev an [[ExprCode]] with unique terms.
   * @return an [[ExprCode]] containing the Java source code to generate the given expression
   */
  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode

  /**
   * Returns `true` if this expression and all its children have been resolved to a specific schema
   * and input data types checking passed, and `false` if it still contains any unresolved
   * placeholders or has data types mismatch.
   * Implementations of expressions should override this if the resolution of this type of
   * expression involves more than just the resolution of its children and type checking.
   */
  lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  def dataType: DataType

  /**
   * Returns true if  all the children of this expression have been resolved to a specific schema
   * and false if any still contains any unresolved placeholders.
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * Returns an expression where a best effort attempt has been made to transform `this` in a way
   * that preserves the result but removes cosmetic variations (case sensitivity, ordering for
   * commutative operations, etc.).
   *
   * `deterministic` expressions where `this.canonicalized == other.canonicalized` will always
   * evaluate to the same result.
   *
   * The process of canonicalization is a one pass, bottum-up expression tree computation based on
   * canonicalizing children before canonicalizing the current node. There is one exception though,
   * as adjacent, same class [[CommutativeExpression]]s canonicalazion happens in a way that calling
   * `canonicalized` on the root:
   *   1. Gathers and canonicalizes the non-commutative (or commutative but not same class) child
   *      expressions of the adjacent expressions.
   *   2. Reorder the canonicalized child expressions by their hashcode.
   * This means that the lazy `cannonicalized` is called and computed only on the root of the
   * adjacent expressions.
   */
  lazy val canonicalized: Expression = withCanonicalizedChildren

  /**
   * The default process of canonicalization. It is a one pass, bottum-up expression tree
   * computation based oncanonicalizing children before canonicalizing the current node.
   */
  final protected def withCanonicalizedChildren: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    withNewChildren(canonicalizedChildren)
  }

  /**
   * Returns true when two expressions will always compute the same result, even if they differ
   * cosmetically (i.e. capitalization of names in attributes may be different).
   *
   * See [[Expression#canonicalized]] for more details.
   */
  final def semanticEquals(other: Expression): Boolean =
    deterministic && other.deterministic && canonicalized == other.canonicalized

  /**
   * Returns a `hashCode` for the calculation performed by this expression. Unlike the standard
   * `hashCode`, an attempt has been made to eliminate cosmetic differences.
   *
   * See [[Expression#canonicalized]] for more details.
   */
  def semanticHash(): Int = canonicalized.hashCode()

  /**
   * Checks the input data types, returns `TypeCheckResult.success` if it's valid,
   * or returns a `TypeCheckResult` with an error message if invalid.
   * Note: it's not valid to call this method until `childrenResolved == true`.
   */
  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  /**
   * Returns a user-facing string representation of this expression's name.
   * This should usually match the name of the function in SQL.
   */
  def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse(nodeName.toLowerCase(Locale.ROOT))

  protected def flatArguments: Iterator[Any] = stringArgs.flatMap {
    case t: Iterable[_] => t
    case single => single :: Nil
  }

  // Marks this as final, Expression.verboseString should never be called, and thus shouldn't be
  // overridden by concrete classes.
  final override def verboseString(maxFields: Int): String = simpleString(maxFields)

  override def simpleString(maxFields: Int): String = toString

  override def toString: String = prettyName + truncatedString(
    flatArguments.toSeq, "(", ", ", ")", SQLConf.get.maxToStringFields)

  /**
   * Returns SQL representation of this expression.  For expressions extending [[NonSQLExpression]],
   * this method may return an arbitrary user facing string.
   */
  def sql: String = {
    val childrenSQL = children.map(_.sql).mkString(", ")
    s"$prettyName($childrenSQL)"
  }

  override def simpleStringWithNodeId(): String = {
    throw SparkException.internalError(s"$nodeName does not implement simpleStringWithNodeId")
  }

  protected def typeSuffix =
    if (resolved) {
      dataType match {
        case LongType => "L"
        case _ => ""
      }
    } else {
      ""
    }
}

/**
 * An expression that cannot be evaluated but is guaranteed to be replaced with a foldable value
 * by query optimizer (e.g. CurrentDate).
 */
trait FoldableUnevaluable extends Expression {
  override def foldable: Boolean = true

  override def eval(input: InternalRow = null): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

/**
 * An expression that cannot be evaluated. These expressions don't live past analysis or
 * optimization time (e.g. Star) and should not be evaluated during query planning and
 * execution.
 */
trait Unevaluable extends Expression with FoldableUnevaluable {

  /** Unevaluable is not foldable by default because we don't have an eval for it.
   * Exception are expressions that will be replaced by a literal by Optimizer (e.g. CurrentDate).
   * Hence we allow overriding overriding of this field in special cases.
   */
  final override def foldable: Boolean = false
}

/**
 * An expression that cannot be analyzed. These expressions don't live analysis time or after
 * and should not be evaluated during query planning and execution.
 */
trait LazyAnalysisExpression extends Expression {
  final override lazy val resolved = false

  final override val nodePatterns: Seq[TreePattern] =
    Seq(LAZY_ANALYSIS_EXPRESSION) ++ nodePatternsInternal()

  // Subclasses can override this function to provide more TreePatterns.
  def nodePatternsInternal(): Seq[TreePattern] = Seq()
}

/**
 * An expression that gets replaced at runtime (currently by the optimizer) into a different
 * expression for evaluation. This is mainly used to provide compatibility with other databases.
 * For example, we use this to support "nvl" by replacing it with "coalesce".
 */
trait RuntimeReplaceable extends Expression {
  def replacement: Expression

  override val nodePatterns: Seq[TreePattern] = Seq(RUNTIME_REPLACEABLE)
  override def nullable: Boolean = replacement.nullable
  override def dataType: DataType = replacement.dataType
  // As this expression gets replaced at optimization with its `child" expression,
  // two `RuntimeReplaceable` are considered to be semantically equal if their "child" expressions
  // are semantically equal.
  override lazy val canonicalized: Expression = replacement.canonicalized

  final override def eval(input: InternalRow = null): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)
  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

/**
 * An add-on of [[RuntimeReplaceable]]. It makes `replacement` the child of the expression, to
 * inherit the analysis rules for it, such as type coercion. The implementation should put
 * `replacement` in the case class constructor, and define a normal constructor that accepts only
 * the original parameters. For an example, see [[TryAdd]]. To make sure the explain plan and
 * expression SQL works correctly, the implementation should also implement the `parameters` method.
 */
trait InheritAnalysisRules extends UnaryLike[Expression] { self: RuntimeReplaceable =>
  override def child: Expression = replacement
  def parameters: Seq[Expression]
  override def flatArguments: Iterator[Any] = parameters.iterator
  // This method is used to generate a SQL string with transformed inputs. This is necessary as
  // the actual inputs are not the children of this expression.
  def makeSQLString(childrenSQL: Seq[String]): String = {
    prettyName + childrenSQL.mkString("(", ", ", ")")
  }
  final override def sql: String = makeSQLString(parameters.map(_.sql))
}

/**
 * An add-on of [[AggregateFunction]]. This gets rewritten (currently by the optimizer) into a
 * different aggregate expression for evaluation. This is mainly used to provide compatibility
 * with other databases. For example, we use this to support every, any/some aggregates by rewriting
 * them with Min and Max respectively.
 */
trait RuntimeReplaceableAggregate extends RuntimeReplaceable { self: AggregateFunction =>
  override def aggBufferSchema: StructType = {
    throw SparkException.internalError(
      "RuntimeReplaceableAggregate.aggBufferSchema should not be called")
  }
  override def aggBufferAttributes: Seq[AttributeReference] = {
    throw SparkException.internalError(
      "RuntimeReplaceableAggregate.aggBufferAttributes should not be called")
  }
  override def inputAggBufferAttributes: Seq[AttributeReference] = {
    throw SparkException.internalError(
      "RuntimeReplaceableAggregate.inputAggBufferAttributes should not be called")
  }
}

/**
 * Expressions that don't have SQL representation should extend this trait.  Examples are
 * `ScalaUDF`, `ScalaUDAF`, and object expressions like `MapObjects` and `Invoke`.
 */
trait NonSQLExpression extends Expression {
  final override def sql: String = {
    transform {
      case a: Attribute => new PrettyAttribute(a)
      case a: Alias => PrettyAttribute(a.sql, a.dataType)
      case p: PythonFuncExpression => PrettyPythonUDF(p.name, p.dataType, p.children)
    }.toString
  }
}


/**
 * An expression that is nondeterministic.
 */
trait Nondeterministic extends Expression {
  final override lazy val deterministic: Boolean = false
  final override def foldable: Boolean = false

  @transient
  private[this] var initialized = false

  /**
   * Initializes internal states given the current partition index and mark this as initialized.
   * Subclasses should override [[initializeInternal()]].
   */
  final def initialize(partitionIndex: Int): Unit = {
    initializeInternal(partitionIndex)
    initialized = true
  }

  protected def initializeInternal(partitionIndex: Int): Unit

  /**
   * @inheritdoc
   * Throws an exception if [[initialize()]] is not called yet.
   * Subclasses should override [[evalInternal()]].
   */
  final override def eval(input: InternalRow = null): Any = {
    require(initialized,
      s"Nondeterministic expression ${this.getClass.getName} should be initialized before eval.")
    evalInternal(input)
  }

  protected def evalInternal(input: InternalRow): Any
}

/**
 * An expression that contains conditional expression branches, so not all branches will be hit.
 * All optimization should be careful with the evaluation order.
 */
trait ConditionalExpression extends Expression {
  final override def foldable: Boolean = children.forall(_.foldable)

  /**
   * Return the children expressions which can always be hit at runtime.
   */
  def alwaysEvaluatedInputs: Seq[Expression]

  /**
   * Return a copy of itself with a new `alwaysEvaluatedInputs`.
   */
  def withNewAlwaysEvaluatedInputs(alwaysEvaluatedInputs: Seq[Expression]): ConditionalExpression

  /**
   * Return groups of branches. For each group, at least one branch will be hit at runtime,
   * so that we can eagerly evaluate the common expressions of a group.
   */
  def branchGroups: Seq[Seq[Expression]]
}

/**
 * A leaf expression, i.e. one without any child expressions.
 */
abstract class LeafExpression extends Expression with LeafLike[Expression]


/**
 * An expression with one input and one output. The output is by default evaluated to null
 * if the input is evaluated to null.
 */
abstract class UnaryExpression extends Expression with UnaryLike[Expression] {

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

  /**
   * Default behavior of evaluation according to the default nullability of UnaryExpression.
   * If subclass of UnaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of UnaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError(this.getClass.getName,
      "eval", "nullSafeEval")

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not null, use `f` to generate the expression.
   *
   * As an example, the following does a boolean inversion (i.e. NOT).
   * {{{
   *   defineCodeGen(ctx, ev, c => s"!($c)")
   * }}}
   *
   * @param f function that accepts a variable name and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: String => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"${ev.value} = ${f(eval)};"
    })
  }

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not null, use `f` to generate the expression.
   *
   * @param f function that accepts the non-null evaluation result name of child and returns Java
   *          code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: String => String): ExprCode = {
    val childGen = child.genCode(ctx)
    val resultCode = f(childGen.value)

    if (nullable) {
      val nullSafeEval = ctx.nullSafeExec(child.nullable, childGen.isNull)(resultCode)
      ev.copy(code = code"""
        ${childGen.code}
        boolean ${ev.isNull} = ${childGen.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(code = code"""
        ${childGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * An expression with SQL query context. The context string can be serialized from the Driver
 * to executors. It will also be kept after rule transforms.
 */
trait SupportQueryContext extends Expression with Serializable {
  protected var queryContext: Option[QueryContext] = initQueryContext()

  def initQueryContext(): Option[QueryContext]

  def getContextOrNull(): QueryContext = queryContext.orNull

  def getContextOrNullCode(ctx: CodegenContext, withErrorContext: Boolean = true): String = {
    if (withErrorContext && queryContext.isDefined) {
      ctx.addReferenceObj("errCtx", queryContext.get)
    } else {
      "null"
    }
  }

  // Note: Even though query contexts are serialized to executors, it will be regenerated from an
  //       empty "Origin" during rule transforms since "Origin"s are not serialized to executors
  //       for better performance. Thus, we need to copy the original query context during
  //       transforms. The query context string is considered as a "tag" on the expression here.
  override def copyTagsFrom(other: Expression): Unit = {
    other match {
      case s: SupportQueryContext =>
        queryContext = s.queryContext
      case _ =>
    }
    super.copyTagsFrom(other)
  }
}

object UnaryExpression {
  def unapply(e: UnaryExpression): Option[Expression] = Some(e.child)
}


/**
 * An expression with two inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class BinaryExpression extends Expression with BinaryLike[Expression] {

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable || right.nullable

  /**
   * Default behavior of evaluation according to the default nullability of BinaryExpression.
   * If subclass of BinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(value1, value2)
      }
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of BinaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError(this.getClass.getName,
      "eval", "nullSafeEval")

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts two variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"${ev.value} = ${f(eval1, eval2)};"
    })
  }

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 2 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val resultCode = f(leftGen.value, rightGen.value)

    if (nullable) {
      val nullSafeEval =
        leftGen.code.toString + ctx.nullSafeExec(left.nullable, leftGen.isNull) {
          rightGen.code.toString + ctx.nullSafeExec(right.nullable, rightGen.isNull) {
            s"""
              ${ev.isNull} = false; // resultCode could change nullability.
              $resultCode
            """
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(code = code"""
        ${leftGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}


object BinaryExpression {
  def unapply(e: BinaryExpression): Option[(Expression, Expression)] = Some((e.left, e.right))
}


/**
 * A [[BinaryExpression]] that is an operator, with two properties:
 *
 * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
 * 2. Two inputs are expected to be of the same type. If the two inputs have different types,
 *    the analyzer will find the tightest common type and do the proper type casting.
 */
abstract class BinaryOperator extends BinaryExpression with ExpectsInputTypes with QueryErrorsBase {

  /**
   * Expected input type from both left/right child expressions, similar to the
   * [[ImplicitCastInputTypes]] trait.
   */
  def inputType: AbstractDataType

  def symbol: String

  def sqlOperator: String = symbol

  override def toString: String = s"($left $sqlOperator $right)"

  override def inputTypes: Seq[AbstractDataType] = Seq(inputType, inputType)

  override def checkInputDataTypes(): TypeCheckResult = {
    // First check whether left and right have the same type, then check if the type is acceptable.
    if (!DataTypeUtils.sameType(left.dataType, right.dataType)) {
      DataTypeMismatch(
        errorSubClass = "BINARY_OP_DIFF_TYPES",
        messageParameters = Map(
          "left" -> toSQLType(left.dataType),
          "right" -> toSQLType(right.dataType)))
    } else if (!inputType.acceptsType(left.dataType)) {
      DataTypeMismatch(
        errorSubClass = "BINARY_OP_WRONG_TYPE",
        messageParameters = Map(
          "inputType" -> toSQLType(inputType),
          "actualDataType" -> toSQLType(left.dataType)))
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def sql: String = s"(${left.sql} $sqlOperator ${right.sql})"
}


object BinaryOperator {
  def unapply(e: BinaryOperator): Option[(Expression, Expression)] = Some((e.left, e.right))
}

/**
 * An expression with three inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class TernaryExpression extends Expression with TernaryLike[Expression] {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of TernaryExpression.
   * If subclass of TernaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value1 = first.eval(input)
    if (value1 != null) {
      val value2 = second.eval(input)
      if (value2 != null) {
        val value3 = third.eval(input)
        if (value3 != null) {
          return nullSafeEval(value1, value2, value3)
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of TernaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError(this.getClass.getName,
      "eval", "nullSafeEval")

  /**
   * Short hand for generating ternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts three variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3) => {
      s"${ev.value} = ${f(eval1, eval2, eval3)};"
    })
  }

  /**
   * Short hand for generating ternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 3 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String) => String): ExprCode = {
    val leftGen = children(0).genCode(ctx)
    val midGen = children(1).genCode(ctx)
    val rightGen = children(2).genCode(ctx)
    val resultCode = f(leftGen.value, midGen.value, rightGen.value)

    if (nullable) {
      val nullSafeEval =
        leftGen.code.toString + ctx.nullSafeExec(children(0).nullable, leftGen.isNull) {
          midGen.code.toString + ctx.nullSafeExec(children(1).nullable, midGen.isNull) {
            rightGen.code.toString + ctx.nullSafeExec(children(2).nullable, rightGen.isNull) {
              s"""
                ${ev.isNull} = false; // resultCode could change nullability.
                $resultCode
              """
            }
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(code = code"""
        ${leftGen.code}
        ${midGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * An expression with four inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class QuaternaryExpression extends Expression with QuaternaryLike[Expression] {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of QuaternaryExpression.
   * If subclass of QuaternaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value1 = first.eval(input)
    if (value1 != null) {
      val value2 = second.eval(input)
      if (value2 != null) {
        val value3 = third.eval(input)
        if (value3 != null) {
          val value4 = fourth.eval(input)
          if (value4 != null) {
            return nullSafeEval(value1, value2, value3, value4)
          }
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of QuaternaryExpression keep the
   *  default nullability, they can override this method to save null-check code.  If we need
   *  full control of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any, input4: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError(this.getClass.getName,
      "eval", "nullSafeEval")

  /**
   * Short hand for generating quaternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts four variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3, eval4) => {
      s"${ev.value} = ${f(eval1, eval2, eval3, eval4)};"
    })
  }

  /**
   * Short hand for generating quaternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 4 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String, String) => String): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thridGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val resultCode = f(firstGen.value, secondGen.value, thridGen.value, fourthGen.value)

    if (nullable) {
      val nullSafeEval =
        firstGen.code.toString + ctx.nullSafeExec(children(0).nullable, firstGen.isNull) {
          secondGen.code.toString + ctx.nullSafeExec(children(1).nullable, secondGen.isNull) {
            thridGen.code.toString + ctx.nullSafeExec(children(2).nullable, thridGen.isNull) {
              fourthGen.code.toString + ctx.nullSafeExec(children(3).nullable, fourthGen.isNull) {
                s"""
                  ${ev.isNull} = false; // resultCode could change nullability.
                  $resultCode
                """
              }
            }
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thridGen.code}
        ${fourthGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * An expression with five inputs and one output. The output is by default evaluated to null if
 * any input is evaluated to null.
 */
abstract class QuinaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of QuinaryExpression. If
   * subclass of QuinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val exprs = children
    val v1 = exprs(0).eval(input)
    if (v1 != null) {
      val v2 = exprs(1).eval(input)
      if (v2 != null) {
        val v3 = exprs(2).eval(input)
        if (v3 != null) {
          val v4 = exprs(3).eval(input)
          if (v4 != null) {
            val v5 = exprs(4).eval(input)
            if (v5 != null) {
              return nullSafeEval(v1, v2, v3, v4, v5)
            }
          }
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation. If subclass of QuinaryExpression keep the default
   * nullability, they can override this method to save null-check code. If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(
      input1: Any,
      input2: Any,
      input3: Any,
      input4: Any,
      input5: Any): Any = {
    throw QueryExecutionErrors.notOverrideExpectedMethodsError(
      this.getClass.getName,
      "eval",
      "nullSafeEval")
  }

  /**
   * Short hand for generating quinary evaluation code. If either of the sub-expressions is null,
   * the result of this computation is assumed to be null.
   *
   * @param f
   *   accepts seven variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, String) => String): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      (eval1, eval2, eval3, eval4, eval5) => {
        s"${ev.value} = ${f(eval1, eval2, eval3, eval4, eval5)};"
      })
  }

  /**
   * Short hand for generating quinary evaluation code. If either of the sub-expressions is null,
   * the result of this computation is assumed to be null.
   *
   * @param f
   *   function that accepts the 5 non-null evaluation result names of children and returns Java
   *   code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, String) => String): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thirdGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val fifthGen = children(4).genCode(ctx)
    val resultCode =
      f(firstGen.value, secondGen.value, thirdGen.value, fourthGen.value, fifthGen.value)

    if (nullable) {
      val nullSafeEval =
        firstGen.code.toString + ctx.nullSafeExec(children(0).nullable, firstGen.isNull) {
          secondGen.code.toString + ctx.nullSafeExec(children(1).nullable, secondGen.isNull) {
            thirdGen.code.toString + ctx.nullSafeExec(children(2).nullable, thirdGen.isNull) {
              fourthGen.code.toString + ctx.nullSafeExec(children(3).nullable, fourthGen.isNull) {
                fifthGen.code.toString + ctx.nullSafeExec(children(4).nullable, fifthGen.isNull) {
                  s"""
                      ${ev.isNull} = false; // resultCode could change nullability.
                      $resultCode
                      """
                }
              }
            }
          }
        }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(
        code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thirdGen.code}
        ${fourthGen.code}
        ${fifthGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""",
        isNull = FalseLiteral)
    }
  }
}

/**
 * An expression with six inputs + 7th optional input and one output.
 * The output is by default evaluated to null if any input is evaluated to null.
 */
abstract class SeptenaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of SeptenaryExpression.
   * If subclass of SeptenaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val exprs = children
    val v1 = exprs(0).eval(input)
    if (v1 != null) {
      val v2 = exprs(1).eval(input)
      if (v2 != null) {
        val v3 = exprs(2).eval(input)
        if (v3 != null) {
          val v4 = exprs(3).eval(input)
          if (v4 != null) {
            val v5 = exprs(4).eval(input)
            if (v5 != null) {
              val v6 = exprs(5).eval(input)
              if (v6 != null) {
                if (exprs.length > 6) {
                  val v7 = exprs(6).eval(input)
                  if (v7 != null) {
                    return nullSafeEval(v1, v2, v3, v4, v5, v6, Some(v7))
                  }
                } else {
                  return nullSafeEval(v1, v2, v3, v4, v5, v6, None)
                }
              }
            }
          }
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of SeptenaryExpression keep the
   * default nullability, they can override this method to save null-check code.  If we need
   * full control of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(
      input1: Any,
      input2: Any,
      input3: Any,
      input4: Any,
      input5: Any,
      input6: Any,
      input7: Option[Any]): Any = {
    throw QueryExecutionErrors.notOverrideExpectedMethodsError(this.getClass.getName,
      "eval", "nullSafeEval")
  }

  /**
   * Short hand for generating septenary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts seven variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, String, String, Option[String]) => String
    ): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3, eval4, eval5, eval6, eval7) => {
      s"${ev.value} = ${f(eval1, eval2, eval3, eval4, eval5, eval6, eval7)};"
    })
  }

  /**
   * Short hand for generating septenary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 7 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, String, String, Option[String]) => String
    ): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thirdGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val fifthGen = children(4).genCode(ctx)
    val sixthGen = children(5).genCode(ctx)
    val seventhGen = if (children.length > 6) Some(children(6).genCode(ctx)) else None
    val resultCode = f(
      firstGen.value,
      secondGen.value,
      thirdGen.value,
      fourthGen.value,
      fifthGen.value,
      sixthGen.value,
      seventhGen.map(_.value))

    if (nullable) {
      val nullSafeEval =
        firstGen.code.toString + ctx.nullSafeExec(children(0).nullable, firstGen.isNull) {
          secondGen.code.toString + ctx.nullSafeExec(children(1).nullable, secondGen.isNull) {
            thirdGen.code.toString + ctx.nullSafeExec(children(2).nullable, thirdGen.isNull) {
              fourthGen.code.toString + ctx.nullSafeExec(children(3).nullable, fourthGen.isNull) {
                fifthGen.code.toString + ctx.nullSafeExec(children(4).nullable, fifthGen.isNull) {
                  sixthGen.code.toString + ctx.nullSafeExec(children(5).nullable, sixthGen.isNull) {
                    val nullSafeResultCode =
                      s"""
                      ${ev.isNull} = false; // resultCode could change nullability.
                      $resultCode
                      """
                    seventhGen.map { gen =>
                      gen.code.toString + ctx.nullSafeExec(children(6).nullable, gen.isNull) {
                        nullSafeResultCode
                      }
                    }.getOrElse(nullSafeResultCode)
                  }
                }
              }
            }
          }
        }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thirdGen.code}
        ${fourthGen.code}
        ${fifthGen.code}
        ${sixthGen.code}
        ${seventhGen.map(_.code).getOrElse("")}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * A trait used for resolving nullable flags, including `nullable`, `containsNull` of [[ArrayType]]
 * and `valueContainsNull` of [[MapType]], containsNull, valueContainsNull flags of the output date
 * type. This is usually utilized by the expressions (e.g. [[CaseWhen]]) that combine data from
 * multiple child expressions of non-primitive types.
 */
trait ComplexTypeMergingExpression extends Expression {

  /**
   * A collection of data types used for resolution the output type of the expression. By default,
   * data types of all child expressions. The collection must not be empty.
   */
  @transient
  lazy val inputTypesForMerging: Seq[DataType] = children.map(_.dataType)

  def dataTypeCheck: Unit = {
    SparkException.require(
      requirement = inputTypesForMerging.nonEmpty,
      errorClass = "COMPLEX_EXPRESSION_UNSUPPORTED_INPUT.NO_INPUTS",
      messageParameters = Map("expression" -> toSQLExpr(this)))
    SparkException.require(
      requirement = TypeCoercion.haveSameType(inputTypesForMerging),
      errorClass = "COMPLEX_EXPRESSION_UNSUPPORTED_INPUT.MISMATCHED_TYPES",
      messageParameters = Map(
        "expression" -> toSQLExpr(this),
        "inputTypes" -> inputTypesForMerging.map(toSQLType).mkString("[", ", ", "]")))
  }

  private lazy val internalDataType: DataType = {
    dataTypeCheck
    inputTypesForMerging.reduceLeft(TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(_, _).get)
  }

  override def dataType: DataType = internalDataType
}

/**
 * Common base trait for user-defined functions, including UDF/UDAF/UDTF of different languages
 * and Hive function wrappers.
 */
trait UserDefinedExpression {
  def name: String
}

trait CommutativeExpression extends Expression {
  /** Collects adjacent commutative operations. */
  private def gatherCommutative(
      e: Expression,
      f: PartialFunction[CommutativeExpression, Seq[Expression]]): Seq[Expression] = {
    val resultBuffer = scala.collection.mutable.Buffer[Expression]()
    val stack = scala.collection.mutable.Stack[Expression](e)

    // [SPARK-49977]: Use iterative approach to avoid creating many temporary List objects
    // for deep expression trees through recursion.
    while (stack.nonEmpty) {
      stack.pop() match {
        case c: CommutativeExpression if f.isDefinedAt(c) =>
          stack.pushAll(f(c))
        case other =>
          resultBuffer += other.canonicalized
      }
    }
    resultBuffer.toSeq
  }

  /**
   * Reorders adjacent commutative operators such as [[And]] in the expression tree, according to
   * the `hashCode` of non-commutative nodes, to remove cosmetic variations.
   */
  protected def orderCommutative(
      f: PartialFunction[CommutativeExpression, Seq[Expression]]): Seq[Expression] =
    gatherCommutative(this, f).sortBy(_.hashCode())

  /**
   * Helper method to generated a canonicalized plan. If the number of operands are
   * greater than the MULTI_COMMUTATIVE_OP_OPT_THRESHOLD, this method creates a
   * [[MultiCommutativeOp]] as the canonicalized plan.
   */
  protected def buildCanonicalizedPlan(
      collectOperands: PartialFunction[Expression, Seq[Expression]],
      buildBinaryOp: (Expression, Expression) => Expression,
      evalMode: Option[EvalMode.Value] = None): Expression = {
    val operands = orderCommutative(collectOperands)
    val reorderResult =
      if (operands.length < SQLConf.get.getConf(MULTI_COMMUTATIVE_OP_OPT_THRESHOLD)) {
        operands.reduce(buildBinaryOp)
      } else {
        MultiCommutativeOp(operands, this.getClass, evalMode)(this)
      }
    reorderResult
  }
}

/**
 * A helper class used by the Commutative expressions during canonicalization. During
 * canonicalization, when we have a long tree of commutative operations, we use the MultiCommutative
 * expression to represent that tree instead of creating new commutative objects.
 * This class is added as a memory optimization for processing large commutative operation trees
 * without creating a large number of new intermediate objects.
 * The MultiCommutativeOp memory optimization is applied to the following commutative
 * expressions:
 *      Add, Multiply, And, Or, BitwiseAnd, BitwiseOr, BitwiseXor.
 * @param operands A sequence of operands that produces a commutative expression tree.
 * @param opCls The class of the root operator of the expression tree.
 * @param evalMode The optional expression evaluation mode.
 * @param originalRoot Root operator of the commutative expression tree before canonicalization.
 *                     This object reference is used to deduce the return dataType of Add and
 *                     Multiply operations when the input datatype is decimal.
 */
case class MultiCommutativeOp(
    operands: Seq[Expression],
    opCls: Class[_],
    evalMode: Option[EvalMode.Value])(originalRoot: Expression) extends Unevaluable {
  // Helper method to deduce the data type of a single operation.
  private def singleOpDataType(lType: DataType, rType: DataType): DataType = {
    originalRoot match {
      case add: Add =>
        (lType, rType) match {
          case (DecimalType.Fixed(p1, s1), DecimalType.Fixed(p2, s2)) =>
            add.resultDecimalType(p1, s1, p2, s2)
          case _ => lType
        }
      case multiply: Multiply =>
        (lType, rType) match {
          case (DecimalType.Fixed(p1, s1), DecimalType.Fixed(p2, s2)) =>
            multiply.resultDecimalType(p1, s1, p2, s2)
          case _ => lType
        }
    }
  }

  override def dataType: DataType = {
    originalRoot match {
      case _: Add | _: Multiply =>
        operands.map(_.dataType).reduce((l, r) => singleOpDataType(l, r))
      case other => other.dataType
    }
  }

  override def nullable: Boolean = operands.exists(_.nullable)

  override def children: Seq[Expression] = operands

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    this.copy(operands = newChildren)(originalRoot)

  override protected final def otherCopyArgs: Seq[AnyRef] = originalRoot :: Nil
}
