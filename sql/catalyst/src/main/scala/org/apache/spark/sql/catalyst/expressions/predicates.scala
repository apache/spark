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

import scala.collection.immutable.TreeSet

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateSafeProjection, GenerateUnsafeProjection, Predicate => BasePredicate}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._


object InterpretedPredicate {
  def create(expression: Expression, inputSchema: Seq[Attribute]): InterpretedPredicate =
    create(BindReferences.bindReference(expression, inputSchema))

  def create(expression: Expression): InterpretedPredicate = new InterpretedPredicate(expression)
}

case class InterpretedPredicate(expression: Expression) extends BasePredicate {
  override def eval(r: InternalRow): Boolean = expression.eval(r).asInstanceOf[Boolean]
}

/**
 * An [[Expression]] that returns a boolean value.
 */
trait Predicate extends Expression {
  override def dataType: DataType = BooleanType
}


trait PredicateHelper {
  protected def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  protected def splitDisjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case Or(cond1, cond2) =>
        splitDisjunctivePredicates(cond1) ++ splitDisjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  // Substitute any known alias from a map.
  protected def replaceAlias(
      condition: Expression,
      aliases: AttributeMap[Expression]): Expression = {
    // Use transformUp to prevent infinite recursion when the replacement expression
    // redefines the same ExprId,
    condition.transformUp {
      case a: Attribute =>
        aliases.getOrElse(a, a)
    }
  }

  /**
   * Returns true if `expr` can be evaluated using only the output of `plan`.  This method
   * can be used to determine when it is acceptable to move expression evaluation within a query
   * plan.
   *
   * For example consider a join between two relations R(a, b) and S(c, d).
   *
   * - `canEvaluate(EqualTo(a,b), R)` returns `true`
   * - `canEvaluate(EqualTo(a,c), R)` returns `false`
   * - `canEvaluate(Literal(1), R)` returns `true` as literals CAN be evaluated on any plan
   */
  protected def canEvaluate(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.subsetOf(plan.outputSet)

  /**
   * Returns true iff `expr` could be evaluated as a condition within join.
   */
  protected def canEvaluateWithinJoin(expr: Expression): Boolean = expr match {
    // Non-deterministic expressions are not allowed as join conditions.
    case e if !e.deterministic => false
    case _: ListQuery | _: Exists =>
      // A ListQuery defines the query which we want to search in an IN subquery expression.
      // Currently the only way to evaluate an IN subquery is to convert it to a
      // LeftSemi/LeftAnti/ExistenceJoin by `RewritePredicateSubquery` rule.
      // It cannot be evaluated as part of a Join operator.
      // An Exists shouldn't be push into a Join operator too.
      false
    case e: SubqueryExpression =>
      // non-correlated subquery will be replaced as literal
      e.children.isEmpty
    case a: AttributeReference => true
    case e: Unevaluable => false
    case e => e.children.forall(canEvaluateWithinJoin)
  }
}

@ExpressionDescription(
  usage = "_FUNC_ expr - Logical not.")
case class Not(child: Expression)
  extends UnaryExpression with Predicate with ImplicitCastInputTypes with NullIntolerant {

  override def toString: String = s"NOT $child"

  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  // +---------+-----------+
  // | CHILD   | NOT CHILD |
  // +---------+-----------+
  // | TRUE    | FALSE     |
  // | FALSE   | TRUE      |
  // | UNKNOWN | UNKNOWN   |
  // +---------+-----------+
  protected override def nullSafeEval(input: Any): Any = !input.asInstanceOf[Boolean]

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"!($c)")
  }

  override def sql: String = s"(NOT ${child.sql})"
}


/**
 * Evaluates to `true` if `list` contains `value`.
 */
@ExpressionDescription(
  usage = "expr1 _FUNC_(expr2, expr3, ...) - Returns true if `expr` equals to any valN.")
case class In(value: Expression, list: Seq[Expression]) extends Predicate {

  require(list != null, "list should not be null")
  override def checkInputDataTypes(): TypeCheckResult = {
    list match {
      case ListQuery(sub, _, _) :: Nil =>
        val valExprs = value match {
          case cns: CreateNamedStruct => cns.valExprs
          case expr => Seq(expr)
        }

        val mismatchedColumns = valExprs.zip(sub.output).flatMap {
          case (l, r) if l.dataType != r.dataType =>
            s"(${l.sql}:${l.dataType.catalogString}, ${r.sql}:${r.dataType.catalogString})"
          case _ => None
        }

        if (mismatchedColumns.nonEmpty) {
          TypeCheckResult.TypeCheckFailure(
            s"""
               |The data type of one or more elements in the left hand side of an IN subquery
               |is not compatible with the data type of the output of the subquery
               |Mismatched columns:
               |[${mismatchedColumns.mkString(", ")}]
               |Left side:
               |[${valExprs.map(_.dataType.catalogString).mkString(", ")}].
               |Right side:
               |[${sub.output.map(_.dataType.catalogString).mkString(", ")}].
             """.stripMargin)
        } else {
          TypeUtils.checkForOrderingExpr(value.dataType, s"function $prettyName")
        }
      case _ =>
        val mismatchOpt = list.find(l => l.dataType != value.dataType)
        if (mismatchOpt.isDefined) {
          TypeCheckResult.TypeCheckFailure(s"Arguments must be same type but were: " +
            s"${value.dataType} != ${mismatchOpt.get.dataType}")
        } else {
          TypeUtils.checkForOrderingExpr(value.dataType, s"function $prettyName")
        }
    }
  }

  override def children: Seq[Expression] = value +: list
  lazy val inSetConvertible = list.forall(_.isInstanceOf[Literal])
  private lazy val ordering = TypeUtils.getInterpretedOrdering(value.dataType)

  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  override def toString: String = s"$value IN ${list.mkString("(", ",", ")")}"

  override def eval(input: InternalRow): Any = {
    val evaluatedValue = value.eval(input)
    if (evaluatedValue == null) {
      null
    } else {
      var hasNull = false
      list.foreach { e =>
        val v = e.eval(input)
        if (v == null) {
          hasNull = true
        } else if (ordering.equiv(v, evaluatedValue)) {
          return true
        }
      }
      if (hasNull) {
        null
      } else {
        false
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val valueGen = value.genCode(ctx)
    val listGen = list.map(_.genCode(ctx))
    ctx.addMutableState("boolean", ev.value, "")
    ctx.addMutableState("boolean", ev.isNull, "")
    val valueArg = ctx.freshName("valueArg")
    val listCode = listGen.map(x =>
      s"""
        if (!${ev.value}) {
          ${x.code}
          if (${x.isNull}) {
            ${ev.isNull} = true;
          } else if (${ctx.genEqual(value.dataType, valueArg, x.value)}) {
            ${ev.isNull} = false;
            ${ev.value} = true;
          }
        }
       """)
    val listCodes = if (ctx.INPUT_ROW != null && ctx.currentVars == null) {
      val args = ("InternalRow", ctx.INPUT_ROW) :: (ctx.javaType(value.dataType), valueArg) :: Nil
      ctx.splitExpressions(listCode, "valueIn", args)
    } else {
      listCode.mkString("\n")
    }
    ev.copy(code = s"""
      ${valueGen.code}
      ${ev.value} = false;
      ${ev.isNull} = ${valueGen.isNull};
      if (!${ev.isNull}) {
        ${ctx.javaType(value.dataType)} $valueArg = ${valueGen.value};
        $listCodes
      }
    """)
  }

  override def sql: String = {
    val childrenSQL = children.map(_.sql)
    val valueSQL = childrenSQL.head
    val listSQL = childrenSQL.tail.mkString(", ")
    s"($valueSQL IN ($listSQL))"
  }
}

/**
 * Optimized version of In clause, when all filter values of In clause are
 * static.
 */
case class InSet(child: Expression, hset: Set[Any]) extends UnaryExpression with Predicate {

  require(hset != null, "hset could not be null")

  override def toString: String = s"$child INSET ${hset.mkString("(", ",", ")")}"

  @transient private[this] lazy val hasNull: Boolean = hset.contains(null)

  override def nullable: Boolean = child.nullable || hasNull

  protected override def nullSafeEval(value: Any): Any = {
    if (set.contains(value)) {
      true
    } else if (hasNull) {
      null
    } else {
      false
    }
  }

  @transient private[this] lazy val set = child.dataType match {
    case _: AtomicType => hset
    case _: NullType => hset
    case _ =>
      // for structs use interpreted ordering to be able to compare UnsafeRows with non-UnsafeRows
      TreeSet.empty(TypeUtils.getInterpretedOrdering(child.dataType)) ++ hset
  }

  def getSet(): Set[Any] = set

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val setName = classOf[Set[Any]].getName
    val InSetName = classOf[InSet].getName
    val childGen = child.genCode(ctx)
    ctx.references += this
    val setTerm = ctx.freshName("set")
    val setNull = if (hasNull) {
      s"""
         |if (!${ev.value}) {
         |  ${ev.isNull} = true;
         |}
       """.stripMargin
    } else {
      ""
    }
    ctx.addMutableState(setName, setTerm,
      s"$setTerm = (($InSetName)references[${ctx.references.size - 1}]).getSet();")
    ev.copy(code = s"""
      ${childGen.code}
      boolean ${ev.isNull} = ${childGen.isNull};
      boolean ${ev.value} = false;
      if (!${ev.isNull}) {
        ${ev.value} = $setTerm.contains(${childGen.value});
        $setNull
      }
     """)
  }

  override def sql: String = {
    val valueSQL = child.sql
    val listSQL = hset.toSeq.map(Literal(_).sql).mkString(", ")
    s"($valueSQL IN ($listSQL))"
  }
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Logical AND.")
case class And(left: Expression, right: Expression) extends BinaryOperator with Predicate {

  override def inputType: AbstractDataType = BooleanType

  override def symbol: String = "&&"

  override def sqlOperator: String = "AND"

  // +---------+---------+---------+---------+
  // | AND     | TRUE    | FALSE   | UNKNOWN |
  // +---------+---------+---------+---------+
  // | TRUE    | TRUE    | FALSE   | UNKNOWN |
  // | FALSE   | FALSE   | FALSE   | FALSE   |
  // | UNKNOWN | UNKNOWN | FALSE   | UNKNOWN |
  // +---------+---------+---------+---------+
  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    if (input1 == false) {
       false
    } else {
      val input2 = right.eval(input)
      if (input2 == false) {
        false
      } else {
        if (input1 != null && input2 != null) {
          true
        } else {
          null
        }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)

    // The result should be `false`, if any of them is `false` whenever the other is null or not.

    // place generated code of eval1 and eval2 in separate methods if their code combined is large
    val combinedLength = eval1.code.length + eval2.code.length
    if (combinedLength > 1024 &&
      // Split these expressions only if they are created from a row object
      (ctx.INPUT_ROW != null && ctx.currentVars == null)) {

      val (eval1FuncName, eval1GlobalIsNull, eval1GlobalValue) =
        ctx.createAndAddFunction(eval1, BooleanType, "eval1Expr")
      val (eval2FuncName, eval2GlobalIsNull, eval2GlobalValue) =
        ctx.createAndAddFunction(eval2, BooleanType, "eval2Expr")
      if (!left.nullable && !right.nullable) {
        val generatedCode = s"""
         $eval1FuncName(${ctx.INPUT_ROW});
         boolean ${ev.value} = false;
         if (${eval1GlobalValue}) {
           $eval2FuncName(${ctx.INPUT_ROW});
           ${ev.value} = ${eval2GlobalValue};
         }
       """
        ev.copy(code = generatedCode, isNull = "false")
      } else {
        val generatedCode = s"""
         $eval1FuncName(${ctx.INPUT_ROW});
         boolean ${ev.isNull} = false;
         boolean ${ev.value} = false;
         if (!${eval1GlobalIsNull} && !${eval1GlobalValue}) {
         } else {
           $eval2FuncName(${ctx.INPUT_ROW});
           if (!${eval2GlobalIsNull} && !${eval2GlobalValue}) {
           } else if (!${eval1GlobalIsNull} && !${eval2GlobalIsNull}) {
             ${ev.value} = true;
           } else {
             ${ev.isNull} = true;
           }
         }
       """
        ev.copy(code = generatedCode)
      }
    } else if (!left.nullable && !right.nullable) {
      ev.copy(code = s"""
        ${eval1.code}
        boolean ${ev.value} = false;

        if (${eval1.value}) {
          ${eval2.code}
          ${ev.value} = ${eval2.value};
        }""", isNull = "false")
    } else {
      ev.copy(code = s"""
        ${eval1.code}
        boolean ${ev.isNull} = false;
        boolean ${ev.value} = false;

        if (!${eval1.isNull} && !${eval1.value}) {
        } else {
          ${eval2.code}
          if (!${eval2.isNull} && !${eval2.value}) {
          } else if (!${eval1.isNull} && !${eval2.isNull}) {
            ${ev.value} = true;
          } else {
            ${ev.isNull} = true;
          }
        }
      """)
    }
  }
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Logical OR.")
case class Or(left: Expression, right: Expression) extends BinaryOperator with Predicate {

  override def inputType: AbstractDataType = BooleanType

  override def symbol: String = "||"

  override def sqlOperator: String = "OR"

  // +---------+---------+---------+---------+
  // | OR      | TRUE    | FALSE   | UNKNOWN |
  // +---------+---------+---------+---------+
  // | TRUE    | TRUE    | TRUE    | TRUE    |
  // | FALSE   | TRUE    | FALSE   | UNKNOWN |
  // | UNKNOWN | TRUE    | UNKNOWN | UNKNOWN |
  // +---------+---------+---------+---------+
  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    if (input1 == true) {
      true
    } else {
      val input2 = right.eval(input)
      if (input2 == true) {
        true
      } else {
        if (input1 != null && input2 != null) {
          false
        } else {
          null
        }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)

    // The result should be `true`, if any of them is `true` whenever the other is null or not.

    // place generated code of eval1 and eval2 in separate methods if their code combined is large
    val combinedLength = eval1.code.length + eval2.code.length
    if (combinedLength > 1024 &&
      // Split these expressions only if they are created from a row object
      (ctx.INPUT_ROW != null && ctx.currentVars == null)) {

      val (eval1FuncName, eval1GlobalIsNull, eval1GlobalValue) =
        ctx.createAndAddFunction(eval1, BooleanType, "eval1Expr")
      val (eval2FuncName, eval2GlobalIsNull, eval2GlobalValue) =
        ctx.createAndAddFunction(eval2, BooleanType, "eval2Expr")
      if (!left.nullable && !right.nullable) {
        val generatedCode = s"""
         $eval1FuncName(${ctx.INPUT_ROW});
         boolean ${ev.value} = true;
         if (!${eval1GlobalValue}) {
           $eval2FuncName(${ctx.INPUT_ROW});
           ${ev.value} = ${eval2GlobalValue};
         }
       """
        ev.copy(code = generatedCode, isNull = "false")
      } else {
        val generatedCode = s"""
         $eval1FuncName(${ctx.INPUT_ROW});
         boolean ${ev.isNull} = false;
         boolean ${ev.value} = true;
         if (!${eval1GlobalIsNull} && ${eval1GlobalValue}) {
         } else {
           $eval2FuncName(${ctx.INPUT_ROW});
           if (!${eval2GlobalIsNull} && ${eval2GlobalValue}) {
           } else if (!${eval1GlobalIsNull} && !${eval2GlobalIsNull}) {
             ${ev.value} = false;
           } else {
             ${ev.isNull} = true;
           }
         }
       """
        ev.copy(code = generatedCode)
      }
    } else if (!left.nullable && !right.nullable) {
      ev.isNull = "false"
      ev.copy(code = s"""
        ${eval1.code}
        boolean ${ev.value} = true;

        if (!${eval1.value}) {
          ${eval2.code}
          ${ev.value} = ${eval2.value};
        }""", isNull = "false")
    } else {
      ev.copy(code = s"""
        ${eval1.code}
        boolean ${ev.isNull} = false;
        boolean ${ev.value} = true;

        if (!${eval1.isNull} && ${eval1.value}) {
        } else {
          ${eval2.code}
          if (!${eval2.isNull} && ${eval2.value}) {
          } else if (!${eval1.isNull} && !${eval2.isNull}) {
            ${ev.value} = false;
          } else {
            ${ev.isNull} = true;
          }
        }
      """)
    }
  }
}


abstract class BinaryComparison extends BinaryOperator with Predicate {

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (ctx.isPrimitiveType(left.dataType)
        && left.dataType != BooleanType // java boolean doesn't support > or < operator
        && left.dataType != FloatType
        && left.dataType != DoubleType) {
      // faster version
      defineCodeGen(ctx, ev, (c1, c2) => s"$c1 $symbol $c2")
    } else {
      defineCodeGen(ctx, ev, (c1, c2) => s"${ctx.genComp(left.dataType, c1, c2)} $symbol 0")
    }
  }

  protected lazy val ordering = TypeUtils.getInterpretedOrdering(left.dataType)
}


object BinaryComparison {
  def unapply(e: BinaryComparison): Option[(Expression, Expression)] = Some((e.left, e.right))
}


/** An extractor that matches both standard 3VL equality and null-safe equality. */
object Equality {
  def unapply(e: BinaryComparison): Option[(Expression, Expression)] = e match {
    case EqualTo(l, r) => Some((l, r))
    case EqualNullSafe(l, r) => Some((l, r))
    case _ => None
  }
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns true if `expr1` equals `expr2`, or false otherwise.")
case class EqualTo(left: Expression, right: Expression)
    extends BinaryComparison with NullIntolerant {

  override def inputType: AbstractDataType = AnyDataType

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes() match {
      case TypeCheckResult.TypeCheckSuccess =>
        // TODO: although map type is not orderable, technically map type should be able to be used
        // in equality comparison, remove this type check once we support it.
        if (left.dataType.existsRecursively(_.isInstanceOf[MapType])) {
          TypeCheckResult.TypeCheckFailure("Cannot use map type in EqualTo, but the actual " +
            s"input type is ${left.dataType.catalogString}.")
        } else {
          TypeCheckResult.TypeCheckSuccess
        }
      case failure => failure
    }
  }

  override def symbol: String = "="

  // +---------+---------+---------+---------+
  // | =       | TRUE    | FALSE   | UNKNOWN |
  // +---------+---------+---------+---------+
  // | TRUE    | TRUE    | FALSE   | UNKNOWN |
  // | FALSE   | FALSE   | TRUE    | UNKNOWN |
  // | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |
  // +---------+---------+---------+---------+
  protected override def nullSafeEval(left: Any, right: Any): Any = ordering.equiv(left, right)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) => ctx.genEqual(left.dataType, c1, c2))
  }
}

@ExpressionDescription(
  usage = """
    expr1 _FUNC_ expr2 - Returns same result as the EQUAL(=) operator for non-null operands,
      but returns true if both are null, false if one of the them is null.
  """)
case class EqualNullSafe(left: Expression, right: Expression) extends BinaryComparison {

  override def inputType: AbstractDataType = AnyDataType

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes() match {
      case TypeCheckResult.TypeCheckSuccess =>
        // TODO: although map type is not orderable, technically map type should be able to be used
        // in equality comparison, remove this type check once we support it.
        if (left.dataType.existsRecursively(_.isInstanceOf[MapType])) {
          TypeCheckResult.TypeCheckFailure("Cannot use map type in EqualNullSafe, but the actual " +
            s"input type is ${left.dataType.catalogString}.")
        } else {
          TypeCheckResult.TypeCheckSuccess
        }
      case failure => failure
    }
  }

  override def symbol: String = "<=>"

  override def nullable: Boolean = false

  // +---------+---------+---------+---------+
  // | <=>     | TRUE    | FALSE   | UNKNOWN |
  // +---------+---------+---------+---------+
  // | TRUE    | TRUE    | FALSE   | FALSE   |
  // | FALSE   | FALSE   | TRUE    | FALSE   |
  // | UNKNOWN | FALSE   | FALSE   | TRUE    |
  // +---------+---------+---------+---------+
  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    val input2 = right.eval(input)
    if (input1 == null && input2 == null) {
      true
    } else if (input1 == null || input2 == null) {
      false
    } else {
      ordering.equiv(input1, input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val equalCode = ctx.genEqual(left.dataType, eval1.value, eval2.value)
    ev.copy(code = eval1.code + eval2.code + s"""
        boolean ${ev.value} = (${eval1.isNull} && ${eval2.isNull}) ||
           (!${eval1.isNull} && !${eval2.isNull} && $equalCode);""", isNull = "false")
  }
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns true if `expr1` is less than `expr2`.")
case class LessThan(left: Expression, right: Expression)
    extends BinaryComparison with NullIntolerant {

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def symbol: String = "<"

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.lt(input1, input2)
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns true if `expr1` is less than or equal to `expr2`.")
case class LessThanOrEqual(left: Expression, right: Expression)
    extends BinaryComparison with NullIntolerant {

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def symbol: String = "<="

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.lteq(input1, input2)
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns true if `expr1` is greater than `expr2`.")
case class GreaterThan(left: Expression, right: Expression)
    extends BinaryComparison with NullIntolerant {

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def symbol: String = ">"

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.gt(input1, input2)
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns true if `expr1` is greater than or equal to `expr2`.")
case class GreaterThanOrEqual(left: Expression, right: Expression)
    extends BinaryComparison with NullIntolerant {

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def symbol: String = ">="

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.gteq(input1, input2)
}
