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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral, GenerateSafeProjection, GenerateUnsafeProjection, Predicate => BasePredicate}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
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

  override def initialize(partitionIndex: Int): Unit = {
    super.initialize(partitionIndex)
    expression.foreach {
      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    }
  }
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
 * Evaluates to `true` if `values` are returned in `query`'s result set.
 */
case class InSubquery(values: Seq[Expression], query: ListQuery)
  extends Predicate with Unevaluable {

  @transient lazy val value: Expression = if (values.length > 1) {
    CreateNamedStruct(values.zipWithIndex.flatMap {
      case (v: NamedExpression, _) => Seq(Literal(v.name), v)
      case (v, idx) => Seq(Literal(s"_$idx"), v)
    })
  } else {
    values.head
  }


  override def checkInputDataTypes(): TypeCheckResult = {
    val mismatchOpt = !DataType.equalsStructurally(query.dataType, value.dataType,
      ignoreNullability = true)
    if (mismatchOpt) {
      if (values.length != query.childOutputs.length) {
        TypeCheckResult.TypeCheckFailure(
          s"""
             |The number of columns in the left hand side of an IN subquery does not match the
             |number of columns in the output of subquery.
             |#columns in left hand side: ${values.length}.
             |#columns in right hand side: ${query.childOutputs.length}.
             |Left side columns:
             |[${values.map(_.sql).mkString(", ")}].
             |Right side columns:
             |[${query.childOutputs.map(_.sql).mkString(", ")}].""".stripMargin)
      } else {
        val mismatchedColumns = values.zip(query.childOutputs).flatMap {
          case (l, r) if l.dataType != r.dataType =>
            Seq(s"(${l.sql}:${l.dataType.catalogString}, ${r.sql}:${r.dataType.catalogString})")
          case _ => None
        }
        TypeCheckResult.TypeCheckFailure(
          s"""
             |The data type of one or more elements in the left hand side of an IN subquery
             |is not compatible with the data type of the output of the subquery
             |Mismatched columns:
             |[${mismatchedColumns.mkString(", ")}]
             |Left side:
             |[${values.map(_.dataType.catalogString).mkString(", ")}].
             |Right side:
             |[${query.childOutputs.map(_.dataType.catalogString).mkString(", ")}].""".stripMargin)
      }
    } else {
      TypeUtils.checkForOrderingExpr(value.dataType, s"function $prettyName")
    }
  }

  override def children: Seq[Expression] = values :+ query
  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)
  override def toString: String = s"$value IN ($query)"
  override def sql: String = s"(${value.sql} IN (${query.sql}))"
}


/**
 * Evaluates to `true` if `list` contains `value`.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "expr1 _FUNC_(expr2, expr3, ...) - Returns true if `expr` equals to any valN.",
  arguments = """
    Arguments:
      * expr1, expr2, expr3, ... - the arguments must be same type.
  """,
  examples = """
    Examples:
      > SELECT 1 _FUNC_(1, 2, 3);
       true
      > SELECT 1 _FUNC_(2, 3, 4);
       false
      > SELECT named_struct('a', 1, 'b', 2) _FUNC_(named_struct('a', 1, 'b', 1), named_struct('a', 1, 'b', 3));
       false
      > SELECT named_struct('a', 1, 'b', 2) _FUNC_(named_struct('a', 1, 'b', 2), named_struct('a', 1, 'b', 3));
       true
  """)
// scalastyle:on line.size.limit
case class In(value: Expression, list: Seq[Expression]) extends Predicate {

  require(list != null, "list should not be null")

  override def checkInputDataTypes(): TypeCheckResult = {
    val mismatchOpt = list.find(l => !DataType.equalsStructurally(l.dataType, value.dataType,
      ignoreNullability = true))
    if (mismatchOpt.isDefined) {
      TypeCheckResult.TypeCheckFailure(s"Arguments must be same type but were: " +
        s"${value.dataType.catalogString} != ${mismatchOpt.get.dataType.catalogString}")
    } else {
      TypeUtils.checkForOrderingExpr(value.dataType, s"function $prettyName")
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
    val javaDataType = CodeGenerator.javaType(value.dataType)
    val valueGen = value.genCode(ctx)
    val listGen = list.map(_.genCode(ctx))
    // inTmpResult has 3 possible values:
    // -1 means no matches found and there is at least one value in the list evaluated to null
    val HAS_NULL = -1
    // 0 means no matches found and all values in the list are not null
    val NOT_MATCHED = 0
    // 1 means one value in the list is matched
    val MATCHED = 1
    val tmpResult = ctx.freshName("inTmpResult")
    val valueArg = ctx.freshName("valueArg")
    // All the blocks are meant to be inside a do { ... } while (false); loop.
    // The evaluation of variables can be stopped when we find a matching value.
    val listCode = listGen.map(x =>
      s"""
         |${x.code}
         |if (${x.isNull}) {
         |  $tmpResult = $HAS_NULL; // ${ev.isNull} = true;
         |} else if (${ctx.genEqual(value.dataType, valueArg, x.value)}) {
         |  $tmpResult = $MATCHED; // ${ev.isNull} = false; ${ev.value} = true;
         |  continue;
         |}
       """.stripMargin)

    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = listCode,
      funcName = "valueIn",
      extraArguments = (javaDataType, valueArg) :: (CodeGenerator.JAVA_BYTE, tmpResult) :: Nil,
      returnType = CodeGenerator.JAVA_BYTE,
      makeSplitFunction = body =>
        s"""
           |do {
           |  $body
           |} while (false);
           |return $tmpResult;
         """.stripMargin,
      foldFunctions = _.map { funcCall =>
        s"""
           |$tmpResult = $funcCall;
           |if ($tmpResult == $MATCHED) {
           |  continue;
           |}
         """.stripMargin
      }.mkString("\n"))

    ev.copy(code =
      code"""
         |${valueGen.code}
         |byte $tmpResult = $HAS_NULL;
         |if (!${valueGen.isNull}) {
         |  $tmpResult = $NOT_MATCHED;
         |  $javaDataType $valueArg = ${valueGen.value};
         |  do {
         |    $codes
         |  } while (false);
         |}
         |final boolean ${ev.isNull} = ($tmpResult == $HAS_NULL);
         |final boolean ${ev.value} = ($tmpResult == $MATCHED);
       """.stripMargin)
  }

  override def sql: String = {
    val valueSQL = value.sql
    val listSQL = list.map(_.sql).mkString(", ")
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

  @transient lazy val set: Set[Any] = child.dataType match {
    case _: AtomicType => hset
    case _: NullType => hset
    case _ =>
      // for structs use interpreted ordering to be able to compare UnsafeRows with non-UnsafeRows
      TreeSet.empty(TypeUtils.getInterpretedOrdering(child.dataType)) ++ hset
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val setTerm = ctx.addReferenceObj("set", set)
    val childGen = child.genCode(ctx)
    val setIsNull = if (hasNull) {
      s"${ev.isNull} = !${ev.value};"
    } else {
      ""
    }
    ev.copy(code =
      code"""
         |${childGen.code}
         |${CodeGenerator.JAVA_BOOLEAN} ${ev.isNull} = ${childGen.isNull};
         |${CodeGenerator.JAVA_BOOLEAN} ${ev.value} = false;
         |if (!${ev.isNull}) {
         |  ${ev.value} = $setTerm.contains(${childGen.value});
         |  $setIsNull
         |}
       """.stripMargin)
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
    if (!left.nullable && !right.nullable) {
      ev.copy(code = code"""
        ${eval1.code}
        boolean ${ev.value} = false;

        if (${eval1.value}) {
          ${eval2.code}
          ${ev.value} = ${eval2.value};
        }""", isNull = FalseLiteral)
    } else {
      ev.copy(code = code"""
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
    if (!left.nullable && !right.nullable) {
      ev.isNull = FalseLiteral
      ev.copy(code = code"""
        ${eval1.code}
        boolean ${ev.value} = true;

        if (!${eval1.value}) {
          ${eval2.code}
          ${ev.value} = ${eval2.value};
        }""", isNull = FalseLiteral)
    } else {
      ev.copy(code = code"""
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

  // Note that we need to give a superset of allowable input types since orderable types are not
  // finitely enumerable. The allowable types are checked below by checkInputDataTypes.
  override def inputType: AbstractDataType = AnyDataType

  override def checkInputDataTypes(): TypeCheckResult = super.checkInputDataTypes() match {
    case TypeCheckResult.TypeCheckSuccess =>
      TypeUtils.checkForOrderingExpr(left.dataType, this.getClass.getSimpleName)
    case failure => failure
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (CodeGenerator.isPrimitiveType(left.dataType)
        && left.dataType != BooleanType // java boolean doesn't support > or < operator
        && left.dataType != FloatType
        && left.dataType != DoubleType) {
      // faster version
      defineCodeGen(ctx, ev, (c1, c2) => s"$c1 $symbol $c2")
    } else {
      defineCodeGen(ctx, ev, (c1, c2) => s"${ctx.genComp(left.dataType, c1, c2)} $symbol 0")
    }
  }

  protected lazy val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
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

// TODO: although map type is not orderable, technically map type should be able to be used
// in equality comparison
@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns true if `expr1` equals `expr2`, or false otherwise.",
  arguments = """
    Arguments:
      * expr1, expr2 - the two expressions must be same type or can be casted to a common type,
          and must be a type that can be used in equality comparison. Map type is not supported.
          For complex types such array/struct, the data types of fields must be orderable.
  """,
  examples = """
    Examples:
      > SELECT 2 _FUNC_ 2;
       true
      > SELECT 1 _FUNC_ '1';
       true
      > SELECT true _FUNC_ NULL;
       NULL
      > SELECT NULL _FUNC_ NULL;
       NULL
  """)
case class EqualTo(left: Expression, right: Expression)
    extends BinaryComparison with NullIntolerant {

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

// TODO: although map type is not orderable, technically map type should be able to be used
// in equality comparison
@ExpressionDescription(
  usage = """
    expr1 _FUNC_ expr2 - Returns same result as the EQUAL(=) operator for non-null operands,
      but returns true if both are null, false if one of the them is null.
  """,
  arguments = """
    Arguments:
      * expr1, expr2 - the two expressions must be same type or can be casted to a common type,
          and must be a type that can be used in equality comparison. Map type is not supported.
          For complex types such array/struct, the data types of fields must be orderable.
  """,
  examples = """
    Examples:
      > SELECT 2 _FUNC_ 2;
       true
      > SELECT 1 _FUNC_ '1';
       true
      > SELECT true _FUNC_ NULL;
       false
      > SELECT NULL _FUNC_ NULL;
       true
  """)
case class EqualNullSafe(left: Expression, right: Expression) extends BinaryComparison {

  override def symbol: String = "<=>"

  override def nullable: Boolean = false

  // +---------+---------+---------+---------+
  // | <=>     | TRUE    | FALSE   | UNKNOWN |
  // +---------+---------+---------+---------+
  // | TRUE    | TRUE    | FALSE   | UNKNOWN |
  // | FALSE   | FALSE   | TRUE    | UNKNOWN |
  // | UNKNOWN | UNKNOWN | UNKNOWN | TRUE    |
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
    ev.copy(code = eval1.code + eval2.code + code"""
        boolean ${ev.value} = (${eval1.isNull} && ${eval2.isNull}) ||
           (!${eval1.isNull} && !${eval2.isNull} && $equalCode);""", isNull = FalseLiteral)
  }
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns true if `expr1` is less than `expr2`.",
  arguments = """
    Arguments:
      * expr1, expr2 - the two expressions must be same type or can be casted to a common type,
          and must be a type that can be ordered. For example, map type is not orderable, so it
          is not supported. For complex types such array/struct, the data types of fields must
          be orderable.
  """,
  examples = """
    Examples:
      > SELECT 1 _FUNC_ 2;
       true
      > SELECT 1.1 _FUNC_ '1';
       false
      > SELECT to_date('2009-07-30 04:17:52') _FUNC_ to_date('2009-07-30 04:17:52');
       false
      > SELECT to_date('2009-07-30 04:17:52') _FUNC_ to_date('2009-08-01 04:17:52');
       true
      > SELECT 1 _FUNC_ NULL;
       NULL
  """)
case class LessThan(left: Expression, right: Expression)
    extends BinaryComparison with NullIntolerant {

  override def symbol: String = "<"

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.lt(input1, input2)
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns true if `expr1` is less than or equal to `expr2`.",
  arguments = """
    Arguments:
      * expr1, expr2 - the two expressions must be same type or can be casted to a common type,
          and must be a type that can be ordered. For example, map type is not orderable, so it
          is not supported. For complex types such array/struct, the data types of fields must
          be orderable.
  """,
  examples = """
    Examples:
      > SELECT 2 _FUNC_ 2;
       true
      > SELECT 1.0 _FUNC_ '1';
       true
      > SELECT to_date('2009-07-30 04:17:52') _FUNC_ to_date('2009-07-30 04:17:52');
       true
      > SELECT to_date('2009-07-30 04:17:52') _FUNC_ to_date('2009-08-01 04:17:52');
       true
      > SELECT 1 _FUNC_ NULL;
       NULL
  """)
case class LessThanOrEqual(left: Expression, right: Expression)
    extends BinaryComparison with NullIntolerant {

  override def symbol: String = "<="

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.lteq(input1, input2)
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns true if `expr1` is greater than `expr2`.",
  arguments = """
    Arguments:
      * expr1, expr2 - the two expressions must be same type or can be casted to a common type,
          and must be a type that can be ordered. For example, map type is not orderable, so it
          is not supported. For complex types such array/struct, the data types of fields must
          be orderable.
  """,
  examples = """
    Examples:
      > SELECT 2 _FUNC_ 1;
       true
      > SELECT 2 _FUNC_ '1.1';
       true
      > SELECT to_date('2009-07-30 04:17:52') _FUNC_ to_date('2009-07-30 04:17:52');
       false
      > SELECT to_date('2009-07-30 04:17:52') _FUNC_ to_date('2009-08-01 04:17:52');
       false
      > SELECT 1 _FUNC_ NULL;
       NULL
  """)
case class GreaterThan(left: Expression, right: Expression)
    extends BinaryComparison with NullIntolerant {

  override def symbol: String = ">"

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.gt(input1, input2)
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns true if `expr1` is greater than or equal to `expr2`.",
  arguments = """
    Arguments:
      * expr1, expr2 - the two expressions must be same type or can be casted to a common type,
          and must be a type that can be ordered. For example, map type is not orderable, so it
          is not supported. For complex types such array/struct, the data types of fields must
          be orderable.
  """,
  examples = """
    Examples:
      > SELECT 2 _FUNC_ 1;
       true
      > SELECT 2.0 _FUNC_ '2.1';
       false
      > SELECT to_date('2009-07-30 04:17:52') _FUNC_ to_date('2009-07-30 04:17:52');
       true
      > SELECT to_date('2009-07-30 04:17:52') _FUNC_ to_date('2009-08-01 04:17:52');
       false
      > SELECT 1 _FUNC_ NULL;
       NULL
  """)
case class GreaterThanOrEqual(left: Expression, right: Expression)
    extends BinaryComparison with NullIntolerant {

  override def symbol: String = ">="

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.gteq(input1, input2)
}
