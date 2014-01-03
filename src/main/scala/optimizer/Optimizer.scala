package catalyst
package optimizer

import catalyst.expressions._
import catalyst.expressions.Alias
import catalyst.plans.logical._
import catalyst.plans.logical.Filter
import catalyst.plans.logical.Project
import catalyst.plans.logical.Subquery
import catalyst.rules._
import catalyst.types.{NullType, DataType}

object Optimize extends RuleExecutor[LogicalPlan] {
  val batches =
    Batch("Subqueries", Once,
      EliminateSubqueries,
      CombineFilters,
      PredicatePushDownThoughProject,
      PushPredicateThroughInnerEqualJoin) :: Nil

}

object EliminateSubqueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}

/**
 * Combines two filter operators into one
 */
object CombineFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ff@Filter(fc, nf@Filter(nc, grandChild)) => Filter(And(nc, fc), grandChild)
  }
}

/**
 * Pushes predicate through project, also inlines project's aliases in filter
 */
object PredicatePushDownThoughProject extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter@Filter(condition, project@Project(fields, grandChild)) =>
      val sourceAliases = fields.collect { case a@Alias(c, _) => a.toAttribute -> c }.toMap
      project.copy(child = filter.copy(
        replaceAlias(condition, sourceAliases),
        grandChild))
  }

  // Assuming the expression cost is minimal, we can replace condition with the alias's child to push down predicate
  def replaceAlias(condition: Expression, sourceAliases: Map[Attribute, Expression]): Expression = {
    condition transform {
      case a: AttributeReference => sourceAliases.getOrElse(a, a)
    }
  }
}

object EmptyExpression extends EmptyExpressionType(null, null)

case class EmptyExpressionType(left: Expression, right: Expression) extends Expression {
  self: Product =>

  /** Returns a Seq of the children of this node */

  def children: Seq[Expression] = Seq.empty

  def dataType: DataType = NullType

  def nullable: Boolean = true

  def references: Set[Attribute] = Set.empty
}

/**
 * Pushes predicate through inner join
 */
object PushPredicateThroughInnerEqualJoin extends Rule[LogicalPlan] {
  class BinaryConditionStatus {
    var left = false
    var right = false
  }

  abstract class Direction
  case object Left extends Direction
  case object Right extends Direction

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter@Filter(_, join@Join(l, r, _, _)) => {
      var joinLeftExpression: Option[Expression] = None
      var joinRightExpression: Option[Expression] = None

      val conditions = filter.splitConjunctivePredicates(filter.condition).filter(
        e => {
          var changed = false
          if(e.references subsetOf l.outputSet) {
            joinLeftExpression = combineExpression(joinLeftExpression, Option(e), And)
            changed = true
          }

          if(e.references subsetOf r.outputSet) {
            joinRightExpression = combineExpression(joinRightExpression, Option(e), And)
            changed = true
          }

          changed
        }
      ).toSet

      val newFilterCondition = rewriteCondition (filter.condition, conditions)

      (newFilterCondition, joinLeftExpression, joinRightExpression) match {
        case (EmptyExpression, _, _) =>
          copyJoin(join, joinLeftExpression, joinRightExpression)
        case (_, None, None) => filter
        case _ => {
          filter.copy(condition = newFilterCondition,
            child = copyJoin(
              join, joinLeftExpression, joinRightExpression
            )
          )
        }
      }
    }
  }

  def rewriteCondition(exp: Expression, removedExps: Set[Expression]): Expression = {
    exp match {
      case b: And => b match {
        case And(e1: Expression, e2:Expression) => {
          val r1 = rewriteCondition(e1, removedExps)
          val r2 = rewriteCondition(e2, removedExps)

          (r1, r2) match {
            case (EmptyExpression, e: Expression) => e
            case (e: Expression, EmptyExpression) => e
            case (EmptyExpression, EmptyExpression) => EmptyExpression
            case _ => And(r1, r2)
          }
        }
      }
      case e => {
        if(removedExps.contains(e))
          EmptyExpression
        else
          e
      }
    }
  }

  def combineExpression[T1, T2](firstExp: Option[T1],
                                secondExp: Option[T2],
                                combineFunc: (T1, T2) => T2): Option[T2] = {
    (firstExp, secondExp) match {
      case (Some(o1), Some(o2)) => Option(combineFunc(o1, o2))
      case (None, Some(o2)) => Option(o2)
      case (Some(o1), None) => Option(o1.asInstanceOf[T2])
      case (None, None) => None
    }
  }

  def copyJoin(join: Join,
               joinLeftExpression: Option[Expression],
               joinRightExpression: Option[Expression]): Join = {
    join.copy(
      left =
        combineExpression(
          joinLeftExpression,
          Option(join.left),
          Filter
        ).get,
      right =
        combineExpression(
          joinRightExpression,
          Option(join.right),
          Filter
        ).get
    )
  }

  def joinFilter(status: BinaryConditionStatus,
                 dir: Direction,
                 expression: Expression,
                 outputSet: Set[Attribute]): Option[Expression] = {
    if (expression.references subsetOf outputSet) {
      dir match {
        case Left => status.left = true
        case Right => status.right = true
      }
      Option(expression)
    } else None
  }
}