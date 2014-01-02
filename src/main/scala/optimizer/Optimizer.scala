package catalyst
package optimizer

import catalyst.expressions._
import catalyst.expressions.Alias
import catalyst.plans.Inner
import catalyst.plans.logical._
import catalyst.plans.logical.Filter
import catalyst.plans.logical.Project
import catalyst.plans.logical.Subquery
import catalyst.rules._
import catalyst.types.DataType

object Optimize extends RuleExecutor[LogicalPlan] {
  val batches =
    Batch("Subqueries", Once,
      EliminateSubqueries,
      CombineFilters,
      PredicatePushDown,
      PredicatePushDownWithAlias,
      PushPredicateThroughInnerEqualJoin) :: Nil

}

object EliminateSubqueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}

object CombineFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ff@Filter(fc, nf@Filter(nc, grandChild)) => Filter(And(nc, fc), grandChild)
  }
}

object PredicatePushDown extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter@Filter(_, project@Project(_, grandChild))
      if filter.references subsetOf (grandChild.outputSet) =>
      project.copy(child = filter.copy(child = grandChild))
  }
}

object EmptyExpression extends EmptyExpressionType(null, null)

case class EmptyExpressionType(left: Expression, right: Expression) extends Expression {
  self: Product =>

  case object NullType extends DataType

  /** Returns a Seq of the children of this node */

  def children: Seq[Expression] = Seq.empty

  def dataType: DataType = NullType

  def nullable: Boolean = true

  def references: Set[Attribute] = Set.empty
}

object PushPredicateThroughInnerEqualJoin extends Rule[LogicalPlan] {
  class BinaryConditionStatus {
    var left = false
    var right = false
  }

  object Direction extends Enumeration {
    type Direction = Value
    val Left, Right = Value
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter@Filter(_, join@Join(l, r, joinType@Inner, cond)) => {
      var joinLeftExpression: Option[Expression] = None
      var joinRightExpression: Option[Expression] = None

      val newFilterCondition = rewriteCondition (filter.condition transform {
        case and: And => {
          val status = new BinaryConditionStatus

          val rightFilter =
            combineExpression(
              joinFilter(status, Direction.Left, and.left, r.outputSet),
              joinFilter(status, Direction.Right, and.right, r.outputSet),
              And
            )
          val leftFilter =
            combineExpression(
              joinFilter(status, Direction.Left, and.left, l.outputSet),
              joinFilter(status, Direction.Right, and.right, l.outputSet),
              And
            )

          joinLeftExpression = combineExpression(joinLeftExpression, leftFilter, And)
          joinRightExpression = combineExpression(joinRightExpression, rightFilter, And)

          (status.left, status.right) match {
            case (true, true) => EmptyExpression
            case (true, false) => and.right
            case (false, true) => and.left
            case (false, false) => and
          }
        }

        case u: BinaryExpression if u.references subsetOf (l.outputSet) => {
          joinLeftExpression = combineExpression(joinLeftExpression, Option(u), And)
          EmptyExpression
        }

        case u: BinaryExpression if u.references subsetOf (r.outputSet) => {
          joinRightExpression = combineExpression(joinRightExpression, Option(u), And)
          EmptyExpression
        }
      })

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

  def rewriteCondition(exp: Expression): Expression = {
    exp match {
      case b: And => b match {
        case And(EmptyExpression, e:Expression) => rewriteCondition(e)
        case And(e:Expression, EmptyExpression) => rewriteCondition(e)
        case And(e:Expression, e2:Expression) => And(rewriteCondition(e), rewriteCondition(e2))
        case And(EmptyExpression, EmptyExpression) => EmptyExpression
      }
      case _ => exp
    }
  }

  def combineExpression[T1, T2](firstExp: Option[T1],
                                secondExp: Option[T2],
                                combineFunc: (T1, T2) => T2): Option[T2] = {
    (firstExp, secondExp) match {
      case (o1: Some[T1], o2: Some[T2]) => Option(combineFunc(o1.get, o2.get))
      case (None, o2: Some[T2]) => o2
      case (o1: Some[T1], None) => Option(o1.get.asInstanceOf[T2])
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
                 dir: Direction.Value,
                 expression: Expression,
                 outputSet: Set[Attribute]): Option[Expression] = {
    if (expression.references subsetOf (outputSet)) {
      dir match {
        case Direction.Left => status.left = true
        case Direction.Right => status.right = true
      }
      Option(expression)
    } else None
  }
}

object PredicatePushDownWithAlias extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter@Filter(condition, project@Project(fields, grandChild)) =>
      val refMissing = filter.references &~ (filter.references intersect grandChild.outputSet)
      val sourceAliases = fields.filter(_.isInstanceOf[Alias]).map(_.asInstanceOf[Alias]).toSet
      if (isAliasOf(refMissing, sourceAliases))
        project.copy(child = filter.copy(
          replaceAlias(condition, sourceAliases),
          grandChild))
      else filter
  }

  def isAliasOf(refMissing: Set[Attribute], sourceAliases: Set[Alias]): Boolean = {
    refMissing.forall(r => sourceAliases.exists(_.exprId == r.exprId))
  }

  // Assuming the expression cost is minimal, we can replace condition with the alias's child to push down predicate
  def replaceAlias(condition: Expression, sourceAliases: Set[Alias]): Expression = {
    condition transform {
      case a: AttributeReference if sourceAliases.exists(_.exprId == a.exprId) =>
        sourceAliases.find(_.exprId == a.exprId).get.child
    }
  }
}