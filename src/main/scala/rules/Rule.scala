package catalyst
package rules

import plans._

abstract class Rule[PlanType <: QueryPlan[_]] {
  val name = {
    val className = getClass.getName
    if(className endsWith "$")
      className.dropRight(1)
    else
      className
  }

  def apply(plan: PlanType): PlanType
}