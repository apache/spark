package catalyst
package plans
package logical

import expressions._

case class Project(projectList: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode