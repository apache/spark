package catalyst
package execution

object DebugQuery {
  def apply(plan: SharkPlan): SharkPlan = {
    val visited = new collection.mutable.HashSet[Long]()
    plan transformDown {
      case s: SharkPlan if !visited.contains(s.id) =>
        visited += s.id
        DebugNode(s)
    }
  }
}

case class DebugNode(child: SharkPlan) extends UnaryNode {
  def references = Set.empty
  def output = child.output
  def execute() = {
    val childRdd = child.execute()
    println(
      s"""
        |=========================
        |${child.simpleString}
        |=========================
      """.stripMargin)
    childRdd.foreach(println(_))
    childRdd
  }
}