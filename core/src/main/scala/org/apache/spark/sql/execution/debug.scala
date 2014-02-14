package org.apache.spark.sql
package execution

object DebugQuery {
  def apply(plan: SparkPlan): SparkPlan = {
    val visited = new collection.mutable.HashSet[Long]()
    plan transform {
      case s: SparkPlan if !visited.contains(s.id) =>
        visited += s.id
        DebugNode(s)
    }
  }
}

case class DebugNode(child: SparkPlan) extends UnaryNode {
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
