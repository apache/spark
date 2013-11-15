package catalyst
package plans
package logical

case class InsertIntoHiveTable(hiveTable: String, child: LogicalPlan) extends UnaryNode {
  def references = Set.empty
  def output = Seq.empty
}