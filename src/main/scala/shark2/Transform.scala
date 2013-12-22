package catalyst
package shark2

import catalyst.expressions._
import shark.SharkContext

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 */
case class Transform(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: SharkPlan)(@transient sc: SharkContext) extends UnaryNode {
  override def otherCopyArgs = sc :: Nil

  def execute() = ???
}