package org.apache.spark.sql
package catalyst
package plans
package logical

import expressions._

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 */
case class ScriptTransformation(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {
  def references = input.flatMap(_.references).toSet
}
