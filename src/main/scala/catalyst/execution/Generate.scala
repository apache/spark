package catalyst
package execution

import catalyst.expressions._
import catalyst.types._

/**
 * Applies a [[catalyst.expressions.Generator Generator]] to a stream of input rows, combining the
 * output of each into a new stream of rows.  This operation is similar to a `flatMap` in functional
 * programming with one important additional feature, which allows the input rows to be joined with
 * their output.
 * @param join  when true, each output row is implicitly joined with the input tuple that produced
 *              it.
 * @param outer when true, each input row will be output at least once, even if the output of the
 *              given `generator` is empty. `outer` has no effect when `join` is false.
 */
case class Generate(
    generator: Generator,
    join: Boolean,
    outer: Boolean,
    child: SharkPlan)
  extends UnaryNode {

  def output =
    if (join) child.output ++ generator.output else generator.output

  def execute() = {
    if (join) {
      val outerNulls = Seq.fill(generator.output.size)(null)
      child.execute().mapPartitions { iter =>
        iter.flatMap {row =>
          val outputRows = generator(row)
          if (outer && outputRows.isEmpty) {
            new GenericRow(row ++ outerNulls) :: Nil
          } else {
            outputRows.map(or => new GenericRow(row ++ or))
          }
        }
      }
    } else {
      child.execute().mapPartitions(iter => iter.flatMap(generator))
    }
  }
}