package catalyst
package examples

import plans.logical.LocalRelation

import execution.TestShark._
import dsl._

object SchemaRddExample {
  def main(args: Array[String]): Unit = {
    // Create an RDD with schema (data STRING, message STRING) and load some sample data into it.
    // Acceptable base data includes Seq[Any] or Seq[TupleN].
    val testLogs = LocalRelation('date.string, 'message.string).loadData(
      ("12/1/2013", "INFO: blah blah") ::
      ("12/2/2013", "WARN: blah blah") :: Nil
    )

    val dateRegEx = "(\\d+)\\/(\\d+)\\/(\\d+)".r
    /**
     * Example using the symbol based API.  In this example, the attribute names that are passed to
     * the first constructor are resolved during catalyst's analysis phase.  Then at runtime only
     * the requested attributes are passed to the UDF.  Since this analysis occurs at runtime,
     * the developer must manually annotate their function with the correct argument types.
     */
    val filtered = testLogs.filter('date) { case dateRegEx(_,day,_) => day.toInt == 1 }
    filtered.toRdd.collect.foreach(println)


    /**
     * Example using the dynamic, ORM-Style API.  Using this API the developer is passed a dynamic
     * row containing all of the attributes from the child operator.  Method calls to the dynamic
     * row result in run-time look-ups for the requested column.
     *
     * Essentially, this means that the call row.attrName is translated by the scala compiler to
     * row.selectDynamic("attrName").  Note that, in this instance, the requested attribute is
     * being resolved at runtime.  Thus, we cannot return typed results.  As such all dynamic calls
     * always return strings.
     */
    val filtered2 = testLogs.filter( _.date match { case dateRegEx(_,day,_) => day.toInt == 1 } )
    filtered2.toRdd.collect.foreach(println)
  }
}