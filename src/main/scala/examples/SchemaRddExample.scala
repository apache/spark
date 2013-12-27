package catalyst
package examples

import plans.logical.LocalRelation

import shark2.TestShark._
import dsl._

object SchemaRddExample {
  def main(args: Array[String]): Unit = {
    val testLogs = LocalRelation('date.string, 'message.string).loadData(
      ("12/1/2013", "INFO: blah blah") ::
      ("12/2/2013", "WARN: blah blah") :: Nil
    )

    val filtered = testLogs.filter('date)((date: String) => new java.util.Date(date).getDay == 1)

    filtered.toRdd.collect.foreach(println)
  }
}