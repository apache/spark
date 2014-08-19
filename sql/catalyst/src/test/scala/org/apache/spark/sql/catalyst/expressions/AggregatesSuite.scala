package org.apache.spark.sql.catalyst.expressions

import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.dsl.expressions._


class AggregatesSuite extends FunSuite {

  val testRows = Seq(1, 1, 2, 2, 3, 3, 4, 4).map(x => {
    val row = new GenericMutableRow(1)
    row(0) = x
    row
  })

  val dataType: DataType = IntegerType
  val exp = BoundReference(0, dataType, true)

  /**
   * ensure whether all merge functions in aggregates have correct output
   * according to the output between update and merge
   */
  def checkMethod(f: AggregateExpression) = {
    val combiner = f.newInstance()
    val combiner1 = f.newInstance()
    val combiner2 = f.newInstance()

    //merge each row of testRow twice into combiner
    testRows.map(combiner.update(_))
    testRows.map(combiner.update(_))

    //merge each row of testRow into combiner1
    testRows.map(combiner1.update(_))

    //merge each row of testRow into combiner2
    testRows.map(combiner2.update(_))

    //merge combiner1 and combiner2 into combiner1
    combiner1.merge(combiner2)

    val r1 = combiner.eval(EmptyRow)
    val r2 = combiner1.eval(EmptyRow)

    //check the output between the up two ways
    assert(r1 == r2, "test suite failed")
  }

  test("max merge test") {
    checkMethod(max(exp))
  }

  test("min merge test") {
    checkMethod(min(exp))
  }

  test("sum merge test") {
    checkMethod(sum(exp))
  }

  test("count merge test") {
    checkMethod(count(exp))
  }

  test("avg merge test") {
    checkMethod(avg(exp))
  }

  test("sum distinct merge test") {
    checkMethod(sumDistinct(exp))
  }

  test("count distinct merge test") {
    checkMethod(countDistinct(exp))
  }

  test("first merge test") {
    checkMethod(first(exp))
  }

  //this test case seems wrong
  //it does not check ApproxCountDistinctPartitionFunction and ApproxCountDistinctMergeFunction
  test("approx count distinct merge test") {
    checkMethod(approxCountDistinct(exp))
  }

}
