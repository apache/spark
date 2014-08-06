package org.apache.spark.sql.catalyst.expressions

import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.dsl.expressions._


class AggregatesSuite extends FunSuite {

  val testRows = Seq(1,1,2,2,3,3,4,4).map(x => {
    val row = new GenericMutableRow(1)
    row(0) = x
    row
  })

  val dataType: DataType = IntegerType

  val exp = BoundReference(0,dataType,true)

  def checkMethod(f:AggregateExpression) = {
    val func = f.newInstance()
    val func1 = f.newInstance()
    val func2 = f.newInstance()
    testRows.map(func.update(_))
    testRows.map(func1.update(_))
    testRows.map(func.update(_))
    testRows.map(func2.update(_))
    func1.merge(func2)
    val r1 = func.eval(EmptyRow)
    val r2 = func1.eval(EmptyRow)
    assert(r1==r2,"test suite failed")
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

  test("approx count distinct merge test") {
    checkMethod(approxCountDistinct(exp))
  }

}
