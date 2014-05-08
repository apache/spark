package org.apache.spark.mllib.util

import org.scalatest.FunSuite
import scala.collection.mutable.ListBuffer

class NumericParserSuite extends FunSuite {

  test("tokenizer") {
    val s = "((1,2),4,[5,6],8)"
    val tokenizer = new NumericTokenizer(s)
    var token = tokenizer.next()
    val tokens = ListBuffer.empty[Any]
    while (token != NumericTokenizer.END) {
      token match {
        case NumericTokenizer.NUMBER =>
          tokens.append(tokenizer.value)
        case other =>
          tokens.append(token)
      }
      token = tokenizer.next()
    }
    val expected = Seq('(', '(', 1.0, 2.0, ')', 4.0, '[', 5.0, 6.0, ']', 8.0, ')')
    assert(expected === tokens)
  }

  test("parser") {
    val s = "((1,2),4,[5,6],8)"
    val parsed = NumericParser.parse(s).asInstanceOf[Seq[_]]
    assert(parsed(0).asInstanceOf[Seq[_]] === Seq(1.0, 2.0))
    assert(parsed(1).asInstanceOf[Double] === 4.0)
    assert(parsed(2).asInstanceOf[Array[Double]] === Array(5.0, 6.0))
    assert(parsed(3).asInstanceOf[Double] === 8.0)
  }
}
