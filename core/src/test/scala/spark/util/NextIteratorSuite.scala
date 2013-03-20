package spark.util

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import scala.collection.mutable.Buffer
import java.util.NoSuchElementException

class NextIteratorSuite extends FunSuite with ShouldMatchers {
  test("one iteration") {
    val i = new StubIterator(Buffer(1))
    i.hasNext should be === true
    i.next should be === 1
    i.hasNext should be === false
    intercept[NoSuchElementException] { i.next() }
  }
  
  test("two iterations") {
    val i = new StubIterator(Buffer(1, 2))
    i.hasNext should be === true
    i.next should be === 1
    i.hasNext should be === true
    i.next should be === 2
    i.hasNext should be === false
    intercept[NoSuchElementException] { i.next() }
  }

  test("empty iteration") {
    val i = new StubIterator(Buffer())
    i.hasNext should be === false
    intercept[NoSuchElementException] { i.next() }
  }

  test("close is called once for empty iterations") {
    val i = new StubIterator(Buffer())
    i.hasNext should be === false
    i.hasNext should be === false
    i.closeCalled should be === 1
  }

  test("close is called once for non-empty iterations") {
    val i = new StubIterator(Buffer(1, 2))
    i.next should be === 1
    i.next should be === 2
    // close isn't called until we check for the next element
    i.closeCalled should be === 0
    i.hasNext should be === false
    i.closeCalled should be === 1
    i.hasNext should be === false
    i.closeCalled should be === 1
  }

  class StubIterator(ints: Buffer[Int])  extends NextIterator[Int] {
    var closeCalled = 0
    
    override def getNext() = {
      if (ints.size == 0) {
        finished = true
        0
      } else {
        ints.remove(0)
      }
    }

    override def close() {
      closeCalled += 1
    }
  }
}
