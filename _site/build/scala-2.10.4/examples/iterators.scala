package examples

object iterators {

  def Array(elems: Double*): Array[Double] = {
    val ar = new Array[Double](elems.length)
    for (i <- 0 until elems.length)
      ar(i) = elems(i)
    ar
  }

  def printArray(xs: Array[Double]) =
    xs.iterator foreach { x => println(x) }

  def findGreater(xs: Array[Double], limit: Double) =
    xs.iterator
      .zip(Iterator.from(0))
      .filter{case Pair(x, i) => x > limit }
      .map{case Pair(x, i) => i}

  def main(args: Array[String]) {
    val ar = Array/*[Double]*/(6, 2, 8, 5, 1)
    printArray(ar)
    println("Elements greater than 3.0:")
    findGreater(ar, 3.0) foreach { x => println(ar(x)) }
  }

}
