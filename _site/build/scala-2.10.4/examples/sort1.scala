package examples

object sort1 {

  def sort(a: List[Int]): List[Int] = {
    if (a.length < 2)
      a
    else {
      val pivot = a(a.length / 2)
      sort(a.filter(x => x < pivot)) :::
           a.filter(x => x == pivot) :::
           sort(a.filter(x => x > pivot))
    }
  }

  def main(args: Array[String]) {
    val xs = List(6, 2, 8, 5, 1)
    println(xs)
    println(sort(xs))
  }

}
