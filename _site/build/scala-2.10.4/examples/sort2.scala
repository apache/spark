package examples

object sort2 {

  def sort(a: List[Int]): List[Int] = {
    if (a.length < 2)
      a
    else {
      val pivot = a(a.length / 2)
      def lePivot(x: Int) = x < pivot
      def gtPivot(x: Int) = x > pivot
      def eqPivot(x: Int) = x == pivot
      sort(a filter lePivot) :::
           (a filter eqPivot) :::
           sort(a filter gtPivot)
    }
  }

  def main(args: Array[String]) {
    val xs = List(6, 2, 8, 5, 1, 8)
    println(xs)
    println(sort(xs))
  }

}
