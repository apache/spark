package examples

import scala.concurrent.{ future, Future }
import scala.concurrent.Await.result
import scala.concurrent.util.Duration

object futures {
  def someLengthyComputation = 1
  def anotherLengthyComputation = 2
  def f(x: Int) = x + x
  def g(x: Int) = x * x

  //f(sLC) + g(aLC) = 6
  def main(args: Array[String]) {
    val d = Duration("1 sec")
    val fgSum =
      for {
        a <- future(someLengthyComputation) map f
        b <- future(anotherLengthyComputation) map g
      } yield (a + b)
    println(result(fgSum, d))

    val alt = List(someLengthyComputation _ -> f _, anotherLengthyComputation _ -> g _)
    val all = Future.traverse(alt)(p => future(p._1()) map p._2)
    println(result(all, d).foldLeft(0)(_ + _))

    val altf = List(
      future(someLengthyComputation) map f,
      future(anotherLengthyComputation) map g
    )
    val res = Future.fold(altf)(0)(_ + _)
    println(result(res, d))
  }
}
