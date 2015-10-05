package examples

object gadts extends App {

  abstract class Term[T]
  case class Lit(x: Int) extends Term[Int]
  case class Succ(t: Term[Int]) extends Term[Int]
  case class IsZero(t: Term[Int]) extends Term[Boolean]
  case class If[T](c: Term[Boolean],
                   t1: Term[T],
                   t2: Term[T]) extends Term[T]

  def eval[T](t: Term[T]): T = t match {
    case Lit(n)        => n
    case Succ(u)       => eval(u) + 1
    case IsZero(u)     => eval(u) == 0
    case If(c, u1, u2) => eval(if (eval(c)) u1 else u2)
  }
  println(
    eval(If(IsZero(Lit(1)), Lit(41), Succ(Lit(41)))))
}

