
package examples.monads

object directInterpreter {

  type Name = String

  trait Term
  case class Var(x: Name) extends Term
  case class Con(n: Int) extends Term
  case class Add(l: Term, r: Term) extends Term
  case class Lam(x: Name, body: Term) extends Term
  case class App(fun: Term, arg: Term) extends Term

  trait Value
  case object Wrong extends Value
  case class Num(n: Int) extends Value
  case class Fun(f: Value => Value)extends Value

  def showval(v: Value): String = v match {
    case Wrong => "<wrong>"
    case Num(n) => n.toString()
    case Fun(f) => "<function>"
  }

  type Environment = List[Pair[Name, Value]]

  def lookup(x: Name, e: Environment): Value = e match {
    case List() => Wrong
    case Pair(y, b) :: e1 => if (x == y) b else lookup(x, e1)
  }

  def add(a: Value, b: Value): Value = Pair(a, b) match {
    case Pair(Num(m), Num(n)) => Num(m + n)
    case _ => Wrong
  }

  def apply(a: Value, b: Value) = a match {
    case Fun(k) => k(b)
    case _ => Wrong
  }

  def interp(t: Term, e: Environment): Value = t match {
    case Var(x) => lookup(x, e)
    case Con(n) => Num(n)
    case Add(l, r) => add(interp(l, e), interp(r, e))
    case Lam(x, t) => Fun(a => interp(t, Pair(x, a) :: e))
    case App(f, t) => apply(interp(f, e), interp(t, e))
  }

  def test(t: Term): String = 
    showval(interp(t, List()))

  val term0 = App(Lam("x", Add(Var("x"), Var("x"))), Add(Con(10), Con(11)))

  def main(args: Array[String]) = println(test(term0))
}
