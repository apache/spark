package examples.parsing.lambda

/**
 * Parser for an untyped lambda calculus: abstract syntax tree
 *
 * @author Miles Sabin (adapted slightly by Adriaan Moors)
 */
trait TestSyntax 
{
  trait Term 
  
  case class Unit() extends Term
  {
    override def toString = "unit"
  }
  
  case class Lit(n: Int) extends Term
  {
    override def toString = n.toString
  }
  
  case class Bool(b: Boolean) extends Term
  {
    override def toString = b.toString
  }
  
  case class Name(name: String) extends Term
  {
    override def toString = name
  }

  case class Ref(n: Name) extends Term
  {
    def value = n
  }
  
  case class Lam(n: Name, l: Term) extends Term
  {
    override def toString = "(\\ "+n+" -> "+l+")"
  } 
  
  case class App(t1: Term, t2: Term) extends Term
  {
    override def toString = "("+t1+" "+t2+")"
  } 
  
  case class Let(n: Name, t1: Term, t2: Term) extends Term
  {
    override def toString = "let "+n+" = "+t1+" in "+t2
  }
  
  case class If(c: Term, t1: Term, t2: Term) extends Term
  {
    override def toString = "if "+c+" then "+t1+" else "+t2
  }
  
  trait PrimTerm extends Term
  {
    def apply(n: Lit) : Term
  }

  case class PrimPlus() extends PrimTerm
  {
    def apply(x: Lit) = new PrimTerm { def apply(y: Lit) = Lit(x.n+y.n) }
  }

  case class PrimMinus() extends PrimTerm
  {
    def apply(x: Lit) = new PrimTerm { def apply(y: Lit) = Lit(x.n-y.n) }
  }
  
  case class PrimMultiply() extends PrimTerm
  {
    def apply(x: Lit) = new PrimTerm { def apply(y: Lit) = Lit(x.n*y.n) }
  }

  case class PrimDivide() extends PrimTerm
  {
    def apply(x: Lit) = new PrimTerm { def apply(y: Lit) = Lit(x.n/y.n) }
  }

  case class PrimEquals() extends PrimTerm
  {
    def apply(x: Lit) = new PrimTerm { def apply(y: Lit) = Bool(x.n == y.n) }
  }
}
