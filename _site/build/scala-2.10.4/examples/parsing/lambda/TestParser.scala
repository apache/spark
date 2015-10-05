package examples.parsing.lambda

import scala.util.parsing.input.Reader
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StdTokenParsers
import scala.util.parsing.combinator.ImplicitConversions

/**
 * Parser for an untyped lambda calculus
 *
 * @author Miles Sabin (adapted slightly by Adriaan Moors)
 */
trait TestParser extends StdTokenParsers with ImplicitConversions with TestSyntax
{
  type Tokens = StdLexical
  val lexical = new StdLexical
  lexical.reserved ++= List("unit", "let", "in", "if", "then", "else")
  lexical.delimiters ++= List("=>", "->", "==", "(", ")", "=", "\\", "+", "-", "*", "/")

  
	def name : Parser[Name] = ident ^^ Name
  
  // meaning of the arguments to the closure during subsequent iterations
  // (...(expr2 op1 expr1) ... op1 expr1)
  //      ^a^^^ ^o^ ^b^^^
  //      ^^^^^^^a^^^^^^^      ^o^ ^^b^^
  def expr1 : Parser[Term] =
    chainl1(expr2, expr1, op1 ^^ {o => (a: Term, b: Term) => App(App(o, a), b)})

  def expr2 : Parser[Term] =
   chainl1(expr3, expr2, op2 ^^ {o => (a: Term, b: Term) => App(App(o, a), b)})
  
  def expr3 : Parser[Term] =
   chainl1(expr4, expr3, op3 ^^ {o => (a: Term, b: Term) => App(App(o, a), b)})
  
  def expr4 : Parser[Term] =
  ( "\\" ~> lambdas
  | ("let" ~> name) ~ ("=" ~> expr1) ~ ("in" ~> expr1) ^^ flatten3(Let)
  | ("if" ~> expr1) ~ ("then" ~> expr1) ~ ("else" ~> expr1) ^^ flatten3(If)
  | chainl1(aexpr, success(App(_: Term, _: Term)))
  )

  def lambdas : Parser[Term] =
    name ~ ("->" ~> expr1 | lambdas) ^^ flatten2(Lam)
      
  def aexpr : Parser[Term] =
  ( numericLit ^^ (_.toInt) ^^ Lit
  | name ^^ Ref
  | "unit" ^^^ Unit()
  | "(" ~> expr1 <~ ")"
  )
  
  def op1 : Parser[Term] =
    "=="  ^^^ Ref(Name("=="))
  
  def op2 : Parser[Term] =
  ( "+" ^^^ Ref(Name("+"))
  | "-" ^^^ Ref(Name("-"))
  )
  
  def op3 : Parser[Term] =
  ( "*" ^^^ Ref(Name("*"))
  | "/" ^^^ Ref(Name("/"))
  )
  
  def parse(r: Reader[Char]) : ParseResult[Term] =
    phrase(expr1)(new lexical.Scanner(r))
}
