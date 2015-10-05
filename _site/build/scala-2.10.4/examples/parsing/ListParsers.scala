package examples.parsing

import scala.util.parsing.combinator.syntactical.StandardTokenParsers

object listParsers extends StandardTokenParsers {   
  lexical.delimiters ++= List("(", ")", ",")

  def expr: Parser[Any] = "(" ~ exprs ~ ")" | ident | numericLit
  def exprs: Parser[Any] = expr ~ rep ("," ~ expr)

  def main(args: Array[String]) {
    val input = args.mkString(" ")
    val tokens = new lexical.Scanner(input)
    println(input)
    println(phrase(expr)(tokens))
  }
}

object listParsers1 extends StandardTokenParsers {   
  lexical.delimiters ++= List("(", ")", ",")

  def expr: Parser[Any] = "(" ~> exprs <~ ")" | ident | numericLit

  def exprs: Parser[List[Any]] = expr ~ rep ("," ~> expr) ^^ { case x ~ y => x :: y }

  def main(args: Array[String]) {
    val input = args.mkString(" ")
    val tokens = new lexical.Scanner(input)
    println(input)
    println(phrase(expr)(tokens))
  }
}
