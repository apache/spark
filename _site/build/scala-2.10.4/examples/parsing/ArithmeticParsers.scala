package examples.parsing

import scala.util.parsing.combinator.syntactical.StandardTokenParsers

object arithmeticParsers extends StandardTokenParsers {   
  lexical.delimiters ++= List("(", ")", "+", "-", "*", "/")

  def expr: Parser[Any] = term ~ rep("+" ~ term | "-" ~ term)
  def term = factor ~ rep("*" ~ factor | "/" ~ factor)
  def factor: Parser[Any] = "(" ~ expr ~ ")" | numericLit

  def main(args: Array[String]) {
    val input = args.mkString(" ")
    val tokens = new lexical.Scanner(input)
    println(input)
    println(phrase(expr)(tokens))
  }
}

object arithmeticParsers1 extends StandardTokenParsers {   
  lexical.delimiters ++= List("(", ")", "+", "-", "*", "/")

  val reduceList: Int ~ List[String ~ Int] => Int = {
    case i ~ ps => (i /: ps)(reduce) 
  }

  def reduce(x: Int, r: String ~ Int) = (r: @unchecked) match {
    case "+" ~ y => x + y
    case "-" ~ y => x - y
    case "*" ~ y => x * y
    case "/" ~ y => x / y
  }

  def expr  : Parser[Int] = term ~ rep ("+" ~ term | "-" ~ term) ^^ reduceList
  def term  : Parser[Int] = factor ~ rep ("*" ~ factor | "/" ~ factor) ^^ reduceList
  def factor: Parser[Int] = "(" ~> expr <~ ")" | numericLit ^^ (_.toInt)

  def main(args: Array[String]) {
    val input = args.mkString(" ")
    val tokens = new lexical.Scanner(input)
    println(input)
    println(phrase(expr)(tokens))
  }
}


object arithmeticParsers2 extends StandardTokenParsers {   

  class Expr
  case class BinOp(op: String, l: Expr, r: Expr) extends Expr
  case class Num(n: Int) extends Expr

  lexical.delimiters ++= List("(", ")", "+", "-", "*", "/")

  val reduceList: Expr ~ List[String ~ Expr] => Expr = {
    case i ~ ps => (i /: ps)(reduce) 
  }

  def reduce(l: Expr, r: String ~ Expr) = BinOp(r._1, l, r._2)
  def mkNum(s: String) = Num(s.toInt)

  def expr  : Parser[Expr] = term ~ rep ("+" ~ term | "-" ~ term) ^^ reduceList
  def term  : Parser[Expr] = factor ~ rep ("*" ~ factor | "/" ~ factor) ^^ reduceList
  def factor: Parser[Expr] = "(" ~> expr <~ ")" | numericLit ^^ ((s: String) => Num(s.toInt))

  def main(args: Array[String]) {
    val input = args.mkString(" ")
    val parse = phrase(expr)
    val tokens = new lexical.Scanner(input)
    println(input)
    println(parse(tokens))
  }
}

