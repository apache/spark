package examples.parsing

import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.CharArrayReader

object listParser {
  abstract class Tree
  case class Id(s: String)          extends Tree
  case class Num(n: Int)            extends Tree
  case class Lst(elems: List[Tree]) extends Tree

  import Character.{isLetter, isLetterOrDigit, isDigit}
  def mkString(cs: List[Any]) = cs.mkString("")

  class ListParsers extends Parsers {
    type Elem = Char
    
    lazy val ident = rep1(elem("letter", isLetter), elem("letter or digit", isLetterOrDigit)) ^^ {cs => Id(mkString(cs))}
    lazy val number = chainl1(elem("digit", isDigit) ^^ (_ - '0'), success{(accum: Int, d: Int) => accum * 10 + d}) ^^ Num
    lazy val list = '(' ~> repsep(expr, ',') <~ ')' ^^ Lst
    lazy val expr: Parser[Tree] = list | ident | number
  }

  def main(args: Array[String]) {
    println(
      if (args.length == 0) "usage: scala examples.parsing.listParser <list-string>"
      else (new ListParsers).expr(new CharArrayReader(args.mkString(" ").toCharArray()))
    )
  }
}
