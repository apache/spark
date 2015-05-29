/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2006-2011, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |                                         **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package examples.parsing

import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StdTokenParsers

/** Parse and evaluate a numeric expression as a sequence of terms, separated by + or -
 *   a term is a sequence of factors, separated by * or /
 *   a factor is a parenthesized expression or a number
 *
 * @author Adriaan Moors 
 */ 
object arithmeticParser extends StdTokenParsers {   
  type Tokens = StdLexical ; val lexical = new StdLexical
  lexical.delimiters ++= List("(", ")", "+", "-", "*", "/")

  lazy val expr =   term*("+" ^^^ {(x: Int, y: Int) => x + y} | "-" ^^^ {(x: Int, y: Int) => x - y})
  lazy val term = factor*("*" ^^^ {(x: Int, y: Int) => x * y} | "/" ^^^ {(x: Int, y: Int) => x / y})
  lazy val factor: Parser[Int] = "(" ~> expr <~ ")" | numericLit ^^ (_.toInt)
  
  def main(args: Array[String]) {
    println(
      if (args.length == 0) "usage: scala examples.parsing.arithmeticParser <expr-string>"
      else expr(new lexical.Scanner(args.mkString(" ")))
    )
  }
}


object arithmeticParserDesugared extends StdTokenParsers {   
  type Tokens = StdLexical ; val lexical = new StdLexical
  lexical.delimiters ++= List("(", ")", "+", "-", "*", "/")

  lazy val expr = chainl1(term, (keyword("+").^^^{(x: Int, y: Int) => x + y}).|(keyword("-").^^^{(x: Int, y: Int) => x - y}))
  lazy val term = chainl1(factor, (keyword("*").^^^{(x: Int, y: Int) => x * y}).|(keyword("/").^^^{(x: Int, y: Int) => x / y}))
  lazy val factor: Parser[Int] = keyword("(").~>(expr.<~(keyword(")"))).|(numericLit.^^(x => x.toInt))   
  
  def main(args: Array[String]) {
    println(
      if (args.length == 0) "usage: scala examples.parsing.arithmeticParserDesugared <expr-string>"
      else expr(new lexical.Scanner(args.mkString(" ")))
    )
  }
}
