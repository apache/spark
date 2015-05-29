package examples.parsing.lambda

import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.StreamReader

import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader

/**
 * Parser for an untyped lambda calculus
 *
 * Usage: scala examples.parsing.lambda.Main <file>
 *
 * (example files: see test/ *.kwi)
 *
 * @author Miles Sabin (adapted slightly by Adriaan Moors)
 */
object Main extends TestParser
{
  def main(args: Array[String]) =
  {
    val in = StreamReader(new InputStreamReader(new FileInputStream(new File(args(0))), "ISO-8859-1"))
    parse(in) match
    {
      case Success(term, _) =>
      {
        println("Term: \n"+term)
      }
      case Failure(msg, remainder) => println("Failure: "+msg+"\n"+"Remainder: \n"+remainder.pos.longString) 
      case Error(msg, remainder) => println("Error: "+msg+"\n"+"Remainder: \n"+remainder.pos.longString) 
    }
  }
}
