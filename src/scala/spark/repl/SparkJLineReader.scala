/* NSC -- new Scala compiler
 * Copyright 2005-2010 LAMP/EPFL
 * @author Stepan Koltsov
 */

package spark.repl

import scala.tools.nsc
import scala.tools.nsc._
import scala.tools.nsc.interpreter
import scala.tools.nsc.interpreter._

import java.io.File
import jline.{ ConsoleReader, ArgumentCompletor, History => JHistory }

/** Reads from the console using JLine */
class SparkJLineReader(interpreter: SparkInterpreter) extends SparkInteractiveReader {
  def this() = this(null)
  
  override lazy val history = Some(History(consoleReader))
  override lazy val completion = Option(interpreter) map (x => new SparkCompletion(x))
  
  val consoleReader = {
    val r = new jline.ConsoleReader()
    r setHistory (History().jhistory)
    r setBellEnabled false 
    completion foreach { c =>
      r addCompletor c.jline
      r setAutoprintThreshhold 250
    }

    r
  }
  
  def readOneLine(prompt: String) = consoleReader readLine prompt
  val interactive = true
}

