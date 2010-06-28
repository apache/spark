/* NSC -- new Scala compiler
 * Copyright 2005-2010 LAMP/EPFL
 * @author Stepan Koltsov
 */

package spark.repl

import scala.tools.nsc
import scala.tools.nsc._
import scala.tools.nsc.interpreter
import scala.tools.nsc.interpreter._

import scala.util.control.Exception._

/** Reads lines from an input stream */
trait SparkInteractiveReader {
  import SparkInteractiveReader._
  import java.io.IOException
  
  protected def readOneLine(prompt: String): String
  val interactive: Boolean
  
  def readLine(prompt: String): String = {
    def handler: Catcher[String] = {
      case e: IOException if restartSystemCall(e) => readLine(prompt)
    }
    catching(handler) { readOneLine(prompt) }
  }
  
  // override if history is available
  def history: Option[History] = None
  def historyList = history map (_.asList) getOrElse Nil
  
  // override if completion is available
  def completion: Option[SparkCompletion] = None
    
  // hack necessary for OSX jvm suspension because read calls are not restarted after SIGTSTP
  private def restartSystemCall(e: Exception): Boolean =
    Properties.isMac && (e.getMessage == msgEINTR)
}


object SparkInteractiveReader {
  val msgEINTR = "Interrupted system call"
  private val exes = List(classOf[Exception], classOf[NoClassDefFoundError])
  
  def createDefault(): SparkInteractiveReader = createDefault(null)
  
  /** Create an interactive reader.  Uses <code>JLineReader</code> if the
   *  library is available, but otherwise uses a <code>SimpleReader</code>. 
   */
  def createDefault(interpreter: SparkInterpreter): SparkInteractiveReader =
    try new SparkJLineReader(interpreter)
    catch {
      case e @ (_: Exception | _: NoClassDefFoundError) =>
        // println("Failed to create SparkJLineReader(%s): %s".format(interpreter, e))
        new SparkSimpleReader
    }
}

