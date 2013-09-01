/* NSC -- new Scala compiler
 * Copyright 2005-2011 LAMP/EPFL
 * @author Stepan Koltsov
 */

package org.apache.spark.repl

import scala.tools.nsc._
import scala.tools.nsc.interpreter._

import scala.tools.jline.console.ConsoleReader
import scala.tools.jline.console.completer._
import session._
import scala.collection.JavaConverters._
import Completion._
import io.Streamable.slurp

/** Reads from the console using JLine */
class SparkJLineReader(val completion: Completion) extends InteractiveReader {
  val interactive = true
  lazy val history: JLineHistory = JLineHistory()
  lazy val keyBindings =
    try KeyBinding parse slurp(term.getDefaultBindings)
    catch { case _: Exception => Nil }

  private def term = consoleReader.getTerminal()
  def reset() = term.reset()
  def init()  = term.init()
  
  def scalaToJline(tc: ScalaCompleter): Completer = new Completer {
    def complete(_buf: String, cursor: Int, candidates: JList[CharSequence]): Int = {
      val buf   = if (_buf == null) "" else _buf      
      val Candidates(newCursor, newCandidates) = tc.complete(buf, cursor)
      newCandidates foreach (candidates add _)
      newCursor
    }
  }
    
  class JLineConsoleReader extends ConsoleReader with ConsoleReaderHelper {
    // working around protected/trait/java insufficiencies.
    def goBack(num: Int): Unit = back(num)
    def readOneKey(prompt: String) = {
      this.print(prompt)
      this.flush()
      this.readVirtualKey()
    }
    def eraseLine() = consoleReader.resetPromptLine("", "", 0)
    def redrawLineAndFlush(): Unit = { flush() ; drawLine() ; flush() }
    
    this setBellEnabled false
    if (history ne NoHistory)
      this setHistory history
    
    if (completion ne NoCompletion) {
      val argCompletor: ArgumentCompleter =
        new ArgumentCompleter(new JLineDelimiter, scalaToJline(completion.completer()))
      argCompletor setStrict false
      
      this addCompleter argCompletor
      this setAutoprintThreshold 400 // max completion candidates without warning
    }
  }
  
  val consoleReader: JLineConsoleReader = new JLineConsoleReader()

  def currentLine: String = consoleReader.getCursorBuffer.buffer.toString
  def redrawLine() = consoleReader.redrawLineAndFlush()
  def eraseLine() = {
    while (consoleReader.delete()) { }
    // consoleReader.eraseLine()
  }
  def readOneLine(prompt: String) = consoleReader readLine prompt
  def readOneKey(prompt: String)  = consoleReader readOneKey prompt
}

object SparkJLineReader {
  def apply(intp: SparkIMain): SparkJLineReader = apply(new SparkJLineCompletion(intp))
  def apply(comp: Completion): SparkJLineReader = new SparkJLineReader(comp)
}
