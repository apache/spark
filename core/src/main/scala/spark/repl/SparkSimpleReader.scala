/* NSC -- new Scala compiler
 * Copyright 2005-2010 LAMP/EPFL
 * @author Stepan Koltsov
 */

package spark.repl

import scala.tools.nsc
import scala.tools.nsc._
import scala.tools.nsc.interpreter
import scala.tools.nsc.interpreter._

import java.io.{ BufferedReader, PrintWriter }
import io.{ Path, File, Directory }

/** Reads using standard JDK API */
class SparkSimpleReader(
  in: BufferedReader, 
  out: PrintWriter, 
  val interactive: Boolean)
extends SparkInteractiveReader {
  def this() = this(Console.in, new PrintWriter(Console.out), true)
  def this(in: File, out: PrintWriter, interactive: Boolean) = this(in.bufferedReader(), out, interactive)

  def close() = in.close()
  def readOneLine(prompt: String): String = {
    if (interactive) {
      out.print(prompt)
      out.flush()
    }
    in.readLine()
  }
}
