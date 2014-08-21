/* NSC -- new Scala compiler
 * Copyright 2002-2013 LAMP/EPFL
 * @author Paul Phillips
 */

package scala.tools.nsc
package interpreter

import reporters._
import SparkIMain._

import scala.reflect.internal.util.Position

/** Like ReplGlobal, a layer for ensuring extra functionality.
  */
class SparkReplReporter(intp: SparkIMain) extends ConsoleReporter(intp.settings, Console.in, new SparkReplStrippingWriter(intp)) {
  def printUntruncatedMessage(msg: String) = withoutTruncating(printMessage(msg))

  /** Whether very long lines can be truncated.  This exists so important
    *  debugging information (like printing the classpath) is not rendered
    *  invisible due to the max message length.
    */
  private var _truncationOK: Boolean = !intp.settings.verbose
  def truncationOK = _truncationOK
  def withoutTruncating[T](body: => T): T = {
    val saved = _truncationOK
    _truncationOK = false
    try body
    finally _truncationOK = saved
  }

  override def warning(pos: Position, msg: String): Unit = withoutTruncating(super.warning(pos, msg))
  override def error(pos: Position, msg: String): Unit   = withoutTruncating(super.error(pos, msg))

  override def printMessage(msg: String) {
    // Avoiding deadlock if the compiler starts logging before
    // the lazy val is complete.
    if (intp.isInitializeComplete) {
      if (intp.totalSilence) {
        if (isReplTrace)
          super.printMessage("[silent] " + msg)
      }
      else super.printMessage(msg)
    }
    else Console.println("[init] " + msg)
  }

  override def displayPrompt() {
    if (intp.totalSilence) ()
    else super.displayPrompt()
  }

}
