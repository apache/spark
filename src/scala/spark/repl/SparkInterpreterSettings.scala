/* NSC -- new Scala compiler
 * Copyright 2005-2010 LAMP/EPFL
 * @author Alexander Spoon
 */

package spark.repl

import scala.tools.nsc
import scala.tools.nsc._

/** Settings for the interpreter
 *
 * @version 1.0
 * @author Lex Spoon, 2007/3/24
 **/
class SparkInterpreterSettings(repl: SparkInterpreter) {
  /** A list of paths where :load should look */
  var loadPath = List(".")

  /** The maximum length of toString to use when printing the result
   *  of an evaluation.  0 means no maximum.  If a printout requires
   *  more than this number of characters, then the printout is
   *  truncated.
   */
  var maxPrintString = 800
  
  /** The maximum number of completion candidates to print for tab
   *  completion without requiring confirmation.
   */
  var maxAutoprintCompletion = 250
  
  /** String unwrapping can be disabled if it is causing issues.
   *  Settings this to false means you will see Strings like "$iw.$iw.".
   */
  var unwrapStrings = true
  
  def deprecation_=(x: Boolean) = {
    val old = repl.settings.deprecation.value
    repl.settings.deprecation.value = x
    if (!old && x) println("Enabled -deprecation output.")
    else if (old && !x) println("Disabled -deprecation output.")
  }
  def deprecation: Boolean = repl.settings.deprecation.value
  
  def allSettings = Map(
    "maxPrintString" -> maxPrintString,
    "maxAutoprintCompletion" -> maxAutoprintCompletion,
    "unwrapStrings" -> unwrapStrings,
    "deprecation" -> deprecation
  )
  
  private def allSettingsString =
    allSettings.toList sortBy (_._1) map { case (k, v) => "  " + k + " = " + v + "\n" } mkString
    
  override def toString = """
    | SparkInterpreterSettings {
    | %s
    | }""".stripMargin.format(allSettingsString)
}

/* Utilities for the InterpreterSettings class
 *
 * @version 1.0
 * @author Lex Spoon, 2007/5/24
 */
object SparkInterpreterSettings {  
  /** Source code for the InterpreterSettings class.  This is 
   *  used so that the interpreter is sure to have the code
   *  available.
   * 
   *  XXX I'm not seeing why this degree of defensiveness is necessary.
   *  If files are missing the repl's not going to work, it's not as if
   *  we have string source backups for anything else.
   */
  val sourceCodeForClass =
"""
package scala.tools.nsc

/** Settings for the interpreter
 *
 * @version 1.0
 * @author Lex Spoon, 2007/3/24
 **/
class SparkInterpreterSettings(repl: Interpreter) {
  /** A list of paths where :load should look */
  var loadPath = List(".")

  /** The maximum length of toString to use when printing the result
   *  of an evaluation.  0 means no maximum.  If a printout requires
   *  more than this number of characters, then the printout is
   *  truncated.
   */
  var maxPrintString = 2400
  
  def deprecation_=(x: Boolean) = {
    val old = repl.settings.deprecation.value
    repl.settings.deprecation.value = x
    if (!old && x) println("Enabled -deprecation output.")
    else if (old && !x) println("Disabled -deprecation output.")
  }
  def deprecation: Boolean = repl.settings.deprecation.value
  
  override def toString =
    "SparkInterpreterSettings {\n" +
//    "  loadPath = " + loadPath + "\n" +
    "  maxPrintString = " + maxPrintString + "\n" +
    "}"
}

"""

}
