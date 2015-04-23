/* NSC -- new Scala compiler
 * Copyright 2005-2013 LAMP/EPFL
 * @author  Paul Phillips
 */

package scala.tools.nsc
package interpreter

import scala.tools.nsc.ast.parser.Tokens.EOF

trait SparkExprTyper {
  val repl: SparkIMain

  import repl._
  import global.{ reporter => _, Import => _, _ }
  import naming.freshInternalVarName

  def symbolOfLine(code: String): Symbol = {
    def asExpr(): Symbol = {
      val name  = freshInternalVarName()
      // Typing it with a lazy val would give us the right type, but runs
      // into compiler bugs with things like existentials, so we compile it
      // behind a def and strip the NullaryMethodType which wraps the expr.
      val line = "def " + name + " = " + code

      interpretSynthetic(line) match {
        case IR.Success =>
          val sym0 = symbolOfTerm(name)
          // drop NullaryMethodType
          sym0.cloneSymbol setInfo exitingTyper(sym0.tpe_*.finalResultType)
        case _          => NoSymbol
      }
    }
    def asDefn(): Symbol = {
      val old = repl.definedSymbolList.toSet

      interpretSynthetic(code) match {
        case IR.Success =>
          repl.definedSymbolList filterNot old match {
            case Nil        => NoSymbol
            case sym :: Nil => sym
            case syms       => NoSymbol.newOverloaded(NoPrefix, syms)
          }
        case _ => NoSymbol
      }
    }
    def asError(): Symbol = {
      interpretSynthetic(code)
      NoSymbol
    }
    beSilentDuring(asExpr()) orElse beSilentDuring(asDefn()) orElse asError()
  }

  private var typeOfExpressionDepth = 0
  def typeOfExpression(expr: String, silent: Boolean = true): Type = {
    if (typeOfExpressionDepth > 2) {
      repldbg("Terminating typeOfExpression recursion for expression: " + expr)
      return NoType
    }
    typeOfExpressionDepth += 1
    // Don't presently have a good way to suppress undesirable success output
    // while letting errors through, so it is first trying it silently: if there
    // is an error, and errors are desired, then it re-evaluates non-silently
    // to induce the error message.
    try beSilentDuring(symbolOfLine(expr).tpe) match {
      case NoType if !silent => symbolOfLine(expr).tpe // generate error
      case tpe               => tpe
    }
    finally typeOfExpressionDepth -= 1
  }

  // This only works for proper types.
  def typeOfTypeString(typeString: String): Type = {
    def asProperType(): Option[Type] = {
      val name = freshInternalVarName()
      val line = "def %s: %s = ???" format (name, typeString)
      interpretSynthetic(line) match {
        case IR.Success =>
          val sym0 = symbolOfTerm(name)
          Some(sym0.asMethod.returnType)
        case _          => None
      }
    }
    beSilentDuring(asProperType()) getOrElse NoType
  }
}
