// scalastyle:off

/* NSC -- new Scala compiler
 * Copyright 2005-2013 LAMP/EPFL
 * @author  Paul Phillips
 */

package org.apache.spark.repl

import scala.tools.nsc._
import scala.tools.nsc.interpreter._

import scala.reflect.internal.util.BatchSourceFile
import scala.tools.nsc.ast.parser.Tokens.EOF

import org.apache.spark.Logging

trait SparkExprTyper extends Logging {
  val repl: SparkIMain

  import repl._
  import global.{ reporter => _, Import => _, _ }
  import definitions._
  import syntaxAnalyzer.{ UnitParser, UnitScanner, token2name }
  import naming.freshInternalVarName

  object codeParser extends { val global: repl.global.type = repl.global } with CodeHandlers[Tree] {
    def applyRule[T](code: String, rule: UnitParser => T): T = {
      reporter.reset()
      val scanner = newUnitParser(code)
      val result  = rule(scanner)

      if (!reporter.hasErrors)
        scanner.accept(EOF)

      result
    }

    def defns(code: String) = stmts(code) collect { case x: DefTree => x }
    def expr(code: String)  = applyRule(code, _.expr())
    def stmts(code: String) = applyRule(code, _.templateStats())
    def stmt(code: String)  = stmts(code).last  // guaranteed nonempty
  }

  /** Parse a line into a sequence of trees. Returns None if the input is incomplete. */
  def parse(line: String): Option[List[Tree]] = debugging(s"""parse("$line")""")  {
    var isIncomplete = false
    reporter.withIncompleteHandler((_, _) => isIncomplete = true) {
      val trees = codeParser.stmts(line)
      if (reporter.hasErrors) {
        Some(Nil)
      } else if (isIncomplete) {
        None
      } else {
        Some(trees)
      }
    }
  }
  // def parsesAsExpr(line: String) = {
  //   import codeParser._
  //   (opt expr line).isDefined
  // }

  def symbolOfLine(code: String): Symbol = {
    def asExpr(): Symbol = {
      val name  = freshInternalVarName()
      // Typing it with a lazy val would give us the right type, but runs
      // into compiler bugs with things like existentials, so we compile it
      // behind a def and strip the NullaryMethodType which wraps the expr.
      val line = "def " + name + " = {\n" + code + "\n}"

      interpretSynthetic(line) match {
        case IR.Success =>
          val sym0 = symbolOfTerm(name)
          // drop NullaryMethodType
          val sym = sym0.cloneSymbol setInfo afterTyper(sym0.info.finalResultType)
          if (sym.info.typeSymbol eq UnitClass) NoSymbol else sym
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
    beQuietDuring(asExpr()) orElse beQuietDuring(asDefn())
  }

  private var typeOfExpressionDepth = 0
  def typeOfExpression(expr: String, silent: Boolean = true): Type = {
    if (typeOfExpressionDepth > 2) {
      logDebug("Terminating typeOfExpression recursion for expression: " + expr)
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
}
