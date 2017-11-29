/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.repl

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter._

class SparkILoopInterpreter(settings: Settings, out: JPrintWriter) extends IMain(settings, out) {
  self =>

  override lazy val memberHandlers = new {
    val intp: self.type = self
  } with MemberHandlers {
    import intp.global._

    override def chooseHandler(member: intp.global.Tree): MemberHandler = member match {
      case member: Import => new SparkImportHandler(member)
      case _ => super.chooseHandler (member)
    }

    class SparkImportHandler(imp: Import) extends ImportHandler(imp: Import) {

      override def targetType: Type = intp.global.rootMirror.getModuleIfDefined("" + expr) match {
        case NoSymbol => intp.typeOfExpression("" + expr)
        case sym => sym.tpe
      }

      private def pos(name: Name, s: String): Int = fixIndexOf(name, safeIndexOf(name, s))
      private def fixIndexOf(name: Name, idx: Int): Int = if (idx == name.length) -1 else idx
      private def safeIndexOf(name: Name, s: String): Int = {
        var i = name.pos(s.charAt(0), 0)
        val sLen = s.length()
        if (sLen == 1) return i
        while (i + sLen <= name.length) {
          var j = 1
          while (s.charAt(j) == name.charAt(i + j)) {
            j += 1
            if (j == sLen) return i
          }
          i = name.pos(s.charAt(0), i + 1)
        }
        name.length
      }

      private def isFlattenedSymbol(sym: Symbol): Boolean =
        sym.owner.isPackageClass &&
          sym.name.containsName(nme.NAME_JOIN_STRING) &&
          sym.owner.info.member(sym.name.take(
            safeIndexOf(sym.name, nme.NAME_JOIN_STRING))) != NoSymbol

      private def importableTargetMembers =
        importableMembers(exitingTyper(targetType)).filterNot(isFlattenedSymbol).toList

      def isIndividualImport(s: ImportSelector): Boolean =
        s.name != nme.WILDCARD && s.rename != nme.WILDCARD
      def isWildcardImport(s: ImportSelector): Boolean =
        s.name == nme.WILDCARD

      // non-wildcard imports
      private def individualSelectors = selectors filter isIndividualImport

      override val importsWildcard: Boolean = selectors exists isWildcardImport

      lazy val importableSymbolsWithRenames: List[(Symbol, Name)] = {
        val selectorRenameMap =
          individualSelectors.flatMap(x => x.name.bothNames zip x.rename.bothNames).toMap
        importableTargetMembers flatMap (m => selectorRenameMap.get(m.name) map (m -> _))
      }

      override lazy val individualSymbols: List[Symbol] = importableSymbolsWithRenames map (_._1)
      override lazy val wildcardSymbols: List[Symbol] =
        if (importsWildcard) importableTargetMembers else Nil

    }

  }

  object expressionTyper extends {
    val repl: SparkILoopInterpreter.this.type = self
  } with SparkExprTyper { }

  override def symbolOfLine(code: String): global.Symbol =
    expressionTyper.symbolOfLine(code)

  override def typeOfExpression(expr: String, silent: Boolean): global.Type =
    expressionTyper.typeOfExpression(expr, silent)

}
