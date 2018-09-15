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

import scala.collection.mutable
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
      case _ => super.chooseHandler(member)
    }

    class SparkImportHandler(imp: Import) extends ImportHandler(imp: Import) {

      override def targetType: Type = intp.global.rootMirror.getModuleIfDefined("" + expr) match {
        case NoSymbol => intp.typeOfExpression("" + expr)
        case sym => sym.tpe
      }

      private def safeIndexOf(name: Name, s: String): Int = fixIndexOf(name, pos(name, s))
      private def fixIndexOf(name: Name, idx: Int): Int = if (idx == name.length) -1 else idx
      private def pos(name: Name, s: String): Int = {
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


  import global.Name
  override def importsCode(wanted: Set[Name], wrapper: Request#Wrapper,
                           definesClass: Boolean, generousImports: Boolean): ComputedImports = {

    import global._
    import definitions.{ ObjectClass, ScalaPackage, JavaLangPackage, PredefModule }
    import memberHandlers._

    val header, code, trailingBraces, accessPath = new StringBuilder
    val currentImps = mutable.HashSet[Name]()
    // only emit predef import header if name not resolved in history, loosely
    var predefEscapes = false

    /**
     * Narrow down the list of requests from which imports
     * should be taken.  Removes requests which cannot contribute
     * useful imports for the specified set of wanted names.
     */
    case class ReqAndHandler(req: Request, handler: MemberHandler)

    def reqsToUse: List[ReqAndHandler] = {
      /**
       * Loop through a list of MemberHandlers and select which ones to keep.
       * 'wanted' is the set of names that need to be imported.
       */
      def select(reqs: List[ReqAndHandler], wanted: Set[Name]): List[ReqAndHandler] = {
        // Single symbol imports might be implicits! See bug #1752.  Rather than
        // try to finesse this, we will mimic all imports for now.
        def keepHandler(handler: MemberHandler) = handler match {
          // While defining classes in class based mode - implicits are not needed.
          case h: ImportHandler if isClassBased && definesClass =>
            h.importedNames.exists(x => wanted.contains(x))
          case _: ImportHandler => true
          case x if generousImports => x.definesImplicit ||
            (x.definedNames exists (d => wanted.exists(w => d.startsWith(w))))
          case x => x.definesImplicit ||
            (x.definedNames exists wanted)
        }

        reqs match {
          case Nil =>
            predefEscapes = wanted contains PredefModule.name ; Nil
          case rh :: rest if !keepHandler(rh.handler) => select(rest, wanted)
          case rh :: rest =>
            import rh.handler._
            val augment = rh match {
              case ReqAndHandler(_, _: ImportHandler) => referencedNames
              case _ => Nil
            }
            val newWanted = wanted ++ augment -- definedNames -- importedNames
            rh :: select(rest, newWanted)
        }
      }

      /** Flatten the handlers out and pair each with the original request */
      select(allReqAndHandlers reverseMap { case (r, h) => ReqAndHandler(r, h) }, wanted).reverse
    }

    // add code for a new object to hold some imports
    def addWrapper() {
      import nme.{ INTERPRETER_IMPORT_WRAPPER => iw }
      code append (wrapper.prewrap format iw)
      trailingBraces append wrapper.postwrap
      accessPath append s".$iw"
      currentImps.clear()
    }

    def maybeWrap(names: Name*) = if (names exists currentImps) addWrapper()

    def wrapBeforeAndAfter[T](op: => T): T = {
      addWrapper()
      try op finally addWrapper()
    }

    // imports from Predef are relocated to the template header to allow hiding.
    def checkHeader(h: ImportHandler) = h.referencedNames contains PredefModule.name

    // loop through previous requests, adding imports for each one
    wrapBeforeAndAfter {
      // Reusing a single temporary value when import from a line with multiple definitions.
      val tempValLines = mutable.Set[Int]()
      for (ReqAndHandler(req, handler) <- reqsToUse) {
        val objName = req.lineRep.readPathInstance
        handler match {
          case h: ImportHandler if checkHeader(h) =>
            header.clear()
            header append f"${h.member}%n"
          // If the user entered an import, then just use it; add an import wrapping
          // level if the import might conflict with some other import
          case x: ImportHandler if x.importsWildcard =>
            wrapBeforeAndAfter(code append (x.member + "\n"))
          case x: ImportHandler =>
            maybeWrap(x.importedNames: _*)
            code append (x.member + "\n")
            currentImps ++= x.importedNames

          case x if isClassBased =>
            for (sym <- x.definedSymbols) {
              maybeWrap(sym.name)
              x match {
                case _: ClassHandler =>
                  code.append(s"import ${objName}${req.accessPath}.`${sym.name}`\n")
                case _ =>
                  val valName = s"${req.lineRep.packageName}${req.lineRep.readName}"
                  if (!tempValLines.contains(req.lineRep.lineId)) {
                    code.append(s"val $valName: ${objName}.type = $objName\n")
                    tempValLines += req.lineRep.lineId
                  }
                  code.append(s"import ${valName}${req.accessPath}.`${sym.name}`\n")
              }
              currentImps += sym.name
            }
          // For other requests, import each defined name.
          // import them explicitly instead of with _, so that
          // ambiguity errors will not be generated. Also, quote
          // the name of the variable, so that we don't need to
          // handle quoting keywords separately.
          case x =>
            for (sym <- x.definedSymbols) {
              maybeWrap(sym.name)
              code append s"import ${x.path}\n"
              currentImps += sym.name
            }
        }
      }
    }

    val computedHeader = if (predefEscapes) header.toString else ""
    ComputedImports(computedHeader, code.toString, trailingBraces.toString, accessPath.toString)
  }

  private def allReqAndHandlers =
    prevRequestList flatMap (req => req.handlers map (req -> _))

}
