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

package org.apache.spark.sql.catalyst.macros

import scala.reflect.macros.whitebox

/**
 * Macro implementation for type class [[DeepClassTag]].
 *
 * @since 3.4.0
 */
class DeepClassTagMacros(val c: whitebox.Context) {
  import c.universe._

  implicit class ModifiersOps(left: Modifiers) {
    def &(right: FlagSet): Modifiers = left match {
      case Modifiers(flags, privateWithin, annots) =>
        Modifiers(flags & right, privateWithin, annots)
    }
  }

  implicit class FlagSetOps(left: FlagSet) {
    def &(right: FlagSet): FlagSet =
      (left.asInstanceOf[Long] & right.asInstanceOf[Long]).asInstanceOf[FlagSet]
    def unary_~ : FlagSet = (~left.asInstanceOf[Long]).asInstanceOf[FlagSet]
  }

  def removeDeferred(tree: TypeDef): TypeDef = tree match {
    case q"$mods type $tpname[..$tparams] = $tpt" =>
      q"${mods & ~Flag.DEFERRED} type $tpname[..$tparams] = $tpt"
  }

  def mkExistentialTypeTree(tpe: Type): Tree = {
    def mkTargs0(count: Int): Seq[TypeName] =
      (0 until count).map(i => TypeName(c.freshName(s"targ$i")))
    def mkTargs(symb: Symbol): Seq[TypeName] = mkTargs0(typeParams(symb).length)
    def typeParams(symb: Symbol): Seq[Symbol] = symb.typeSignature.typeParams

    val typeSymbol = tpe.typeSymbol
    val targs = mkTargs(typeSymbol)
    val tparams = typeParams(typeSymbol)
    val targDefs: Seq[Tree] = tparams.zip(targs).map { case (tparam, targ) =>
      val targParamDefs: Seq[TypeDef] =
        mkTargs(tparam).map(targ => removeDeferred(q"type $targ"))
      q"type $targ[..$targParamDefs]"
    }
    tq"${tpe.typeSymbol}[..$targs] forSome { ..$targDefs }"
  }

  val catalyst = q"_root_.org.apache.spark.sql.catalyst"
  val ClassTagApplication = tq"$catalyst.ClassTagApplication"
  val scalaT = q"_root_.scala"

  def mkApplication(tpe: Type): Tree = {
    val dealiased = tpe.dealias
    val existentialTpeTree = mkExistentialTypeTree(dealiased)
    val tpeCtag = q"$scalaT.reflect.classTag[$existentialTpeTree]"
    val targCtags = dealiased.typeArgs.map(mkApplication)
    q"new $ClassTagApplication($tpeCtag, $scalaT.List.apply[$ClassTagApplication](..$targCtags))"
  }

  def mkDeepClassTagImpl[T: WeakTypeTag]: Tree = {
    val T = weakTypeOf[T]
    q"new $catalyst.DeepClassTag[$T](${mkApplication(T)})"
  }
}
