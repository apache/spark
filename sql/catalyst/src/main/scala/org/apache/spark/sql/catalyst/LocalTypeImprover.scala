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

package org.apache.spark.sql.catalyst

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

import ScalaReflection.universe.{appliedType, internal, Type}
import macros.{DeepClassTagMacros, DeepDealiaserMacros, LocalTypeImproverMacros}

case class ClassTagApplication(tycon: ClassTag[_], targs: List[ClassTagApplication])

/**
 * A type class generalizing `ClassTag`. It recursively attaches `ClassTag`s to type arguments,
 * their type arguments, etc.
 *
 * Implemented via a macro because there are too many shapes of type constructors and it's
 * rather boilerplaty to implement the type-class instances manually, especially resolving
 * implicit ambiguities and implicit divergence.
 *
 * @since 3.4.0
 */
@implicitNotFound("Must be classes: ${T}, its type arguments, their type arguments, etc.")
class DeepClassTag[T](val classTags: ClassTagApplication)

object DeepClassTag {
  def apply[T: DeepClassTag]: DeepClassTag[T] = implicitly[DeepClassTag[T]]

  import scala.language.experimental.macros
  implicit def mkDeepClassTag[T]: DeepClassTag[T] = macro DeepClassTagMacros.mkDeepClassTagImpl[T]
}

/**
 * Enhanced version of `.dealias`. It is recursively applied to type arguments, their type
 * arguments, etc.
 *
 * Executed at compile time because Scala runtime reflection not completely supports dealiasing
 * type aliases of local types.
 *
 * @since 3.4.0
 */
object DeepDealiaser {
  import scala.language.experimental.macros
  def dealiasStaticType[T]: Type = macro DeepDealiaserMacros.dealiasStaticTypeImpl[T]
}

/**
 * Transforms a type having `WeakTypeTag` but not having `TypeTag` (i.e. Scala free type, type of
 * local case class, their type aliases, etc.) into the one having `TypeTag` using `ClassTag`.
 *
 * E.g. it transforms `TypeRef(NoPrefix, TypeName("Local"), List())` into
 * `TypeRef(NoPrefix, TypeName("$Local$1"), List())` for local (method-inner) class `Local`.
 *
 * Handles type arguments recursively.
 *
 * Implemented via a macro because [[DeepDealiaser]] is a macro too and we need to postpone the
 * inference of type parameter `T` till macro expansion.
 *
 * @since 3.4.0
 */
object LocalTypeImprover {
  import scala.language.experimental.macros

  def improveStaticType[T: DeepClassTag]: Type =
    macro LocalTypeImproverMacros.improveStaticTypeImpl[T]

  def improveDynamicType(tpe: Type, classTags: ClassTagApplication): Type = {
    val newTycon = improveFreeType(tpe, classTags.tycon.runtimeClass)
    val targs = tpe.dealias.typeArgs
    assert(
      targs.length == classTags.targs.length,
      s"( $targs ).length == ( ${classTags.targs} ).length"
    )
    val newArgs = targs.zip(classTags.targs).map((improveDynamicType _).tupled)
    appliedType(newTycon, newArgs)
  }

  def improveFreeType(tpe: Type, cls: Class[_]): Type =
    if (internal.isFreeType(tpe.typeSymbol)) {
      val typeArgs = tpe.dealias.typeArgs
      val typeConstructor = ScalaReflection.mirror.classSymbol(cls).toType.typeConstructor
      appliedType(typeConstructor, typeArgs)
    } else tpe
}
