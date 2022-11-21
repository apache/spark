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

import ScalaReflection.universe.{appliedType, internal, weakTypeOf, Type, WeakTypeTag}
import macros.DeepClassTagMacros

case class ClassTagApplication(tycon: ClassTag[_], targs: List[ClassTagApplication])

/**
 * A type class generalizing `ClassTag`. It recursively attaches `ClassTag`s to type arguments,
 * their type arguments, etc.
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
 * Transforms a type having `WeakTypeTag` but not having `TypeTag` (i.e. Scala free type, type of
 * local case class, etc) into the one having `TypeTag` using `ClassTag`.
 *
 * E.g. it transforms `TypeRef(NoPrefix, TypeName("Local"), List())` into
 * `TypeRef(NoPrefix, TypeName("$Local$1"), List())` for local (method-inner) class `Local`.
 *
 * Handles type arguments recursively.
 *
 * In runtime reflection, dealiasing seems not to work for local type aliases.
 *
 * @since 3.4.0
 */
object LocalTypeImprover {
  def improveStaticType[T: WeakTypeTag : DeepClassTag]: Type =
    improveDynamicType(weakTypeOf[T], DeepClassTag[T].classTags)

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
