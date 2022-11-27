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

import scala.reflect.macros.blackbox

/**
 * Macro implementation for [[ExpressionEncoder]] of local classes.
 *
 * @since 3.4.0
 */
class ExpressionEncoderMacros(val c: blackbox.Context) extends StandardTrees {
  import c.universe._

  def localImpl[T: WeakTypeTag]()(classTag: Tree, deepClassTag: Tree): Tree = {
    val T = weakTypeOf[T]
    val tpe = q"$LocalTypeImprover.improveStaticType[$T]($deepClassTag)"
    val serializer = q"$ScalaReflection.serializerForType($tpe)"
    val deserializer = q"$ScalaReflection.deserializerForType($tpe)"
    q"new $ExpressionEncoder[$T]($serializer, $deserializer, $classTag)"
  }
}
