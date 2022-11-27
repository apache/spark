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
 * A convenience trait to mix-in for macros.
 *
 * @since 3.4.0
 */
trait StandardTrees {
  val c: blackbox.Context
  import c.universe._

  lazy val catalyst = q"$rootT.org.apache.spark.sql.catalyst"
  lazy val ClassTagApplication = tq"$catalyst.ClassTagApplication"
  lazy val DeepClassTag = tq"$catalyst.DeepClassTag"
  lazy val DeepDealiaser = q"$catalyst.DeepDealiaser"
  lazy val ExpressionEncoder = tq"$catalyst.encoders.ExpressionEncoder"
  lazy val ListT = q"$scalaT.`package`.List"
  lazy val LocalTypeImprover = q"$catalyst.LocalTypeImprover"
  lazy val reflectT = q"$scalaT.reflect"
  lazy val rootT = q"_root_"
  lazy val ru = q"$scalaT.reflect.runtime.`package`.universe"
  lazy val scalaT = q"$rootT.scala"
  lazy val ScalaReflection = q"$catalyst.ScalaReflection"
}
