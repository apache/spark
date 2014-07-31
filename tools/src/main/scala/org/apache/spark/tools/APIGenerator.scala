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

package org.apache.spark.tools

import scala.collection.mutable
import scala.reflect.runtime.universe.runtimeMirror
import scala.reflect.runtime.{universe => unv}

/**
 * Scans all the jars and outputs all the public API's starting with package name
 * org.apache.spark.
 */
object APIGenerator {
  private val classLoader = Thread.currentThread().getContextClassLoader
  private val mirror = runtimeMirror(classLoader)

  import Utils._

  def shouldExclude(name: String) = {
    // Heuristic to remove JVM classes that do not correspond to user-facing classes in Scala
    name.contains("anon") || // usually $anon$$, $anonfun and such.
    name.endsWith("$class") ||
    name.contains("$sp") ||
    name.contains("org.apache.spark.repl") ||
    name.contains("org.apache.spark.deploy") ||
    (! name.contains("org.apache.spark")) || // anything not apache spark
    name.contains("$$") // consecutive $$ signifies inner method or not exposed member.
  }

  def generate() = {
    val classes = getClasses("org.apache.spark", classLoader, shouldExclude)
    val publicMembers = mutable.SortedSet[String]()

    for (className <- classes) {
      try {
        val classSymbol = mirror.classSymbol(className)
        val moduleSymbol = mirror.staticModule(className.getName)
        publicMembers ++= classSymbol.typeSignature.members.filterNot(x =>
          x.fullName.startsWith("java") || x.fullName.startsWith("scala"))
          .filterNot(x => x.isPrivate || x.isProtected || x.isSynthetic || isPackagePrivate(x)
          || isDeveloperApi(x) || isExperimental(x)).map(_.fullName).toSet
      } catch {
        case t: Throwable =>
          println(s"[Warn] Failed to instrument class:$className for reason ${t.getMessage}")
      }
    }
    publicMembers.filterNot(shouldExclude)
  }

  def main(args: Array[String]) {
    scala.tools.nsc.io.File("spark-public-api-sorted.txt").
      writeAll(generate().mkString("\n"))
  }
}
