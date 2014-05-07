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

import java.io.File
import java.util.jar.JarFile

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe.runtimeMirror

/**
 * A tool for generating classes to be excluded during binary checking with MIMA. It is expected
 * that this tool is run with ./spark-class.
 *
 * MIMA itself only supports JVM-level visibility and doesn't account for package-private classes.
 * This tool looks at all currently package-private classes and generates exclusions for them. Note
 * that this approach is not sound. It can lead to false positives if we move or rename a previously
 * package-private class. It can lead to false negatives if someone explicitly makes a class
 * package-private that wasn't before. This exists only to help catch certain classes of changes
 * which might be difficult to catch during review.
 */
object GenerateMIMAIgnore {
  private val classLoader = Thread.currentThread().getContextClassLoader
  private val mirror = runtimeMirror(classLoader)

  private def classesPrivateWithin(packageName: String): Set[String] = {

    val classes = getClasses(packageName)
    val privateClasses = mutable.HashSet[String]()

    def isPackagePrivate(className: String) = {
      try {
        /* Couldn't figure out if it's possible to determine a-priori whether a given symbol
           is a module or class. */

        val privateAsClass = mirror
          .classSymbol(Class.forName(className, false, classLoader))
          .privateWithin
          .fullName
          .startsWith(packageName)

        val privateAsModule = mirror
          .staticModule(className)
          .privateWithin
          .fullName
          .startsWith(packageName)

        privateAsClass || privateAsModule
      } catch {
        case _: Throwable => {
          println("Error determining visibility: " + className)
          false
        }
      }
    }

    for (className <- classes) {
      val directlyPrivateSpark = isPackagePrivate(className)

      /* Inner classes defined within a private[spark] class or object are effectively
         invisible, so we account for them as package private. */
      val indirectlyPrivateSpark = {
        val maybeOuter = className.toString.takeWhile(_ != '$')
        if (maybeOuter != className) {
          isPackagePrivate(maybeOuter)
        } else {
          false
        }
      }
      if (directlyPrivateSpark || indirectlyPrivateSpark) privateClasses += className
    }
    privateClasses.flatMap(c => Seq(c, c.replace("$", "#"))).toSet
  }

  def main(args: Array[String]) {
    scala.tools.nsc.io.File(".mima-excludes").
      writeAll(classesPrivateWithin("org.apache.spark").mkString("\n"))
    println("Created : .mima-excludes in current directory.")
  }


  private def shouldExclude(name: String) = {
    // Heuristic to remove JVM classes that do not correspond to user-facing classes in Scala
    name.contains("anon") ||
    name.endsWith("$class") ||
    name.contains("$sp") ||
    name.contains("hive") ||
    name.contains("Hive")
  }

  /**
   * Scans all classes accessible from the context class loader which belong to the given package
   * and subpackages both from directories and jars present on the classpath.
   */
  private def getClasses(packageName: String): Set[String] = {
    val path = packageName.replace('.', '/')
    val resources = classLoader.getResources(path)

    val jars = resources.filter(x => x.getProtocol == "jar")
      .map(_.getFile.split(":")(1).split("!")(0)).toSeq

    jars.flatMap(getClassesFromJar(_, path))
      .map(_.getName)
      .filterNot(shouldExclude).toSet
  }

  /**
   * Get all classes in a package from a jar file.
   */
  private def getClassesFromJar(jarPath: String, packageName: String) = {
    val jar = new JarFile(new File(jarPath))
    val enums = jar.entries().map(_.getName).filter(_.startsWith(packageName))
    val classes = for (entry <- enums if entry.endsWith(".class"))
      yield Class.forName(entry.replace('/', '.').stripSuffix(".class"), false, classLoader)
    classes
  }
}
