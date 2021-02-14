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

// scalastyle:off classforname
package org.apache.spark.tools

import scala.collection.mutable
import scala.reflect.runtime.{universe => unv}
import scala.reflect.runtime.universe.runtimeMirror
import scala.util.Try

import org.clapper.classutil.ClassFinder
import org.objectweb.asm.Opcodes

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

  private def isPackagePrivate(sym: unv.Symbol) =
    !sym.privateWithin.fullName.startsWith("<none>")

  private def isPackagePrivateModule(moduleSymbol: unv.ModuleSymbol) =
    !moduleSymbol.privateWithin.fullName.startsWith("<none>")

  /**
   * For every class checks via scala reflection if the class itself or contained members
   * are package private.
   * Returns the tuple of such classes and members.
   */
  private def privateWithin(packageName: String): (Set[String], Set[String]) = {

    val classes = getClasses(packageName)
    val ignoredClasses = mutable.HashSet[String]()
    val ignoredMembers = mutable.HashSet[String]()

    for (className <- classes) {
      try {
        val classSymbol = mirror.classSymbol(Class.forName(className, false, classLoader))
        val moduleSymbol = mirror.staticModule(className)
        val directlyPrivateSpark =
          isPackagePrivate(classSymbol) ||
          isPackagePrivateModule(moduleSymbol) ||
          classSymbol.isPrivate
        /* Inner classes defined within a private[spark] class or object are effectively
         invisible, so we account for them as package private. */
        lazy val indirectlyPrivateSpark = {
          val maybeOuter = className.toString.takeWhile(_ != '$')
          if (maybeOuter != className) {
            isPackagePrivate(mirror.classSymbol(Class.forName(maybeOuter, false, classLoader))) ||
              isPackagePrivateModule(mirror.staticModule(maybeOuter))
          } else {
            false
          }
        }
        if (directlyPrivateSpark || indirectlyPrivateSpark) {
          ignoredClasses += className
        }
        // check if this class has package-private/annotated members.
        ignoredMembers ++= getAnnotatedOrPackagePrivateMembers(classSymbol)

      } catch {
        // scalastyle:off println
        case _: Throwable => println("Error instrumenting class:" + className)
        // scalastyle:on println
      }
    }
    (ignoredClasses.flatMap(c => Seq(c, c.replace("$", "#"))).toSet, ignoredMembers.toSet)
  }

  /**
   * Scala reflection does not let us see inner function even if they are upgraded
   * to public for some reason. So had to resort to java reflection to get all inner
   * functions with $$ in there name.
   */
  def getInnerFunctions(classSymbol: unv.ClassSymbol): Seq[String] = {
    try {
      Class.forName(classSymbol.fullName, false, classLoader).getMethods.map(_.getName)
        .filter(_.contains("$$")).map(classSymbol.fullName + "." + _)
    } catch {
      case t: Throwable =>
        // scalastyle:off println
        println("[WARN] Unable to detect inner functions for class:" + classSymbol.fullName)
        // scalastyle:on println
        Seq.empty[String]
    }
  }

  private def getAnnotatedOrPackagePrivateMembers(classSymbol: unv.ClassSymbol) = {
    classSymbol.typeSignature.members.filterNot(x =>
      x.fullName.startsWith("java") || x.fullName.startsWith("scala")
    ).filter(x => isPackagePrivate(x)).map(_.fullName) ++ getInnerFunctions(classSymbol)
  }

  def main(args: Array[String]): Unit = {
    import scala.tools.nsc.io.File
    val (privateClasses, privateMembers) = privateWithin("org.apache.spark")
    val previousContents = Try(File(".generated-mima-class-excludes").lines()).
      getOrElse(Iterator.empty).mkString("\n")
    File(".generated-mima-class-excludes")
      .writeAll(previousContents + privateClasses.mkString("\n"))
    // scalastyle:off println
    println("Created : .generated-mima-class-excludes in current directory.")
    val previousMembersContents = Try(File(".generated-mima-member-excludes").lines)
      .getOrElse(Iterator.empty).mkString("\n")
    File(".generated-mima-member-excludes").writeAll(previousMembersContents +
      privateMembers.mkString("\n"))
    println("Created : .generated-mima-member-excludes in current directory.")
    // scalastyle:on println
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
    val finder = ClassFinder(maybeOverrideAsmVersion = Some(Opcodes.ASM7))
    finder
      .getClasses
      .map(_.name)
      .filter(_.startsWith(packageName))
      .filterNot(shouldExclude)
      .toSet
  }
}
// scalastyle:on classforname
