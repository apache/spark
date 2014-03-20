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
import scala.collection.JavaConversions.enumerationAsScalaIterator

/**
 * Mima(TODO: Paste URL here) generates a lot of false positives as it does not detect
 * private[x] as internal APIs.
 */
object GenerateMIMAIgnore {

  def classesWithPrivateWithin(packageName: String, excludePackages: Seq[String]): Set[String] = {
    import scala.reflect.runtime.universe.runtimeMirror
    val classLoader: ClassLoader = Thread.currentThread().getContextClassLoader
    val mirror = runtimeMirror(classLoader)
    val classes = Utils.getClasses(packageName, classLoader)
    val privateClasses = mutable.HashSet[String]()
    for (x <- classes) {
      try {
        // some of the classnames throw malformed class name exceptions and weird Match errors.
        if (excludePackages.forall(!x.startsWith(_)) &&
          mirror.staticClass(x).privateWithin.toString.trim != "<none>") {
          privateClasses += x
        }
      } catch {
        case e: Throwable => // println(e)
      }
    }
    privateClasses.toSet
  }

  def main(args: Array[String]) {
    scala.tools.nsc.io.File(".mima-exclude").
      writeAll(classesWithPrivateWithin("org.apache.spark", args).mkString("\n"))
    println("Created : .mima-exclude in current directory.")
  }

}

object Utils {

  /**
   * Get all classes in a package from a jar file.
   */
  def getAllClasses(jarPath: String, packageName: String) = {
    val jar = new JarFile(new File(jarPath))
    val enums = jar.entries().map(_.getName).filter(_.startsWith(packageName))
    val classes = mutable.HashSet[Class[_]]()
    for (entry <- enums) {
      if (!entry.endsWith("/") && !entry.endsWith("MANIFEST.MF") && !entry.endsWith("properties")) {
        try {
          classes += Class.forName(entry.trim.replaceAll(".class", "").replace('/', '.'))
        } catch {
          case e: Throwable => // println(e) // It may throw a few ClassNotFoundExceptions
        }
      }
    }
    classes
  }

  /**
   * Scans all classes accessible from the context class loader which belong to the given package
   * and subpackages both from directories and jars present on the classpath.
   */
  def getClasses(packageName: String,
      classLoader: ClassLoader = Thread.currentThread().getContextClassLoader): Set[String] = {
    val path = packageName.replace('.', '/')
    val resources = classLoader.getResources(path).toArray
    val jars = resources.filter(x => x.getProtocol == "jar")
      .map(_.getFile.split(":")(1).split("!")(0))
    val classesFromJars = jars.map(getAllClasses(_, path)).flatten
    val dirs = resources.filter(x => x.getProtocol == "file")
      .map(x => new File(x.getFile.split(":")(1)))
    val classFromDirs = dirs.map(findClasses(_, packageName)).flatten
    (classFromDirs ++ classesFromJars).map(_.getCanonicalName).filter(_ != null).toSet
  }

  private def findClasses(directory: File, packageName: String): Seq[Class[_]] = {
    val classes = mutable.ArrayBuffer[Class[_]]()
    if (!directory.exists()) {
      return classes
    }
    val files = directory.listFiles()
    for (file <- files) {
      if (file.isDirectory) {
        classes ++= findClasses(file, packageName + "." + file.getName)
      } else if (file.getName.endsWith(".class")) {
        classes += Class.forName(packageName + '.' + file.getName.substring(0,
          file.getName.length() - 6))
      }
    }
    classes
  }
}
