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

import java.io._

import scala.util.matching.Regex

import org.objectweb.asm._
import org.objectweb.asm.commons._
import sbtassembly.Plugin._

/**
 * Relocates classes that match the configuration to a new location. Tries to match the options
 * available in the maven-shade-plugin.
 *
 * @param prefix Prefix that classes to be relocated must match.
 * @param shaded New prefix for classes that match.
 * @param includes Regexes for classes to include inside the matching package (empty = all).
 * @param excludes Regexes for classes to exclude from the matching package (empty = none).
 */
class Relocator(prefix: String, shaded: String, includes: Seq[Regex], excludes: Seq[Regex]) {

  /**
   * Renames a Java class name based on the configured rules for this relocator.
   *
   * @param name Class name to relocate.
   * @return Relocated name (may be same as original).
   */
  def rename(name: String): String = {
    val javaName = name.replace('/', '.')
    if (shouldRename(javaName)) {
      val renamed = shaded + javaName.substring(prefix.length())
      renamed.replace('.', '/')
    } else {
      name
    }
  }

  private def shouldRename(name: String) =
    name.startsWith(prefix) && isIncluded(name) && !isExcluded(name)

  private def isIncluded(name: String) =
    includes.isEmpty || !includes.filter { m => m.pattern.matcher(name).matches() }.isEmpty

  private def isExcluded(name: String) =
    !excludes.isEmpty && !excludes.filter { m => m.pattern.matcher(name).matches() }.isEmpty

}

class RelocatorRemapper(relocators: List[Relocator]) extends Remapper {

  override def mapValue(obj: Object) = {
    if (obj.isInstanceOf[String]) {
      rename(obj.asInstanceOf[String])
    } else {
      super.mapValue(obj)
    }
  }

  override def map(name: String) = {
    rename(name)
  }

  def rename(name: String): String = {
    var result = name
    relocators.foreach { r => result = r.rename(result) }
    result
  }

}

/**
 * Tries to emulate part of the class relocation behavior of maven-shade-plugin. Classes that
 * should be relocated are moved to a new location, and all classes are passed through the
 * remapper so that references to relocated classes are fixed.
 *
 * @param relocators List of relocators to apply to classes being shaded.
 */
class ShadeStrategy(relocators: List[Relocator]) extends MergeStrategy {

  private val remapper = new RelocatorRemapper(relocators)

  def name = "shade"

  override def apply(tempDir: File, path: String, files: Seq[File]) = {
    val (file, newPath) =
      if (relocators.isEmpty || !files.head.getAbsolutePath().endsWith(".class")) {
        (files.head, path)
      } else {
        val className = path.substring(0, path.length() - ".class".length())
        (remap(files.head, tempDir), remapper.rename(className) + ".class")
      }
    Right(Seq(file -> newPath))
  }

  private def remap(klass: File, tempDir: File): File = {
    var in: Option[FileInputStream] = None
    var out: Option[FileOutputStream] = None
    try {
      in = Some(new FileInputStream(klass))

      val writer = new ClassWriter(0)
      val visitor= new RemappingClassAdapter(writer, remapper)
      val reader = new ClassReader(in.get)
      reader.accept(visitor, ClassReader.EXPAND_FRAMES)

      val remappedPath = File.createTempFile(klass.getName(), null, tempDir)
      out = Some(new FileOutputStream(remappedPath))
      out.get.write(writer.toByteArray())
      out.get.close()

      remappedPath
    } finally {
      in.foreach { _.close() }
      out.foreach { _.close() }
    }
  }

}
