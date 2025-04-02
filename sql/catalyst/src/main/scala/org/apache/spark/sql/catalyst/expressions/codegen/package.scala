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

package org.apache.spark.sql.catalyst.expressions

import scala.reflect.internal.util.AbstractFileClassLoader

import org.apache.spark.sql.catalyst.rules
import org.apache.spark.util.Utils

/**
 * A collection of generators that build custom bytecode at runtime for performing the evaluation
 * of catalyst expression.
 */
package object codegen {

  /** Canonicalizes an expression so those that differ only by names can reuse the same code. */
  object ExpressionCanonicalizer extends rules.RuleExecutor[Expression] {
    val batches =
      Batch("CleanExpressions", FixedPoint(20), CleanExpressions) :: Nil

    object CleanExpressions extends rules.Rule[Expression] {
      def apply(e: Expression): Expression = e transform {
        case Alias(c, _) => c
      }
    }
  }

  /**
   * Dumps the bytecode from a class to the screen using javap.
   */
  object DumpByteCode {
    import scala.sys.process._
    val dumpDirectory = Utils.createTempDir()
    dumpDirectory.mkdir()

    def apply(obj: Any): Unit = {
      val generatedClass = obj.getClass
      val classLoader =
        generatedClass
          .getClassLoader
          .asInstanceOf[AbstractFileClassLoader]
      val generatedBytes = classLoader.classBytes(generatedClass.getName)

      val packageDir = new java.io.File(dumpDirectory, generatedClass.getPackage.getName)
      if (!packageDir.exists()) { packageDir.mkdir() }

      val classFile =
        new java.io.File(packageDir, generatedClass.getName.split("\\.").last + ".class")

      val outfile = new java.io.FileOutputStream(classFile)
      outfile.write(generatedBytes)
      outfile.close()

      // scalastyle:off println
      println(
        s"javap -p -v -classpath ${dumpDirectory.getCanonicalPath} ${generatedClass.getName}".!!)
      // scalastyle:on println
    }
  }
}
