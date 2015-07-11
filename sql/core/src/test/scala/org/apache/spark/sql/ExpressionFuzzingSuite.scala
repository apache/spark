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

package org.apache.spark.sql

import java.io.File

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.NullType
import org.clapper.classutil.ClassFinder

class ExpressionFuzzingSuite extends SparkFunSuite {
  lazy val expressionClasses: Seq[Class[_]] = {
    val finder = ClassFinder(
      System.getProperty("java.class.path").split(':').map(new File(_)) .filter(_.exists))
    val classes = finder.getClasses().toIterator
    ClassFinder.concreteSubclasses(classOf[Expression].getName, classes)
      .map(c => Class.forName(c.name)).toSeq
  }

  expressionClasses.foreach(println)
  for (c <- expressionClasses) {
    val singleExprConstructor = c.getConstructors.filter { c =>
      c.getParameterTypes.toSeq == Seq(classOf[Expression])
    }
    singleExprConstructor.foreach { cons =>
      try {
        val expr: Expression = cons.newInstance(Literal(null)).asInstanceOf[Expression]
        val row: InternalRow = new GenericInternalRow(Array[Any](null))
        val gened = GenerateMutableProjection.generate(Seq(expr), Seq(AttributeReference("f", NullType)()))
        gened()
//        expr.eval(row)
        println(s"Passed $c")
      } catch {
        case e: Exception =>
          println(s"Got exception $e while testing $c")
          e.printStackTrace(System.out)
      }

    }
  }
}
