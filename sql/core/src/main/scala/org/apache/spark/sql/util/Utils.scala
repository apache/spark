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

package org.apache.spark.sql.util

import java.lang.reflect.InvocationTargetException

import scala.util.{Failure, Success, Try}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils.classForName

object Utils extends Logging {

  /**
   * Create instances of extension classes.
   *
   * The classes in the given list must:
   * - Be sub-classes of the given base class.
   * - Provide either a no-arg constructor, or a 1-arg constructor that takes a SparkConf.
   *
   * The constructors are allowed to throw "UnsupportedOperationException" if the extension does not
   * want to be registered; this allows the implementations to check the Spark configuration (or
   * other state) and decide they do not need to be added. A log message is printed in that case.
   * Other exceptions are bubbled up.
   */
  def loadExtensions[T <: AnyRef](
      extClass: Class[T],
      classes: Seq[String],
      conf: SparkConf,
      sqlConf: SQLConf): Seq[T] = {
    classes.flatMap { name =>
      try {
        val klass = classForName[T](name)
        require(extClass.isAssignableFrom(klass),
          s"$name is not a subclass of ${extClass.getName()}.")

        val ext = Try(klass.getConstructor(classOf[SparkConf])) match {
          case Success(ctor) =>
            ctor.newInstance(conf)

          case Failure(_) =>
            Try(klass.getConstructor(classOf[SQLConf])) match {
              case Success(ctor) =>
                ctor.newInstance(sqlConf)
              case Failure(_) =>
                klass.getConstructor().newInstance()
            }
        }

        Some(ext)
      } catch {
        case _: NoSuchMethodException =>
          throw new SparkException(
            s"$name did not have a zero-argument constructor or a" +
              " single-argument constructor that accepts SparkConf. Note: if the class is" +
              " defined inside of another Scala class, then its constructors may accept an" +
              " implicit parameter that references the enclosing class; in this case, you must" +
              " define the class as a top-level class in order to prevent this extra" +
              " parameter from breaking Spark's ability to find a valid constructor.")

        case e: InvocationTargetException =>
          e.getCause() match {
            case uoe: UnsupportedOperationException =>
              logDebug(s"Extension $name not being initialized.", uoe)
              logInfo(s"Extension $name not being initialized.")
              None

            case null => throw e

            case cause => throw cause
          }
      }
    }
  }
}
