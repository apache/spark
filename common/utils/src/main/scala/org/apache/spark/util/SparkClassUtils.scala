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
package org.apache.spark.util

import java.util.Random

import scala.util.Try

private[spark] trait SparkClassUtils {
  val random = new Random()

  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  // scalastyle:off classforname
  /**
   * Preferred alternative to Class.forName(className), as well as
   * Class.forName(className, initialize, loader) with current thread's ContextClassLoader.
   */
  def classForName[C](
      className: String,
      initialize: Boolean = true,
      noSparkClassLoader: Boolean = false): Class[C] = {
    if (!noSparkClassLoader) {
      Class.forName(className, initialize, getContextOrSparkClassLoader).asInstanceOf[Class[C]]
    } else {
      Class.forName(className, initialize, Thread.currentThread().getContextClassLoader).
        asInstanceOf[Class[C]]
    }
    // scalastyle:on classforname
  }

  /** Determines whether the provided class is loadable in the current thread. */
  def classIsLoadable(clazz: String): Boolean = {
    Try { classForName(clazz, initialize = false) }.isSuccess
  }

  /**
   * Determines whether the provided class is loadable in the current thread and assignable
   * from the target class.
   *
   * @param clazz the fully qualified class name of the class to check
   *              for loadability and inheritance from `parent`
   * @param targetClass the target class which the class represented. If target
   *               is null, only checks if the class is loadable
   * @return true if `clazz` is loadable and assignable from `target`, otherwise false
   */
  def classIsLoadableAndAssignableFrom(
      clazz: String,
      targetClass: Class[_]): Boolean = {
    Try {
      val cls = classForName(clazz, initialize = false)
      targetClass == null || targetClass.isAssignableFrom(cls)
    }.getOrElse(false)
  }
}

private[spark] object SparkClassUtils extends SparkClassUtils
