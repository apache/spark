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
   * Returns true if and only if the underlying class is a member class.
   *
   * Note: jdk8u throws a "Malformed class name" error if a given class is a deeply-nested
   * inner class (See SPARK-34607 for details). This issue has already been fixed in jdk9+, so
   * we can remove this helper method safely if we drop the support of jdk8u.
   */
  def isMemberClass(cls: Class[_]): Boolean = {
    try {
      cls.isMemberClass
    } catch {
      case _: InternalError =>
        // We emulate jdk8u `Class.isMemberClass` below:
        //   public boolean isMemberClass() {
        //     return getSimpleBinaryName() != null && !isLocalOrAnonymousClass();
        //   }
        // `getSimpleBinaryName()` returns null if a given class is a top-level class,
        // so we replace it with `cls.getEnclosingClass != null`. The second condition checks
        // if a given class is not a local or an anonymous class, so we replace it with
        // `cls.getEnclosingMethod == null` because `cls.getEnclosingMethod()` return a value
        // only in either case (JVM Spec 4.8.6).
        //
        // Note: The newer jdk evaluates `!isLocalOrAnonymousClass()` first,
        // we reorder the conditions to follow it.
        cls.getEnclosingMethod == null && cls.getEnclosingClass != null
    }
  }
}

private[spark] object SparkClassUtils extends SparkClassUtils
