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


private[spark] case class IgnoreCorruptFilesError(errorClassName: String, errorKeyMsg: String)

/**
 * Ignore corrupt files utility methods.
 */
private[spark] object IgnoreCorruptFilesUtils {

  def errorClassesStrToSeq(str: String): Seq[IgnoreCorruptFilesError] = {
    Option(str).filter(_.trim.nonEmpty).map { s =>
      s.split(",").map(_.trim).map {
        case each if each.nonEmpty =>
          val classInfo = each.split(":", 2).map(_.trim)
          val className = classInfo(0)
          val classValue = if (classInfo.length > 1) classInfo(1) else ""
          IgnoreCorruptFilesError(className, classValue)
      }.filter(_.errorClassName != "").toSeq
    }.getOrElse(Seq.empty)
  }

  private def containsErrorClass(
      errorClasses: Seq[IgnoreCorruptFilesError],
      error: Exception): Boolean = {
    Option(errorClasses).exists(_.exists { errorClass =>
      val classNameMatch = error.getClass.getName == errorClass.errorClassName
      classNameMatch && Option(error.getMessage).exists(_.contains(errorClass.errorKeyMsg))
    })
  }

  def ignoreCorruptFiles(
      ignoreCorruptFilesFlag: Boolean,
      errorClasses: Seq[IgnoreCorruptFilesError],
      error: Exception): Boolean = {
    if (ignoreCorruptFilesFlag) {
      errorClasses.isEmpty || containsErrorClass(errorClasses, error)
    } else {
      false
    }
  }
}
