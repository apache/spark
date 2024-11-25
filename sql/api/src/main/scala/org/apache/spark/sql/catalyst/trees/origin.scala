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
package org.apache.spark.sql.catalyst.trees

import java.util.regex.Pattern

import org.apache.spark.QueryContext
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.util.ArrayImplicits._

/**
 * Contexts of TreeNodes, including location, SQL text, object type and object name. The only
 * supported object type is "VIEW" now. In the future, we may support SQL UDF or other objects
 * which contain SQL text.
 */
case class Origin(
    line: Option[Int] = None,
    startPosition: Option[Int] = None,
    startIndex: Option[Int] = None,
    stopIndex: Option[Int] = None,
    sqlText: Option[String] = None,
    objectType: Option[String] = None,
    objectName: Option[String] = None,
    stackTrace: Option[Array[StackTraceElement]] = None,
    pysparkErrorContext: Option[(String, String)] = None) {

  lazy val context: QueryContext = if (stackTrace.isDefined) {
    DataFrameQueryContext(stackTrace.get.toImmutableArraySeq, pysparkErrorContext)
  } else {
    SQLQueryContext(line, startPosition, startIndex, stopIndex, sqlText, objectType, objectName)
  }

  def getQueryContext: Array[QueryContext] = {
    Some(context).filter {
      case s: SQLQueryContext => s.isValid
      case _ => true
    }.toArray
  }
}

/**
 * Helper trait for objects that can be traced back to an [[Origin]].
 */
trait WithOrigin {
  def origin: Origin
}

/**
 * Provides a location for TreeNodes to ask about the context of their origin. For example, which
 * line of code is currently being parsed.
 */
object CurrentOrigin {
  private val value = new ThreadLocal[Origin]() {
    override def initialValue: Origin = Origin()
  }

  def get: Origin = value.get()
  def set(o: Origin): Unit = value.set(o)

  def reset(): Unit = value.set(Origin())

  def setPosition(line: Int, start: Int): Unit = {
    value.set(value.get.copy(line = Some(line), startPosition = Some(start)))
  }

  def withOrigin[A](o: Origin)(f: => A): A = {
    // remember the previous one so it can be reset to this
    // this way withOrigin can be recursive
    val previous = get
    set(o)
    val ret =
      try f
      finally { set(previous) }
    ret
  }

  /**
   * This helper function captures the Spark API and its call site in the user code from the
   * current stacktrace.
   *
   * As adding `withOrigin` explicitly to all Spark API definition would be a huge change,
   * `withOrigin` is used only at certain places where all API implementation surely pass through
   * and the current stacktrace is filtered to the point where first Spark API code is invoked
   * from the user code.
   *
   * As there might be multiple nested `withOrigin` calls (e.g. any Spark API implementations can
   * invoke other APIs) only the first `withOrigin` is captured because that is closer to the user
   * code.
   *
   * `withOrigin` has non-trivial performance overhead, since it collects a stack trace. This
   * feature can be disabled by setting "spark.sql.dataFrameQueryContext.enabled" to "false".
   *
   * @param f
   *   The function that can use the origin.
   * @return
   *   The result of `f`.
   */
  private[sql] def withOrigin[T](f: => T): T = {
    if (CurrentOrigin.get.stackTrace.isDefined || !SqlApiConf.get.dataFrameQueryContextEnabled) {
      f
    } else {
      val st = Thread.currentThread().getStackTrace
      var i = 0
      // Find the beginning of Spark code traces
      while (i < st.length && !sparkCode(st(i))) i += 1
      // Stop at the end of the first Spark code traces
      while (i < st.length && sparkCode(st(i))) i += 1
      val origin = Origin(
        stackTrace =
          Some(st.slice(from = i - 1, until = i + SqlApiConf.get.stackTracesInDataFrameContext)),
        pysparkErrorContext = PySparkCurrentOrigin.get())
      CurrentOrigin.withOrigin(origin)(f)
    }
  }

  private val sparkCodePattern = Pattern.compile("(org\\.apache\\.spark\\.sql\\." +
    "(?:api\\.)?" +
    "(?:functions|Column|ColumnName|SQLImplicits|Dataset|DataFrameStatFunctions|DatasetHolder)" +
    "(?:|\\..*|\\$.*))" +
    "|(scala\\.collection\\..*)")

  private def sparkCode(ste: StackTraceElement): Boolean = {
    sparkCodePattern.matcher(ste.getClassName).matches()
  }
}

/**
 * Provides detailed error context information on PySpark.
 */
object PySparkCurrentOrigin {
  private val pysparkErrorContext = new ThreadLocal[Option[(String, String)]]() {
    override def initialValue(): Option[(String, String)] = None
  }

  def set(fragment: String, callSite: String): Unit = {
    pysparkErrorContext.set(Some((fragment, callSite)))
  }

  def get(): Option[(String, String)] = pysparkErrorContext.get()

  def clear(): Unit = pysparkErrorContext.remove()
}
