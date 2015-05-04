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

package org.apache.spark.rdd

import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.SparkContext

/**
 * A general, named code block representing an operation that instantiates RDDs.
 *
 * All RDDs instantiated in the corresponding code block will store a pointer to this object.
 * Examples include, but will not be limited to, existing RDD operations, such as textFile,
 * reduceByKey, and treeAggregate.
 *
 * An operator scope may be nested in other scopes. For instance, a SQL query may enclose
 * scopes associated with the public RDD APIs it uses under the hood.
 *
 * There is no particular relationship between an operator scope and a stage or a job.
 * A scope may live inside one stage (e.g. map) or span across multiple jobs (e.g. take).
 */
private[spark] class OperatorScope(val name: String, parent: Option[OperatorScope] = None) {
  val id: Int = OperatorScope.nextScopeId()

  def toJson: String = {
    OperatorScope.jsonMapper.writeValueAsString(this)
  }

  /**
   * Return a list of scopes that this scope is a part of, including this scope itself.
   * The result is ordered from the outermost scope (eldest ancestor) to this scope.
   */
  @JsonIgnore
  def getAllScopes: Seq[OperatorScope] = {
    parent.map(_.getAllScopes).getOrElse(Seq.empty) ++ Seq(this)
  }
}

/**
 * A collection of utility methods to construct a hierarchical representation of RDD scopes.
 * An RDD scope tracks the series of operations that created a given RDD.
 */
private[spark] object OperatorScope {
  private val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val scopeCounter = new AtomicInteger(0)

  def fromJson(s: String): OperatorScope = {
    jsonMapper.readValue(s, classOf[OperatorScope])
  }

  /** Return a globally unique operator scope ID. */
  def nextScopeId(): Int = scopeCounter.getAndIncrement

  /**
   * Execute the given body such that all RDDs created in this body will have the same scope.
   * The name of the scope will be the name of the method that immediately encloses this one.
   *
   * Note: Return statements are NOT allowed in body.
   */
  private[spark] def withScope[T](
      sc: SparkContext,
      allowNesting: Boolean = false)(body: => T): T = {
    val callerMethodName = Thread.currentThread.getStackTrace()(3).getMethodName
    withScope[T](sc, callerMethodName, allowNesting)(body)
  }

  /**
   * Execute the given body such that all RDDs created in this body will have the same scope.
   *
   * If nesting is allowed, this concatenates the previous scope with the new one in a way that
   * signifies the hierarchy. Otherwise, if nesting is not allowed, then any children calls to
   * this method executed in the body will have no effect.
   *
   * Note: Return statements are NOT allowed in body.
   */
  private[spark] def withScope[T](
      sc: SparkContext,
      name: String,
      allowNesting: Boolean = false)(body: => T): T = {
    // Save the old scope to restore it later
    val scopeKey = SparkContext.RDD_SCOPE_KEY
    val noOverrideKey = SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY
    val oldScopeJson = sc.getLocalProperty(scopeKey)
    val oldScope = Option(oldScopeJson).map(OperatorScope.fromJson)
    val oldNoOverride = sc.getLocalProperty(noOverrideKey)
    try {
      // Set the scope only if the higher level caller allows us to do so
      if (sc.getLocalProperty(noOverrideKey) == null) {
        sc.setLocalProperty(scopeKey, new OperatorScope(name, oldScope).toJson)
      }
      // Optionally disallow the child body to override our scope
      if (!allowNesting) {
        sc.setLocalProperty(noOverrideKey, "true")
      }
      body
    } finally {
      // Remember to restore any state that was modified before exiting
      sc.setLocalProperty(scopeKey, oldScopeJson)
      sc.setLocalProperty(noOverrideKey, oldNoOverride)
    }
  }
}
