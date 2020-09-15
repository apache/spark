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

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude, JsonPropertyOrder}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Objects

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

/**
 * A general, named code block representing an operation that instantiates RDDs.
 *
 * All RDDs instantiated in the corresponding code block will store a pointer to this object.
 * Examples include, but will not be limited to, existing RDD operations, such as textFile,
 * reduceByKey, and treeAggregate.
 *
 * An operation scope may be nested in other scopes. For instance, a SQL query may enclose
 * scopes associated with the public RDD APIs it uses under the hood.
 *
 * There is no particular relationship between an operation scope and a stage or a job.
 * A scope may live inside one stage (e.g. map) or span across multiple jobs (e.g. take).
 */
@JsonInclude(Include.NON_ABSENT)
@JsonPropertyOrder(Array("id", "name", "parent"))
private[spark] class RDDOperationScope(
                                        val name: String,
                                        val parent: Option[RDDOperationScope] = None,
                                        val id: String = RDDOperationScope.nextScopeId().toString) {

  def toJson: String = {
    RDDOperationScope.jsonMapper.writeValueAsString(this)
  }

  /**
   * Return a list of scopes that this scope is a part of, including this scope itself.
   * The result is ordered from the outermost scope (eldest ancestor) to this scope.
   */
  @JsonIgnore
  def getAllScopes: Seq[RDDOperationScope] = {
    parent.map(_.getAllScopes).getOrElse(Seq.empty) ++ Seq(this)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case s: RDDOperationScope =>
        id == s.id && name == s.name && parent == s.parent
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(id, name, parent)

  override def toString: String = toJson
}

/**
 * A collection of utility methods to construct a hierarchical representation of RDD scopes.
 * An RDD scope tracks the series of operations that created a given RDD.
 */
private[spark] object RDDOperationScope extends Logging {
  private val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val scopeCounter = new AtomicInteger(0)

  def fromJson(s: String): RDDOperationScope = {
    jsonMapper.readValue(s, classOf[RDDOperationScope])
  }

  /** Return a globally unique operation scope ID. */
  def nextScopeId(): Int = scopeCounter.getAndIncrement

  /**
   * Execute the given body such that all RDDs created in this body will have the same scope.
   * The name of the scope will be the first method name in the stack trace that is not the
   * same as this method's.
   *
   * Note: Return statements are NOT allowed in body.
   *
   */
  private[spark] def withScope[T](
                                   sc: SparkContext,
                                   allowNesting: Boolean = false)(body: => T): T = {

    ///设置跟踪堆的轨迹的scope名字
    val ourMethodName: String = "withScope"


    // 获取当前线程的
    val callerMethodName: String = Thread.currentThread.getStackTrace()
      //移除前几个匹配断言函数的元素，移除该线程下的方法名不是ourMethodName（withScope）的前几个(以ourMethodName分割)元素
      .dropWhile(_.getMethodName != ourMethodName) //首先会调用 getStackTrace
      // 获取线程中方法名不是 ourMethodName（withScope）的方法名
      .find(_.getMethodName != ourMethodName)
      .map(_.getMethodName) // 最终得到的结果 就是具体的调用算子（map reduce ...）
      .getOrElse {
        // Log a warning just in case, but this should almost certainly never happen
        // 异常日志 ，通常情况下不会触发
        logWarning("No valid method name for this RDD operation scope!")
        "N/A"
      }



    //调用withScope 方法
    withScope[T](sc, callerMethodName, allowNesting, ignoreParent = false)(body)
  }

  /**
   * Execute the given body such that all RDDs created in this body will have the same scope.
   *
   * If nesting is allowed, any subsequent calls to this method in the given body will instantiate
   * child scopes that are nested within our scope. Otherwise, these calls will take no effect.
   *
   * Additionally, the caller of this method may optionally ignore the configurations and scopes
   * set by the higher level caller. In this case, this method will ignore the parent caller's
   * intention to disallow nesting, and the new scope instantiated will not have a parent. This
   * is useful for scoping physical operations in Spark SQL, for instance.
   *
   * Note: Return statements are NOT allowed in body.
   */
  private[spark] def withScope[T](
                                   sc: SparkContext, // sparkcontext
                                   name: String, //方法名
                                   allowNesting: Boolean, // 是否允许嵌套
                                   ignoreParent: Boolean
                                 )
                                 (body: => T): T = {
    // Save the old scope to restore it later
    val scopeKey: String = SparkContext.RDD_SCOPE_KEY
    val noOverrideKey: String = SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY
    val oldScopeJson: String = sc.getLocalProperty(scopeKey)
    val oldScope: Option[RDDOperationScope] = Option(oldScopeJson).map(RDDOperationScope.fromJson)
    val oldNoOverride: String = sc.getLocalProperty(noOverrideKey)
    try {
      if (ignoreParent) {
        // Ignore all parent settings and scopes and start afresh with our own root scope
        sc.setLocalProperty(scopeKey, new RDDOperationScope(name).toJson)
      } else if (sc.getLocalProperty(noOverrideKey) == null) {
        // Otherwise, set the scope only if the higher level caller allows us to do so
        sc.setLocalProperty(scopeKey, new RDDOperationScope(name, oldScope).toJson)
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
