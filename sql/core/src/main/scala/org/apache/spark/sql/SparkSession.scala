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

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.config.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.internal.{SessionState, SharedState}
import org.apache.spark.util.Utils


/**
 * The entry point to Spark execution.
 */
class SparkSession private(
    sparkContext: SparkContext,
    existingSharedState: Option[SharedState]) { self =>

  def this(sc: SparkContext) {
    this(sc, None)
  }

  /**
   * Start a new session where configurations, temp tables, temp functions etc. are isolated.
   */
  def newSession(): SparkSession = {
    // Note: materialize the shared state here to ensure the parent and child sessions are
    // initialized with the same shared state.
    new SparkSession(sparkContext, Some(sharedState))
  }

  @transient
  protected[sql] lazy val sharedState: SharedState = {
    existingSharedState.getOrElse(
      SparkSession.reflect[SharedState, SparkContext](
        SparkSession.sharedStateClassName(sparkContext.conf),
        sparkContext))
  }

  @transient
  protected[sql] lazy val sessionState: SessionState = {
    SparkSession.reflect[SessionState, SQLContext](
      SparkSession.sessionStateClassName(sparkContext.conf),
      new SQLContext(self, isRootContext = false))
  }

}


private object SparkSession {

  private def sharedStateClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => "org.apache.spark.sql.hive.HiveSharedState"
      case "in-memory" => classOf[SharedState].getCanonicalName
    }
  }

  private def sessionStateClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => "org.apache.spark.sql.hive.HiveSessionState"
      case "in-memory" => classOf[SessionState].getCanonicalName
    }
  }

  /**
   * Helper method to create an instance of [[T]] using a single-arg constructor that
   * accepts an [[Arg]].
   */
  private def reflect[T, Arg <: AnyRef](
      className: String,
      ctorArg: Arg)(implicit ctorArgTag: ClassTag[Arg]): T = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag.runtimeClass)
      ctor.newInstance(ctorArg).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

}
