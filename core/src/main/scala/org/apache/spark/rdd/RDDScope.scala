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

import org.apache.spark.annotation.RDDScoped

/**
 * A collection of utility methods to construct a hierarchical representation of RDD scopes.
 * An RDD scope tracks the series of operations that created a given RDD.
 */
private[spark] object RDDScope {

  // Symbol for delimiting each level of the hierarchy
  // e.g. grandparent;parent;child
  val SCOPE_NESTING_DELIMITER = ";"

  // Symbol for delimiting the scope name from the ID within each level
  val SCOPE_NAME_DELIMITER = "_"

  // Counter for generating scope IDs, for differentiating
  // between different scopes of the same name
  private val scopeCounter = new AtomicInteger(0)

  // Consider only methods that belong to these classes as potential RDD operations
  // This is to limit the amount of reflection we do when we traverse the stack trace
  private val classesWithScopeMethods = Set(
    "org.apache.spark.SparkContext",
    "org.apache.spark.rdd.RDD",
    "org.apache.spark.rdd.PairRDDFunctions",
    "org.apache.spark.rdd.AsyncRDDActions"
  )

  /**
   * Make a globally unique scope ID from the scope name.
   *
   * For instance:
   *   textFile -> textFile_0
   *   textFile -> textFile_1
   *   map -> map_2
   *   name;with_sensitive;characters -> name-with-sensitive-characters_3
   */
  private def makeScopeId(name: String): String = {
    name.replace(SCOPE_NESTING_DELIMITER, "-").replace(SCOPE_NAME_DELIMITER, "-") +
      SCOPE_NAME_DELIMITER + scopeCounter.getAndIncrement
  }

  /**
   * Retrieve the hierarchical scope from the stack trace when an RDD is first created.
   *
   * This considers all methods marked with the @RDDScoped annotation and chains them together
   * in the order they are invoked. Each level in the scope hierarchy represents a unique
   * invocation of a particular RDD operation.
   *
   * For example: treeAggregate_0;reduceByKey_1;combineByKey_2;mapPartitions_3
   * This means this RDD is created by the user calling treeAggregate, which calls
   * `reduceByKey`, and then `combineByKey`, and then `mapPartitions` to create this RDD.
   */
  private[spark] def getScope: Option[String] = {

    // TODO: Note that this approach does not correctly associate the same invocation across RDDs
    // For instance, a call to `textFile` creates both a HadoopRDD and a MapPartitionsRDD, but
    // there is no way to associate the invocation across these two RDDs to draw the same scope
    // around them. This is because the stack trace simply does not provide information for us
    // to make any reasonable association across RDDs. We may need a higher level approach that
    // involves setting common variables before and after the RDD operation itself.

    val rddScopeNames = Thread.currentThread.getStackTrace
      // Avoid reflecting on all classes in the stack trace
      .filter { ste => classesWithScopeMethods.contains(ste.getClassName) }
      // Return the corresponding method if it has the @RDDScoped annotation
      .flatMap { ste =>
        // Note that this is an approximation since we match the method only by name
        // Unfortunate we cannot be more precise because the stack trace does not include
        // parameter information
        Class.forName(ste.getClassName).getDeclaredMethods.find { m =>
          m.getName == ste.getMethodName &&
          m.getDeclaredAnnotations.exists { a =>
            a.annotationType() == classOf[RDDScoped]
          }
        }
      }
      // Use the method name as the scope name for now
      .map { m => m.getName }

    // It is common for such methods to internally invoke other methods with the same name
    // as aliases (e.g. union, reduceByKey). Here we remove adjacent duplicates such that
    // the scope chain does not capture this (e.g. a, a, b, c, b, c, c => a, b, c, b, c).
    // This is surprisingly difficult to express even in Scala.
    var prev: String = null
    val dedupedRddScopeNames = rddScopeNames.flatMap { n =>
      if (n != prev) {
        prev = n
        Some(n)
      } else {
        None
      }
    }

    // Chain scope IDs to denote hierarchy, with outermost scope first
    val rddScopeIds = dedupedRddScopeNames.map(makeScopeId)
    if (rddScopeIds.nonEmpty) {
      Some(rddScopeIds.reverse.mkString(SCOPE_NESTING_DELIMITER))
    } else {
      None
    }
  }

}
