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

package org.apache.spark.sql.catalyst.catalog

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TempViewAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.StringUtils


/**
 * A thread-safe manager for a list of temp views, providing atomic operations to manage temp views.
 * Note that, the temp view name is always case-sensitive here, callers are responsible to format
 * the view name w.r.t. case-sensitivity config.
 */
class TempViewManager {

  /** List of temporary views, mapping from view name to logical plan. */
  @GuardedBy("this")
  private val tempViews = new mutable.HashMap[String, LogicalPlan]

  def get(name: String): Option[LogicalPlan] = synchronized {
    tempViews.get(name)
  }

  def create(
      name: String,
      viewDefinition: LogicalPlan,
      overrideIfExists: Boolean): Unit = synchronized {
    if (!overrideIfExists && tempViews.contains(name)) {
      throw new TempViewAlreadyExistsException(name)
    }
    tempViews.put(name, viewDefinition)
  }

  def update(
      name: String,
      viewDefinition: LogicalPlan): Boolean = synchronized {
    // Only update it when the view with the given name exits.
    if (tempViews.contains(name)) {
      tempViews.put(name, viewDefinition)
      true
    } else {
      false
    }
  }

  def remove(name: String): Boolean = synchronized {
    tempViews.remove(name).isDefined
  }

  def rename(oldName: String, newName: String): Boolean = synchronized {
    if (tempViews.contains(oldName)) {
      if (tempViews.contains(newName)) {
        throw new AnalysisException(
          s"rename temporary view from '$oldName' to '$newName': destination view already exists")
      }

      val viewDefinition = tempViews(oldName)
      tempViews.remove(oldName)
      tempViews.put(newName, viewDefinition)
      true
    } else {
      false
    }
  }

  def listNames(pattern: String): Seq[String] = synchronized {
    StringUtils.filterPattern(tempViews.keys.toSeq, pattern)
  }

  def clear(): Unit = synchronized {
    tempViews.clear()
  }
}
