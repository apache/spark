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

package org.apache.spark.launcher

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Exposes methods from the launcher library that are used by the YARN backend.
 */
private[spark] object YarnCommandBuilderUtils {

  def quoteForBatchScript(arg: String): String = {
    CommandBuilderUtils.quoteForBatchScript(arg)
  }

  /**
   * Adds the perm gen configuration to the list of java options if needed and not yet added.
   *
   * Note that this method adds the option based on the local JVM version; if the node where
   * the container is running has a different Java version, there's a risk that the option will
   * not be added (e.g. if the AM is running Java 8 but the container's node is set up to use
   * Java 7).
   */
  def addPermGenSizeOpt(args: ListBuffer[String]): Unit = {
    CommandBuilderUtils.addPermGenSizeOpt(args.asJava)
  }

}
