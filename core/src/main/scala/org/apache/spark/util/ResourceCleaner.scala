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

import scala.collection.mutable.ArrayBuffer

/**
 * Util for ensuring that resources get properly cleaned up.  This lets code create a resource and
 * also indicate code that *should* be run later to clean the resource up (eg. in a finally
 * block for exception handling).  Note that this does not do clean anything up itself.
 */
private[spark] trait ResourceCleaner {
  def addCleaner(f: () => Unit): Unit
}

/**
 * Basic ResourceCleaner that just stores cleaner functions.  The user must ensure that
 * {{doCleanup}} is invoked inside a {{finally}} block.
 */
private[spark] class SimpleResourceCleaner extends ResourceCleaner {
  private val cleanerFuncs = new ArrayBuffer[() => Unit]

  override def addCleaner(f: () => Unit): Unit = {
    cleanerFuncs += f
  }

  def doCleanup(): Unit = {
    cleanerFuncs.foreach{ f => f() }
  }
}
