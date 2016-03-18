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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.analysis._

private[spark] trait CatalystConf {
  def caseSensitiveAnalysis: Boolean

  /**
   * Returns the [[Resolver]] for the current configuration, which can be used to determin if two
   * identifiers are equal.
   */
  def resolver: Resolver = {
    if (caseSensitiveAnalysis) {
      caseSensitiveResolution
    } else {
      caseInsensitiveResolution
    }
  }
}

/**
 * A trivial conf that is empty.  Used for testing when all
 * relations are already filled in and the analyser needs only to resolve attribute references.
 */
object EmptyConf extends CatalystConf {
  override def caseSensitiveAnalysis: Boolean = {
    throw new UnsupportedOperationException
  }
}

/** A CatalystConf that can be used for local testing. */
case class SimpleCatalystConf(caseSensitiveAnalysis: Boolean) extends CatalystConf {
}
