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

package org.apache.spark.ml.util

import java.util.UUID

import org.apache.spark.annotation.DeveloperApi


/**
 * :: DeveloperApi ::
 *
 * Trait for an object with an immutable unique ID that identifies itself and its derivatives.
 *
 * WARNING: There have not yet been final discussions on this API, so it may be broken in future
 *          releases.
 */
@DeveloperApi
trait Identifiable {

  /**
   * An immutable unique ID for the object and its derivatives.
   */
  val uid: String

  override def toString: String = uid
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
object Identifiable {

  /**
   * Returns a random UID that concatenates the given prefix, "_", and 12 random hex chars.
   */
  def randomUID(prefix: String): String = {
    prefix + "_" + UUID.randomUUID().toString.takeRight(12)
  }
}
