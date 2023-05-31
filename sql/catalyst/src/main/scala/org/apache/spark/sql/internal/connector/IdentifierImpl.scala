/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.internal.connector

import java.util.StringJoiner

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.Identifier

/**
 * An {@link Identifier} implementation.
 */
@Evolving
case class IdentifierImpl(namespace: Array[String], name: String)
  extends Identifier {

  assert(namespace != null, "Identifier namespace cannot be null")
  assert(name != null, "Identifier name cannot be null")

  override def toString: String = {
    val joiner = new StringJoiner(".")
    for (p <- namespace) {
      joiner.add(quoteIfNeeded(p))
    }
    joiner.add(quoteIfNeeded(name))
    joiner.toString
  }

}
