/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.auth

import java.util.Locale


abstract class SaslQOP(saslQop: String) {
  override def toString: String = saslQop
}

object SaslQOP {

  def values: Seq[SaslQOP] = STR_TO_ENUM.values.toSeq

  private val STR_TO_ENUM: Map[String, SaslQOP] = Map(
    "auth" -> AUTH,
    "auth-int" -> AUTH_INT,
    "auth-conf" -> AUTH_CONF
  )

  def fromString(str: String): SaslQOP = {
    if (str == null) {
      throw new IllegalArgumentException("Unknown auth type: " + str +
        " Allowed values are: " + STR_TO_ENUM.keySet)
    } else {
      val saslQOP = STR_TO_ENUM.get(str.toLowerCase(Locale.ROOT))
      if (saslQOP.isEmpty) {
        throw new IllegalArgumentException("Unknown auth type: " + str +
          " Allowed values are: " + STR_TO_ENUM.keySet)
      } else {
        saslQOP.get
      }
    }

  }

  // Authentication only.
  case object AUTH extends SaslQOP("auth")

  // Authentication and integrity checking by using signatures.
  case object AUTH_INT extends SaslQOP("auth-int")

  // Authentication, integrity and confidentiality checking by using signatures and encryption.
  case object AUTH_CONF extends SaslQOP("auth-conf")

}
