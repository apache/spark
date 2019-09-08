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

package org.apache.spark.sql.hive.thriftserver

object ServiceUtils {

  /*
   * Get the index separating the user name from domain name (the user's name up
   * to the first '/' or '@').
   *
   * @param userName full user name.
   * @return index of domain match or -1 if not found
   */
  def indexOfDomainMatch(userName: String): Int = {
    if (userName == null) {
      return -1
    }
    val idx = userName.indexOf('/')
    val idx2 = userName.indexOf('@')
    var endIdx = Math.min(idx, idx2) // Use the earlier match.
    // Unless at least one of '/' or '@' was not found, in
    // which case, user the latter match.
    if (endIdx == -1) {
      endIdx = Math.max(idx, idx2)
    }
    endIdx
  }
}
