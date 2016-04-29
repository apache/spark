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

package org.apache.spark.security

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * This class is responsible for getting the groups for a particular user in Unix based
 * environments. This implementation uses the Unix Shell based id command to fetch the user groups
 * for the specified user. It does not cache the user groups as the invocations are expected
 * to be infrequent.
 */

private[spark] class ShellBasedGroupsMappingProvider extends GroupMappingServiceProvider
  with Logging {

  override def getGroups(username: String): Set[String] = {
    val userGroups = getUnixGroups(username)
    logDebug("User: " + username + " Groups: " + userGroups.mkString(","))
    userGroups
  }

  // shells out a "bash -c id -Gn username" to get user groups
  private def getUnixGroups(username: String): Set[String] = {
    val cmdSeq = Seq("bash", "-c", "id -Gn " + username)
    // we need to get rid of the trailing "\n" from the result of command execution
    Utils.executeAndGetOutput(cmdSeq).stripLineEnd.split(" ").toSet
  }
}
