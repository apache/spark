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

/**
 * This Spark trait is used for mapping a given userName to a set of groups which it belongs to.
 * This is useful for specifying a common group of admins/developers to provide them admin, modify
 * and/or view access rights. Based on whether access control checks are enabled using
 * spark.acls.enable, every time a user tries to access or modify the application, the
 * SecurityManager gets the corresponding groups a user belongs to from the instance of the groups
 * mapping provider specified by the entry spark.user.groups.mapping.
 */

trait GroupMappingServiceProvider {

  /**
   * Get the groups the user belongs to.
   * @param userName User's Name
   * @return set of groups that the user belongs to. Empty in case of an invalid user.
   */
  def getGroups(userName : String) : Set[String]

}
