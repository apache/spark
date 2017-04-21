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

package org.apache.spark.deploy.security

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Methods in [[HadoopAccessManager]] return scheduler-specific information related to how Hadoop
 * delegation tokens should be fetched.
 */
private[spark] trait HadoopAccessManager {

  /** The user allowed to renew delegation tokens */
  def getTokenRenewer: String

  /** The renewal interval, or [[None]] if the token shouldn't be renewed */
  def getTokenRenewalInterval: Option[Long]

  /** The set of hadoop file systems to fetch delegation tokens for */
  def hadoopFSsToAccess: Set[Path]
}

