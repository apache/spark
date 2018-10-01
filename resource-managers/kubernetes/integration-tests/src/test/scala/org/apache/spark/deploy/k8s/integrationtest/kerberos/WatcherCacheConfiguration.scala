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
package org.apache.spark.deploy.k8s.integrationtest.kerberos

/**
 * A collection of functions that together represent a WatcherCache. The functin of these
 * WatcherCaches are to watch the KerberosStorage object and insure they are properly created
 * by blocking with a condition.
 */
private[spark] trait WatcherCacheConfiguration[T <: KerberosStorage] {

  /**
   * This function defines the boolean condition which would block the
   * completion of the deploy() block
   */
  def check(name: String): Boolean

  /**
   * This functions deploys the KerberosStorage object by having the KubernetesClient
   * create the resulting KerberosStorage object.
   */
  def deploy(storage: T) : Unit

  /**
   * This function closes all Watcher threads.
   */
  def stopWatch(): Unit
}
