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

package org.apache.spark.scheduler

import org.apache.spark.resource.{ResourceAllocator, ResourceInformation}

/**
 * Class to hold information about a type of Resource on an Executor. This information is managed
 * by SchedulerBackend, and TaskScheduler shall schedule tasks on idle Executors based on the
 * information.
 * @param name Resource name
 * @param addresses Resource addresses provided by the executor
 */
private[spark] class ExecutorResourceInfo(
    name: String,
    addresses: Seq[String])
  extends ResourceInformation(name, addresses.toArray) with ResourceAllocator {

  override protected def resourceName = this.name
  override protected def resourceAddresses = this.addresses
  def totalAddressesAmount: Int = this.addresses.length

}
