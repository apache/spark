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

package org.apache.spark

import org.apache.spark.annotation.Evolving

/**
 * Class to hold information about a type of Resource. A resource could be a GPU, FPGA, etc.
 * The units are resource specific and could be something like MB or GB for memory.
 * The array of addresses are resource specific and its up to the user to interpret the address.
 * The units and addresses could be empty if they don't apply to that resource.
 *
 * One example is GPUs, where the addresses would be the indices of the GPUs, the count would be the
 * number of GPUs and the units would be an empty string.
 *
 * @param name the name of the resource
 * @param units the units of the resources, can be an empty string if units don't apply
 * @param count the number of resources available
 * @param addresses an optional array of strings describing the addresses of the resource
 */
@Evolving
case class ResourceInformation(
    val name: String,
    val units: String,
    val count: Long,
    val addresses: Array[String] = Array.empty)
