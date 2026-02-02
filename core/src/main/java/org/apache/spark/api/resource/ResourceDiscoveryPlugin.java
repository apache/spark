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

package org.apache.spark.api.resource;

import java.util.Optional;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.SparkConf;
import org.apache.spark.resource.ResourceInformation;
import org.apache.spark.resource.ResourceRequest;

/**
 * :: DeveloperApi ::
 * A plugin that can be dynamically loaded into a Spark application to control how custom
 * resources are discovered. Plugins can be chained to allow different plugins to handle
 * different resource types.
 * <p>
 * Plugins must implement the function discoveryResource.
 *
 * @since 3.0.0
 */
@DeveloperApi
public interface ResourceDiscoveryPlugin {
  /**
   * Discover the addresses of the requested resource.
   * <p>
   * This method is called early in the initialization of the Spark Executor/Driver/Worker.
   * This function is responsible for discovering the addresses of the resource which Spark will
   * then use for scheduling and eventually providing to the user.
   * Depending on the deployment mode and and configuration of custom resources, this could be
   * called by the Spark Driver, the Spark Executors, in standalone mode the Workers, or all of
   * them. The ResourceRequest has a ResourceID component that can be used to distinguish which
   * component it is called from and what resource its being called for.
   * This will get called once for each resource type requested and its the responsibility of
   * this function to return enough addresses of that resource based on the request. If
   * the addresses do not meet the requested amount, Spark will fail.
   * If this plugin doesn't handle a particular resource, it should return an empty Optional
   * and Spark will try other plugins and then last fall back to the default discovery script
   * plugin.
   *
   * @param request The ResourceRequest that to be discovered.
   * @param sparkConf SparkConf
   * @return An {@link Optional} containing a {@link ResourceInformation} object containing
   * the resource name and the addresses of the resource. If it returns {@link Optional#EMPTY}
   * other plugins will be called.
   */
  Optional<ResourceInformation> discoverResource(ResourceRequest request, SparkConf sparkConf);
}
