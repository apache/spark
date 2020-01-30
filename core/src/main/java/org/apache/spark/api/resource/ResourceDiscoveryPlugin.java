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

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.SparkConf;
import org.apache.spark.resource.ResourceInformation;
import org.apache.spark.resource.ResourceRequest;

/**
 * :: DeveloperApi ::
 * A plugin that can be dynamically loaded into a Spark application to control how custom
 * resources are discovered.
 * <p>
 * Plugins must implement the function discoveryResource.
 *
 * @since 3.0.0
 */
@DeveloperApi
public interface ResourceDiscoveryPlugin {
  ResourceInformation discoverResource(ResourceRequest request, SparkConf sparkConf);
}
