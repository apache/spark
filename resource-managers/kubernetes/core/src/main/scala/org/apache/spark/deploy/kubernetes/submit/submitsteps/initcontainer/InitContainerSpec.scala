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
package org.apache.spark.deploy.kubernetes.submit.submitsteps.initcontainer

import io.fabric8.kubernetes.api.model.{Container, HasMetadata, Pod}

/**
 * Represents a given configuration of the init-container, informing the main
 * InitContainerBootstrapStep of how the driver should be configured. This includes:
 * <p>
 * - What properties should be set on the init-container,
 * - What Spark properties should be set on the driver's SparkConf given this init-container,
 * - The spec of the init container itself,
 * - The spec of the main container so that it can be modified to share volumes with the
 *   init-container
 * - The spec of the pod EXCEPT for the addition of the given init-container (e.g. volumes
 *   the init-container needs or modifications to a main container that shares data with the
 *   init-container),
 * - Any Kubernetes resources that need to be created for the init-container's function.
 */
private[spark] case class InitContainerSpec(
    initContainerProperties: Map[String, String],
    additionalDriverSparkConf: Map[String, String],
    initContainer: Container,
    driverContainer: Container,
    podToInitialize: Pod,
    initContainerDependentResources: Seq[HasMetadata])
