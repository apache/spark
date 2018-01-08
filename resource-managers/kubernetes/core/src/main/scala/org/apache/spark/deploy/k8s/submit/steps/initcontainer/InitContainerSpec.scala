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
package org.apache.spark.deploy.k8s.submit.steps.initcontainer

import io.fabric8.kubernetes.api.model.{Container, HasMetadata, Pod}

/**
 * Represents a specification of the init-container for the driver pod.
 *
 * @param properties properties that should be set on the init-container
 * @param driverSparkConf Spark configuration properties that will be carried back to the driver
 * @param initContainer the init-container object
 * @param driverContainer the driver container object
 * @param driverPod the driver pod object
 * @param dependentResources resources the init-container depends on to work
 */
private[spark] case class InitContainerSpec(
    properties: Map[String, String],
    driverSparkConf: Map[String, String],
    initContainer: Container,
    driverContainer: Container,
    driverPod: Pod,
    dependentResources: Seq[HasMetadata])
