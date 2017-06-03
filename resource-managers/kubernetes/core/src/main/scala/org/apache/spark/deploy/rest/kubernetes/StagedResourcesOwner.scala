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
package org.apache.spark.deploy.rest.kubernetes

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

object StagedResourcesOwnerType extends Enumeration {
  type OwnerType = Value
  // In more generic scenarios, we might want to be watching Deployments, etc.
  val Pod = Value
}

class StagedResourcesOwnerTypeReference extends TypeReference[StagedResourcesOwnerType.type]

case class StagedResourcesOwner(
    ownerNamespace: String,
    ownerLabels: Map[String, String],
    @JsonScalaEnumeration(classOf[StagedResourcesOwnerTypeReference])
        ownerType: StagedResourcesOwnerType.OwnerType)
