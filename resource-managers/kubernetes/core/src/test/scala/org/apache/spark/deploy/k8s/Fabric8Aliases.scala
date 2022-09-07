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
package org.apache.spark.deploy.k8s

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapList, HasMetadata, PersistentVolumeClaim, PersistentVolumeClaimList, Pod, PodList}
import io.fabric8.kubernetes.client.dsl.{FilterWatchListDeletable, MixedOperation, NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable, PodResource, Resource}

object Fabric8Aliases {
  type PODS = MixedOperation[Pod, PodList, PodResource[Pod]]
  type CONFIG_MAPS = MixedOperation[
    ConfigMap, ConfigMapList, Resource[ConfigMap]]
  type LABELED_PODS = FilterWatchListDeletable[Pod, PodList]
  type LABELED_CONFIG_MAPS = FilterWatchListDeletable[ConfigMap, ConfigMapList]
  type SINGLE_POD = PodResource[Pod]
  type RESOURCE_LIST = NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable[
    HasMetadata]
  type PERSISTENT_VOLUME_CLAIMS = MixedOperation[PersistentVolumeClaim, PersistentVolumeClaimList,
    Resource[PersistentVolumeClaim]]
  type LABELED_PERSISTENT_VOLUME_CLAIMS =
    FilterWatchListDeletable[PersistentVolumeClaim, PersistentVolumeClaimList]
}
