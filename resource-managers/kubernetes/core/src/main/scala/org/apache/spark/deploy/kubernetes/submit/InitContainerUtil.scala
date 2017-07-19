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
package org.apache.spark.deploy.kubernetes.submit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.fabric8.kubernetes.api.model.{Container, Pod, PodBuilder}
import scala.collection.JavaConverters._

import org.apache.spark.deploy.kubernetes.constants._

private[spark] object InitContainerUtil {

  private val OBJECT_MAPPER = new ObjectMapper().registerModule(DefaultScalaModule)

  def appendInitContainer(originalPodSpec: Pod, initContainer: Container): Pod = {
    val resolvedInitContainers = originalPodSpec
      .getMetadata
      .getAnnotations
      .asScala
      .get(INIT_CONTAINER_ANNOTATION)
      .map { existingInitContainerAnnotation =>
        val existingInitContainers = OBJECT_MAPPER.readValue(
          existingInitContainerAnnotation, classOf[List[Container]])
        existingInitContainers ++ Seq(initContainer)
      }.getOrElse(Seq(initContainer))
    val resolvedSerializedInitContainers = OBJECT_MAPPER.writeValueAsString(resolvedInitContainers)
    new PodBuilder(originalPodSpec)
      .editMetadata()
        .removeFromAnnotations(INIT_CONTAINER_ANNOTATION)
        .addToAnnotations(INIT_CONTAINER_ANNOTATION, resolvedSerializedInitContainers)
        .endMetadata()
      .build()
  }
}
