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

package org.apache.spark.deploy.history.yarn.integration

import java.io.{FileNotFoundException, InputStream}
import java.net.URI

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.`type`.TypeFactory
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.sun.jersey.api.client.config.ClientConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntities, TimelineEntity}

import org.apache.spark.Logging
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.server.TimelineQueryClient

/**
 * A timeline query client which is driven by a resource.
 *
 * All entities are expected to be of the entity type used in list requests; there's no
 * attempt to support multiple entity types.
 *
 * @param resource resource within test classpath
 * @param timelineURI URI of the timeline service
 * @param conf configuration
 * @param jerseyClientConfig jersey client config
 */
class ResourceDrivenTimelineQueryClient(
    resource: String,
    timelineURI: URI,
    conf: Configuration,
    jerseyClientConfig: ClientConfig)
    extends TimelineQueryClient(timelineURI, conf, jerseyClientConfig) with Logging {

  /** Loaded timeline entity instance */
  private var entities: TimelineEntities = _

  /** list of entities within */
  private var entityList: List[TimelineEntity] = _

  init()

  /**
   * load in the JSON
   */
  private def init(): Unit = {
    val in = this.getClass.getClassLoader.getResourceAsStream(resource)
    require(in != null, s"Failed to load resource $resource")
    val mapper = new ObjectMapper() with ScalaObjectMapper
    // make deserializer use JAXB annotations (only)
    mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(TypeFactory.defaultInstance))
    entities = mapper.readValue[TimelineEntities](in)
    entityList = entities.getEntities.asScala.toList

    logDebug(s"$entities entities")
    entityList.foreach { e =>
      log.debug(s"${describeEntity(e)}")
    }
  }

  override def endpointCheck (): Unit = {}

  /**
   * List entities. No filtering is performed.
   * @return a possibly empty list of entities
   */
  override def listEntities(
      entityType: String,
      primaryFilter: Option[(String, String)],
      secondaryFilters: Map[String, String],
      fields: Seq[String],
      limit: Option[Long],
      windowStart: Option[Long],
      windowEnd: Option[Long],
      fromId: Option[String],
      fromTs: Option[Long]): List[TimelineEntity] = {
    entityList
  }

  /**
   * Get an entity
   * @param entityType type
   * @param entityId the entity
   * @return the entity if it was found
   */
  override def getEntity(entityType: String, entityId: String): TimelineEntity = {
    entityList.find(_.getEntityId == entityId) match {
      case Some(e) => e
      case None => throw new FileNotFoundException(s"No entity $entityId")
    }
  }
}
