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

import java.io.ObjectInput

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.config
import org.apache.spark.shuffle.api.Location
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Factory for creating the [[Location]] of [[MapStatus]]. It creates [[BlockManagerId]]
 * by default if the custom location is not configured.
 */
private[spark] class MapStatusLocationFactory(conf: SparkConf) {
  private val locationConstructor = {
    val locationBaseClass = classOf[Location]
    conf.get(config.SHUFFLE_LOCATION_PLUGIN_CLASS).map { className =>
      val clazz = Utils.classForName(className)
      require(locationBaseClass.isAssignableFrom(clazz),
        s"$className is not a subclass of ${locationBaseClass.getName}.")
      try {
        clazz.getConstructor()
      } catch {
        case _: NoSuchMethodException =>
          throw new SparkException(s"$className did not have a zero-argument constructor.")
      }
    }.orNull
  }

  // The cache is for reusing the same location instance for the equal locations,
  // which helps reduce the objects in JVM.
  val locationCache: LoadingCache[Location, Location] = CacheBuilder.newBuilder()
    .maximumSize(conf.get(config.SHUFFLE_LOCATION_CACHE_SIZE))
    .build(
      new CacheLoader[Location, Location]() {
        override def load(loc: Location): Location = loc
      }
    )

  def load(in: ObjectInput): Location = Utils.tryOrIOException {
    locationCache.get {
      Option(locationConstructor).map { ctr =>
        val loc = ctr.newInstance().asInstanceOf[Location]
        loc.readExternal(in)
        loc
      }.getOrElse(BlockManagerId(in))
    }
  }
}
