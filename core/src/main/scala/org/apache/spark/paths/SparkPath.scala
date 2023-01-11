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

package org.apache.spark.paths

import java.net.URI

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonSerializer, SerializerProvider}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * A SparkPath is the result of a URI.toString call.
 */
@JsonSerialize(using = classOf[SparkPathJsonSerializer])
@JsonDeserialize(using = classOf[SparkPathJsonDeserializer])
case class SparkPath private (private val underlying: String) {
  def uriEncoded: String = underlying
  def toUri: URI = new URI(underlying)
  def toPath: Path = new Path(toUri)
  override def toString: String = underlying
}

object SparkPath {
  def fromPathString(str: String): SparkPath = fromPath(new Path(str))
  def fromPath(path: Path): SparkPath = fromUri(path.toUri)
  def fromFileStatus(fs: FileStatus): SparkPath = fromPath(fs.getPath)
  def fromUri(uri: URI): SparkPath = fromUriString(uri.toString)
  def fromUriString(str: String): SparkPath = SparkPath(str)
}

class SparkPathJsonSerializer extends JsonSerializer[SparkPath] {
  override def serialize(
      value: SparkPath,
      gen: JsonGenerator,
      serializers: SerializerProvider): Unit = {
    gen.writeString(value.uriEncoded)
  }
}
class SparkPathJsonDeserializer extends JsonDeserializer[SparkPath] {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): SparkPath = {
    SparkPath.fromUriString(p.readValueAs(classOf[String]))
  }
}
