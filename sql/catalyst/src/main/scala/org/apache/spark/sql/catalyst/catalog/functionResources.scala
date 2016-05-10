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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.AnalysisException

/** An trait that represents the type of a resourced needed by a function. */
abstract class FunctionResourceType(val resourceType: String)

object JarResource extends FunctionResourceType("jar")

object FileResource extends FunctionResourceType("file")

// We do not allow users to specify a archive because it is YARN specific.
// When loading resources, we will throw an exception and ask users to
// use --archive with spark submit.
object ArchiveResource extends FunctionResourceType("archive")

object FunctionResourceType {
  def fromString(resourceType: String): FunctionResourceType = {
    resourceType.toLowerCase match {
      case "jar" => JarResource
      case "file" => FileResource
      case "archive" => ArchiveResource
      case other =>
        throw new AnalysisException(s"Resource Type '$resourceType' is not supported.")
    }
  }
}

case class FunctionResource(resourceType: FunctionResourceType, uri: String)

/**
 * A simple trait representing a class that can be used to load resources used by
 * a function. Because only a SQLContext can load resources, we create this trait
 * to avoid of explicitly passing SQLContext around.
 */
trait FunctionResourceLoader {
  def loadResource(resource: FunctionResource): Unit
}

object DummyFunctionResourceLoader extends FunctionResourceLoader {
  override def loadResource(resource: FunctionResource): Unit = {
    throw new UnsupportedOperationException
  }
}
