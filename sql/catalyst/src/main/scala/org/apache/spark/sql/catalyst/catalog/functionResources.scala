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

sealed trait FunctionResourceType

object JarResource extends FunctionResourceType

object FileResource extends FunctionResourceType

object ArchiveResource extends FunctionResourceType

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

trait FunctionResourceLoader {
  def loadResource(resource: FunctionResource): Unit
}

class DummyFunctionResourceLoader extends FunctionResourceLoader {
  override def loadResource(resource: FunctionResource): Unit = {
    throw new UnsupportedOperationException
  }
}
