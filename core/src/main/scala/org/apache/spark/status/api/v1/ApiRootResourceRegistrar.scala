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
package org.apache.spark.status.api.v1

/**
 * Contributes JAX-RS root resource classes to the status REST API served under `/api`.
 *
 * The REST API registers its resources explicitly instead of scanning the
 * `org.apache.spark.status.api.v1` package, so a root resource that lives outside spark-core
 * (for example in sql/core, streaming or Spark Connect) has to be contributed through this
 * trait. Implementations are discovered with [[java.util.ServiceLoader]]: list the
 * implementation class in
 * `META-INF/services/org.apache.spark.status.api.v1.ApiRootResourceRegistrar`.
 *
 * A contributed root resource may use the same `@Path` as [[ApiRootResource]] (`/v1`); Jersey
 * merges root resources that share a path.
 */
private[spark] trait ApiRootResourceRegistrar {

  /** The JAX-RS root resource classes (annotated with `@Path`) to register under `/api`. */
  def rootResources: Seq[Class[_]]
}
