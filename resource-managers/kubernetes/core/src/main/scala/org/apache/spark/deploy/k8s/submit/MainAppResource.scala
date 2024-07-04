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
package org.apache.spark.deploy.k8s.submit

import org.apache.spark.annotation.{DeveloperApi, Since, Stable}

/**
 * :: DeveloperApi ::
 *
 * All traits and classes in this file are used by K8s module and Spark K8s operator.
 */

@Stable
@DeveloperApi
@Since("2.3.0")
sealed trait MainAppResource

@Stable
@DeveloperApi
@Since("2.4.0")
sealed trait NonJVMResource

@Stable
@DeveloperApi
@Since("3.0.0")
case class JavaMainAppResource(primaryResource: Option[String])
  extends MainAppResource

@Stable
@DeveloperApi
@Since("2.4.0")
case class PythonMainAppResource(primaryResource: String)
  extends MainAppResource with NonJVMResource

@Stable
@DeveloperApi
@Since("2.4.0")
case class RMainAppResource(primaryResource: String)
  extends MainAppResource with NonJVMResource
