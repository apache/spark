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

package org.apache.spark.annotation

import scala.annotation.StaticAnnotation
import scala.annotation.meta._

/**
 * A Scala annotation that specifies the Spark version when a definition was added.
 * Different from the `@since` tag in JavaDoc, this annotation does not require explicit JavaDoc and
 * hence works for overridden methods that inherit API documentation directly from parents.
 * The limitation is that it does not show up in the generated Java API documentation.
 */
@param @field @getter @setter @beanGetter @beanSetter
private[spark] class Since(version: String) extends StaticAnnotation
