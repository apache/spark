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

package org.apache.spark.sql.columnar

import org.apache.spark.annotation.Evolving

/**
 * An implementation of a columnar user-defined function for an engine other than Spark, such as
 * a native engine that replaces Spark's operators. Spark does not interpret it: it only carries
 * it to the physical plan, where the engine that recognizes `kind` finds it.
 *
 * Whoever provides several implementations of a function makes sure that they return the same
 * results.
 *
 * @param kind the kind of the implementation, which names the engine or the format that it is
 *             for.
 * @param properties what the engine needs to find and to load the implementation, such as the
 *                   path of a library and the name of the function in it.
 * @since 4.4.0
 */
@Evolving
case class ColumnarUDFImplementation(kind: String, properties: Map[String, String])
