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

package org.apache.spark.graphframes

// All the public exceptions thrown by GraphFrame methods

/**
 * Exception thrown when a pattern String for motif finding cannot be parsed.
 */
class InvalidParseException(message: String) extends Exception(message)

/**
 * Thrown when a GraphFrame algorithm is given a vertex ID which does not exist in the graph.
 */
class NoSuchVertexException(message: String) extends Exception(message)

/**
 * Exception thrown when a parsed pattern for motif finding cannot be translated into a DataFrame
 * query.
 */
class InvalidPatternException() extends Exception()

/**
 * Exception that should not be reachable
 */
class GraphFramesUnreachableException()
    extends Exception("This exception should not be reachable")

/**
 * Exception thrown when an invalid property group is encountered.
 *
 * This exception typically indicates that an operation or configuration is using a property group
 * that is not supported, invalid, or improperly defined.
 *
 * @param message
 *   A detailed error message describing the issue.
 */
class InvalidPropertyGroupException(message: String) extends Exception(message)

/**
 * Exception thrown when the graph is invalid, e.g. duplicate vertices, inconsistency between
 * vertex set and edges src / dst, etc.
 *
 * @param message
 *   A descriptive error message providing details about why the graph operation is invalid.
 */
class InvalidGraphException(message: String) extends Exception(message)

class GraphFramesW2VException(message: String) extends Exception(message)

class GraphFramesUnsupportedVertexTypeException(message: String) extends Exception(message)

/**
 * Exception thrown when a Spark version requirement is not met.
 *
 * @param version
 *   The minimum version of Apache Spark required.
 */
class GraphFramesSparkVersionException(version: String)
    extends Exception(
      s"Called GraphFrames feature requires at least $version or above version of Apache Spark")
