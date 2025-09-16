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

description = "Spark Project Shuffle Streaming Service"

dependencies {
    // Spark internal modules
    implementation(project(":common:network-common"))
    implementation(project(":common:utils-java"))
    implementation(project(":common:tags"))

    // Apache Commons
    implementation("org.apache.commons:commons-lang3:${SparkVersions.commonsLang3}")

    // Netty dependencies - needed for direct usage of ByteBuf and other Netty classes
    implementation("io.netty:netty-all:${SparkVersions.netty}")

    // Jackson dependencies - needed for direct usage of ObjectMapper and annotations
    implementation("com.fasterxml.jackson.core:jackson-databind:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.core:jackson-annotations:${SparkVersions.jackson}")

    // Metrics
    implementation("io.dropwizard.metrics:metrics-core:${SparkVersions.codahaleMetrics}")

    // Provided dependencies
    implementation("org.slf4j:slf4j-api:${SparkVersions.slf4j}") // Note: was 'provided' in Maven
    implementation("com.google.guava:guava:33.4.0-jre")
    implementation("org.roaringbitmap:RoaringBitmap:${SparkVersions.roaringBitmap}")

    // Test dependencies
    testImplementation("org.apache.logging.log4j:log4j-api:${SparkVersions.log4j}")
    testImplementation("org.apache.logging.log4j:log4j-core:${SparkVersions.log4j}")
    testImplementation("org.apache.logging.log4j:log4j-1.2-api:${SparkVersions.log4j}")
    testImplementation("org.apache.logging.log4j:log4j-slf4j2-impl:${SparkVersions.log4j}")
    testImplementation("org.mockito:mockito-core:${SparkVersions.mockito}")
    testImplementation("net.bytebuddy:byte-buddy")
    testImplementation("net.bytebuddy:byte-buddy-agent")
    testImplementation("commons-io:commons-io:${SparkVersions.commonsIo}")
}
