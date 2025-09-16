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

import org.gradle.api.tasks.scala.ScalaCompile

plugins {
    scala
    `java-library`
}

description = "Spark Project Catalyst"

dependencies {
    // Core Spark modules - ensuring proper dependencies as defined in Maven pom.xml
    implementation(project(":core"))
    implementation(project(":sql:api")) {
        // Exclude spark-connect-shims as per Maven exclusion
        exclude(group = "org.apache.spark", module = "spark-connect-shims_${SparkVersions.scalaBinary}")
    }
    implementation(project(":common:utils"))        // For Logging
    implementation(project(":common:utils-java"))   // For LogKeys
    implementation(project(":common:unsafe"))
    implementation(project(":common:sketch"))
    implementation(project(":common:variant"))      // For QueryContext
    implementation(project(":common:tags"))
    implementation(project(":common:network-common")) // For network utils

    // Scala runtime and reflection
    implementation("org.scala-lang:scala-library:${SparkVersions.scala}")
    implementation("org.scala-lang:scala-reflect:${SparkVersions.scala}")
    implementation("org.scala-lang.modules:scala-parallel-collections_${SparkVersions.scalaBinary}:${SparkVersions.scalaParallelCollections}")

    // Janino - code generation
    implementation("org.codehaus.janino:janino:${SparkVersions.janino}")
    implementation("org.codehaus.janino:commons-compiler:${SparkVersions.janino}")

    // Apache Commons
    implementation("commons-codec:commons-codec:${SparkVersions.commonsCodec}")

    // Univocity parsers for CSV processing
    implementation("com.univocity:univocity-parsers:${SparkVersions.univocityParsers}")

    // XML Schema support
    implementation("org.apache.ws.xmlschema:xmlschema-core:${SparkVersions.xmlschemaCore}")

    // DataSketches for approximate algorithms
    implementation("org.apache.datasketches:datasketches-java:${SparkVersions.datasketches}")

    // JSON processing
    implementation("org.json4s:json4s-jackson_${SparkVersions.scalaBinary}:${SparkVersions.json4s}")
    implementation("org.json4s:json4s-core_${SparkVersions.scalaBinary}:${SparkVersions.json4s}")

    // Kryo serialization
    implementation("com.esotericsoftware:kryo-shaded:${SparkVersions.kryo}")

    // Hadoop
    implementation("org.apache.hadoop:hadoop-client-api:${SparkVersions.hadoop}")

    // SLF4J logging
    implementation("org.slf4j:slf4j-api:${SparkVersions.slf4j}")

    // JSR305 annotations (Nullable)
    implementation("com.google.code.findbugs:jsr305:${SparkVersions.jsr305}")

    // Avro
    implementation("org.apache.avro:avro:${SparkVersions.avro}")

    // Guava for cache
    implementation("com.google.guava:guava:${SparkVersions.guava}")

    // Jackson Scala module
    implementation("com.fasterxml.jackson.core:jackson-databind:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.module:jackson-module-scala_${SparkVersions.scalaBinary}:${SparkVersions.jackson}")

    // Codahale Metrics
    implementation("io.dropwizard.metrics:metrics-core:${SparkVersions.codahaleMetrics}")

    // Apache Commons Text for StringEscapeUtils
    implementation("org.apache.commons:commons-text:${SparkVersions.commonsText}")

    // Scala Parser Combinators for JSON path parsing
    implementation("org.scala-lang.modules:scala-parser-combinators_${SparkVersions.scalaBinary}:${SparkVersions.scalaParserCombinators}")

    // LZ4 compression
    implementation("org.lz4:lz4-java:${SparkVersions.lz4}")

    // Apache Commons Math3 for random number generation
    implementation("org.apache.commons:commons-math3:${SparkVersions.commonsMath3}")

    // Scala XML for XML processing
    implementation("org.scala-lang.modules:scala-xml_${SparkVersions.scalaBinary}:${SparkVersions.scalaXml}")

    // Launcher module for SparkLauncher
    implementation(project(":launcher"))

    // More complete Hadoop client
    implementation("org.apache.hadoop:hadoop-client-runtime:${SparkVersions.hadoop}")

    // Complete Arrow dependencies for vector operations
    implementation("org.apache.arrow:arrow-memory-core:${SparkVersions.arrow}")

    // We already inherit these from sql:api but need to be explicit
    implementation("org.apache.arrow:arrow-vector:${SparkVersions.arrow}")
    implementation("org.apache.arrow:arrow-memory-netty:${SparkVersions.arrow}")

    // Test dependencies
    testImplementation(project(":core", "tests"))
    testImplementation(project(":common:tags", "tests"))
    testImplementation("org.scalatest:scalatest_${SparkVersions.scalaBinary}:${SparkVersions.scalatest}")
    testImplementation("org.mockito:mockito-core:${SparkVersions.mockito}")
    testImplementation("net.bytebuddy:byte-buddy:${SparkVersions.byteBuddy}")
    testImplementation("net.bytebuddy:byte-buddy-agent:${SparkVersions.byteBuddy}")
    testImplementation("org.scalacheck:scalacheck_${SparkVersions.scalaBinary}:1.18.1")
}

// Configure mixed Scala/Java compilation
tasks.withType<ScalaCompile>().configureEach {
    // Include Java sources in Scala compilation so Java can reference Scala classes
    source(sourceSets["main"].java.srcDirs)
    source(sourceSets["main"].scala.srcDirs)

    // Increase memory for large SQL catalyst compilation
    scalaCompileOptions.forkOptions.jvmArgs = listOf("-Xmx8g", "-XX:MaxMetaspaceSize=2g")

    // Suppress deprecation warnings during Maven to Gradle migration
    scalaCompileOptions.additionalParameters = listOf(
        "-Wconf:cat=deprecation:w"
    )
}

// Prevent Java compilation task from running separately since Scala compiler handles both
tasks.named("compileJava") {
    enabled = false
}

// Configure test jar creation to match Maven behavior
tasks.named<Jar>("jar") {
    archiveBaseName.set("spark-catalyst_${SparkVersions.scalaBinary}")
}

// Create a test jar similar to Maven's test-jar goal
val testJar by tasks.registering(Jar::class) {
    archiveBaseName.set("spark-catalyst_${SparkVersions.scalaBinary}")
    archiveClassifier.set("tests")
    from(sourceSets.test.get().output)
}

configurations {
    create("testJar") {
        extendsFrom(configurations.testImplementation.get())
    }
}

artifacts {
    add("testJar", testJar)
}
