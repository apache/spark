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
import org.gradle.api.plugins.antlr.AntlrTask

plugins {
    scala
    `java-library`
    antlr
}

description = "Spark Project SQL API"

dependencies {
    // Scala runtime
    implementation("org.scala-lang:scala-library:${SparkVersions.scala}")
    implementation("org.scala-lang:scala-reflect:${SparkVersions.scala}")
    implementation("org.scala-lang.modules:scala-parser-combinators_${SparkVersions.scalaBinary}:${SparkVersions.scalaParserCombinators}")

    // Core Spark modules - need these for annotations and LogKeys
    implementation(project(":common:utils"))
    implementation(project(":common:utils-java"))  // For LogKeys
    implementation(project(":common:unsafe"))
    implementation(project(":common:sketch"))
    implementation(project(":common:tags"))        // For annotations like @Stable, @DeveloperApi
    implementation(project(":sql:connect:shims"))

    // Apache Commons
    implementation("org.apache.commons:commons-lang3:${SparkVersions.commonsLang3}")

    // JSON processing
    implementation("org.json4s:json4s-jackson_${SparkVersions.scalaBinary}:${SparkVersions.json4s}")
    implementation("com.fasterxml.jackson.core:jackson-databind:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.module:jackson-module-scala_${SparkVersions.scalaBinary}:${SparkVersions.jackson}")

    // ANTLR for SQL parsing
    implementation("org.antlr:antlr4-runtime:${SparkVersions.antlr4}")

    // Apache Arrow
    implementation("org.apache.arrow:arrow-vector:${SparkVersions.arrow}")
    implementation("org.apache.arrow:arrow-memory-netty:${SparkVersions.arrow}")

    // Kryo serialization
    implementation("com.esotericsoftware:kryo-shaded:${SparkVersions.kryo}")

    // Apache Ivy for artifact resolution
    implementation("org.apache.ivy:ivy:${SparkVersions.ivy}")

    // ANTLR for SQL parsing
    antlr("org.antlr:antlr4:${SparkVersions.antlr4}")

    // JSR305 annotations (Nullable, Nonnull)
    implementation("com.google.code.findbugs:jsr305:${SparkVersions.jsr305}")
}

// Configure mixed Scala/Java compilation
tasks.withType<ScalaCompile>().configureEach {
    // Include Java sources in Scala compilation so Java can reference Scala classes
    source(sourceSets["main"].java.srcDirs)
    source(sourceSets["main"].scala.srcDirs)

    // Exclude ANTLR intermediate files that are not source code
    exclude("**/*.interp")
    exclude("**/*.tokens")

    // Suppress deprecation warnings during Maven to Gradle migration
    scalaCompileOptions.additionalParameters = listOf(
        "-Wconf:cat=deprecation:w"
    )
}

// Prevent Java compilation task from running separately since Scala compiler handles both
tasks.named("compileJava") {
    enabled = false
}

tasks.named<Jar>("jar") {
    archiveBaseName.set("spark-sql-api_${SparkVersions.scalaBinary}")
}

// Configure ANTLR code generation
tasks.named<AntlrTask>("generateGrammarSource") {
    arguments = arguments + listOf("-visitor", "-package", "org.apache.spark.sql.catalyst.parser")
    outputDirectory = layout.buildDirectory.dir("generated-src/antlr/main/org/apache/spark/sql/catalyst/parser").get().asFile
}

// Ensure ANTLR generation happens before Scala compilation
tasks.named("compileScala") {
    dependsOn("generateGrammarSource")
}
