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

// Root project - no plugins applied directly

group = "org.apache.spark"
version = SparkVersions.spark
description = "Apache Spark Project Parent POM"

repositories {
    mavenCentral()
    maven("https://repository.apache.org/content/repositories/snapshots/")
    maven("https://repository.apache.org/content/groups/staging/")
}

// Configure all subprojects
subprojects {
    apply(plugin = "spark-common")

    // Common dependency management
    configurations.all {
        resolutionStrategy {
            // Force specific versions to avoid conflicts
            force(
                "org.scala-lang:scala-library:${SparkVersions.scala}",
                "org.scala-lang:scala-reflect:${SparkVersions.scala}",
                "org.scala-lang:scala-compiler:${SparkVersions.scala}",
                "org.apache.hadoop:hadoop-client-api:${SparkVersions.hadoop}",
                "org.apache.hadoop:hadoop-client-runtime:${SparkVersions.hadoop}",
                "com.google.protobuf:protobuf-java:${SparkVersions.protobuf}",
                "org.slf4j:slf4j-api:${SparkVersions.slf4j}",
                "org.apache.logging.log4j:log4j-core:${SparkVersions.log4j}",
                "org.apache.logging.log4j:log4j-api:${SparkVersions.log4j}"
            )
        }
    }

    // Exclude problematic transitive dependencies
    configurations.all {
        exclude(group = "javax.servlet", module = "servlet-api")
        exclude(group = "org.mortbay.jetty", module = "servlet-api")
        exclude(group = "commons-beanutils", module = "commons-beanutils-core")
        exclude(group = "org.apache.curator", module = "curator-recipes")
        exclude(group = "org.jboss.netty")
        exclude(group = "io.netty", module = "netty")
    }
}

// Global tasks
tasks.register("assembleAll") {
    description = "Assembles all subprojects"
    group = "build"
    dependsOn(subprojects.map { "${it.path}:assemble" })
}

tasks.register("testAll") {
    description = "Runs tests in all subprojects"
    group = "verification"
    dependsOn(subprojects.map { "${it.path}:test" })
}

tasks.register("publishAll") {
    description = "Publishes all subprojects"
    group = "publishing"
    dependsOn(subprojects.map { "${it.path}:publish" })
}

// Build profiles support
val profiles = project.findProperty("profiles")?.toString()?.split(",") ?: emptyList()

// Configure based on active profiles
if (profiles.contains("hadoop-provided")) {
    subprojects {
        configurations.all {
            exclude(group = "org.apache.hadoop")
        }
    }
}

if (profiles.contains("hive-provided")) {
    subprojects {
        configurations.all {
            exclude(group = "org.apache.hive")
            exclude(group = "org.spark-project.hive")
        }
    }
}

if (profiles.contains("derby-provided")) {
    subprojects {
        configurations.all {
            exclude(group = "org.apache.derby")
        }
    }
}

// Task to display project structure
tasks.register("projectStructure") {
    description = "Displays the project structure"
    group = "help"
    doLast {
        println("Spark Project Structure:")
        subprojects.forEach { project ->
            println("  ${project.path}")
        }
    }
}