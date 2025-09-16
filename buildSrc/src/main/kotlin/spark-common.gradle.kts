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

/**
 * Common configuration for all Spark modules
 */

plugins {
    `java-library`
    scala
    `maven-publish`
}

group = "org.apache.spark"
version = SparkVersions.spark

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(SparkVersions.java))
    }
    withSourcesJar()
    withJavadocJar()
}

scala {
    zincVersion.set("1.9.5")
}

tasks.withType<ScalaCompile>().configureEach {
    scalaCompileOptions.apply {
        additionalParameters = listOf(
            "-feature",
            "-deprecation",
            "-Xfatal-warnings",
            "-unchecked",
            "-language:existentials",
            "-language:higherKinds",
            "-language:implicitConversions",
            "-Wconf:cat=other-match-analysis:error",
            "-Wconf:cat=unchecked:error",
            "-Wconf:cat=deprecation:error"
        )
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.apply {
        encoding = "UTF-8"
        compilerArgs.addAll(listOf(
            "-Xlint:all",
            "-Xlint:-serial",
            "-Xlint:-processing",
            "-parameters"
        ))
    }
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
    jvmArgs(
        "-Xmx4g",
        "-XX:+UseG1GC",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+UseCGroupMemoryLimitForHeap"
    )
}

repositories {
    mavenCentral()
    maven("https://repository.apache.org/content/repositories/snapshots/")
    maven("https://repository.apache.org/content/groups/staging/")
}

dependencies {
    // Common Scala dependencies
    implementation("org.scala-lang:scala-library:${SparkVersions.scala}")
    implementation("org.scala-lang:scala-reflect:${SparkVersions.scala}")

    // Common test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter:${SparkVersions.junit}")
    testImplementation("org.scalatest:scalatest_${SparkVersions.scalaBinary}:${SparkVersions.scalatest}")
    testImplementation("org.mockito:mockito-core:${SparkVersions.mockito}")
}

// Common publishing configuration
publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("Apache Spark")
                description.set("Apache Spark is a unified analytics engine for large-scale data processing.")
                url.set("https://spark.apache.org/")

                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.html")
                        distribution.set("repo")
                    }
                }

                organization {
                    name.set("Apache Software Foundation")
                    url.set("https://www.apache.org")
                }

                scm {
                    connection.set("scm:git:git@github.com:apache/spark.git")
                    developerConnection.set("scm:git:https://gitbox.apache.org/repos/asf/spark.git")
                    url.set("scm:git:git@github.com:apache/spark.git")
                }

                issueManagement {
                    system.set("JIRA")
                    url.set("https://issues.apache.org/jira/browse/SPARK")
                }
            }
        }
    }
}