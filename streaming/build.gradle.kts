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

// Module-specific dependencies based on pom.xml

dependencies {
    // Core Spark dependencies
    implementation(project(":core"))
    implementation(project(":common:tags"))     // Provides @DeveloperApi annotation
    implementation(project(":common:utils"))    // Provides EnumUtil class
    implementation(project(":common:utils-java"))  // Provides LogKeys class

    // Scala runtime and modules
    implementation("org.scala-lang:scala-library:${SparkVersions.scala}")
    implementation("org.scala-lang:scala-reflect:${SparkVersions.scala}")
    implementation("org.scala-lang.modules:scala-xml_${SparkVersions.scalaBinary}:${SparkVersions.scalaXml}")
    implementation("org.scala-lang.modules:scala-parallel-collections_${SparkVersions.scalaBinary}:${SparkVersions.scalaParallelCollections}")

    // JAX-RS for REST APIs
    implementation("jakarta.ws.rs:jakarta.ws.rs-api:3.0.0")

    // Jakarta Servlet API
    implementation("jakarta.servlet:jakarta.servlet-api:${SparkVersions.jakartaServlet}")

    // Python integration
    implementation("net.sf.py4j:py4j:${SparkVersions.py4j}")

    // Kryo serialization
    implementation("com.esotericsoftware:kryo-shaded:${SparkVersions.kryo}")

    // Hadoop for Path class
    implementation("org.apache.hadoop:hadoop-client-api:${SparkVersions.hadoop}")
    implementation("org.apache.hadoop:hadoop-common:${SparkVersions.hadoop}")

    // Test dependencies
    testImplementation(project(":common:tags", "tests"))
    testImplementation(project(":core", "tests"))
}

// Configure Scala compilation to suppress deprecation warnings
tasks.withType<org.gradle.api.tasks.scala.ScalaCompile>().configureEach {
    scalaCompileOptions.additionalParameters = listOf(
        "-Wconf:cat=deprecation:w"
    )
}
