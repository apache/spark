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

description = "Spark Project Unsafe"

dependencies {
    // Spark internal module dependencies
    implementation(project(":common:tags"))
    implementation(project(":common:utils"))
    implementation(project(":common:utils-java"))
    implementation(project(":common:variant"))

    // ICU internationalization
    implementation("com.ibm.icu:icu4j:${SparkVersions.icu4j}")

    // Kryo serialization
    implementation("com.twitter:chill_${SparkVersions.scalaBinary}:${SparkVersions.chill}")
    implementation("com.esotericsoftware:kryo-shaded:${SparkVersions.kryo}")
    implementation("org.objenesis:objenesis:${SparkVersions.objenesis}")

    // Annotations
    implementation("com.google.code.findbugs:jsr305:${SparkVersions.jsr305}")

    // Provided dependencies
    compileOnly("org.slf4j:slf4j-api:${SparkVersions.slf4j}")

    // Test dependencies
    testImplementation("org.mockito:mockito-core:${SparkVersions.mockito}")
    testImplementation("net.bytebuddy:byte-buddy:${SparkVersions.byteBuddy}")
    testImplementation("net.bytebuddy:byte-buddy-agent:${SparkVersions.byteBuddy}")
    testImplementation("org.apache.commons:commons-lang3:${SparkVersions.commonsLang3}")
    testImplementation("org.apache.commons:commons-text:${SparkVersions.commonsText}")
}
