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

addSbtPlugin("software.purpledragon" % "sbt-checkstyle-plugin" % "4.0.0")

// sbt-checkstyle-plugin uses an old version of checkstyle. Match it to Maven's.
// If you are changing the dependency setting for checkstyle plugin,
// please check pom.xml in the root of the source tree too.
libraryDependencies += "com.puppycrawl.tools" % "checkstyle" % "9.3"

// checkstyle uses guava 31.0.1-jre.
libraryDependencies += "com.google.guava" % "guava" % "31.0.1-jre"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.0.0")

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.4")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.0")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")

libraryDependencies += "org.ow2.asm"  % "asm" % "9.4"

libraryDependencies += "org.ow2.asm"  % "asm-commons" % "9.4"

addSbtPlugin("com.simplytyped" % "sbt-antlr4" % "0.8.3")

addSbtPlugin("com.typesafe.sbt" % "sbt-pom-reader" % "2.2.0")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
