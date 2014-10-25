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

import sbt._
import sbt.Keys.version

import com.typesafe.tools.mima.core._
import com.typesafe.tools.mima.core.MissingClassProblem
import com.typesafe.tools.mima.core.MissingTypesProblem
import com.typesafe.tools.mima.core.ProblemFilters._
import com.typesafe.tools.mima.plugin.MimaKeys.{binaryIssueFilters, previousArtifact}
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings


object MimaBuild {

  def excludeMember(fullName: String) = Seq(
      ProblemFilters.exclude[MissingMethodProblem](fullName),
      // Sometimes excluded methods have default arguments and 
      // they are translated into public methods/fields($default$) in generated
      // bytecode. It is not possible to exhaustively list everything.
      // But this should be okay.
      ProblemFilters.exclude[MissingMethodProblem](fullName+"$default$2"),
      ProblemFilters.exclude[MissingMethodProblem](fullName+"$default$1"),
      ProblemFilters.exclude[MissingFieldProblem](fullName),
      ProblemFilters.exclude[IncompatibleResultTypeProblem](fullName),
      ProblemFilters.exclude[IncompatibleMethTypeProblem](fullName),
      ProblemFilters.exclude[IncompatibleFieldTypeProblem](fullName)
    )

  // Exclude a single class and its corresponding object
  def excludeClass(className: String) = Seq(
      excludePackage(className),
      ProblemFilters.exclude[MissingClassProblem](className),
      ProblemFilters.exclude[MissingTypesProblem](className),
      excludePackage(className + "$"),
      ProblemFilters.exclude[MissingClassProblem](className + "$"),
      ProblemFilters.exclude[MissingTypesProblem](className + "$")
    )

  // Exclude a Spark class, that is in the package org.apache.spark
  def excludeSparkClass(className: String) = {
    excludeClass("org.apache.spark." + className)
  }

  // Exclude a Spark package, that is in the package org.apache.spark
  def excludeSparkPackage(packageName: String) = {
    excludePackage("org.apache.spark." + packageName)
  }

  def ignoredABIProblems(base: File, currentSparkVersion: String) = {

    // Excludes placed here will be used for all Spark versions
    val defaultExcludes = Seq()

    // Read package-private excludes from file
    val classExcludeFilePath = file(base.getAbsolutePath + "/.generated-mima-class-excludes")
    val memberExcludeFilePath = file(base.getAbsolutePath + "/.generated-mima-member-excludes")

    val ignoredClasses: Seq[String] =
      if (!classExcludeFilePath.exists()) {
        Seq()
      } else {
        IO.read(classExcludeFilePath).split("\n")
      }

    val ignoredMembers: Seq[String] =
      if (!memberExcludeFilePath.exists()) {
      Seq()
    } else {
      IO.read(memberExcludeFilePath).split("\n")
    }

    defaultExcludes ++ ignoredClasses.flatMap(excludeClass) ++
    ignoredMembers.flatMap(excludeMember) ++ MimaExcludes.excludes(currentSparkVersion)
  }

  def mimaSettings(sparkHome: File, projectRef: ProjectRef) = {
    val organization = "org.apache.spark"
    val previousSparkVersion = "1.1.0"
    val fullId = "spark-" + projectRef.project + "_2.10"
    mimaDefaultSettings ++ 
    Seq(previousArtifact := Some(organization % fullId % previousSparkVersion),
      binaryIssueFilters ++= ignoredABIProblems(sparkHome, version.value))
  }

}
