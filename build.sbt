import AssemblyKeys._ // put this at the top of the file

name := "catalyst"

organization := "com.databricks"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.3"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.0-incubating-SNAPSHOT"

// Hive 0.10.0 relies on a weird version of jdo that is not published anywhere... Remove when we upgrade to 0.11.0
libraryDependencies += "javax.jdo" % "jdo2-api" % "2.3-ec" from "http://www.datanucleus.org/downloads/maven2/javax/jdo/jdo2-api/2.3-ec/jdo2-api-2.3-ec.jar"

libraryDependencies ++= Seq(
 "org.apache.hadoop" % "hadoop-client" % "1.0.4",
 "org.scalatest" %% "scalatest" % "1.9.1" % "test",
 //"net.hydromatic" % "optiq-core" % "0.4.16-SNAPSHOT",
 "org.apache.hive" % "hive-metastore" % "0.10.0",
 "org.apache.hive" % "hive-exec" % "0.10.0",
 "org.apache.hive" % "hive-builtins" % "0.10.0",
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.1")

// Multiple queries rely on the TestShark singleton.  See comments there for more details.
parallelExecution in Test := false

resolvers ++= Seq(
    // For Optiq
    "Conjars Repository" at "http://conjars.org/repo/",
    // For jdo-2 required by Hive < 0.12.0
    "Datanucleus Repository" at "http://www.datanucleus.org/downloads/maven2")

resolvers += "Databees" at "http://repository-databricks.forge.cloudbees.com/snapshot/"

initialCommands in console := """
import catalyst.analysis._
import catalyst.dsl._
import catalyst.errors._
import catalyst.expressions._
import catalyst.frontend._
import catalyst.plans.logical._
import catalyst.rules._
import catalyst.types._
import catalyst.util._
import catalyst.execution.TestShark._"""

site.settings

ghpages.settings

git.remoteRepo := "git@github.com:marmbrus/catalyst.git"

site.settings

site.includeScaladoc()

assemblySettings

test in assembly := {}

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}