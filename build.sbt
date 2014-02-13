
lazy val catalyst = Project("catalyst", file("catalyst"), settings = catalystSettings)
lazy val core = Project("core", file("core"), settings = coreSettings).dependsOn(catalyst)
lazy val shark = Project("shark", file("shark"), settings = sharkSettings).dependsOn(core)

def sharedSettings = Defaults.defaultSettings ++ Seq(
  organization := "org.apache.spark.sql",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.10.3",
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
  // Common Dependencies.
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "1.9.1" % "test",
    "com.typesafe" %% "scalalogging-slf4j" % "1.0.1")
) ++ org.scalastyle.sbt.ScalastylePlugin.Settings

def catalystSettings = sharedSettings ++ Seq(
  name := "catalyst",
  // The mechanics of rewriting expression ids to compare trees in some test cases makes
  // assumptions about the the expression ids being contiguious.  Running tests in parallel breaks
  // this non-deterministically.  TODO: FIX THIS.
  parallelExecution in Test := false
)

def coreSettings = sharedSettings ++ Seq(
  name := "core",
  libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.0-incubating"
)

def sharkSettings = sharedSettings ++ Seq(
  name := "shark",
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % "1.0.4",
    "org.apache.hive" % "hive-metastore" % "0.12.0",
    "org.apache.hive" % "hive-exec" % "0.12.0",
    "org.apache.hive" % "hive-serde" % "0.12.0"),
  // Multiple queries rely on the TestShark singleton.  See comments there for more details.
  parallelExecution in Test := false,
  initialCommands in console :=
    """
      |import org.apache.spark.sql.catalyst.analysis._
      |import org.apache.spark.sql.catalyst.dsl._
      |import org.apache.spark.sql.catalyst.errors._
      |import org.apache.spark.sql.catalyst.expressions._
      |import org.apache.spark.sql.catalyst.plans.logical._
      |import org.apache.spark.sql.catalyst.rules._
      |import org.apache.spark.sql.catalyst.types._
      |import org.apache.spark.sql.catalyst.util._
      |import org.apache.spark.sql.execution
      |import org.apache.spark.sql.shark._
      |import org.apache.spark.sql.shark.TestShark._""".stripMargin
)