val sparkVersion = settingKey[String]("Spark version")

lazy val V = _root_.scalafix.sbt.BuildInfo
inThisBuild(
  List(
    organization := "com.holdenkarau",
    homepage := Some(url("https://github.com/holdenk/spark-auto-upgrade")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    sparkVersion := System.getProperty("sparkVersion", "2.4.0"),
    developers := List(
      Developer(
        "holdenk",
        "Holden Karau",
        "holden@pigscanfly.ca",
        url("https://github.com/holdenk/spark-auto-upgrade")
      )
    ),
    scalaVersion := V.scala212,
    addCompilerPlugin(scalafixSemanticdb),
    scalacOptions ++= List(
      "-Yrangepos",
      "-P:semanticdb:synthetics:on"
    )
  )
)

skip in publish := true


lazy val rules = project.settings(
  moduleName := "spark-scalafix-rules",
  libraryDependencies += "ch.epfl.scala" %% "scalafix-core" % V.scalafixVersion,
)

lazy val input = project.settings(
  skip in publish := true,
  libraryDependencies ++= Seq(
    "org.scalacheck" %% "scalacheck" % "1.14.0",
    "org.apache.spark" %% "spark-core"        % sparkVersion.value,
    "org.apache.spark" %% "spark-sql"         % sparkVersion.value,
    "org.apache.spark" %% "spark-hive"        % sparkVersion.value)
)

lazy val output = project.settings(
  skip in publish := true,
  libraryDependencies ++= Seq(
    "org.scalacheck" %% "scalacheck" % "1.14.0",
    "org.apache.spark" %% "spark-core"        % sparkVersion.value,
    "org.apache.spark" %% "spark-sql"         % sparkVersion.value,
    "org.apache.spark" %% "spark-hive"        % sparkVersion.value)
)

lazy val tests = project
  .settings(
    skip in publish := true,
    libraryDependencies += "ch.epfl.scala" % "scalafix-testkit" % V.scalafixVersion % Test cross CrossVersion.full,
    compile.in(Compile) :=
      compile.in(Compile).dependsOn(compile.in(input, Compile), compile.in(output, Compile)).value,
    scalafixTestkitOutputSourceDirectories :=
      sourceDirectories.in(output, Compile).value,
    scalafixTestkitInputSourceDirectories :=
      sourceDirectories.in(input, Compile).value,
    scalafixTestkitInputClasspath :=
      fullClasspath.in(input, Compile).value,
  )
  .dependsOn(rules)
  .enablePlugins(ScalafixTestkitPlugin)
