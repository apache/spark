resolvers ++= Seq(
  "sbt-idea-repo" at "http://mpeltonen.github.com/maven/",
  Classpaths.typesafeResolver
)

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "0.11.0-SNAPSHOT")

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse" % "1.4.0")

addSbtPlugin("com.eed3si9n" %% "sbt-assembly" % "0.6")
