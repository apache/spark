resolvers += {
  val typesafeRepoUrl = new java.net.URL("http://repo.typesafe.com/typesafe/releases")
  val pattern = Patterns(false, "[organisation]/[module]/[sbtversion]/[revision]/[type]s/[module](-[classifier])-[revision].[ext]")
  Resolver.url("Typesafe Repository", typesafeRepoUrl)(pattern)
}

resolvers += "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"

libraryDependencies ++= Seq(
  "com.github.mpeltonen" %% "sbt-idea" % "0.10.0-SNAPSHOT"
// FIXME Uncomment once version for SBT 0.10.1 is available "com.eed3si9n" %% "sbt-assembly" % "0.2"
)

libraryDependencies <<= (libraryDependencies, sbtVersion) { (deps, version) =>
  deps :+ ("com.typesafe.sbteclipse" %% "sbteclipse" % "1.2" extra("sbtversion" -> version))
}