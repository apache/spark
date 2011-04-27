import sbt._

class SparkProjectPlugins(info: ProjectInfo) extends PluginDefinition(info) {
  val eclipse = "de.element34" % "sbt-eclipsify" % "0.7.0"

  val sbtIdeaRepo = "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"
  val sbtIdea = "com.github.mpeltonen" % "sbt-idea-plugin" % "0.2.0"

  val codaRepo = "Coda Hale's Repository" at "http://repo.codahale.com/"
  val assemblySBT = "com.codahale" % "assembly-sbt" % "0.1.1"
}
