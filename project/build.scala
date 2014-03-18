import sbt._
import com.typesafe.sbt.pom.{PomBuild, SbtPomKeys}
import net.virtualvoid.sbt.graph.Plugin.graphSettings


object MyBuild extends PomBuild {
  override def settings = {
    println("Settings accessed") // Temprorary debug statement
    super.settings ++ Seq(SbtPomKeys.profiles := Seq())
  }

  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    super.projectDefinitions(baseDirectory).map { x => 
      x.settings(graphSettings: _*)
    }
  }
}

