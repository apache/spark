import sbt._
import com.typesafe.sbt.pom.{PomBuild, SbtPomKeys}

object MyBuild extends PomBuild {
  override def settings = {
    println("Settings accessed") // Temprorary debug statement
    super.settings ++ Seq(SbtPomKeys.profiles := Seq("yarn"))
  }
}

