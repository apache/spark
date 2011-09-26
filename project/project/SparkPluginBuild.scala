import sbt._

object SparkPluginDef extends Build {
  lazy val root = Project("plugins", file(".")) dependsOn(junitXmlListener)
  /* This is not published in a Maven repository, so we get it from GitHub directly */
  lazy val junitXmlListener = uri("git://github.com/ijuma/junit_xml_listener.git#fe434773255b451a38e8d889536ebc260f4225ce")
}