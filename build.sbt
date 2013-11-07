libraryDependencies += "edu.berkeley.cs.amplab" %% "shark" % "0.8.0"

libraryDependencies ++= Seq(
 "org.apache.hadoop" % "hadoop-client" % "1.0.4",
 "org.scalatest" %% "scalatest" % "1.9.1" % "test")

scalaVersion := "2.10.3"