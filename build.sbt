resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies += "edu.berkeley.cs.amplab" %% "shark" % "0.9.0-SNAPSHOT"

libraryDependencies += "javax.jdo" % "jdo2-api" % "2.3-ec" from "http://www.datanucleus.org/downloads/maven2/javax/jdo/jdo2-api/2.3-ec/jdo2-api-2.3-ec.jar"

libraryDependencies ++= Seq(
 "org.apache.hadoop" % "hadoop-client" % "1.0.4",
 "org.scalatest" %% "scalatest" % "1.9.1" % "test",
 //"net.hydromatic" % "optiq-core" % "0.4.16-SNAPSHOT",
 "org.apache.hive" % "hive-metastore" % "0.10.0",
 "org.apache.hive" % "hive-exec" % "0.10.0",
 "org.apache.hive" % "hive-builtins" % "0.10.0")


resolvers ++= Seq(
    // For Optiq
    "Conjars Repository" at "http://conjars.org/repo/",
    // For jdo-2 required by Hive < 0.12.0
    "Datanucleus Repository" at "http://www.datanucleus.org/downloads/maven2")


scalaVersion := "2.10.3"

initialCommands in console := """
import catalyst.analysis._
import catalyst.errors._
import catalyst.expressions._
import catalyst.frontend._
import catalyst.plans.logical._
import catalyst.plans.physical
import catalyst.rules._
import catalyst.types._
import catalyst.util._
lazy val testShark = new catalyst.util.TestShark
import testShark._"""