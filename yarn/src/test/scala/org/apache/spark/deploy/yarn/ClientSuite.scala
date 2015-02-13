/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.yarn

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.Matchers

import scala.collection.JavaConversions._
import scala.collection.mutable.{ HashMap => MutableHashMap }
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.spark.{SparkException, SparkConf}
import org.apache.spark.util.Utils

class ClientSuite extends FunSuite with Matchers {

  test("default Yarn application classpath") {
    Client.getDefaultYarnApplicationClasspath should be(Some(Fixtures.knownDefYarnAppCP))
  }

  test("default MR application classpath") {
    Client.getDefaultMRApplicationClasspath should be(Some(Fixtures.knownDefMRAppCP))
  }

  test("resultant classpath for an application that defines a classpath for YARN") {
    withAppConf(Fixtures.mapYARNAppConf) { conf =>
      val env = newEnv
      Client.populateHadoopClasspath(conf, env)
      classpath(env) should be(
        flatten(Fixtures.knownYARNAppCP, Client.getDefaultMRApplicationClasspath))
    }
  }

  test("resultant classpath for an application that defines a classpath for MR") {
    withAppConf(Fixtures.mapMRAppConf) { conf =>
      val env = newEnv
      Client.populateHadoopClasspath(conf, env)
      classpath(env) should be(
        flatten(Client.getDefaultYarnApplicationClasspath, Fixtures.knownMRAppCP))
    }
  }

  test("resultant classpath for an application that defines both classpaths, YARN and MR") {
    withAppConf(Fixtures.mapAppConf) { conf =>
      val env = newEnv
      Client.populateHadoopClasspath(conf, env)
      classpath(env) should be(flatten(Fixtures.knownYARNAppCP, Fixtures.knownMRAppCP))
    }
  }

  private val SPARK = "local:/sparkJar"
  private val USER = "local:/userJar"
  private val ADDED = "local:/addJar1,local:/addJar2,/addJar3"

  test("Local jar URIs") {
    val conf = new Configuration()
    val sparkConf = new SparkConf().set(Client.CONF_SPARK_JAR, SPARK)
      .set("spark.yarn.user.classpath.first", "true")
    val env = new MutableHashMap[String, String]()
    val args = new ClientArguments(Array("--jar", USER, "--addJars", ADDED), sparkConf)

    Client.populateClasspath(args, conf, sparkConf, env)

    val cp = env("CLASSPATH").split(":|;|<CPS>")
    s"$SPARK,$USER,$ADDED".split(",").foreach({ entry =>
      val uri = new URI(entry)
      if (Client.LOCAL_SCHEME.equals(uri.getScheme())) {
        cp should contain (uri.getPath())
      } else {
        cp should not contain (uri.getPath())
      }
    })
    if (classOf[Environment].getMethods().exists(_.getName == "$$")) {
      cp should contain("{{PWD}}")
    } else if (Utils.isWindows) {
      cp should contain("%PWD%")
    } else {
      cp should contain(Environment.PWD.$())
    }
    cp should not contain (Client.SPARK_JAR)
    cp should not contain (Client.APP_JAR)
  }

  test("Jar path propagation through SparkConf") {
    val conf = new Configuration()
    val sparkConf = new SparkConf().set(Client.CONF_SPARK_JAR, SPARK)
    val args = new ClientArguments(Array("--jar", USER, "--addJars", ADDED), sparkConf)

    val client = spy(new Client(args, conf, sparkConf))
    doReturn(new Path("/")).when(client).copyFileToRemote(any(classOf[Path]),
      any(classOf[Path]), anyShort())

    val tempDir = Utils.createTempDir()
    try {
      client.prepareLocalResources(tempDir.getAbsolutePath())
      sparkConf.getOption(Client.CONF_SPARK_USER_JAR) should be (Some(USER))

      // The non-local path should be propagated by name only, since it will end up in the app's
      // staging dir.
      val expected = ADDED.split(",")
        .map(p => {
          val uri = new URI(p)
          if (Client.LOCAL_SCHEME == uri.getScheme()) {
            p
          } else {
            Option(uri.getFragment()).getOrElse(new File(p).getName())
          }
        })
        .mkString(",")

      sparkConf.getOption(Client.CONF_SPARK_YARN_SECONDARY_JARS) should be (Some(expected))
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("check access nns empty") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.access.namenodes", "")
    val nns = Client.getNameNodesToAccess(sparkConf)
    nns should be(Set())
  }

  test("check access nns unset") {
    val sparkConf = new SparkConf()
    val nns = Client.getNameNodesToAccess(sparkConf)
    nns should be(Set())
  }

  test("check access nns") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.access.namenodes", "hdfs://nn1:8032")
    val nns = Client.getNameNodesToAccess(sparkConf)
    nns should be(Set(new Path("hdfs://nn1:8032")))
  }

  test("check access nns space") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.access.namenodes", "hdfs://nn1:8032, ")
    val nns = Client.getNameNodesToAccess(sparkConf)
    nns should be(Set(new Path("hdfs://nn1:8032")))
  }

  test("check access two nns") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.access.namenodes", "hdfs://nn1:8032,hdfs://nn2:8032")
    val nns = Client.getNameNodesToAccess(sparkConf)
    nns should be(Set(new Path("hdfs://nn1:8032"), new Path("hdfs://nn2:8032")))
  }

  test("check token renewer") {
    val hadoopConf = new Configuration()
    hadoopConf.set("yarn.resourcemanager.address", "myrm:8033")
    hadoopConf.set("yarn.resourcemanager.principal", "yarn/myrm:8032@SPARKTEST.COM")
    val renewer = Client.getTokenRenewer(hadoopConf)
    renewer should be ("yarn/myrm:8032@SPARKTEST.COM")
  }

  test("check token renewer default") {
    val hadoopConf = new Configuration()
    val caught =
      intercept[SparkException] {
        Client.getTokenRenewer(hadoopConf)
      }
    assert(caught.getMessage === "Can't get Master Kerberos principal for use as renewer")
  }

  object Fixtures {

    val knownDefYarnAppCP: Seq[String] =
      getFieldValue[Array[String], Seq[String]](classOf[YarnConfiguration],
                                                "DEFAULT_YARN_APPLICATION_CLASSPATH",
                                                Seq[String]())(a => a.toSeq)


    val knownDefMRAppCP: Seq[String] =
      getFieldValue2[String, Array[String], Seq[String]](
        classOf[MRJobConfig],
        "DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH",
        Seq[String]())(a => a.split(","))(a => a.toSeq)

    val knownYARNAppCP = Some(Seq("/known/yarn/path"))

    val knownMRAppCP = Some(Seq("/known/mr/path"))

    val mapMRAppConf =
      Map("mapreduce.application.classpath" -> knownMRAppCP.map(_.mkString(":")).get)

    val mapYARNAppConf =
      Map(YarnConfiguration.YARN_APPLICATION_CLASSPATH -> knownYARNAppCP.map(_.mkString(":")).get)

    val mapAppConf = mapYARNAppConf ++ mapMRAppConf
  }

  def withAppConf(m: Map[String, String] = Map())(testCode: (Configuration) => Any) {
    val conf = new Configuration
    m.foreach { case (k, v) => conf.set(k, v, "ClientSpec") }
    testCode(conf)
  }

  def newEnv = MutableHashMap[String, String]()

  def classpath(env: MutableHashMap[String, String]) = env(Environment.CLASSPATH.name).split(":|;|<CPS>")

  def flatten(a: Option[Seq[String]], b: Option[Seq[String]]) = (a ++ b).flatten.toArray

  def getFieldValue[A, B](clazz: Class[_], field: String, defaults: => B)(mapTo: A => B): B =
    Try(clazz.getField(field)).map(_.get(null).asInstanceOf[A]).toOption.map(mapTo).getOrElse(defaults)

  def getFieldValue2[A: ClassTag, A1: ClassTag, B](
        clazz: Class[_],
        field: String,
        defaults: => B)(mapTo:  A => B)(mapTo1: A1 => B) : B = {
    Try(clazz.getField(field)).map(_.get(null)).map {
      case v: A => mapTo(v)
      case v1: A1 => mapTo1(v1)
      case _ => defaults
    }.toOption.getOrElse(defaults)
  }

}
