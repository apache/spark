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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._

import scala.collection.JavaConversions._
import scala.collection.mutable.{ HashMap => MutableHashMap }
import scala.util.Try


class ClientBaseSuite extends FunSuite {

  test("default Yarn application classpath") {
    ClientBase.getDefaultYarnApplicationClasspath should be(Some(Fixtures.knownDefYarnAppCP))
  }

  test("default MR application classpath") {
    ClientBase.getDefaultMRApplicationClasspath should be(Some(Fixtures.knownDefMRAppCP))
  }

  test("resultant classpath for an application that defines a classpath for YARN") {
    withAppConf(Fixtures.mapYARNAppConf) { conf =>
      val env = newEnv
      ClientBase.populateHadoopClasspath(conf, env)
      classpath(env) should be(
        flatten(Fixtures.knownYARNAppCP, ClientBase.getDefaultMRApplicationClasspath))
    }
  }

  test("resultant classpath for an application that defines a classpath for MR") {
    withAppConf(Fixtures.mapMRAppConf) { conf =>
      val env = newEnv
      ClientBase.populateHadoopClasspath(conf, env)
      classpath(env) should be(
        flatten(ClientBase.getDefaultYarnApplicationClasspath, Fixtures.knownMRAppCP))
    }
  }

  test("resultant classpath for an application that defines both classpaths, YARN and MR") {
    withAppConf(Fixtures.mapAppConf) { conf =>
      val env = newEnv
      ClientBase.populateHadoopClasspath(conf, env)
      classpath(env) should be(flatten(Fixtures.knownYARNAppCP, Fixtures.knownMRAppCP))
    }
  }

  object Fixtures {

    val knownDefYarnAppCP: Seq[String] =
      getFieldValue[Array[String], Seq[String]](classOf[YarnConfiguration],
                                                "DEFAULT_YARN_APPLICATION_CLASSPATH",
                                                Seq[String]())(a => a.toSeq)


    val knownDefMRAppCP: Seq[String] =
      getFieldValue[String, Seq[String]](classOf[MRJobConfig],
                                         "DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH",
                                         Seq[String]())(a => a.split(","))

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
    m.foreach { case (k, v) => conf.set(k, v, "ClientBaseSpec") }
    testCode(conf)
  }

  def newEnv = MutableHashMap[String, String]()

  def classpath(env: MutableHashMap[String, String]) = env(Environment.CLASSPATH.name).split(":|;")

  def flatten(a: Option[Seq[String]], b: Option[Seq[String]]) = (a ++ b).flatten.toArray

  def getFieldValue[A, B](clazz: Class[_], field: String, defaults: => B)(mapTo: A => B): B =
    Try(clazz.getField(field)).map(_.get(null).asInstanceOf[A]).toOption.map(mapTo).getOrElse(defaults)

}
