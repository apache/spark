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

package org.apache.spark.scheduler.cluster.nomad

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Files

import org.apache.spark.TestUtils
import org.apache.spark.deploy.nomad.BaseNomadClusterSuite
import org.apache.spark.tags.ExtendedNomadTest

@ExtendedNomadTest
class NomadPythonSuite extends BaseNomadClusterSuite {

  test("run Python application in client mode") {
    testPySpark(ClientMode)
  }

  test("run Python application in cluster mode") {
    testPySpark(ClusterMode)
  }

  private def testPySpark(deployMode: DeployMode): Unit = {
    val moduleDir = deployMode match {
      case ClientMode =>
        tempDir
      case ClusterMode =>
        val subdir = new File(tempDir, "pyModules")
        subdir.mkdir()
        subdir
    }
    val pyModule = new File(moduleDir, "mod1.py")
    Files.write(TEST_PYMODULE, pyModule, StandardCharsets.UTF_8)

    val mod2Archive =
      new File(TestUtils.createJarWithFiles(Map("mod2.py" -> TEST_PYMODULE), moduleDir).toURI)

    val finalState = runSpark(
      deployMode,
      FileResource(createFile("test", ".py", TEST_PYFILE)),
      sparkArgs = Seq(
        "--py-files" -> Seq(pyModule, mod2Archive).map(resource(deployMode, _)).mkString(",")
      ),
      appArgs = Seq(httpServer.url("/result"))
    )
    checkResult(finalState, "/result" -> "success")
  }

  private val TEST_PYFILE =
    """import httplib
      |from urlparse import urlparse
      |def put(url_string, content):
      |    url = urlparse(url_string)
      |    if (url.scheme != "http"):
      |        raise Exception("Url scheme needs to be http.")
      |    conn = httplib.HTTPConnection(url.hostname, url.port)
      |    conn.request("PUT", url.path, content)
      |    response = conn.getresponse()
      |    print "PUT", url, "->", response.status, response.reason
      |
      |import sys
      |import mod1
      |import mod2
      |from operator import add
      |from pyspark import SparkConf, SparkContext
      |if __name__ == "__main__":
      |    if len(sys.argv) != 2:
      |        print >> sys.stderr, "Usage: test.py [result URL]"
      |        exit(-1)
      |    sc = SparkContext(conf=SparkConf())
      |    result = "failure"
      |    rdd = sc.parallelize(range(10)).map(lambda x: x * mod1.func() * mod2.func())
      |    cnt = rdd.count()
      |    if cnt == 10:
      |        result = "success"
      |    put(sys.argv[1], result)
      |    sc.stop()
      |""".stripMargin

  private val TEST_PYMODULE =
    """def func():
      |    return 42
      |""".stripMargin

}
