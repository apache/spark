#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

job "template" {

  meta {
    "hi" = "there"
  }

  group "driver" {

    task "drive" {
      meta {
        "spark.nomad.role" = "driver"
      }
    }

    task "some-driver-sidecar" {
      driver = "exec"
      config {
        command = "/bin/bash"
        args = [ "-c", "echo hi" ]
      }
      resources {
        cpu = 20
        memory = 10
        network {
          mbits = 1
          port "foo" {}
        }
      }
      logs {
        max_files     = 1
        max_file_size = 1
      }
    }

  }

  group "executor" {

    task "executor" {
      meta {
        "spark.nomad.role" = "executor"
      }
    }

    task "some-executor-sidecar" {
      driver = "exec"
      config {
        command = "/bin/bash"
        args = [ "-c", "echo hi" ]
      }
      resources {
        cpu = 20
        memory = 10
        network {
          mbits = 1
          port "bar" {}
        }
      }
      logs {
        max_files     = 1
        max_file_size = 1
      }
    }

  }

}
