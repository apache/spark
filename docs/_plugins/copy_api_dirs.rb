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

require 'fileutils'
include FileUtils

if not (ENV['SKIP_API'] == '1' or ENV['SKIP_SCALADOC'] == '1')
  # Build Scaladoc for Java/Scala
  projects = ["core", "examples", "repl", "bagel", "graphx", "streaming", "mllib"]

  puts "Moving to project root and building scaladoc."
  curr_dir = pwd
  cd("..")

  puts "Running sbt/sbt doc from " + pwd + "; this may take a few minutes..."
  puts `sbt/sbt doc`

  puts "Moving back into docs dir."
  cd("docs")

  # Copy over the scaladoc from each project into the docs directory.
  # This directory will be copied over to _site when `jekyll` command is run.
  projects.each do |project_name|
    source = "../" + project_name + "/target/scala-2.10/api"
    dest = "api/" + project_name

    puts "echo making directory " + dest
    mkdir_p dest

    # From the rubydoc: cp_r('src', 'dest') makes src/dest, but this doesn't.
    puts "cp -r " + source + "/. " + dest
    cp_r(source + "/.", dest)
  end

  # Build Epydoc for Python
  puts "Moving to python directory and building epydoc."
  cd("../python")
  puts `epydoc --config epydoc.conf`

  puts "Moving back into docs dir."
  cd("../docs")

  puts "echo making directory pyspark"
  mkdir_p "pyspark"

  puts "cp -r ../python/docs/. api/pyspark"
  cp_r("../python/docs/.", "api/pyspark")

  cd("..")
end
