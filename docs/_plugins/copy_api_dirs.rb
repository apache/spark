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

  puts "Moving to project root and building API docs."
  curr_dir = pwd
  cd("..")

  puts "Running 'sbt/sbt -Pkinesis-asl compile unidoc' from " + pwd + "; this may take a few minutes..."
  puts `sbt/sbt -Pkinesis-asl compile unidoc`

  puts "Moving back into docs dir."
  cd("docs")

  # Copy over the unified ScalaDoc for all projects to api/scala.
  # This directory will be copied over to _site when `jekyll` command is run.
  source = "../target/scala-2.10/unidoc"
  dest = "api/scala"

  puts "Making directory " + dest
  mkdir_p dest

  # From the rubydoc: cp_r('src', 'dest') makes src/dest, but this doesn't.
  puts "cp -r " + source + "/. " + dest
  cp_r(source + "/.", dest)

  # Append custom JavaScript
  js = File.readlines("./js/api-docs.js")
  js_file = dest + "/lib/template.js"
  File.open(js_file, 'a') { |f| f.write("\n" + js.join()) }

  # Append custom CSS
  css = File.readlines("./css/api-docs.css")
  css_file = dest + "/lib/template.css"
  File.open(css_file, 'a') { |f| f.write("\n" + css.join()) }

  # Copy over the unified JavaDoc for all projects to api/java.
  source = "../target/javaunidoc"
  dest = "api/java"

  puts "Making directory " + dest
  mkdir_p dest

  puts "cp -r " + source + "/. " + dest
  cp_r(source + "/.", dest)

  # Build Epydoc for Python
  puts "Moving to python directory and building epydoc."
  cd("../python")
  puts `epydoc --config epydoc.conf`

  puts "Moving back into docs dir."
  cd("../docs")

  puts "Making directory api/python"
  mkdir_p "api/python"

  puts "cp -r ../python/docs/. api/python"
  cp_r("../python/docs/.", "api/python")

  cd("..")
end
