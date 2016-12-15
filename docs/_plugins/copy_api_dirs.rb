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

if not (ENV['SKIP_API'] == '1')
  if not (ENV['SKIP_SCALADOC'] == '1')
    # Build Scaladoc for Java/Scala

    puts "Moving to project root and building API docs."
    curr_dir = pwd
    cd("..")

    puts "Running 'build/sbt -Pkinesis-asl clean compile unidoc' from " + pwd + "; this may take a few minutes..."
    system("build/sbt -Pkinesis-asl clean compile unidoc") || raise("Unidoc generation failed")

    puts "Moving back into docs dir."
    cd("docs")

    puts "Removing old docs"
    puts `rm -rf api`

    # Copy over the unified ScalaDoc for all projects to api/scala.
    # This directory will be copied over to _site when `jekyll` command is run.
    source = "../target/scala-2.11/unidoc"
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

    # Begin updating JavaDoc files for badge post-processing
    puts "Updating JavaDoc files for badge post-processing"
    js_script_start = '<script defer="defer" type="text/javascript" src="'
    js_script_end = '.js"></script>'

    javadoc_files = Dir["./" + dest + "/**/*.html"]
    javadoc_files.each do |javadoc_file|
      # Determine file depths to reference js files
      slash_count = javadoc_file.count "/"
      i = 3
      path_to_js_file = ""
      while (i < slash_count) do
        path_to_js_file = path_to_js_file + "../"
        i += 1
      end

      # Create script elements to reference js files
      javadoc_jquery_script = js_script_start + path_to_js_file + "lib/jquery" + js_script_end;
      javadoc_api_docs_script = js_script_start + path_to_js_file + "lib/api-javadocs" + js_script_end;
      javadoc_script_elements = javadoc_jquery_script + javadoc_api_docs_script

      # Add script elements to JavaDoc files
      javadoc_file_content = File.open(javadoc_file, "r") { |f| f.read }
      javadoc_file_content = javadoc_file_content.sub("</body>", javadoc_script_elements + "</body>")
      File.open(javadoc_file, "w") { |f| f.puts(javadoc_file_content) }

    end
    # End updating JavaDoc files for badge post-processing

    puts "Copying jquery.js from Scala API to Java API for page post-processing of badges"
    jquery_src_file = "./api/scala/lib/jquery.js"
    jquery_dest_file = "./api/java/lib/jquery.js"
    mkdir_p("./api/java/lib")
    cp(jquery_src_file, jquery_dest_file)

    puts "Copying api_javadocs.js to Java API for page post-processing of badges"
    api_javadocs_src_file = "./js/api-javadocs.js"
    api_javadocs_dest_file = "./api/java/lib/api-javadocs.js"
    cp(api_javadocs_src_file, api_javadocs_dest_file)

    puts "Appending content of api-javadocs.css to JavaDoc stylesheet.css for badge styles"
    css = File.readlines("./css/api-javadocs.css")
    css_file = dest + "/stylesheet.css"
    File.open(css_file, 'a') { |f| f.write("\n" + css.join()) }
  end

  # Build Sphinx docs for Python

  puts "Moving to python/docs directory and building sphinx."
  cd("../python/docs")
  system("make html") || raise("Python doc generation failed")

  puts "Moving back into home dir."
  cd("../../")

  puts "Making directory api/python"
  mkdir_p "docs/api/python"

  puts "cp -r python/docs/_build/html/. docs/api/python"
  cp_r("python/docs/_build/html/.", "docs/api/python")

  # Build SparkR API docs
  puts "Moving to R directory and building roxygen docs."
  cd("R")
  system("./create-docs.sh") || raise("R doc generation failed")

  puts "Moving back into home dir."
  cd("../")

  puts "Making directory api/R"
  mkdir_p "docs/api/R"

  puts "cp -r R/pkg/html/. docs/api/R"
  cp_r("R/pkg/html/.", "docs/api/R")

  puts "cp R/pkg/DESCRIPTION docs/api"
  cp("R/pkg/DESCRIPTION", "docs/api")

end
