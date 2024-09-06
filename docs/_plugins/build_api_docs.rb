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

# This include enables functions like `cd` and `cp_r`.
require 'fileutils'
include FileUtils

THIS_DIR = File.dirname(__FILE__)
SPARK_PROJECT_ROOT = File.expand_path(THIS_DIR + "/../..")
$spark_package_is_built = false

def print_header(text)
  banner = "* #{text} *"
  banner_bar = "*" * banner.size

  puts ""
  puts banner_bar
  puts banner
  puts banner_bar
end

def build_spark_if_necessary
  if $spark_package_is_built
    return
  end

  print_header "Building Spark."
  cd(SPARK_PROJECT_ROOT)
  command = "build/sbt -Phive -Pkinesis-asl clean package"
  puts "Running '#{command}'; this may take a few minutes..."
  system(command) || raise("Failed to build Spark")
  $spark_package_is_built = true
end

def copy_and_update_scala_docs(source, dest)
    puts "Making directory " + dest
    mkdir_p dest

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
end

def copy_and_update_java_docs(source, dest, scala_source)
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

  puts "Copying jquery.min.js from Scala API to Java API for page post-processing of badges"
  jquery_src_file = scala_source + "/lib/jquery.min.js"
  jquery_dest_file = dest + "/lib/jquery.min.js"
  mkdir_p(dest + "/lib")
  cp(jquery_src_file, jquery_dest_file)

  puts "Copying api_javadocs.js to Java API for page post-processing of badges"
  api_javadocs_src_file = "./js/api-javadocs.js"
  api_javadocs_dest_file = dest + "/lib/api-javadocs.js"
  cp(api_javadocs_src_file, api_javadocs_dest_file)

  puts "Appending content of api-javadocs.css to JavaDoc stylesheet.css for badge styles"
  css = File.readlines("./css/api-javadocs.css")
  css_file = dest + "/stylesheet.css"
  File.open(css_file, 'a') { |f| f.write("\n" + css.join()) }
end


def build_scala_and_java_docs
  build_spark_if_necessary

  print_header "Building Scala and Java API docs."
  cd(SPARK_PROJECT_ROOT)

  command = "build/sbt -Pkinesis-asl unidoc"
  puts "Running '#{command}'..."
  system(command) || raise("Unidoc generation failed")

  puts "Moving back into docs dir."
  cd("docs")

  puts "Removing old docs"
  system("rm -rf api")

  # Copy over the unified ScalaDoc for all projects to api/scala.
  # This directory will be copied over to _site when `jekyll` command is run.
  copy_and_update_scala_docs("../target/scala-2.13/unidoc", "api/scala")
  # copy_and_update_scala_docs("../connector/connect/client/jvm/target/scala-2.13/unidoc", "api/connect/scala")

  # Copy over the unified JavaDoc for all projects to api/java.
  copy_and_update_java_docs("../target/javaunidoc", "api/java", "api/scala")
  # copy_and_update_java_docs("../connector/connect/client/jvm/target/javaunidoc", "api/connect/java", "api/connect/scala")
end

def build_python_docs
  build_spark_if_necessary

  print_header "Building Python API docs."
  cd("#{SPARK_PROJECT_ROOT}/python/docs")
  system("make html") || raise("Python doc generation failed")

  puts "Moving back into docs dir."
  cd("../../docs")

  puts "Making directory api/python"
  mkdir_p "api/python"

  puts "cp -r ../python/docs/build/html/. api/python"
  rm_r("../python/docs/build/html/_sources")
  cp_r("../python/docs/build/html/.", "api/python")
end

def build_r_docs
  print_header "Building R API docs."
  cd("#{SPARK_PROJECT_ROOT}/R")
  system("./create-docs.sh") || raise("R doc generation failed")

  puts "Moving back into docs dir."
  cd("../docs")

  puts "Making directory api/R"
  mkdir_p "api/R"

  puts "cp -r ../R/pkg/docs/. api/R"
  cp_r("../R/pkg/docs/.", "api/R")
end

def build_sql_docs
  build_spark_if_necessary

  print_header "Building SQL API docs."
  cd("#{SPARK_PROJECT_ROOT}/sql")
  system("./create-docs.sh") || raise("SQL doc generation failed")

  puts "Moving back into docs dir."
  cd("../docs")

  puts "Making directory api/sql"
  mkdir_p "api/sql"

  puts "cp -r ../sql/site/. api/sql"
  cp_r("../sql/site/.", "api/sql")
end

def build_error_docs
  print_header "Building error docs."

  if !system("which python3 >/dev/null 2>&1")
    raise("Missing python3 in your path, stopping error doc generation")
  end

  system("python3 '#{SPARK_PROJECT_ROOT}/docs/_plugins/build-error-docs.py'") \
  || raise("Error doc generation failed")
end

if not (ENV['SKIP_ERRORDOC'] == '1')
  build_error_docs
end

if not (ENV['SKIP_API'] == '1')
  if not (ENV['SKIP_SCALADOC'] == '1')
    build_scala_and_java_docs
  end

  if not (ENV['SKIP_PYTHONDOC'] == '1')
    build_python_docs
  end

  if not (ENV['SKIP_RDOC'] == '1')
    build_r_docs
  end

  if not (ENV['SKIP_SQLDOC'] == '1')
    build_sql_docs
  end
end
