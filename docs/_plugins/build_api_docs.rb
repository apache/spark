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
  # If spark has already been compiled on the host, skip here.
  if ENV['SPARK_DOCS_IS_BUILT_ON_HOST'] == '1'
    return
  end

  if $spark_package_is_built
    return
  end

  print_header "Building Spark."
  cd(SPARK_PROJECT_ROOT)
  command = "NO_PROVIDED_SPARK_JARS=0 build/sbt -Phive -Pkinesis-asl clean package"
  puts "Running '#{command}'; this may take a few minutes..."
  system(command) || raise("Failed to build Spark")
  # SPARK-53327: Use the modified ResourceImpl.class in spark-catalyst which is compatible with Java 25
  system("zip -d assembly/target/scala-2.13/jars/datasketches-memory-3.0.2.jar org/apache/datasketches/memory/internal/ResourceImpl.class")
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

def build_spark_scala_and_java_docs_if_necessary
  # If spark's docs has already been compiled on the host, skip here.
  if ENV['SPARK_DOCS_IS_BUILT_ON_HOST'] == '1'
    return
  end

  command = "build/sbt -Pkinesis-asl unidoc"
  puts "Running '#{command}'..."

  # Two filter passes on the unidoc output, plus an additive fatal-error summary:
  #
  # 1. Genjavadoc-stub diagnostic blocks (~28 `[error]` lines on stubs under
  #    `target/java/`, plus 3-5 continuation lines each). Inert because
  #    `--ignore-source-errors` is set; matched by message text so legitimate
  #    doclint diagnostics on stub paths still pass through.
  #
  # 2. `-verbose` progress lines (~13K total): `Loading source file ...`,
  #    `[parsing started/completed ...]`, `[loading /path/X.class]`,
  #    `Generating .../X.html`. These are dominant in the log when `-verbose`
  #    is set (which it is in `JavaUnidoc / unidoc / javacOptions` to surface
  #    per-file `error: reference not found` diagnostics) but carry no signal
  #    of their own. Suppressing them brings the visible log from ~17K to ~5K
  #    lines on a typical run while leaving every diagnostic untouched.
  #
  # 3. Fatal-error summary (additive, drops no log lines). The filtered log is
  #    still ~4K lines and most `error:` text in it is non-fatal source-loading
  #    chatter, so the build-failing diagnostics are hard to spot. After the
  #    pipe closes, we print a `Fatal javadoc errors (N): ...` block and emit
  #    `::error file=,line=::` GitHub Actions annotations so they surface in the
  #    PR check panel. Captured strictly within the Standard Doclet phase
  #    bracketed by `Building tree for all the packages and classes...` and
  #    `Building index for all classes...`, which is where doclint diagnostics
  #    are emitted -- this matches what javadoc counts toward exit code 1.
  #    Self-checked against javadoc's own `N errors` summary line; a mismatch
  #    emits a `::warning::` so future phase-marker drift is visible.
  ansi = /\e\[[0-9;]*[A-Za-z]/
  stub_header = %r{
    \[(?:error|warn)\]\s+
    \S*?/target/java/\S+\.java:\d+(?::\d+)?:\s+
    error:\s+
    (?:cannot\s+find\s+symbol
     |illegal\s+combination\s+of\s+modifiers
     |non-static\s+type\s+variable\b
     |.*?\s+is\s+not\s+public\s+in\s+\S+;\s+cannot\s+be\s+accessed\s+from\s+outside\s+package)
  }x
  stub_cont = %r{\A\s*\[(?:error|warn)\]\s+(?!/\S+\.java:\d+(?::\d+)?:\s)}
  verbose_line = %r{
    \[(?:error|warn)\]\s+
    (?:Loading\s+source\s+file\s
     |\[parsing\s+(?:started|completed)\s
     |\[loading\s
     |\[checking\s
     |\[wrote\s
     |Generating\s+\S+\.html
    )
  }x

  # Doclint phase tracking for the trailing summary. Standard Doclet bookends the
  # phase that produces build-failing diagnostics with these marker lines; any
  # `error:` outside this window is source-loading noise that does not contribute
  # to javadoc's exit code. The summary below captures only the fatal ones and
  # re-emits them as GitHub Actions annotations so they surface in the PR check
  # panel instead of being buried in a 4K-line log.
  doclint_start   = %r{\bBuilding\s+tree\s+for\s+all\s+the\s+packages\s+and\s+classes\b}
  doclint_end     = %r{\bBuilding\s+index\s+for\s+all\s+classes\b}
  doclint_diag    = %r{\A\[warn\]\s+(?<path>\S+):(?<lineno>\d+)(?::\d+)?:\s+error:\s+(?<msg>.+?)\s*\z}
  doclint_cont    = %r{\A\[warn\]\s(?!\S+:\d+(?::\d+)?:\s+error:)(?<content>.*?)\s*\z}
  doclint_summary = %r{\A\[warn\]\s+(?<count>[\d,]+)\s+errors?\s*\z}

  in_stub = false
  in_doclint = false
  fatal_diagnostics = []
  pending_context_lines = 0  # snippet + caret lines that follow each diag header
  reported_error_count = nil

  IO.popen("#{command} 2>&1", 'r') do |pipe|
    pipe.each_line do |line|
      plain = line.gsub(ansi, '')

      if plain =~ doclint_start
        in_doclint = true
      elsif in_doclint && plain =~ doclint_end
        in_doclint = false
        pending_context_lines = 0
      end

      if in_doclint && (m = plain.match(doclint_diag))
        fatal_diagnostics << {
          path: m[:path], line: m[:lineno], msg: m[:msg], context: []
        }
        pending_context_lines = 2
      elsif in_doclint && pending_context_lines > 0 &&
            (m = plain.match(doclint_cont)) && !fatal_diagnostics.empty?
        fatal_diagnostics.last[:context] << m[:content]
        pending_context_lines -= 1
      end

      if reported_error_count.nil? && (m = plain.match(doclint_summary))
        reported_error_count = m[:count].delete(',').to_i
      end

      if plain =~ verbose_line
        in_stub = false
        # suppress -verbose progress line
      elsif plain =~ stub_header
        in_stub = true
      elsif in_stub && plain =~ stub_cont
        # continuation of a stub block; suppress
      else
        in_stub = false
        $stdout.write(line)
        $stdout.flush
      end
    end
  end

  unless fatal_diagnostics.empty?
    bar = "=" * 72
    puts ""
    puts bar
    puts "Fatal javadoc errors (#{fatal_diagnostics.size}):"
    puts bar
    fatal_diagnostics.each_with_index do |d, i|
      puts "  #{i + 1}. #{d[:path]}:#{d[:line]}: #{d[:msg]}"
      d[:context].each { |c| puts "       #{c}" }
    end
    puts bar
    puts ""

    # GitHub Actions inline annotations. `%`, `\r`, `\n` require URL-style
    # escaping per the workflow command spec; newlines render as multiple
    # lines inside the annotation, so the source snippet and caret display
    # under the error message in the PR check panel.
    project_root = SPARK_PROJECT_ROOT + '/'
    fatal_diagnostics.each do |d|
      rel = d[:path].start_with?(project_root) ? d[:path][project_root.length..] : d[:path]
      full = ([d[:msg]] + d[:context]).join("\n")
      enc = full.gsub(/[%\r\n]/, '%' => '%25', "\r" => '%0D', "\n" => '%0A')
      puts "::error file=#{rel},line=#{d[:line]},title=javadoc::#{enc}"
    end
  end

  if reported_error_count && reported_error_count != fatal_diagnostics.size
    puts "::warning::Javadoc reported #{reported_error_count} errors but " \
         "build_api_docs.rb captured #{fatal_diagnostics.size}. The doclint " \
         "phase markers may have shifted; please update build_api_docs.rb."
  end

  raise("Unidoc generation failed") unless $?.success?
end

def build_scala_and_java_docs
  build_spark_if_necessary

  print_header "Building Scala and Java API docs."
  cd(SPARK_PROJECT_ROOT)

  build_spark_scala_and_java_docs_if_necessary

  puts "Moving back into docs dir."
  cd("docs")

  puts "Removing old docs"
  system("rm -rf api")

  # Copy over the unified ScalaDoc for all projects to api/scala.
  # This directory will be copied over to _site when `jekyll` command is run.
  copy_and_update_scala_docs("../target/scala-2.13/unidoc", "api/scala")

  # Copy over the unified JavaDoc for all projects to api/java.
  copy_and_update_java_docs("../target/javaunidoc", "api/java", "api/scala")
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
