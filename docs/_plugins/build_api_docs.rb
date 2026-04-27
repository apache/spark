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
  # Maven may leave POM-only org.hamcrest:hamcrest-core trees under ~/.m2; SBT/Coursier then
  # fails with "file:.../hamcrest-core-*.jar: not found". Clear before invoking SBT.
  hamcrest_m2 = File.join(Dir.home, '.m2/repository/org/hamcrest/hamcrest-core')
  FileUtils.rm_rf(hamcrest_m2)
  command = "NO_PROVIDED_SPARK_JARS=0 build/sbt -Phive -Pkinesis-asl clean package"
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

def build_spark_scala_and_java_docs_if_necessary
  # If spark's docs has already been compiled on the host, skip here.
  if ENV['SPARK_DOCS_IS_BUILT_ON_HOST'] == '1'
    return
  end

  command = "build/sbt -Pkinesis-asl unidoc"
  puts "Running '#{command}'..."
  # Tee sbt output to a log file so we can diagnose failures. The most common
  # unidoc failure is a javadoc crash mid-stream while generating HTML for a
  # specific class, buried under ~100 benign errors on genjavadoc-generated
  # Java stubs (e.g. target/java/org/apache/spark/ErrorInfo.java). Without the
  # diagnostic below, the real culprit -- the source whose doc tripped javadoc
  # -- is effectively invisible in the CI log.
  log_file = File.join(SPARK_PROJECT_ROOT, "target", "unidoc-build.log")
  mkdir_p(File.dirname(log_file))
  success = stream_and_capture(command, log_file)
  unless success
    diagnose_unidoc_failure(log_file)
    raise("Unidoc generation failed")
  end
end

# Runs `command`, streaming every line to both stdout and `log_file`. Returns
# true iff the command exited 0. Ruby-only; no shell pipefail reliance.
def stream_and_capture(command, log_file)
  File.open(log_file, 'w') do |f|
    IO.popen("#{command} 2>&1", 'r') do |pipe|
      pipe.each_line do |line|
        $stdout.write(line)
        $stdout.flush
        f.write(line)
      end
    end
  end
  $?.success?
end

# Returns the diff range covering files this PR changed, or nil if we can't
# determine a meaningful range. The CI docs job squash-merges the PR onto
# apache/spark master and creates a single "Merged commit"; in that layout
# `HEAD~1` is the master tip and `HEAD` carries the PR's changes. Outside CI
# (or on apache/spark itself, where no merge happens) we have nothing to
# compare against, so return nil and skip the scan.
def pr_diff_range
  return nil unless ENV['GITHUB_ACTIONS'] == 'true'
  return nil unless system('git rev-parse --verify HEAD~1 >/dev/null 2>&1')
  # Distinguish "PR squash-merged onto master" from "running on apache/spark
  # master directly" by looking at HEAD's commit subject. The sync step in
  # `.github/workflows/build_and_test.yml` always uses this exact subject.
  subject = `git log -1 --format=%s HEAD 2>/dev/null`.strip
  return nil unless subject == 'Merged commit'
  'HEAD~1...HEAD'
end

# Scans Java/Scala files this PR changes for doc-tag patterns that have
# previously crashed the standard doclet's tree builder. Returns an array of
# {file:, line:, message:} hashes (possibly empty); returns nil if the scan
# cannot run (e.g. the diff range is unavailable). Each hazard pattern below
# is keyed to a real failure mode -- not a stylistic preference -- so a hit
# is high-confidence actionable.
def scan_pr_doc_hazards
  range = pr_diff_range
  return nil unless range
  changed_raw = `git diff --name-only #{range} -- '*.java' '*.scala' 2>/dev/null`
  changed = changed_raw.lines.map(&:strip).reject(&:empty?)
  return [] if changed.empty?

  hazards = []
  changed.each do |path|
    next unless File.file?(path)
    File.foreach(path).with_index(1) do |line, lineno|
      # Pattern: `@inheritDoc` written as a block tag on its own line.
      # `@inheritDoc` is only valid as the inline tag `{@inheritDoc}`; Spark
      # also registers a custom lowercase block tag `@inheritdoc` via
      # `-tag inheritdoc` in `SparkBuild.scala`. The camelCase block form is
      # neither, and javadoc 17's standard doclet has been observed to
      # hard-exit during "Building tree" when a default-method override
      # carries it (SPARK-56636). Suggest the standard inline form.
      if line =~ /^\s*\*\s*@inheritDoc\s*$/
        hazards << {
          file: path,
          line: lineno,
          message: '@inheritDoc as block tag is invalid; use {@inheritDoc} (inline) ' \
                   'or @inheritdoc (lowercase block, registered via -tag inheritdoc)'
        }
      end
    end
  end
  hazards
rescue => e
  # Never let the scan obscure the underlying unidoc failure.
  $stderr.puts "  (PR-scope hazard scan failed: #{e.class}: #{e.message})"
  nil
end

# Scans the captured unidoc log and prints a pointer to the most likely
# culprit source file. The heuristic: when javadoc dies mid-HTML-generation,
# the last "Generating .../X.html" line before "javadoc exited with exit code"
# names the class that tripped it. When the crash predates HTML output (i.e.
# javadoc dies during "Building tree" / "Building index"), no per-class line
# exists -- in that case fall back to a PR-scope scan for known doc-tag
# hazards that match this failure shape (see `scan_pr_doc_hazards`).
def diagnose_unidoc_failure(log_file)
  return unless File.exist?(log_file)
  begin
    lines = File.readlines(log_file)

    javadoc_exit_idx = lines.rindex { |l| l.include?("javadoc exited with exit code") }
    last_generating = nil
    if javadoc_exit_idx
      # Strip ANSI color codes so the regex matches sbt-coloured output too.
      ansi = /\e\[[0-9;]*[A-Za-z]/
      lines[0...javadoc_exit_idx].reverse_each do |line|
        if line.gsub(ansi, '') =~ %r{Generating .+/javaunidoc/(\S+?\.html)\.\.\.}
          last_generating = $1
          break
        end
      end
    end

    banner = "=" * 78
    $stderr.puts ""
    $stderr.puts banner
    $stderr.puts "Unidoc failed -- diagnostic summary"
    $stderr.puts banner
    if last_generating
      class_path = last_generating.sub(/\.html$/, '')
      class_name = class_path.tr('/', '.')
      $stderr.puts ""
      $stderr.puts "  Javadoc crashed while generating: #{last_generating}"
      $stderr.puts "  Likely culprit: doc comment in #{class_name}"
      $stderr.puts ""
      $stderr.puts "  Javadoc can hard-exit (not just warn) on specific scaladoc"
      $stderr.puts "  patterns once they have been passed through genjavadoc --"
      $stderr.puts "  wiki-style `[[Class]]` / `[[method]]` links or inline-backticked"
      $stderr.puts "  code refs in the Scala source for the class above are common"
      $stderr.puts "  triggers. Start by auditing any recent doc-string changes in"
      $stderr.puts "  that source file."
      $stderr.puts ""
      $stderr.puts "  NOTE: the '[error]' lines above on files under"
      $stderr.puts "  target/java/... are benign genjavadoc stubs -- every PR"
      $stderr.puts "  emits them and they do not cause the exit. Ignore them."
    elsif javadoc_exit_idx
      $stderr.puts ""
      $stderr.puts "  Javadoc exited but no class HTML generation was in progress;"
      $stderr.puts "  the crash predates HTML output. Most often this means a doc"
      $stderr.puts "  tag the standard doclet's tree builder cannot dispatch on, a"
      $stderr.puts "  malformed @link target, or (rarely) a CLI / classpath issue."
      hazards = scan_pr_doc_hazards
      if hazards && !hazards.empty?
        $stderr.puts ""
        $stderr.puts "  PR-scope scan found likely culprits in files this PR changes:"
        hazards.each do |h|
          $stderr.puts "    * #{h[:file]}:#{h[:line]}: #{h[:message]}"
          # Also emit a GitHub Actions annotation so the finding shows up
          # inline on the PR's "Files changed" tab without needing to read
          # the full job log.
          if ENV['GITHUB_ACTIONS'] == 'true'
            puts "::error file=#{h[:file]},line=#{h[:line]},title=Unidoc hazard::#{h[:message]}"
          end
        end
        $stderr.puts ""
        $stderr.puts "  These doc-tag patterns are known to crash the doclet's tree"
        $stderr.puts "  builder. Fix one and re-run; if unidoc still fails, scroll up"
        $stderr.puts "  to the full sbt output for further clues."
      end
    else
      $stderr.puts ""
      $stderr.puts "  Could not locate a 'javadoc exited with exit code' marker in"
      $stderr.puts "  the log; the failure is likely outside the javaunidoc step"
      $stderr.puts "  (scaladoc / sbt / build env). See the full sbt output above."
    end
    $stderr.puts banner
    $stderr.puts ""
  rescue => e
    # Never let the diagnostic helper itself obscure the underlying unidoc
    # failure: if anything here goes wrong (e.g. encoding error reading the
    # log), report it briefly and let the caller raise the real error.
    $stderr.puts "(diagnostic helper failed: #{e.class}: #{e.message})"
  end
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
