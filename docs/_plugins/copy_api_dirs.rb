require 'fileutils'
include FileUtils

if ENV['SKIP_SCALADOC'] != '1'
  projects = ["core", "examples", "repl", "bagel", "streaming"]

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
    source = "../" + project_name + "/target/scala-2.9.2/api"
    dest = "api/" + project_name

    puts "echo making directory " + dest
    mkdir_p dest

    # From the rubydoc: cp_r('src', 'dest') makes src/dest, but this doesn't.
    puts "cp -r " + source + "/. " + dest
    cp_r(source + "/.", dest)
  end
end

if ENV['SKIP_EPYDOC'] != '1'
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
